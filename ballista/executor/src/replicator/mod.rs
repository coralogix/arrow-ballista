use std::sync::Arc;

use ballista_core::async_reader::AsyncStreamReader;
use ballista_core::error::BallistaError;
use ballista_core::replicator::Command;
use bytes::Bytes;

use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::record_batch::RecordBatch;
use futures::io::BufReader;
use lazy_static::lazy_static;
use object_store::{path::Path, ObjectStore};
use prometheus::{
    register_histogram, register_int_counter, register_int_counter_vec, Histogram,
    IntCounter, IntCounterVec,
};
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;
use tokio::{fs::File, sync::mpsc};
use tracing::{info, warn};

use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

lazy_static! {
    static ref REPLICATION_LATENCY_SECONDS: Histogram = register_histogram!(
        "ballista_replicator_latency",
        "Replication latency in seconds",
        vec![0.01, 0.03, 0.05, 0.1, 0.3, 0.5, 1.0, 3.0]
    )
    .unwrap();
    static ref TOTAL_FILES: IntCounter = register_int_counter!(
        "ballista_replicator_total_files",
        "Number of files for replication"
    )
    .unwrap();
    static ref REPLICATED_FILES: IntCounter = register_int_counter!(
        "ballista_replicator_replicated_files",
        "Number of replicated files"
    )
    .unwrap();
    static ref REPLICATION_FAILURE: IntCounterVec = register_int_counter_vec!(
        "ballista_replicator_failure",
        "Number of replication failures",
        &["reason"]
    )
    .unwrap();
}

pub async fn start_replication(
    executor_id: String,
    object_store: Arc<dyn ObjectStore>,
    mut receiver: mpsc::Receiver<Command>,
) -> Result<(), BallistaError> {
    while let Some(Command::Replicate { job_id, path }) = receiver.recv().await {
        TOTAL_FILES.inc();
        let destination = format!("{}{}", executor_id, path);
        let start = Instant::now();
        info!(executor_id, job_id, destination, path, "Start replication");

        match Path::parse(destination) {
            Ok(dest) => match load_file(path.as_str()).await {
                Ok(reader) => {
                    if let Err(error) =
                        replicate_to_object_store(&dest, reader, object_store.clone())
                            .await
                    {
                        warn!(
                            executor_id,
                            job_id,
                            path,
                            ?error,
                            "Failed to upload file to object store"
                        );
                    } else {
                        let elapsed = start.elapsed();
                        REPLICATED_FILES.inc();
                        REPLICATION_LATENCY_SECONDS.observe(elapsed.as_secs_f64());
                        info!(executor_id, job_id, path, "Replication complete");
                    }
                }
                Err(error) => {
                    REPLICATION_FAILURE.with_label_values(&["open_file"]).inc();
                    warn!(executor_id, job_id, path, ?error, "Failed to open file");
                }
            },
            Err(error) => {
                REPLICATION_FAILURE.with_label_values(&["parse_path"]).inc();
                warn!(
                    executor_id,
                    job_id,
                    path,
                    ?error,
                    "Failed to parse replication path"
                );
            }
        }
    }

    Ok(())
}

async fn load_file(
    path: &str,
) -> Result<AsyncStreamReader<BufReader<Compat<File>>>, BallistaError> {
    let file = File::open(path).await?;
    let reader = AsyncStreamReader::try_new(file.compat(), None).await?;

    Ok(reader)
}

fn serialize_batch(batch: RecordBatch) -> Result<bytes::Bytes, BallistaError> {
    let mut serialized_data = Vec::with_capacity(batch.get_array_memory_size());

    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .unwrap_or(IpcWriteOptions::default());

    {
        let mut writer = StreamWriter::try_new_with_options(
            &mut serialized_data,
            batch.schema().as_ref(),
            options,
        )?;

        writer.write(&batch)?;
        writer.finish()?;
    }

    Ok(Bytes::from(serialized_data))
}

async fn replicate_to_object_store(
    destination: &Path,
    mut reader: AsyncStreamReader<BufReader<Compat<File>>>,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), BallistaError> {
    let (_, mut upload) = object_store.put_multipart(destination).await.map_err(|e| {
        REPLICATION_FAILURE
            .with_label_values(&["create_writer"])
            .inc();
        BallistaError::General(format!("Failed to create object store writer: {:?}", e))
    })?;

    while let Some(batch) = reader.maybe_next().await.transpose() {
        if let Ok(batch) = batch {
            let data = serialize_batch(batch)?;
            upload.write_all(&data).await.map_err(|e| {
                REPLICATION_FAILURE
                    .with_label_values(&["write_batch"])
                    .inc();
                BallistaError::General(format!("Failed to write batch: {:?}", e))
            })?;
        }
    }

    upload.shutdown().await.map_err(|e| {
        REPLICATION_FAILURE
            .with_label_values(&["writer_shutdown"])
            .inc();
        BallistaError::General(format!("Failed to shutdown async writer: {:?}", e))
    })?;

    Ok(())
}
#[cfg(test)]
mod tests {
    use ballista_core::{
        execution_plans::batch_stream_from_object_store,
        utils::{collect_stream, write_stream_to_disk},
    };
    use datafusion::{
        arrow::{
            array::{StringArray, UInt32Array},
            datatypes::{DataType, Field, Schema, SchemaRef},
            record_batch::RecordBatch,
        },
        error::Result,
        physical_plan::{
            stream::RecordBatchStreamAdapter, RecordBatchStream,
            SendableRecordBatchStream,
        },
    };
    use futures::Stream;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use std::{pin::Pin, sync::Arc};
    use tempfile::TempDir;

    use crate::replicator::{load_file, replicate_to_object_store, serialize_batch};

    pub struct OneElementStream {
        schema: Arc<Schema>,
        elem: Option<RecordBatch>,
    }

    impl OneElementStream {
        fn new(schema: Arc<Schema>, elem: RecordBatch) -> Self {
            Self {
                schema,
                elem: Some(elem),
            }
        }
    }

    impl Stream for OneElementStream {
        type Item = Result<RecordBatch>;

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            match self.get_mut().elem.take() {
                Some(elem) => std::task::Poll::Ready(Some(Ok(elem))),
                _ => std::task::Poll::Ready(None),
            }
        }
    }

    impl RecordBatchStream for OneElementStream {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    #[tokio::test]
    async fn load_from_local() -> Result<()> {
        let tmp_dir = TempDir::new().unwrap();
        let file = tmp_dir.path().join("1.data");
        let file_path = file.to_str().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
            ],
        )
        .unwrap();

        let mut stream: Pin<Box<dyn RecordBatchStream + Send>> =
            Box::pin(OneElementStream::new(schema, batch.clone()));

        let stats = write_stream_to_disk(&mut stream, file_path, &Default::default())
            .await
            .unwrap();

        assert!(stats.num_batches().unwrap() == 1);
        let mut reader = load_file(file_path).await.unwrap();

        let actual_batch = reader.maybe_next().await.unwrap().unwrap();
        assert_eq!(actual_batch, batch);

        let actual_batch = reader.maybe_next().await.unwrap();
        assert!(actual_batch.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn upload_to_object_store() -> Result<()> {
        let tmp_dir = TempDir::new().unwrap();
        let file = tmp_dir.path().join("1.data");
        let file_path = file.to_str().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let object_store = Arc::new(InMemory::new());

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
            ],
        )
        .unwrap();

        let mut stream: Pin<Box<dyn RecordBatchStream + Send>> =
            Box::pin(OneElementStream::new(schema, batch.clone()));

        let stats = write_stream_to_disk(&mut stream, file_path, &Default::default())
            .await
            .unwrap();

        assert!(stats.num_batches().unwrap() == 1);
        let reader = load_file(file_path).await.unwrap();
        let destination: Path = Path::parse("2.data").unwrap();
        replicate_to_object_store(&destination, reader, object_store.clone())
            .await
            .unwrap();

        let object_meta = object_store.head(&destination).await.unwrap();
        assert!(object_meta.size == 552);
        Ok(())
    }

    #[tokio::test]
    async fn load_from_object_store() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let object_store = Arc::new(InMemory::new());

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
            ],
        )
        .unwrap();

        let bytes = serialize_batch(batch.clone()).unwrap();
        let destination: Path = Path::parse("2.data").unwrap();

        object_store.put(&destination, bytes).await?;
        let stream =
            batch_stream_from_object_store("id".to_string(), &destination, object_store)
                .await?;

        let mut stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(stream.schema(), stream));

        let batches = collect_stream(&mut stream).await.unwrap();

        assert!(batches.len() == 1);
        assert_eq!(batches[0], batch);
        Ok(())
    }

    #[test]
    fn parse_path() {
        let path_str = "executor-dataprime-query-engine-79779bbd8c-l6gp6/data/shuffles/555585-other-10821e40-22f4-4a02-8817-05d7f52bksf2-4fAJ0vZhxCP-combined-statistics/1/efec3c4b-8465-43f9-b597-3f5ce1489808/data.arrow";

        assert!(Path::parse(path_str).is_ok())
    }
}
