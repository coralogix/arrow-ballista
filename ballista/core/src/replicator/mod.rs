use std::sync::Arc;

use bytes::Bytes;

use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::record_batch::RecordBatch;
use object_store::{path::Path, ObjectStore};
use tokio::io::AsyncWriteExt;
use tokio::{fs::File, sync::mpsc};
use tracing::{info, warn};

use crate::error::BallistaError;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use self::async_reader::AsyncFileReader;
pub mod async_reader;

pub enum Command {
    Replicate { path: String },
}

pub async fn start_replication(
    base_path: String,
    object_store: Arc<dyn ObjectStore>,
    mut receiver: mpsc::Receiver<Command>,
) -> Result<(), BallistaError> {
    while let Some(Command::Replicate { path }) = receiver.recv().await {
        let destination = format!("{}/{}", base_path, path);
        info!(destination, "Start replication");

        match Path::parse(destination) {
            Ok(dest) => match load_file(path.as_str()).await {
                Ok(reader) => {
                    if let Err(error) =
                        replicate_to_object_store(&dest, reader, object_store.clone())
                            .await
                    {
                        warn!(?error, ?path, "Failed to upload file to object store");
                    }
                }
                Err(error) => {
                    warn!(?error, ?path, "Failed to open file");
                }
            },
            Err(error) => {
                warn!(?error, ?path, "Failed to parse replication path");
            }
        }
    }

    Ok(())
}

async fn load_file(path: &str) -> Result<AsyncFileReader<Compat<File>>, BallistaError> {
    let file = File::open(path).await?;
    let reader = AsyncFileReader::try_new(file.compat(), None).await?;

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
    mut reader: AsyncFileReader<Compat<File>>,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), BallistaError> {
    let (_, mut upload) = object_store.put_multipart(destination).await.map_err(|e| {
        BallistaError::General(format!("Failed to create object store writer: {:?}", e))
    })?;

    while let Some(batch) = reader.maybe_next().await.transpose() {
        if let Ok(batch) = batch {
            let data = serialize_batch(batch)?;
            upload.write_all(&data).await?;
        }
    }

    upload.flush().await?;
    upload.shutdown().await?;

    Ok(())
}
#[cfg(test)]
mod tests {
    use std::{pin::Pin, sync::Arc};

    use crate::{execution_plans::batch_stream_from_object_store, utils::collect_stream};
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

        let stats = crate::utils::write_stream_to_disk(
            &mut stream,
            file_path,
            &Default::default(),
        )
        .await
        .unwrap();

        assert!(stats.num_batches.unwrap() == 1);
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

        let stats = crate::utils::write_stream_to_disk(
            &mut stream,
            file_path,
            &Default::default(),
        )
        .await
        .unwrap();

        assert!(stats.num_batches.unwrap() == 1);
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
}
