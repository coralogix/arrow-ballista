use std::{collections::HashMap, fmt, sync::Arc};

use async_stream::stream;
use datafusion::{
    arrow::{
        array::ArrayRef,
        buffer::MutableBuffer,
        datatypes::{Schema, SchemaRef},
        error::ArrowError,
        ipc::{
            convert,
            reader::{read_dictionary, read_record_batch},
            root_as_message, MessageHeader,
        },
        record_batch::RecordBatch,
    },
    physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream},
};
use futures::{future::BoxFuture, io::BufReader, AsyncBufRead, AsyncRead, AsyncReadExt};
use lazy_static::lazy_static;
use prometheus::{
    register_counter_vec, register_histogram_vec, CounterVec, HistogramVec,
};
use tokio::time::Instant;

const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

lazy_static! {
    static ref BALLISTA_ASYNC_STREAM_READER_LATENCY: HistogramVec =
        register_histogram_vec!(
            "ballista_async_stream_reader_latency",
            "Ballista async stream reader latency",
            &["type"],
            vec![0.01, 0.03, 0.05, 0.1, 0.3, 0.5, 1.0, 3.0, 5.0, 9.0, 20.0]
        )
        .unwrap();
    static ref BALLISTA_ASYNC_STREAM_READER_CAPACITY: CounterVec = register_counter_vec!(
        "ballista_async_stream_reader_capacity",
        "Ballista async stream reader capacity",
        &["type"]
    )
    .unwrap();
    static ref BALLISTA_ASYNC_STREAM_READER_SIZE: CounterVec = register_counter_vec!(
        "ballista_async_stream_reader_size",
        "Ballista async stream reader size",
        &["type"]
    )
    .unwrap();
}

pub struct AsyncStreamReaderMetrics {
    pub label: String,
    pub started_at: Instant,
    pub num_rows: usize,
    pub size: usize,
}

impl AsyncStreamReaderMetrics {
    pub fn new(label: String) -> Self {
        Self {
            label,
            started_at: Instant::now(),
            num_rows: 0,
            size: 0,
        }
    }
}

/// Arrow Stream reader
pub struct AsyncStreamReader<R: AsyncRead + Unpin + Send> {
    /// Stream reader
    reader: R,

    /// The schema that is read from the stream's first message
    schema: SchemaRef,

    /// Optional dictionaries for each schema field.
    ///
    /// Dictionaries may be appended to in the streaming format.
    dictionaries_by_id: HashMap<i64, ArrayRef>,

    /// An indicator of whether the stream is complete.
    ///
    /// This value is set to `true` the first time the reader's `next()` returns `None`.
    finished: bool,

    /// Optional projection
    projection: Option<(Vec<usize>, Schema)>,
    metrics: AsyncStreamReaderMetrics,
}

impl<R: AsyncRead + Unpin + Send> fmt::Debug for AsyncStreamReader<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        f.debug_struct("AsyncStreamReader<R>")
            .field("reader", &"BufReader<..>")
            .field("schema", &self.schema)
            .field("dictionaries_by_id", &self.dictionaries_by_id)
            .field("finished", &self.finished)
            .field("projection", &self.projection)
            .finish()
    }
}

impl<R: AsyncBufRead + Unpin + Send> AsyncStreamReader<R> {
    /// Try to create a new stream reader but do not wrap the reader in a BufReader.
    ///
    /// Unless you need the AsyncStreamReader to be unbuffered you likely want to use `AsyncStreamReader::try_new` instead.
    pub async fn try_new_unbuffered(
        mut reader: R,
        projection: Option<Vec<usize>>,
        label: String,
    ) -> Result<AsyncStreamReader<R>, ArrowError> {
        // determine metadata length
        let mut meta_size: [u8; 4] = [0; 4];
        reader.read_exact(&mut meta_size).await?;
        let meta_len = {
            // If a continuation marker is encountered, skip over it and read
            // the size from the next four bytes.
            if meta_size == CONTINUATION_MARKER {
                reader.read_exact(&mut meta_size).await?;
            }
            i32::from_le_bytes(meta_size)
        };

        let mut meta_buffer = vec![0; meta_len as usize];
        reader.read_exact(&mut meta_buffer).await?;

        let message = root_as_message(meta_buffer.as_slice()).map_err(|err| {
            ArrowError::ParseError(format!("Unable to get root as message: {err:?}"))
        })?;
        // message header is a Schema, so read it
        let ipc_schema = message.header_as_schema().ok_or_else(|| {
            ArrowError::ParseError("Unable to read IPC message as schema".to_string())
        })?;
        let schema = convert::fb_to_schema(ipc_schema);

        // Create an array of optional dictionary value arrays, one per field.
        let dictionaries_by_id = HashMap::new();

        let projection = match projection {
            Some(projection_indices) => {
                let schema = schema.project(&projection_indices)?;
                Some((projection_indices, schema))
            }
            _ => None,
        };
        let metrics = AsyncStreamReaderMetrics::new(label.clone());
        Ok(Self {
            reader,
            schema: Arc::new(schema),
            finished: false,
            dictionaries_by_id,
            projection,
            metrics,
        })
    }

    /// Return the schema of the stream
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Check if the stream is finished
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn maybe_next(
        &mut self,
    ) -> BoxFuture<'_, Result<Option<RecordBatch>, ArrowError>> {
        if self.finished {
            return Box::pin(futures::future::ok(None));
        }

        Box::pin(async move {
            // determine metadata length
            let mut meta_size: [u8; 4] = [0; 4];

            if let Err(e) = self.reader.read_exact(&mut meta_size).await {
                return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    // Handle EOF without the "0xFFFFFFFF 0x00000000"
                    // valid according to:
                    // https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
                    self.finished = true;
                    Ok(None)
                } else {
                    Err(ArrowError::from(e))
                };
            }

            let meta_len = {
                // If a continuation marker is encountered, skip over it and read
                // the size from the next four bytes.
                if meta_size == CONTINUATION_MARKER {
                    self.reader.read_exact(&mut meta_size).await?;
                }
                i32::from_le_bytes(meta_size)
            };

            if meta_len == 0 {
                // the stream has ended, mark the reader as finished
                self.finished = true;
                return Ok(None);
            }

            let mut meta_buffer = vec![0; meta_len as usize];
            self.reader.read_exact(&mut meta_buffer).await?;

            let vecs = &meta_buffer.to_vec();
            let message = root_as_message(vecs).map_err(|err| {
                ArrowError::ParseError(format!("Unable to get root as message: {err:?}"))
            })?;

            match message.header_type() {
            MessageHeader::Schema => Err(ArrowError::IpcError(
                "Not expecting a schema when messages are read".to_string(),
            )),
            MessageHeader::RecordBatch => {
                let batch = message.header_as_record_batch().ok_or_else(|| {
                    ArrowError::IpcError(
                        "Unable to read IPC message as record batch".to_string(),
                    )
                })?;
                // read the block that makes up the record batch into a buffer
                let mut buf = MutableBuffer::from_len_zeroed(message.bodyLength() as usize);
                self.reader.read_exact(&mut buf).await?;

                let batch = read_record_batch(&buf.into(), batch, self.schema(), &self.dictionaries_by_id, self.projection.as_ref().map(|x| x.0.as_ref()), &message.version())?;
                self.metrics.num_rows += batch.num_rows();
                self.metrics.size += batch.get_array_memory_size();
                Ok(Some(batch))
            }
            MessageHeader::DictionaryBatch => {
                let batch = message.header_as_dictionary_batch().ok_or_else(|| {
                    ArrowError::IpcError(
                        "Unable to read IPC message as dictionary batch".to_string(),
                    )
                })?;
                // read the block that makes up the dictionary batch into a buffer
                let mut buf = MutableBuffer::from_len_zeroed(message.bodyLength() as usize);
                self.reader.read_exact(&mut buf).await?;

                read_dictionary(
                    &buf.into(), batch, &self.schema, &mut self.dictionaries_by_id, &message.version()
                )?;

                // read the next message until we encounter a RecordBatch
                self.maybe_next().await
            }
            MessageHeader::NONE => {
                Ok(None)
            }
            t => Err(ArrowError::InvalidArgumentError(
                format!("Reading types other than record batches not yet supported, unable to read {t:?} ")
            )),
        }
        })
    }

    /// Gets a reference to the underlying reader.
    ///
    /// It is inadvisable to directly read from the underlying reader.
    pub fn get_ref(&self) -> &R {
        &self.reader
    }

    /// Gets a mutable reference to the underlying reader.
    ///
    /// It is inadvisable to directly read from the underlying reader.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }
}

impl<R: AsyncBufRead + Unpin + Send + 'static> AsyncStreamReader<R> {
    pub fn to_stream(mut self) -> SendableRecordBatchStream {
        let schema = self.schema();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream! {
                while let Some(batch) = self.maybe_next().await.transpose() {
                    yield batch.map_err(|err| err.into())
                }
            },
        ))
    }
}

impl<R: AsyncRead + Unpin + Send> AsyncStreamReader<BufReader<R>> {
    /// Try to create a new stream reader with the reader wrapped in a BufReader
    ///
    /// The first message in the stream is the schema, the reader will fail if it does not
    /// encounter a schema.
    /// To check if the reader is done, use `is_finished(self)`
    pub async fn try_new(
        reader: R,
        projection: Option<Vec<usize>>,
        label: String,
    ) -> Result<Self, ArrowError> {
        Self::try_new_unbuffered(BufReader::new(reader), projection, label).await
    }
}

impl<R: AsyncRead + Unpin + Send> Drop for AsyncStreamReader<R> {
    fn drop(&mut self) {
        BALLISTA_ASYNC_STREAM_READER_LATENCY
            .with_label_values(&[self.metrics.label.as_str()])
            .observe(self.metrics.started_at.elapsed().as_secs_f64());
        BALLISTA_ASYNC_STREAM_READER_CAPACITY
            .with_label_values(&[self.metrics.label.as_str()])
            .inc_by(self.metrics.num_rows as f64);
        BALLISTA_ASYNC_STREAM_READER_SIZE
            .with_label_values(&[self.metrics.label.as_str()])
            .inc_by(self.metrics.size as f64);
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use tokio::{fs::File, sync::Semaphore, task::JoinError};
    use tokio_util::compat::TokioAsyncReadCompatExt;

    use crate::{
        async_reader::AsyncStreamReader,
        serde::protobuf::execution_error::DatafusionError, utils,
    };

    #[tokio::test]
    async fn load_shuffle_file_a_lot() -> Result<(), DatafusionError> {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests/data.arrow");
        let mut handles = Vec::with_capacity(1000);
        let semaphore = Arc::new(Semaphore::new(1000));
        for _ in 0..1000 {
            let path = path.clone();
            let sem = semaphore.clone();
            handles.push(tokio::spawn(async move {
                let permit = sem.acquire().await.unwrap();
                let file = File::open(path).await.unwrap();
                let reader =
                    AsyncStreamReader::try_new(file.compat(), None, "test".to_string())
                        .await
                        .unwrap();
                let mut stream = reader.to_stream();
                drop(permit);
                let result = utils::collect_stream(&mut stream).await.unwrap();
                !result.is_empty()
            }))
        }

        let result: Result<Vec<bool>, JoinError> = futures::future::join_all(handles)
            .await
            .into_iter()
            .collect();
        assert!(result.unwrap().iter().all(|v| *v));
        Ok(())
    }
}
