// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Client API for sending requests to executors.

use std::{
    collections::HashMap,
    convert::TryInto,
    fmt,
    sync::Arc,
    task::{Context, Poll},
};

use crate::error::{BallistaError, Result};
use crate::serde::scheduler::Action;

use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use datafusion::arrow::{
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
};
use datafusion::error::DataFusionError;

use crate::serde::protobuf;
use crate::utils::create_grpc_client_connection;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{
    future::BoxFuture, io::BufReader, AsyncRead, AsyncReadExt, Stream, StreamExt,
    TryStreamExt,
};
use log::debug;
use prost::Message;

// Set the max gRPC message size to 64 MiB. This is quite large
// but we have to send execution plans over gRPC and they can be large.
const MAX_GRPC_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Client for interacting with Ballista executors.
#[derive(Clone)]
pub struct BallistaClient {
    flight_client: FlightServiceClient<tonic::transport::channel::Channel>,
}

impl BallistaClient {
    /// Create a new BallistaClient to connect to the executor listening on the specified
    /// host and port
    pub async fn try_new(host: &str, port: u16) -> Result<Self> {
        let addr = format!("http://{host}:{port}");
        debug!("BallistaClient connecting to {}", addr);
        let connection =
            create_grpc_client_connection(addr.clone())
                .await
                .map_err(|e| {
                    BallistaError::GrpcConnectionError(format!(
                    "Error connecting to Ballista scheduler or executor at {addr}: {e:?}"
                ))
                })?;
        let flight_client = FlightServiceClient::new(connection)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);

        debug!("BallistaClient connected OK");

        Ok(Self { flight_client })
    }

    /// Fetch a partition from an executor
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_partition(
        &mut self,
        executor_id: &str,
        job_id: &str,
        stage_id: usize,
        output_partition: usize,
        map_partitions: &[usize],
        path: &str,
        host: &str,
        port: u16,
    ) -> Result<SendableRecordBatchStream> {
        let action = Action::FetchPartition {
            job_id: job_id.to_string(),
            stage_id,
            partition_id: output_partition,
            path: path.to_owned(),
            host: host.to_owned(),
            port,
        };
        self.execute_action(&action)
            .await
            .map_err(|error| match error {
                // map grpc connection error to partition fetch error.
                BallistaError::GrpcActionError(msg) => BallistaError::FetchFailed(
                    executor_id.to_owned(),
                    stage_id,
                    map_partitions.to_vec(),
                    msg,
                ),
                other => other,
            })
    }

    /// Execute an action and retrieve the results
    pub async fn execute_action(
        &mut self,
        action: &Action,
    ) -> Result<SendableRecordBatchStream> {
        let serialized_action: protobuf::Action = action.to_owned().try_into()?;

        let mut buf: Vec<u8> = Vec::with_capacity(serialized_action.encoded_len());

        serialized_action
            .encode(&mut buf)
            .map_err(|e| BallistaError::GrpcActionError(format!("{e:?}")))?;

        let request = tonic::Request::new(Ticket { ticket: buf.into() });

        let stream = self
            .flight_client
            .do_get(request)
            .await
            .map_err(|e| BallistaError::GrpcActionError(format!("{e:?}")))?
            .into_inner();

        // the schema should be the first message returned, else client should error
        let stream = stream.map_err(FlightError::Tonic);
        let mut stream = FlightDataDecoder::new(stream);
        let schema = loop {
            match stream.next().await {
                None => {}
                Some(Ok(fd)) => match &fd.payload {
                    DecodedPayload::None => {}
                    DecodedPayload::Schema(schema) => {
                        break schema.clone();
                    }
                    DecodedPayload::RecordBatch(_) => {}
                },
                Some(Err(e)) => return Err(BallistaError::Internal(e.to_string())),
            }
        };
        let stream = FlightDataStream::new(stream, schema);
        Ok(Box::pin(stream))
    }
}

struct FlightDataStream {
    stream: FlightDataDecoder,
    schema: SchemaRef,
}

impl FlightDataStream {
    pub fn new(stream: FlightDataDecoder, schema: SchemaRef) -> Self {
        Self { stream, schema }
    }
}

impl Stream for FlightDataStream {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(fd)) => match fd.payload {
                DecodedPayload::None => None,
                DecodedPayload::Schema(_) => {
                    Some(Err(DataFusionError::Internal("Got 2 schemas".to_string())))
                }
                DecodedPayload::RecordBatch(batch) => Some(Ok(batch)),
            },
            Some(Err(e)) => Some(Err(DataFusionError::Internal(e.to_string()))),
            None => None,
        })
    }
}

impl RecordBatchStream for FlightDataStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

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

impl<R: AsyncRead + Unpin + Send> AsyncStreamReader<R> {
    /// Try to create a new stream reader but do not wrap the reader in a BufReader.
    ///
    /// Unless you need the AsyncStreamReader to be unbuffered you likely want to use `AsyncStreamReader::try_new` instead.
    pub async fn try_new_unbuffered(
        mut reader: R,
        projection: Option<Vec<usize>>,
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
        Ok(Self {
            reader,
            schema: Arc::new(schema),
            finished: false,
            dictionaries_by_id,
            projection,
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
        Box::pin(async move {
            if self.finished {
                return Ok(None);
            }
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

                read_record_batch(&buf.into(), batch, self.schema(), &self.dictionaries_by_id, self.projection.as_ref().map(|x| x.0.as_ref()), &message.version()).map(Some)
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

impl<R: AsyncRead + Unpin + Send> AsyncStreamReader<BufReader<R>> {
    /// Try to create a new stream reader with the reader wrapped in a BufReader
    ///
    /// The first message in the stream is the schema, the reader will fail if it does not
    /// encounter a schema.
    /// To check if the reader is done, use `is_finished(self)`
    pub async fn try_new(
        reader: R,
        projection: Option<Vec<usize>>,
    ) -> Result<Self, ArrowError> {
        Self::try_new_unbuffered(BufReader::new(reader), projection).await
    }
}
