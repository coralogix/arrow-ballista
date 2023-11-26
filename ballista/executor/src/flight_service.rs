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

//! Implementation of the Apache Arrow Flight protocol that wraps an executor.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::buffer::MutableBuffer;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::reader::{read_dictionary, read_record_batch};
use arrow::ipc::writer::IpcWriteOptions;
use arrow::ipc::{convert, root_as_message, CompressionType, MessageHeader};
use ballista_core::serde::decode_protobuf;
use ballista_core::serde::scheduler::Action as BallistaAction;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty,
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PutResult, SchemaResult, Ticket,
};
use datafusion::arrow::{
    error::ArrowError, ipc::reader::FileReader, record_batch::RecordBatch,
};
use futures::future::BoxFuture;
use futures::io::BufReader;
use futures::{AsyncRead, AsyncReadExt, Stream, TryStreamExt};

use object_store::path::Path;
use object_store::ObjectStore;
use std::io::{Read, Seek};
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::error::SendError;
use tokio::{sync::mpsc::Sender, task};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};

// TODO this is currently configured in two different places
// 4 MiB
const MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

/// Service implementing the Apache Arrow Flight Protocol
#[derive(Clone)]
pub struct BallistaFlightService {
    executor_id: String,
}

impl BallistaFlightService {
    pub fn new(executor_id: impl Into<String>) -> Self {
        Self {
            executor_id: executor_id.into(),
        }
    }
}

type BoxedFlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

fn decode_partition_location(ticket: &Ticket) -> Result<String, Status> {
    match &decode_protobuf(&ticket.ticket)
        .map_err(|e| Status::internal(format!("Ballista Error: {e:?}")))?
    {
        BallistaAction::FetchPartition { path, .. } => Ok(path.clone()),
    }
}

async fn do_get_aux(
    executor_id: String,
    path: &str,
) -> Result<Response<BoxedFlightStream<FlightData>>, Status> {
    info!(
        executor_id = executor_id,
        path, "fetching shuffle partition"
    );
    let file = File::open(path).map_err(|e| {
        Status::internal(format!("Failed to open partition file at {path}: {e:?}"))
    })?;
    let reader = FileReader::try_new(file, None).map_err(from_arrow_err)?;
    let schema = reader.schema();

    let (tx, rx) = channel(2);

    let executor_id = executor_id.clone();
    task::spawn_blocking(move || {
        if let Err(e) = read_partition(reader, tx) {
            warn!(executor_id, error = %e, "error streaming shuffle partition");
        }
    });
    let write_options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .map_err(from_arrow_err)?;

    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_max_flight_data_size(MAX_MESSAGE_SIZE)
        .with_schema(schema)
        .with_options(write_options)
        .build(ReceiverStream::new(rx))
        .map_err(|err| Status::from_error(Box::new(err)));

    Ok(Response::new(
        Box::pin(flight_data_stream) as BoxedFlightStream<FlightData>
    ))
}

#[tonic::async_trait]
impl FlightService for BallistaFlightService {
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let path = decode_partition_location(&ticket)?;

        info!(
            executor_id = self.executor_id,
            path, "fetching shuffle partition"
        );
        let file = File::open(path.as_str()).map_err(|e| {
            Status::internal(format!("Failed to open partition file at {path}: {e:?}"))
        })?;
        let reader = FileReader::try_new(file, None).map_err(from_arrow_err)?;
        let schema = reader.schema();

        let (tx, rx) = channel(2);

        let executor_id = self.executor_id.clone();
        task::spawn_blocking(move || {
            if let Err(e) = read_partition(reader, tx) {
                warn!(executor_id, error = %e, "error streaming shuffle partition");
            }
        });

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_max_flight_data_size(MAX_MESSAGE_SIZE)
            .with_schema(schema)
            .build(ReceiverStream::new(rx))
            .map_err(|err| Status::from_error(Box::new(err)));

        Ok(Response::new(
            Box::pin(flight_data_stream) as Self::DoGetStream
        ))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let token = uuid::Uuid::new_v4();
        info!("do_handshake token={}", token);

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: token.as_bytes().to_vec().into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        let str = format!("Bearer {token}");
        let mut resp: Response<
            Pin<Box<dyn Stream<Item = Result<_, Status>> + Send + 'static>>,
        > = Response::new(Box::pin(output));
        let md = MetadataValue::try_from(str)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        resp.metadata_mut().insert("authorization", md);

        Ok(resp)
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}

fn read_partition<T>(
    reader: FileReader<T>,
    tx: Sender<Result<RecordBatch, FlightError>>,
) -> Result<(), FlightError>
where
    T: Read + Seek,
{
    if tx.is_closed() {
        return Err(FlightError::Tonic(Status::internal(
            "Can't send a batch, channel is closed",
        )));
    }

    for batch in reader {
        tx.blocking_send(batch.map_err(|err| err.into()))
            .map_err(|err| {
                if let SendError(Err(err)) = err {
                    err
                } else {
                    FlightError::Tonic(Status::internal(
                        "Can't send a batch, something went wrong",
                    ))
                }
            })?
    }
    Ok(())
}

async fn read_stream_partition<T: AsyncRead + Unpin + Send>(
    mut reader: AsyncStreamReader<T>,
    tx: Sender<Result<RecordBatch, FlightError>>,
) -> Result<(), FlightError> {
    if tx.is_closed() {
        return Err(FlightError::Tonic(Status::internal(
            "Can't send a batch, channel is closed",
        )));
    }

    while let Some(batch) = reader.maybe_next().await.transpose() {
        tx.send(batch.map_err(|err| err.into()))
            .await
            .map_err(|err| {
                if let SendError(Err(err)) = err {
                    err
                } else {
                    FlightError::Tonic(Status::internal(
                        "Can't send a batch, something went wrong",
                    ))
                }
            })?
    }

    Ok(())
}

fn from_arrow_err(e: ArrowError) -> Status {
    Status::internal(format!("ArrowError: {e:?}"))
}

/// Service implementing the Apache Arrow Flight Protocol
#[derive(Clone)]
pub struct BallistaFlightServiceWithFallback {
    executor_id: String,
    object_store: Arc<dyn ObjectStore>,
}

impl BallistaFlightServiceWithFallback {
    pub fn new(
        executor_id: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            executor_id: executor_id.into(),
            object_store,
        }
    }
}

#[tonic::async_trait]
impl FlightService for BallistaFlightServiceWithFallback {
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let path = decode_partition_location(&request.into_inner())?;

        match do_get_aux(self.executor_id.clone(), path.as_str()).await {
            Ok(response) => Ok(response),
            Err(_) => {
                warn!("Failed to fetch partition from executor, attempting to read from object store");
                let path = Path::parse(format!("{}/{}", self.executor_id, path)).unwrap();
                let stream = self
                    .object_store
                    .as_ref()
                    .get(&path)
                    .await
                    .map_err(|err| {
                        Status::internal(format!(
                            "Failed to fetch partition from object store: {err:?}"
                        ))
                    })?
                    .into_stream();

                let async_reader = stream.map_err(|e| e.into()).into_async_read();
                let (tx, rx) = channel(2);
                let reader = AsyncStreamReader::try_new(async_reader, None)
                    .await
                    .map_err(from_arrow_err)?;
                let schema = reader.schema();
                let executor_id = self.executor_id.clone();
                task::spawn(async move {
                    if let Err(e) = read_stream_partition(reader, tx).await {
                        warn!(executor_id, error = %e, "error streaming shuffle partition");
                    }
                });

                let write_options = IpcWriteOptions::default()
                    .try_with_compression(Some(CompressionType::LZ4_FRAME))
                    .map_err(from_arrow_err)?;

                let flight_data_stream = FlightDataEncoderBuilder::new()
                    .with_max_flight_data_size(MAX_MESSAGE_SIZE)
                    .with_options(write_options)
                    .with_schema(schema)
                    .build(ReceiverStream::new(rx))
                    .map_err(|err| Status::from_error(Box::new(err)));

                Ok(Response::new(
                    Box::pin(flight_data_stream) as Self::DoGetStream
                ))
            }
        }
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let token = uuid::Uuid::new_v4();
        info!("do_handshake token={}", token);

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: token.as_bytes().to_vec().into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        let str = format!("Bearer {token}");
        let mut resp: Response<
            Pin<Box<dyn Stream<Item = Result<_, Status>> + Send + 'static>>,
        > = Response::new(Box::pin(output));
        let md = MetadataValue::try_from(str)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        resp.metadata_mut().insert("authorization", md);

        Ok(resp)
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
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
    /// Unless you need the StreamReader to be unbuffered you likely want to use `StreamReader::try_new` instead.
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

    fn maybe_next(&mut self) -> BoxFuture<'_, Result<Option<RecordBatch>, ArrowError>> {
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
