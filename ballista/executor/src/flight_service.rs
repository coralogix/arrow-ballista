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

use std::convert::TryFrom;
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;

use arrow::ipc::writer::IpcWriteOptions;
use arrow::ipc::CompressionType;
use ballista_core::error::BallistaError;
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
use futures::{Stream, TryStreamExt};
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
    match &decode_protobuf(&ticket.ticket).map_err(|e| from_ballista_err(&e))? {
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
    let file = File::open(path)
        .map_err(|e| {
            BallistaError::General(format!(
                "Failed to open partition file at {path}: {e:?}"
            ))
        })
        .map_err(|e| from_ballista_err(&e))?;
    let reader = FileReader::try_new(file, None).map_err(|e| from_arrow_err(&e))?;
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
        .map_err(|e| from_arrow_err(&e))?;

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
        let file = File::open(path.as_str())
            .map_err(|e| {
                BallistaError::General(format!(
                    "Failed to open partition file at {path}: {e:?}"
                ))
            })
            .map_err(|e| from_ballista_err(&e))?;
        let reader = FileReader::try_new(file, None).map_err(|e| from_arrow_err(&e))?;
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

fn from_arrow_err(e: &ArrowError) -> Status {
    Status::internal(format!("ArrowError: {e:?}"))
}

fn from_ballista_err(e: &BallistaError) -> Status {
    Status::internal(format!("Ballista Error: {e:?}"))
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
                let bytes = self
                    .object_store
                    .as_ref()
                    .get(&path)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                let reader = FileReader::try_new(std::io::Cursor::new(bytes), None)
                    .map_err(|e| from_arrow_err(&e))?;
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
