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

use async_trait::async_trait;
use datafusion::common::stats::Precision;
use futures::io::BufReader;
use moka::future::Cache;
use object_store::path::Path;
use object_store::ObjectStore;
use prometheus::{
    register_histogram_vec, register_int_counter, register_int_counter_vec, HistogramVec,
    IntCounter, IntCounterVec,
};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::result;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::fs::File;
use tokio::sync::mpsc::Receiver;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::async_reader::AsyncStreamReader;
use crate::client::BallistaClient;
use crate::serde::protobuf::ShuffleReaderExecNodeOptions;
use crate::serde::scheduler::{ExecutorMetadata, PartitionLocation, PartitionStats};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;

use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    ColumnStatistics, DisplayAs, DisplayFormatType, EmptyRecordBatchStream,
    ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt, TryStreamExt};

use crate::error::BallistaError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::common::AbortOnDropMany;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use itertools::Itertools;
use lazy_static::lazy_static;
use rand::prelude::SliceRandom;
use rand::thread_rng;
use tokio::sync::mpsc;
use tokio::sync::mpsc::channel;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

lazy_static! {
    static ref SHUFFLE_READER_FETCH_PARTITION_LATENCY: HistogramVec =
        register_histogram_vec!(
            "ballista_shuffle_reader_fetch_partition_latency",
            "Fetch partition latency in seconds",
            &["type"],
            vec![0.01, 0.03, 0.05, 0.1, 0.3, 0.5, 1.0, 3.0, 9.0, 20.0],
        )
        .unwrap();
    static ref SHUFFLE_READER_FETCH_PARTITION_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "ballista_shuffle_reader_fetch_partition_total",
            "Number of fetch partition calls",
            &["type"]
        )
        .unwrap();
    static ref SHUFFLE_READER_FAILED_REMOTE_FETCH: IntCounter = register_int_counter!(
        "ballista_shuffle_reader_failed_remote_fetch",
        "Number of failed remote fetch calls"
    )
    .unwrap();
    static ref SHUFFLE_READER_SAVED_REMOTE_FETCH: IntCounter = register_int_counter!(
        "ballista_shuffle_reader_saved_remote_fetch",
        "Number of saved remote fetch calls"
    )
    .unwrap();
}

#[derive(Debug, Clone)]
pub struct ShuffleReaderExecOptions {
    pub partition_fetch_parallelism: usize,
}

impl From<&ShuffleReaderExecNodeOptions> for ShuffleReaderExecOptions {
    fn from(val: &ShuffleReaderExecNodeOptions) -> Self {
        ShuffleReaderExecOptions {
            partition_fetch_parallelism: val.partition_fetch_parallelism as usize,
        }
    }
}

impl From<&ShuffleReaderExecOptions> for ShuffleReaderExecNodeOptions {
    fn from(val: &ShuffleReaderExecOptions) -> Self {
        Self {
            partition_fetch_parallelism: val.partition_fetch_parallelism as u32,
        }
    }
}

#[cfg(test)]
impl Default for ShuffleReaderExecOptions {
    fn default() -> Self {
        Self {
            partition_fetch_parallelism: 50,
        }
    }
}

/// ShuffleReaderExec reads partitions that have already been materialized by a ShuffleWriterExec
/// being executed by an executor
#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    /// Each partition of a shuffle can read data from multiple locations
    pub partition: Vec<Vec<PartitionLocation>>,
    pub(crate) schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    object_store: Option<Arc<dyn ObjectStore>>,
    clients: Arc<Cache<String, BallistaClient>>,
    pub options: Arc<ShuffleReaderExecOptions>,
}

impl ShuffleReaderExec {
    /// Create a new ShuffleReaderExec
    pub fn new(
        partition: Vec<Vec<PartitionLocation>>,
        schema: SchemaRef,
        object_store: Option<Arc<dyn ObjectStore>>,
        clients: Arc<Cache<String, BallistaClient>>,
        options: Arc<ShuffleReaderExecOptions>,
    ) -> Self {
        Self {
            partition,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            object_store,
            clients,
            options,
        }
    }
}

impl DisplayAs for ShuffleReaderExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ShuffleReaderExec: partitions={}", self.partition.len())
            }
        }
    }
}

impl ExecutionPlan for ShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        // TODO partitioning may be known and could be populated here
        // see https://github.com/apache/arrow-datafusion/issues/758
        Partitioning::UnknownPartitioning(self.partition.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Ballista ShuffleReaderExec does not support with_new_children()".to_owned(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let task_id = context.task_id().unwrap_or_else(|| partition.to_string());
        info!(
            task_id,
            partition,
            partition_location_count = self.partition[partition].len(),
            "executing shuffle read"
        );

        let mut partition_locations = HashMap::new();
        for p in &self.partition[partition] {
            partition_locations
                .entry(p.executor_meta.host.clone())
                .or_insert_with(Vec::new)
                .push(p.clone());
        }
        // Sort partitions for evenly send fetching partition requests to avoid hot executors within one task
        let mut partition_locations: Vec<PartitionLocation> = partition_locations
            .into_values()
            .flat_map(|ps| {
                ps.into_iter()
                    .filter(|p| p.partition_stats.num_rows.map_or(true, |v| v > 0))
                    .enumerate()
            })
            .sorted_by(|(p1_idx, _), (p2_idx, _)| Ord::cmp(p1_idx, p2_idx))
            .map(|(_, p)| p)
            .collect();

        if partition_locations.is_empty() {
            info!(
                task_id,
                partition,
                partition_location_count = self.partition[partition].len(),
                "There are no partitions to fetch, returning an empty stream"
            );
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                EmptyRecordBatchStream::new(self.schema()),
            )));
        }

        // Shuffle partitions for evenly send fetching partition requests to avoid hot executors within multiple tasks
        partition_locations.shuffle(&mut thread_rng());

        let response_receiver = if let Some(object_store) = self.object_store.as_ref() {
            send_fetch_partitions_with_fallback(
                task_id,
                partition,
                partition_locations,
                object_store.clone(),
                self.clients.clone(),
                self.options.as_ref(),
            )
        } else {
            send_fetch_partitions(
                task_id,
                partition,
                partition_locations,
                self.clients.clone(),
                self.options.as_ref(),
            )
        };

        let result = RecordBatchStreamAdapter::new(
            Arc::new(self.schema.as_ref().clone()),
            response_receiver.try_flatten(),
        );
        Ok(Box::pin(result))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(stats_for_partitions(
            self.schema.fields().len(),
            self.partition
                .iter()
                .flatten()
                .map(|loc| loc.partition_stats),
        ))
    }
}

fn stats_for_partitions(
    num_fields: usize,
    partition_stats: impl Iterator<Item = PartitionStats>,
) -> Statistics {
    // TODO stats: add column statistics to PartitionStats
    let (num_rows, total_byte_size) =
        partition_stats.fold((Some(0), Some(0)), |(num_rows, total_byte_size), part| {
            // if any statistic is unkown it makes the entire statistic unkown
            let num_rows = num_rows.zip(part.num_rows).map(|(a, b)| a + b as usize);
            let total_byte_size = total_byte_size
                .zip(part.num_bytes)
                .map(|(a, b)| a + b as usize);
            (num_rows, total_byte_size)
        });

    Statistics {
        num_rows: num_rows.map(Precision::Exact).unwrap_or(Precision::Absent),
        total_byte_size: total_byte_size
            .map(Precision::Exact)
            .unwrap_or(Precision::Absent),
        column_statistics: vec![ColumnStatistics::new_unknown(); num_fields],
    }
}

/// Adapter for a tokio ReceiverStream that implements the SendableRecordBatchStream interface
struct AbortableReceiverStream {
    inner: ReceiverStream<result::Result<SendableRecordBatchStream, BallistaError>>,

    task_id: String,
    partition: usize,

    #[allow(dead_code)]
    drop_helper: AbortOnDropMany<()>,
}

impl AbortableReceiverStream {
    /// Construct a new SendableRecordBatchReceiverStream which will send batches of the specified schema from inner
    pub fn create(
        task_id: String,
        partition: usize,
        rx: Receiver<Result<SendableRecordBatchStream, BallistaError>>,
        join_handles: Vec<JoinHandle<()>>,
    ) -> AbortableReceiverStream {
        let inner = ReceiverStream::new(rx);
        Self {
            inner,
            task_id,
            partition,
            drop_helper: AbortOnDropMany(join_handles),
        }
    }
}

impl Stream for AbortableReceiverStream {
    type Item = result::Result<SendableRecordBatchStream, ArrowError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner
            .poll_next_unpin(cx)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))
    }
}

impl Drop for AbortableReceiverStream {
    fn drop(&mut self) {
        info!(
            task_id = self.task_id,
            partition = self.partition,
            "receiver stream complete"
        );
    }
}

fn send_fetch_partitions_with_fallback(
    task_id: String,
    partition: usize,
    partition_locations: Vec<PartitionLocation>,
    object_store: Arc<dyn ObjectStore>,
    clients: Arc<Cache<String, BallistaClient>>,
    options: &ShuffleReaderExecOptions,
) -> AbortableReceiverStream {
    let (response_sender, response_receiver) =
        mpsc::channel(options.partition_fetch_parallelism);
    let mut join_handles = Vec::with_capacity(3);
    let (local_locations, remote_locations): (Vec<_>, Vec<_>) = partition_locations
        .into_iter()
        .partition(check_is_local_location);

    info!(
        task_id,
        local_shuffles = local_locations.len(),
        remote_shuffles = remote_locations.len(),
        "fetching partitions with fallback"
    );

    // keep local shuffle files reading in serial order for memory control.
    let sender_for_local = response_sender.clone();
    join_handles.push(tokio::spawn(async move {
        for p in local_locations.iter() {
            let now = Instant::now();
            SHUFFLE_READER_FETCH_PARTITION_TOTAL
                .with_label_values(&["local"])
                .inc();
            let r = PartitionReaderEnum::Local.fetch_partition(p).await;
            SHUFFLE_READER_FETCH_PARTITION_LATENCY
                .with_label_values(&["local"])
                .observe(now.elapsed().as_secs_f64());
            if let Err(e) = sender_for_local.send(r).await {
                warn!(
                    job_id = p.job_id,
                    stage_id = p.stage_id,
                    partition_id = p.output_partition,
                    "Fail to send response event to the channel due to {}",
                    e
                );
            }
        }
    }));

    let (failed_partition_sender, mut failed_partition_receiver) = mpsc::channel(2);
    let sender_to_remote = response_sender.clone();
    join_handles.push(tokio::spawn(async move {
        for p in remote_locations.iter() {
            let now = Instant::now();
            let permit = sender_to_remote.reserve().await.unwrap();
            SHUFFLE_READER_FETCH_PARTITION_TOTAL
                .with_label_values(&["remote"])
                .inc();
            let failed_partition_sender = failed_partition_sender.clone();
            let result = PartitionReaderEnum::FlightRemote {
                clients: clients.clone(),
            }
            .fetch_partition(p)
            .await;
            SHUFFLE_READER_FETCH_PARTITION_LATENCY
                .with_label_values(&["remote"])
                .observe(now.elapsed().as_secs_f64());
            match result {
                Ok(batch_stream) => permit.send(Ok(batch_stream)),

                Err(error) => {
                    drop(permit);
                    warn!(
                        job_id = p.job_id,
                        stage_id = p.stage_id,
                        partition_id = p.output_partition,
                        ?error,
                        "Fail to fetch remote partition",
                    );
                    SHUFFLE_READER_FAILED_REMOTE_FETCH.inc();
                    if let Err(error) = failed_partition_sender.send(p.clone()).await {
                        warn!(
                            job_id = p.job_id,
                            stage_id = p.stage_id,
                            partition_id = p.output_partition,
                            ?error,
                            "Fail to send failed partition to channel",
                        );
                    }
                }
            }
        }
    }));

    join_handles.push(tokio::spawn(async move {
        while let Some(partition) = failed_partition_receiver.recv().await {
            let now = Instant::now();
            let permit = response_sender.reserve().await.unwrap();
            SHUFFLE_READER_FETCH_PARTITION_TOTAL
                .with_label_values(&["object_store"])
                .inc();
            let r = PartitionReaderEnum::ObjectStoreRemote {
                object_store: object_store.clone(),
            }
            .fetch_partition(&partition)
            .await;
            SHUFFLE_READER_FETCH_PARTITION_LATENCY
                .with_label_values(&["object_store"])
                .observe(now.elapsed().as_secs_f64());

            if r.is_ok() {
                SHUFFLE_READER_SAVED_REMOTE_FETCH.inc();
            }

            permit.send(r);
        }
    }));

    AbortableReceiverStream::create(task_id, partition, response_receiver, join_handles)
}

fn send_fetch_partitions(
    task_id: String,
    partition: usize,
    partition_locations: Vec<PartitionLocation>,
    clients: Arc<Cache<String, BallistaClient>>,
    options: &ShuffleReaderExecOptions,
) -> AbortableReceiverStream {
    let (response_sender, response_receiver) =
        channel(options.partition_fetch_parallelism);
    let mut join_handles = Vec::with_capacity(2);
    let (local_locations, remote_locations): (Vec<_>, Vec<_>) = partition_locations
        .into_iter()
        .partition(check_is_local_location);

    info!(
        task_id,
        local_shuffles = local_locations.len(),
        remote_shuffles = remote_locations.len(),
        "fetching partitions"
    );

    // keep local shuffle files reading in serial order for memory control.
    let sender_for_local = response_sender.clone();
    join_handles.push(tokio::spawn(async move {
        for p in local_locations.iter() {
            let now = Instant::now();
            SHUFFLE_READER_FETCH_PARTITION_TOTAL
                .with_label_values(&["local"])
                .inc();
            let r = PartitionReaderEnum::Local.fetch_partition(p).await;
            SHUFFLE_READER_FETCH_PARTITION_LATENCY
                .with_label_values(&["local"])
                .observe(now.elapsed().as_secs_f64());
            if let Err(e) = sender_for_local.send(r).await {
                warn!(
                    job_id = p.job_id,
                    stage_id = p.stage_id,
                    partition_id = p.output_partition,
                    "Fail to send response event to the channel due to {}",
                    e
                );
            }
        }
    }));

    join_handles.push(tokio::spawn(async move {
        for p in remote_locations.iter() {
            let now = Instant::now();
            let permit = response_sender.reserve().await.unwrap();
            SHUFFLE_READER_FETCH_PARTITION_TOTAL
                .with_label_values(&["remote"])
                .inc();
            let r = PartitionReaderEnum::FlightRemote {
                clients: clients.clone(),
            }
            .fetch_partition(p)
            .await;
            SHUFFLE_READER_FETCH_PARTITION_LATENCY
                .with_label_values(&["remote"])
                .observe(now.elapsed().as_secs_f64());

            if r.is_err() {
                SHUFFLE_READER_FAILED_REMOTE_FETCH.inc();
            }

            permit.send(r)
        }
    }));

    AbortableReceiverStream::create(task_id, partition, response_receiver, join_handles)
}

fn check_is_local_location(location: &PartitionLocation) -> bool {
    std::path::Path::new(location.path.as_str()).exists()
}

/// Partition reader Trait, different partition reader can have
#[async_trait]
trait PartitionReader: Send + Sync + Clone {
    // Read partition data from PartitionLocation
    async fn fetch_partition(
        &self,
        location: &PartitionLocation,
    ) -> result::Result<SendableRecordBatchStream, BallistaError>;
}

#[derive(Clone)]
enum PartitionReaderEnum {
    Local,
    FlightRemote {
        clients: Arc<Cache<String, BallistaClient>>,
    },
    ObjectStoreRemote {
        object_store: Arc<dyn ObjectStore>,
    },
}

#[async_trait]
impl PartitionReader for PartitionReaderEnum {
    // Notice return `BallistaError::FetchFailed` will let scheduler re-schedule the task.
    async fn fetch_partition(
        &self,
        location: &PartitionLocation,
    ) -> result::Result<SendableRecordBatchStream, BallistaError> {
        match self {
            PartitionReaderEnum::Local { .. } => fetch_partition_local(location).await,
            PartitionReaderEnum::FlightRemote { clients } => {
                fetch_partition_remote(location, clients.as_ref()).await
            }
            PartitionReaderEnum::ObjectStoreRemote { object_store } => {
                fetch_partition_object_store(location, object_store.clone()).await
            }
        }
    }
}

async fn get_executor_client(
    clients: &Cache<String, BallistaClient>,
    location: &PartitionLocation,
    metadata: &ExecutorMetadata,
) -> Result<BallistaClient, BallistaError> {
    clients
        .try_get_with_by_ref(
            &metadata.host,
            BallistaClient::try_new(&metadata.host, metadata.port),
        )
        .await
        .map_err(|error| match error.as_ref() {
            BallistaError::GrpcConnectionError(msg) => BallistaError::FetchFailed(
                metadata.id.clone(),
                location.stage_id,
                location.map_partitions.clone(),
                msg.clone(),
            ),
            other => BallistaError::Internal(other.to_string()),
        })
}

async fn fetch_partition_remote(
    location: &PartitionLocation,
    clients: &Cache<String, BallistaClient>,
) -> Result<SendableRecordBatchStream, BallistaError> {
    let metadata = &location.executor_meta;
    let mut ballista_client = get_executor_client(clients, location, metadata).await?;

    ballista_client
        .fetch_partition(
            &metadata.id,
            &location.job_id,
            location.stage_id,
            location.output_partition,
            &location.map_partitions,
            &location.path,
            &metadata.host,
            metadata.port,
        )
        .await
}

async fn fetch_partition_local(
    location: &PartitionLocation,
) -> result::Result<SendableRecordBatchStream, BallistaError> {
    let path = &location.path;
    let metadata = &location.executor_meta;

    let reader = fetch_partition_local_inner(path).await.map_err(|e| {
        // return BallistaError::FetchFailed may let scheduler retry this task.
        BallistaError::FetchFailed(
            metadata.id.clone(),
            location.stage_id,
            location.map_partitions.clone(),
            e.to_string(),
        )
    })?;

    Ok(reader.to_stream())
}

async fn fetch_partition_local_inner(
    path: &str,
) -> result::Result<AsyncStreamReader<BufReader<Compat<File>>>, BallistaError> {
    let file = File::open(path).await.map_err(|e| {
        BallistaError::General(format!("Failed to open partition file at {path}: {e:?}"))
    })?;
    let reader = AsyncStreamReader::try_new(file.compat(), None)
        .await
        .map_err(|e| {
            BallistaError::General(format!(
                "Failed to new arrow StreamReader at {path}: {e:?}"
            ))
        })?;
    Ok(reader)
}

pub async fn fetch_partition_object_store(
    location: &PartitionLocation,
    object_store: Arc<dyn ObjectStore>,
) -> result::Result<SendableRecordBatchStream, BallistaError> {
    let executor_id = location.executor_meta.id.clone();
    let path = Path::parse(format!("{}{}", executor_id, location.path)).map_err(|e| {
        BallistaError::General(format!("Failed to parse partition location - {:?}", e))
    })?;

    batch_stream_from_object_store(
        executor_id,
        &path,
        location.stage_id,
        &location.map_partitions,
        object_store,
    )
    .await
}

pub async fn batch_stream_from_object_store(
    executor_id: String,
    path: &Path,
    stage_id: usize,
    map_partitions: &[usize],
    object_store: Arc<dyn ObjectStore>,
) -> Result<SendableRecordBatchStream, BallistaError> {
    let stream = object_store
        .as_ref()
        .get(path)
        .await
        .map_err(|e| match e {
            object_store::Error::NotFound { path, source: _ } => {
                BallistaError::FetchFailed(
                    executor_id.clone(),
                    stage_id,
                    map_partitions.to_vec(),
                    format!("Partition not found in object store - {}", path),
                )
            }
            _ => BallistaError::General(format!("Failed to fetch partition - {:?}", e)),
        })?
        .into_stream();

    let async_reader = stream.map_err(|e| e.into()).into_async_read();
    let reader = AsyncStreamReader::try_new(async_reader, None)
        .await
        .map_err(|e| {
            BallistaError::General(format!(
                "Failed to build async partition reader - {:?}",
                e
            ))
        })?;
    Ok(reader.to_stream())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plans::ShuffleWriterExec;
    use crate::serde::scheduler::{ExecutorMetadata, ExecutorSpecification};
    use crate::utils;
    use datafusion::arrow::array::{Int32Array, StringArray, UInt32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::ipc::writer::StreamWriter;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::common;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::prelude::SessionContext;
    use tempfile::{tempdir, TempDir};

    #[tokio::test]
    async fn test_stats_for_partitions_empty() {
        let result = stats_for_partitions(0, std::iter::empty());

        let exptected = Statistics {
            num_rows: Precision::Exact(0),
            total_byte_size: Precision::Exact(0),
            column_statistics: vec![],
        };

        assert_eq!(result, exptected);
    }

    #[tokio::test]
    async fn test_stats_for_partitions_full() {
        let part_stats = vec![
            PartitionStats {
                num_rows: Some(10),
                num_bytes: Some(84),
                num_batches: Some(1),
            },
            PartitionStats {
                num_rows: Some(4),
                num_bytes: Some(65),
                num_batches: None,
            },
        ];

        let result = stats_for_partitions(0, part_stats.into_iter());

        let exptected = Statistics {
            num_rows: Precision::Exact(14),
            total_byte_size: Precision::Exact(149),
            column_statistics: vec![],
        };

        assert_eq!(result, exptected);
    }

    #[tokio::test]
    async fn test_stats_for_partitions_missing() {
        let part_stats = vec![
            PartitionStats {
                num_rows: Some(10),
                num_bytes: Some(84),
                num_batches: Some(1),
            },
            PartitionStats {
                num_rows: None,
                num_bytes: None,
                num_batches: None,
            },
        ];

        let result = stats_for_partitions(0, part_stats.into_iter());

        let exptected = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };

        assert_eq!(result, exptected);
    }

    #[tokio::test]
    async fn test_fetch_partitions_error_mapping() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let _job_id = "test_job_1";
        let mut partitions: Vec<PartitionLocation> = vec![];
        for _partition_id in 0..4 {
            partitions.push(PartitionLocation {
                job_id: "".to_string(),
                stage_id: 0,
                map_partitions: vec![0],
                output_partition: 0,
                executor_meta: ExecutorMetadata {
                    id: "executor_1".to_string(),
                    host: "executor_1".to_string(),
                    port: 7070,
                    grpc_port: 8080,
                    specification: ExecutorSpecification {
                        task_slots: 1,
                        version: "".to_string(),
                    },
                },
                partition_stats: Default::default(),
                path: "test_path".to_string(),
            })
        }

        let shuffle_reader_exec = ShuffleReaderExec::new(
            vec![partitions],
            Arc::new(schema),
            None,
            Arc::new(Cache::new(10)),
            Arc::new(ShuffleReaderExecOptions::default()),
        );
        let mut stream = shuffle_reader_exec.execute(0, task_ctx)?;
        let batches = utils::collect_stream(&mut stream).await;
        assert!(batches.is_err());

        // BallistaError::FetchFailed -> ArrowError::ExternalError -> ballistaError::FetchFailed
        let ballista_error = batches.unwrap_err();
        assert!(matches!(
            ballista_error,
            BallistaError::FetchFailed(_, _, _, _)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_send_fetch_partitions_1() {
        test_send_fetch_partitions(1, &ShuffleReaderExecOptions::default()).await;
    }

    #[tokio::test]
    async fn test_send_fetch_partitions_n() {
        test_send_fetch_partitions(4, &ShuffleReaderExecOptions::default()).await;
    }

    #[tokio::test]
    async fn test_read_local_shuffle() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let work_dir = TempDir::new().unwrap();
        let input = ShuffleWriterExec::try_new(
            "local_file".to_owned(),
            1,
            vec![0],
            create_test_data_plan().unwrap(),
            work_dir.into_path().to_str().unwrap().to_owned(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 1)),
            None,
        )
        .unwrap();

        let mut stream = input.execute(0, task_ctx).unwrap();

        let batches = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
            .unwrap();

        let path = batches[0].columns()[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // from to input partitions test the first one with two batches
        let file_path = path.value(0);
        let reader = fetch_partition_local_inner(file_path).await.unwrap();

        let mut stream: SendableRecordBatchStream = reader.to_stream();

        let result = utils::collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
            .unwrap();

        assert_eq!(result.len(), 2);
        for b in result {
            assert_eq!(b, create_test_batch())
        }
    }

    async fn test_send_fetch_partitions(
        partition_num: usize,
        options: &ShuffleReaderExecOptions,
    ) {
        let schema = get_test_partition_schema();
        let data_array = Int32Array::from(vec![1]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(data_array)])
                .unwrap();
        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("shuffle_data");
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = StreamWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let partition_locations = get_test_partition_locations(
            partition_num,
            file_path.to_str().unwrap().to_string(),
        );

        let response_receiver = send_fetch_partitions(
            "job_id".to_string(),
            0,
            partition_locations,
            Arc::new(Cache::new(10)),
            options,
        );

        let stream = RecordBatchStreamAdapter::new(
            Arc::new(schema),
            response_receiver.try_flatten(),
        );

        let result = common::collect(Box::pin(stream)).await.unwrap();
        assert_eq!(partition_num, result.len());
    }

    fn get_test_partition_locations(n: usize, path: String) -> Vec<PartitionLocation> {
        (0..n)
            .map(|partition_id| PartitionLocation {
                job_id: "job".to_string(),
                stage_id: 1,
                output_partition: partition_id,
                map_partitions: vec![0],
                executor_meta: ExecutorMetadata {
                    id: format!("exec{partition_id}"),
                    host: "localhost".to_string(),
                    port: 50051,
                    grpc_port: 50052,
                    specification: ExecutorSpecification {
                        task_slots: 12,
                        version: "0".to_string(),
                    },
                },
                partition_stats: Default::default(),
                path: path.clone(),
            })
            .collect()
    }

    fn get_test_partition_schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Int32, false)])
    }

    // create two partitions each has two same batches
    fn create_test_data_plan() -> Result<Arc<dyn ExecutionPlan>> {
        let batch = create_test_batch();
        let partition = vec![batch.clone(), batch];
        let partitions = vec![partition.clone(), partition];
        Ok(Arc::new(MemoryExec::try_new(
            &partitions,
            create_test_schema(),
            None,
        )?))
    }

    fn create_test_batch() -> RecordBatch {
        RecordBatch::try_new(
            create_test_schema(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![
                    Some("rust"),
                    Some("datafusion"),
                    Some("ballista"),
                ])),
            ],
        )
        .unwrap()
    }

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("number", DataType::UInt32, true),
            Field::new("str", DataType::Utf8, true),
        ]))
    }
}
