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

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::common::Statistics;
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::common::AbortOnDropMany;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::sorts::streaming_merge::streaming_merge;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};
use log::debug;
use std::any::Any;
use std::sync::Arc;
use std::task::Poll;
use tokio::sync::mpsc;
use tracing::Instrument;

#[derive(Debug, Clone)]
pub struct CoalesceTasksExec {
    /// Partitions of input plan to coalesce
    partitions: Vec<usize>,
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// sorting expression
    order_by: Option<Vec<PhysicalSortExpr>>,
}

impl CoalesceTasksExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        partitions: Vec<usize>,
        order_by: Option<Vec<PhysicalSortExpr>>,
    ) -> Self {
        Self {
            partitions,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            order_by,
        }
    }

    pub fn partitions(&self) -> &[usize] {
        &self.partitions
    }
}

impl DisplayAs for CoalesceTasksExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                match self.output_ordering() {
                    Some(expr) => write!(
                        f,
                        "CoalesceTasksExec: sort_expr=[{}]",
                        PhysicalSortExpr::format_list(expr)
                    ),
                    _ => write!(f, "CoalesceTasksExec"),
                }
            }
        }
    }
}

impl ExecutionPlan for CoalesceTasksExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.order_by.as_deref()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            partitions: self.partitions.clone(),
            input: children[0].clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            order_by: self.order_by.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_partitions = self.input.output_partitioning().partition_count();

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        // record the (very) minimal work done so that
        // elapsed_compute is not reported as 0
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        // If only executing a single partition, execute it directly
        if self.partitions.len() == 1 {
            return self.input.execute(self.partitions[0], context);
        }

        match self.order_by.as_ref() {
            Some(sort_expr) => {
                let reservation =
                    MemoryConsumer::new(format!("CoalesceTaslsExec[{partition}]"))
                        .register(&context.runtime_env().memory_pool);
                let receivers = (0..input_partitions)
                    .map(|partition| {
                        Ok(spawn_buffered(
                            self.input.execute(partition, context.clone())?,
                            1,
                        ))
                    })
                    .collect::<Result<_>>()?;

                let result = streaming_merge(
                    receivers,
                    self.schema(),
                    sort_expr,
                    BaselineMetrics::new(&self.metrics, partition),
                    context.session_config().batch_size(),
                    None,
                    reservation,
                )?;

                Ok(result)
            }
            _ => {
                // use a stream that allows each sender to put in at
                // least one result in an attempt to maximize
                // parallelism.
                let (sender, receiver) =
                    mpsc::channel::<Result<RecordBatch>>(input_partitions);

                // spawn independent tasks whose resulting streams (of batches)
                // are sent to the channel for consumption.
                let mut join_handles = Vec::with_capacity(input_partitions);
                for partition in self.partitions.iter().copied() {
                    let input = self.input.clone();
                    let context = context.clone();
                    let output = sender.clone();

                    let fut = async move {
                        let mut stream = match input.execute(partition, context) {
                            Err(e) => {
                                // If send fails, plan being torn down,
                                // there is no place to send the error.
                                output.send(Err(e)).await.ok();
                                debug!(
                                    "Stopping execution: error executing input: {}",
                                    DisplayableExecutionPlan::new(input.as_ref())
                                        .one_line()
                                );
                                return;
                            }
                            Ok(stream) => stream,
                        };

                        while let Some(item) = stream.next().await {
                            // If send fails, plan being torn down,
                            // there is no place to send the error.
                            if output.send(item).await.is_err() {
                                debug!(
                            "Stopping execution: output is gone, plan cancelling: {}",
                            DisplayableExecutionPlan::new(input.as_ref()).one_line()
                        );
                                return;
                            }
                        }
                    };

                    join_handles.push(tokio::spawn(fut.in_current_span()));
                }

                Ok(Box::pin(MergeStream {
                    input: receiver,
                    schema: self.schema(),
                    baseline_metrics,
                    drop_helper: AbortOnDropMany(join_handles),
                }))
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }
}

struct MergeStream {
    schema: SchemaRef,
    input: mpsc::Receiver<Result<RecordBatch>>,
    baseline_metrics: BaselineMetrics,
    #[allow(unused)]
    drop_helper: AbortOnDropMany<()>,
}

impl Stream for MergeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_recv(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for MergeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// If running in a tokio context spawns the execution of `stream` to a separate task
/// allowing it to execute in parallel with an intermediate buffer of size `buffer`
pub fn spawn_buffered(
    mut input: SendableRecordBatchStream,
    buffer: usize,
) -> SendableRecordBatchStream {
    // Use tokio only if running from a multi-thread tokio context
    match tokio::runtime::Handle::try_current() {
        Ok(handle)
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread =>
        {
            let mut builder = RecordBatchReceiverStream::builder(input.schema(), buffer);

            let sender = builder.tx();

            builder.spawn(async move {
                while let Some(item) = input.next().await {
                    if sender.send(item).await.is_err() {
                        // receiver dropped when query is shutdown early (e.g., limit) or error,
                        // no need to return propagate the send error.
                        return Ok(());
                    }
                }

                Ok(())
            });

            builder.build()
        }
        _ => input,
    }
}
