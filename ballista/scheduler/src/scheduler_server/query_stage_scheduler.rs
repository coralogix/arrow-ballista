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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tracing::{debug, error, info};

use crate::metrics::SchedulerMetricsCollector;
use crate::scheduler_server::timestamp_millis;
use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::{EventAction, EventSender};
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::state::executor_manager::ExecutorReservation;

use crate::state::SchedulerState;

pub(crate) struct QueryStageScheduler<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    state: Arc<SchedulerState<T, U>>,
    metrics_collector: Arc<dyn SchedulerMetricsCollector>,
    pending_tasks: AtomicUsize,
    tasks_per_tick: usize,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> QueryStageScheduler<T, U> {
    pub(crate) fn new(
        state: Arc<SchedulerState<T, U>>,
        metrics_collector: Arc<dyn SchedulerMetricsCollector>,
        tasks_per_tick: usize,
    ) -> Self {
        Self {
            state,
            metrics_collector,
            pending_tasks: AtomicUsize::default(),
            tasks_per_tick,
        }
    }

    pub(crate) fn set_pending_tasks(&self, tasks: usize) {
        self.pending_tasks.store(tasks, Ordering::Release);
        self.metrics_collector
            .set_pending_tasks_queue_size(tasks as u64);
    }

    pub(crate) fn pending_tasks(&self) -> usize {
        self.pending_tasks.load(Ordering::Acquire)
    }

    pub(crate) fn metrics_collector(&self) -> &dyn SchedulerMetricsCollector {
        self.metrics_collector.as_ref()
    }

    async fn process_event(
        &self,
        event: QueryStageSchedulerEvent,
        tx_event: EventSender<QueryStageSchedulerEvent>,
    ) -> Result<()> {
        match event {
            QueryStageSchedulerEvent::JobQueued {
                job_id,
                job_name,
                session_ctx,
                plan,
                queued_at,
            } => {
                info!(job_id, job_name, "job queued");

                self.state
                    .task_manager
                    .queue_job(&job_id, &job_name, queued_at)
                    .await?;

                let state = self.state.clone();
                tokio::spawn(async move {
                    let event = if let Err(e) = state
                        .submit_job(&job_id, &job_name, session_ctx, &plan, queued_at)
                        .await
                    {
                        error!(job_id, error = %e, "error planning job");
                        let fail_message = format!("Error planning job {job_id}: {e:?}");
                        QueryStageSchedulerEvent::JobPlanningFailed {
                            job_id,
                            job_name,
                            fail_message,
                            queued_at,
                            failed_at: timestamp_millis(),
                            error: Arc::new(e),
                        }
                    } else {
                        QueryStageSchedulerEvent::JobSubmitted {
                            job_id,
                            queued_at,
                            submitted_at: timestamp_millis(),
                            resubmit: false,
                        }
                    };

                    tx_event.post_event(event);
                });
            }
            QueryStageSchedulerEvent::JobSubmitted {
                job_id,
                queued_at,
                submitted_at,
                resubmit,
            } => {
                if !resubmit {
                    self.metrics_collector.record_submitted(
                        &job_id,
                        queued_at,
                        submitted_at,
                    );

                    info!(job_id, "job submitted");
                } else {
                    debug!(job_id, "job resubmitted");
                }

                if self.state.config.is_push_staged_scheduling() {
                    // submit a scheduler tick.
                    tx_event.post_event(QueryStageSchedulerEvent::Tick);
                }
            }
            QueryStageSchedulerEvent::JobPlanningFailed {
                job_id,
                job_name,
                fail_message,
                queued_at,
                failed_at,
                error,
            } => {
                self.metrics_collector
                    .record_failed(&job_id, queued_at, failed_at);

                error!(job_id, fail_message,%error,"job planning failed");
                self.state
                    .task_manager
                    .fail_unscheduled_job(&job_id, &job_name, queued_at, error)
                    .await?;
            }
            QueryStageSchedulerEvent::JobFinished {
                job_id,
                queued_at,
                completed_at,
            } => {
                self.metrics_collector
                    .record_completed(&job_id, queued_at, completed_at);

                info!(job_id, "job_finished");

                self.state.task_manager.succeed_job(&job_id).await?;
                self.state.clean_up_successful_job(job_id);
            }
            QueryStageSchedulerEvent::JobRunningFailed {
                job_id,
                queued_at,
                failed_at,
                error,
            } => {
                self.metrics_collector
                    .record_failed(&job_id, queued_at, failed_at);

                error!(job_id, "job running failed");
                let (running_tasks, _pending_tasks) =
                    self.state.task_manager.abort_job(&job_id, error).await?;

                if !running_tasks.is_empty() {
                    tx_event
                        .post_event(QueryStageSchedulerEvent::CancelTasks(running_tasks));
                }
                self.state.clean_up_failed_job(job_id);
            }
            QueryStageSchedulerEvent::JobUpdated(job_id) => {
                info!(job_id, "job updated");
                self.state.task_manager.update_job(&job_id).await?;
            }
            QueryStageSchedulerEvent::JobCancel(job_id) => {
                self.metrics_collector.record_cancelled(&job_id);

                info!(job_id, "job cancelled");
                let (running_tasks, _pending_tasks) =
                    self.state.task_manager.cancel_job(&job_id).await?;
                self.state.clean_up_failed_job(job_id);

                tx_event.post_event(QueryStageSchedulerEvent::CancelTasks(running_tasks));
            }
            QueryStageSchedulerEvent::TaskUpdating(executor_id, tasks_status, offer) => {
                debug!(
                    executor_id,
                    "processing task status updates: {:?}", tasks_status
                );

                // if we are using push-bases scheduling and the offer flag is set then
                // offer the task slots for scheduling
                if offer {
                    // each task can consume multiple slots, so ensure here that we count each task partition
                    let total_num_tasks = tasks_status
                        .iter()
                        .map(|status| status.partitions.len())
                        .sum::<usize>();
                    let reservations = (0..total_num_tasks)
                        .map(|_| ExecutorReservation::new_free(executor_id.to_owned()))
                        .collect();

                    tx_event.post_event(QueryStageSchedulerEvent::ReservationOffering(
                        reservations,
                    ));
                } else {
                    debug!(
                        executor_id,
                        "not re-offering task slots for task status update"
                    )
                }

                let num_status = tasks_status.len();
                if let Err(e) = self
                    .state
                    .update_task_statuses(&executor_id, tasks_status, tx_event)
                    .await
                {
                    error!(
                        executor_id,
                        num_status,
                        error = %e,
                        "failed to update task status",
                    );
                    // TODO error handling
                }
            }
            QueryStageSchedulerEvent::SchedulerLost(
                scheduler_id,
                executor_id,
                task_status,
            ) => {
                if self.state.config.is_push_staged_scheduling() {
                    let num_slots = task_status
                        .into_iter()
                        .map(|status| status.partitions.len())
                        .sum::<usize>();

                    let reservations = (0..num_slots)
                        .map(|_| ExecutorReservation::new_free(executor_id.clone()))
                        .collect();
                    info!(
                        num_slots,
                        executor_id,
                        scheduler_id,
                        "returning task slots for lost scheduler"
                    );

                    // for now, just return the slots to the pool
                    tx_event.post_event(QueryStageSchedulerEvent::ReservationOffering(
                        reservations,
                    ));
                }
            }
            QueryStageSchedulerEvent::ReservationOffering(mut reservations) => {
                if self.state.config.is_push_staged_scheduling() {
                    if reservations.len() > self.tasks_per_tick {
                        tx_event.post_event(
                            QueryStageSchedulerEvent::ReservationOffering(
                                reservations.split_off(self.tasks_per_tick),
                            ),
                        );
                    }

                    self.state
                        .offer_reservation(reservations, tx_event.clone())
                        .await?;

                    let pending = self.state.task_manager.get_pending_task_count();

                    self.set_pending_tasks(pending);
                }
            }
            QueryStageSchedulerEvent::ExecutorLost(executor_id, _) => {
                self.state.task_manager.executor_lost(&executor_id).await;
            }
            QueryStageSchedulerEvent::CancelTasks(tasks) => {
                self.state.executor_manager.cancel_running_tasks(tasks);
            }
            QueryStageSchedulerEvent::JobDataClean(job_id) => {
                self.state.executor_manager.clean_up_job_data(job_id);
            }
            QueryStageSchedulerEvent::Tick => {
                let pending_tasks = self.state.task_manager.get_pending_task_count();

                self.set_pending_tasks(pending_tasks);

                if pending_tasks > 0 {
                    let num_tasks = pending_tasks.min(self.tasks_per_tick);

                    let live_executors = self
                        .state
                        .executor_manager
                        .get_alive_executors_within_one_minute();

                    self.state.reserve(
                        num_tasks as u32,
                        live_executors,
                        tx_event.clone(),
                    );
                }
            }
            QueryStageSchedulerEvent::CircuitBreakerTripped {
                job_id,
                stage_id,
                labels,
                preempt_stage,
            } => {
                let events = self
                    .state
                    .task_manager
                    .trip_circuit_breaker(job_id, stage_id, labels, preempt_stage)
                    .await?;

                for event in events {
                    tx_event.post_event(event);
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    EventAction<QueryStageSchedulerEvent> for QueryStageScheduler<T, U>
{
    fn on_start(&self) {
        info!("Starting QueryStageScheduler");
    }

    fn on_stop(&self) {
        info!("Stopping QueryStageScheduler")
    }

    async fn on_receive(
        &self,
        event: QueryStageSchedulerEvent,
        tx_event: &flume::Sender<QueryStageSchedulerEvent>,
        _rx_event: &flume::Receiver<QueryStageSchedulerEvent>,
    ) -> Result<()> {
        let tx_event = EventSender::new(tx_event.clone());

        let start = Instant::now();
        let event_type = event.event_type();

        match self.process_event(event, tx_event).await {
            Ok(_) => {
                self.metrics_collector
                    .record_process_event(event_type, start.elapsed().as_millis() as u64);
                Ok(())
            }
            Err(e) => {
                self.metrics_collector
                    .record_process_event(event_type, start.elapsed().as_millis() as u64);
                self.metrics_collector.record_event_failed(event_type);
                Err(e)
            }
        }
    }

    fn on_error(&self, error: BallistaError) {
        error!("Error received by QueryStageScheduler: {:?}", error);
    }
}

#[cfg(test)]
mod tests {
    use crate::config::SchedulerConfig;
    use crate::scheduler_server::event::QueryStageSchedulerEvent;
    use crate::test_utils::{await_condition, SchedulerTest, TestMetricsCollector};
    use ballista_core::config::TaskSchedulingPolicy;
    use ballista_core::error::Result;
    use ballista_core::event_loop::EventAction;
    use ballista_core::serde::protobuf::job_status::Status;
    use ballista_core::serde::protobuf::JobStatus;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, sum, LogicalPlan};
    use datafusion::test_util::scan_empty_with_partitions;
    use std::sync::Arc;
    use std::time::Duration;

    #[ignore]
    #[tokio::test]
    async fn test_job_resubmit() -> Result<()> {
        let plan = test_plan(10);

        let metrics_collector = Arc::new(TestMetricsCollector::default());

        // Set resubmit interval of 50ms
        let mut test = SchedulerTest::new(
            SchedulerConfig::default()
                .with_scheduler_tick_interval_ms(50)
                .with_scheduler_policy(TaskSchedulingPolicy::PushStaged),
            metrics_collector.clone(),
            0,
            0,
            None,
            false,
        )
        .await?;

        test.submit("job-id", "job-name", &plan).await?;

        let query_stage_scheduler = test.query_stage_scheduler();

        let (tx, rx) = flume::unbounded::<QueryStageSchedulerEvent>();

        let event = QueryStageSchedulerEvent::JobSubmitted {
            job_id: "job-id".to_string(),
            queued_at: 0,
            submitted_at: 0,
            resubmit: false,
        };

        query_stage_scheduler.on_receive(event, &tx, &rx).await?;

        let next_event = rx.recv_async().await.unwrap();

        println!("received {next_event:?}");

        assert!(matches!(next_event, QueryStageSchedulerEvent::Tick));

        let next_event = rx.recv_async().await.unwrap();

        println!("received {next_event:?}");

        assert!(matches!(next_event, QueryStageSchedulerEvent::Tick));

        Ok(())
    }

    #[tokio::test]
    async fn test_update_status_no_offer() -> Result<()> {
        let plan = test_plan(10);

        let metrics_collector = Arc::new(TestMetricsCollector::default());

        let mut test = SchedulerTest::new(
            SchedulerConfig::default()
                .with_scheduler_policy(TaskSchedulingPolicy::PushStaged),
            metrics_collector.clone(),
            2,
            1,
            None,
            false,
        )
        .await?;

        test.submit("job-1", "", &plan).await?;

        // First stage has 10 tasks, two of which should be scheduled immediately
        expect_pending_tasks(&test, 8).await;

        // Tick without re-offering the task slot
        test.tick_with_offer(false).await?;

        // // Since we didn't re-offer, all 9 pending tasks should still be pending
        expect_pending_tasks(&test, 8).await;

        test.tick().await?;

        // New task should be scheduled on remaining executor which re-offered reservations
        expect_pending_tasks(&test, 7).await;

        // Complete the 8 remaining tasks in the first stage
        for _ in 0..7 {
            test.tick().await?;
        }

        // The second stage should be resolved so we should have a new pending task
        expect_pending_tasks(&test, 1).await;

        // complete the final task
        test.tick().await?;

        expect_pending_tasks(&test, 0).await;

        // complete the final task by ticking twice
        test.tick().await?;
        test.tick().await?;

        // Job should be finished now
        let final_status = test.await_completion_timeout("job-1", 5_000).await?;

        assert!(matches!(
            final_status,
            JobStatus {
                status: Some(Status::Successful(_)),
                ..
            }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_pending_task_metric() -> Result<()> {
        let plan = test_plan(10);

        let metrics_collector = Arc::new(TestMetricsCollector::default());

        let mut test = SchedulerTest::new(
            SchedulerConfig::default()
                .with_scheduler_policy(TaskSchedulingPolicy::PushStaged),
            metrics_collector.clone(),
            1,
            1,
            None,
            false,
        )
        .await?;

        test.submit("job-1", "", &plan).await?;

        // First stage has 10 tasks, one of which should be scheduled immediately
        expect_pending_tasks(&test, 9).await;

        test.tick().await?;

        // First task completes and another should be scheduler, so we should have 8
        expect_pending_tasks(&test, 8).await;

        // Complete the 8 remaining tasks in the first stage
        for _ in 0..8 {
            test.tick().await?;
        }

        // The second stage should be resolved so we should have a new pending task
        expect_pending_tasks(&test, 1).await;

        // complete the final task
        test.tick().await?;

        expect_pending_tasks(&test, 0).await;

        // complete the final task by ticking twice
        test.tick().await?;

        // Job should be finished now
        let final_status = test.await_completion_timeout("job-1", 5_000).await?;

        assert!(matches!(
            final_status,
            JobStatus {
                status: Some(Status::Successful(_)),
                ..
            }
        ));

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_pending_task_metric_on_cancellation() -> Result<()> {
        let plan = test_plan(10);

        let metrics_collector = Arc::new(TestMetricsCollector::default());

        let mut test = SchedulerTest::new(
            SchedulerConfig::default()
                .with_scheduler_policy(TaskSchedulingPolicy::PushStaged),
            metrics_collector.clone(),
            1,
            1,
            None,
            false,
        )
        .await?;

        test.submit("job-1", "", &plan).await?;

        // First stage has 10 tasks, one of which should be scheduled immediately
        expect_pending_tasks(&test, 9).await;

        test.tick().await?;

        // First task completes and another should be scheduler, so we should have 8
        expect_pending_tasks(&test, 8).await;

        test.cancel("job-1").await?;

        // First task completes and another should be scheduler, so we should have 8
        expect_pending_tasks(&test, 0).await;

        Ok(())
    }

    async fn expect_pending_tasks(test: &SchedulerTest, expected: usize) {
        let success = await_condition(Duration::from_millis(500), 20, || {
            let pending_tasks = test.pending_tasks();

            futures::future::ready(Ok(pending_tasks == expected))
        })
        .await
        .unwrap();

        assert!(
            success,
            "Expected {} pending tasks but found {}",
            expected,
            test.pending_tasks()
        );
    }

    fn test_plan(partitions: usize) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        scan_empty_with_partitions(None, &schema, Some(vec![0, 1]), partitions)
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap()
    }
}
