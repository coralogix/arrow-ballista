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

use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::iter::FromIterator;
use std::sync::Arc;

use ballista_core::client::LimitedBallistaClient;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{
    accept, visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor, Partitioning,
};
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use itertools::Itertools;
use moka::future::Cache;
use object_store::ObjectStore;
use tracing::{error, info, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::{
    ShuffleReaderExecOptions, ShuffleWriterExec, UnresolvedShuffleExec,
};
use ballista_core::serde::protobuf::failed_task::FailedReason;
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{
    self, execution_graph_stage::StageType, FailedTask, JobStatus, ResultLost,
    RunningJob, SuccessfulJob, TaskStatus,
};
use ballista_core::serde::protobuf::{
    execution_error, job_status, ExecutionError, FailedJob, Metric, MetricType,
    PlanMetrics, ShuffleWritePartition, StageMetrics, SuccessfulTask,
};
use ballista_core::serde::protobuf::{task_status, RunningTask};
use ballista_core::serde::scheduler::{
    ExecutorMetadata, PartitionLocation, PartitionStats,
};
use ballista_core::serde::BallistaCodec;
use datafusion_proto::physical_plan::AsExecutionPlan;

use crate::display::print_stage_metrics;
use crate::planner::DistributedPlanner;
use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::scheduler_server::timestamp_millis;
pub(crate) use crate::state::execution_graph::execution_stage::{
    ExecutionStage, FailedStage, ResolvedStage, SuccessfulStage, TaskInfo,
    UnresolvedStage,
};
use crate::state::task_manager::UpdatedStages;

mod execution_stage;

/// Represents the DAG for a distributed query plan.
///
/// A distributed query plan consists of a set of stages which must be executed sequentially.
///
/// Each stage consists of a set of partitions which can be executed in parallel, where each partition
/// represents a `Task`, which is the basic unit of scheduling in Ballista.
///
/// As an example, consider a SQL query which performs a simple aggregation:
///
/// `SELECT id, SUM(gmv) FROM some_table GROUP BY id`
///
/// This will produce a DataFusion execution plan that looks something like
///
///
///   CoalesceBatchesExec: target_batch_size=4096
///     RepartitionExec: partitioning=Hash([Column { name: "id", index: 0 }], 4)
///       AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[SUM(some_table.gmv)]
///         TableScan: some_table
///
/// The Ballista `DistributedPlanner` will turn this into a distributed plan by creating a shuffle
/// boundary (called a "Stage") whenever the underlying plan needs to perform a repartition.
/// In this case we end up with a distributed plan with two stages:
///
///
/// ExecutionGraph[job_id=job, session_id=session, available_tasks=1, complete=false]
/// =========UnResolvedStage[id=2, children=1]=========
/// Inputs{1: StageOutput { partition_locations: {}, complete: false }}
/// ShuffleWriterExec: None
///   AggregateExec: mode=FinalPartitioned, gby=[id@0 as id], aggr=[SUM(?table?.gmv)]
///     CoalesceBatchesExec: target_batch_size=4096
///       UnresolvedShuffleExec
/// =========ResolvedStage[id=1, partitions=1]=========
/// ShuffleWriterExec: Some(Hash([Column { name: "id", index: 0 }], 4))
///   AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[SUM(?table?.gmv)]
///     TableScan: some_table
///
///
/// The DAG structure of this `ExecutionGraph` is encoded in the stages. Each stage's `input` field
/// will indicate which stages it depends on, and each stage's `output_links` will indicate which
/// stage it needs to publish its output to.
///
/// If a stage has `output_links` is empty then it is the final stage in this query, and it should
/// publish its outputs to the `ExecutionGraph`s `output_locations` representing the final query results.

#[derive(Clone)]
pub struct ExecutionGraph {
    /// Curator scheduler name. Can be `None` is `ExecutionGraph` is not currently curated by any scheduler
    scheduler_id: Option<String>,
    /// ID for this job
    job_id: String,
    /// Job name, can be empty string
    job_name: String,
    /// Session ID for this job
    session_id: String,
    /// Status of this job
    status: JobStatus,
    /// Timestamp of when this job was submitted
    queued_at: u64,
    /// Job start time
    start_time: u64,
    /// Job end time
    end_time: u64,
    /// Map from Stage ID -> ExecutionStage
    stages: HashMap<usize, ExecutionStage>,
    /// Total number fo output partitions
    output_partitions: usize,
    /// Locations of this `ExecutionGraph` final output locations
    output_locations: Vec<PartitionLocation>,
    /// Task ID generator, generate unique TID in the execution graph
    task_id_gen: usize,
    /// Failed stage attempts, record the failed stage attempts to limit the retry times.
    /// Map from Stage ID -> Set<Stage_ATTPMPT_NUM>
    failed_stage_attempts: HashMap<usize, HashSet<usize>>,
    pub(crate) circuit_breaker_tripped: bool,
    pub(crate) circuit_breaker_tripped_labels: HashSet<String>,
    pub(crate) warnings: Vec<String>,
}

#[derive(Clone)]
pub struct RunningTaskInfo {
    pub task_id: usize,
    pub job_id: String,
    pub stage_id: usize,
    pub partition_id: usize,
    pub executor_id: String,
}

impl ExecutionGraph {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        scheduler_id: &str,
        job_id: &str,
        job_name: &str,
        session_id: &str,
        plan: Arc<dyn ExecutionPlan>,
        queued_at: u64,
        object_store: Option<Arc<dyn ObjectStore>>,
        warnings: Vec<String>,
        clients: Arc<Cache<String, LimitedBallistaClient>>,
        shuffle_reader_options: Arc<ShuffleReaderExecOptions>,
    ) -> Result<Self> {
        let mut planner = DistributedPlanner::new();

        let output_partitions = plan.output_partitioning().partition_count();

        let shuffle_stages = planner.plan_query_stages(job_id, plan)?;

        let builder = match object_store {
            Some(object_store) => {
                ExecutionStageBuilder::new(object_store, clients, shuffle_reader_options)
            }
            _ => ExecutionStageBuilder {
                clients,
                shuffle_reader_options,
                current_stage_id: 0,
                stage_dependencies: HashMap::new(),
                output_links: HashMap::new(),
                object_store: None,
            },
        };
        let stages = builder.build(shuffle_stages)?;

        let started_at = timestamp_millis();

        Ok(Self {
            scheduler_id: Some(scheduler_id.to_string()),
            job_id: job_id.to_string(),
            job_name: job_name.to_string(),
            session_id: session_id.to_string(),
            status: JobStatus {
                job_id: job_id.to_string(),
                job_name: job_name.to_string(),
                status: Some(Status::Running(RunningJob {
                    queued_at,
                    started_at,
                    scheduler: scheduler_id.to_string(),
                })),
            },
            queued_at,
            start_time: started_at,
            end_time: 0,
            stages,
            output_partitions,
            output_locations: vec![],
            task_id_gen: 0,
            failed_stage_attempts: HashMap::new(),
            circuit_breaker_tripped: false,
            circuit_breaker_tripped_labels: HashSet::new(),
            warnings,
        })
    }

    pub fn next_task_id(&mut self) -> usize {
        let new_tid = self.task_id_gen;
        self.task_id_gen += 1;
        new_tid
    }

    pub fn output_locations(&self) -> &[PartitionLocation] {
        &self.output_locations
    }

    pub fn start_time(&self) -> u64 {
        self.start_time
    }

    pub fn queued_at(&self) -> u64 {
        self.queued_at
    }

    pub fn end_time(&self) -> u64 {
        self.end_time
    }

    pub fn status(&self) -> JobStatus {
        self.status.clone()
    }

    pub fn job_id(&self) -> &str {
        self.job_id.as_str()
    }

    pub fn job_name(&self) -> &str {
        self.job_name.as_str()
    }

    pub fn session_id(&self) -> &str {
        self.session_id.as_str()
    }

    pub(crate) fn calculate_stage_metrics(
        stages: &HashMap<usize, ExecutionStage>,
    ) -> Result<Vec<StageMetrics>> {
        stages
            .iter()
            .sorted_by_key(|(stage_id, _)| **stage_id)
            .map(|(stage_id, stage)| {
                if let Some(metric_set) = stage.stage_metrics().as_ref() {
                    StageMetricsBuilder::new(
                        *stage_id,
                        stage.plan(),
                        metric_set.as_slice(),
                    )
                    .build()
                } else {
                    Ok(StageMetrics {
                        stage_id: *stage_id as i64,
                        partition_id: 0,
                        plan_metrics: vec![],
                    })
                }
            })
            .collect::<Result<Vec<_>>>()
    }

    pub(crate) fn calculate_completed_stages_and_total_duration(
        stages: &HashMap<usize, ExecutionStage>,
    ) -> (usize, u64) {
        let mut completed_stages = 0;
        let mut total_task_duration_ms = 0;
        for stage in stages.values() {
            if let ExecutionStage::Successful(stage) = stage {
                completed_stages += 1;
                for task in stage.task_infos.iter().filter(|t| t.is_finished()) {
                    total_task_duration_ms += task.execution_time() as u64
                }
            }

            if let ExecutionStage::Running(stage) = stage {
                for task in stage
                    .task_infos
                    .iter()
                    .flatten()
                    .filter(|t| t.is_finished())
                {
                    total_task_duration_ms += task.execution_time() as u64
                }
            }
        }

        (completed_stages, total_task_duration_ms)
    }

    pub(crate) fn stages(&self) -> &HashMap<usize, ExecutionStage> {
        &self.stages
    }

    pub(crate) fn stage_count(&self) -> usize {
        self.stages.len()
    }

    /// An ExecutionGraph is successful if all its stages are successful
    pub fn is_successful(&self) -> bool {
        self.stages
            .values()
            .all(|s| matches!(s, ExecutionStage::Successful(_)))
    }

    pub fn is_failed(&self) -> bool {
        matches!(self.status.status.as_ref(), Some(Status::Failed(_)))
    }

    /// Revive the execution graph by converting the resolved stages to running stages
    /// If any stages are converted, return true; else false.
    pub fn revive(&mut self) -> bool {
        let running_stages = self
            .stages
            .values()
            .filter_map(|stage| {
                if let ExecutionStage::Resolved(resolved_stage) = stage {
                    Some(resolved_stage.to_running())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if !running_stages.is_empty() {
            for running_stage in running_stages {
                self.stages.insert(
                    running_stage.stage_id,
                    ExecutionStage::Running(running_stage),
                );
            }
            return true;
        }

        false
    }

    /// Update task statuses and task metrics in the graph.
    /// This will also push shuffle partitions to their respective shuffle read stages.
    pub fn update_task_status(
        &mut self,
        executor: &ExecutorMetadata,
        task_statuses: Vec<TaskStatus>,
        max_task_failures: usize,
        max_stage_failures: usize,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        self.update_task_status_internal(
            Some(executor),
            task_statuses,
            max_task_failures,
            max_stage_failures,
        )
    }

    // If executor is None,
    // we do not collect output partitions.
    // E.g. in case of circuit breaker.
    fn update_task_status_internal(
        &mut self,
        executor: Option<&ExecutorMetadata>,
        task_statuses: Vec<TaskStatus>,
        max_task_failures: usize,
        max_stage_failures: usize,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        let job_id = self.job_id().to_owned();
        // First of all, classify the statuses by stages
        let mut job_task_statuses: HashMap<usize, Vec<TaskStatus>> = HashMap::new();
        for task_status in task_statuses {
            let stage_id = task_status.stage_id as usize;
            let stage_task_statuses = job_task_statuses.entry(stage_id).or_default();
            stage_task_statuses.push(task_status);
        }

        // Revive before updating due to some updates not saved
        // It will be refined later
        self.revive();

        let current_running_stages: HashSet<usize> =
            HashSet::from_iter(self.running_stages());

        // Copy the failed stage attempts from self
        let mut failed_stage_attempts_tmp: HashMap<usize, HashSet<usize>> =
            HashMap::new();
        for (stage_id, attempts) in self.failed_stage_attempts.iter() {
            failed_stage_attempts_tmp
                .insert(*stage_id, HashSet::from_iter(attempts.iter().copied()));
        }

        let mut resolved_stages = HashSet::new();
        let mut successful_stages = HashSet::new();
        let mut failed_stages: HashMap<usize, Arc<execution_error::Error>> =
            HashMap::new();
        let mut rollback_running_stages = HashMap::new();
        let mut resubmit_successful_stages: HashMap<usize, HashSet<usize>> =
            HashMap::new();
        let mut reset_running_stages: HashMap<usize, HashSet<usize>> = HashMap::new();

        for (stage_id, stage_task_statuses) in job_task_statuses {
            if let Some(stage) = self.stages.get_mut(&stage_id) {
                if let ExecutionStage::Running(running_stage) = stage {
                    let mut locations = vec![];
                    for task_status in stage_task_statuses.into_iter() {
                        {
                            let task_stage_attempt_num =
                                task_status.stage_attempt_num as usize;
                            if task_stage_attempt_num < running_stage.stage_attempt_num {
                                warn!(
                                            job_id, task_id = task_status.task_id, task_stage_attempt_num, stage_id, stage_attempt = running_stage.stage_attempt_num,
                                            "ignoring task status update, more recent stage attempt running",
                                        );
                                continue;
                            }
                            let partitions = &task_status.partitions;
                            let task_identity = format!(
                                "TID {} {}/{}.{}/{:?}",
                                task_status.task_id,
                                job_id,
                                stage_id,
                                task_stage_attempt_num,
                                partitions
                            );
                            let operator_metrics = task_status.metrics.clone();

                            if !running_stage.update_task_info(&task_status) {
                                continue;
                            }

                            if let Some(task_status::Status::Failed(failed_task)) =
                                task_status.status
                            {
                                match failed_task.failed_reason.as_ref() {
                                    Some(FailedReason::FetchPartitionError(
                                        fetch_partiton_error,
                                    )) => {
                                        let failed_attempts = failed_stage_attempts_tmp
                                            .entry(stage_id)
                                            .or_default();
                                        failed_attempts.insert(task_stage_attempt_num);
                                        if failed_attempts.len() < max_stage_failures {
                                            let map_stage_id = fetch_partiton_error
                                                .map_stage_id
                                                as usize;

                                            let executor_id =
                                                fetch_partiton_error.executor_id.clone();

                                            if !failed_stages.is_empty() {
                                                warn!(job_id, task_identity, "ignoring fetch partition error, stage was failed");
                                            } else {
                                                // There are different removal strategies here.
                                                // We can choose just remove the map_partition_id in the FetchPartitionError, when resubmit the input stage, there are less tasks
                                                // need to rerun, but this might miss many more bad input partitions, lead to more stage level retries in following.
                                                // Here we choose remove all the bad input partitions which match the same executor id in this single input stage.
                                                // There are other more aggressive approaches, like considering the executor is lost and check all the running stages in this graph.
                                                // Or count the fetch failure number on executor and mark the executor lost globally.
                                                let removed_map_partitions =
                                                    running_stage
                                                        .remove_input_partitions(
                                                            map_stage_id,
                                                            &executor_id,
                                                        )?;

                                                let failure_reasons: &mut HashSet<
                                                    String,
                                                > = rollback_running_stages
                                                    .entry(stage_id)
                                                    .or_default();
                                                failure_reasons.insert(executor_id);

                                                let missing_inputs =
                                                    resubmit_successful_stages
                                                        .entry(map_stage_id)
                                                        .or_default();
                                                missing_inputs
                                                    .extend(removed_map_partitions);
                                                warn!(job_id = self.job_id, stage_id, map_stage_id, task_identity, "resubmitting current running stage and parent stage, error fetching partition");
                                            }
                                        } else {
                                            let error_msg = format!(
                                                "Stage {} has failed {} times, \
                                                    most recent failure reason: {:?}",
                                                stage_id,
                                                max_stage_failures,
                                                failed_task.failed_reason
                                            );
                                            error!(job_id = self.job_id, max_stage_failures, reason = ?failed_task.failed_reason, "stage failed");
                                            failed_stages.insert(
                                                stage_id,
                                                Arc::new(
                                                    execution_error::Error::Internal(
                                                        execution_error::Internal {
                                                            message: error_msg,
                                                        },
                                                    ),
                                                ),
                                            );
                                        }
                                    }
                                    Some(FailedReason::ExecutionError(
                                        ExecutionError { error },
                                    )) => {
                                        if let Some(err) = error {
                                            failed_stages
                                                .insert(stage_id, Arc::new(err.clone()));
                                        } else {
                                            failed_stages.insert(
                                                stage_id,
                                                Arc::new(
                                                    execution_error::Error::Internal(
                                                        execution_error::Internal {
                                                            message:
                                                                "Unknown execution error"
                                                                    .to_string(),
                                                        },
                                                    ),
                                                ),
                                            );
                                        }
                                    }
                                    Some(_) => {
                                        if failed_task.retryable
                                            && failed_task.count_to_failures
                                        {
                                            if running_stage.task_failures
                                                < max_task_failures
                                            {
                                                // The failure TaskInfo is ignored and set to None here
                                                running_stage.reset_task_info(
                                                    partitions
                                                        .iter()
                                                        .map(|p| *p as usize),
                                                );
                                                running_stage.task_failures += 1;
                                            } else {
                                                let error_msg = format!(
                                                            "Stage {} had {} task failures, fail the stage, most recent failure reason: {:?}",
                                                            stage_id, max_task_failures, failed_task.failed_reason
                                                        );
                                                error!(job_id, stage_id, max_task_failures, reason = ?failed_task.failed_reason, "stage failed, max task failures exceeded");
                                                failed_stages.insert(
                                                    stage_id,
                                                    Arc::new(
                                                        execution_error::Error::Internal(
                                                            execution_error::Internal {
                                                                message: error_msg,
                                                            },
                                                        ),
                                                    ),
                                                );
                                            }
                                        } else if failed_task.retryable {
                                            // TODO add new struct to track all the failed task infos
                                            // The failure TaskInfo is ignored and set to None here
                                            running_stage.reset_task_info(
                                                partitions.iter().map(|p| *p as usize),
                                            );
                                        }
                                    }
                                    None => {
                                        let error_msg = format!(
                                                    "Task {partitions:?} in Stage {stage_id} failed with unknown failure reasons, fail the stage");
                                        error!(?partitions, stage_id, job_id, "task failed for unknown reason, failing stage");
                                        failed_stages.insert(
                                            stage_id,
                                            Arc::new(execution_error::Error::Internal(
                                                execution_error::Internal {
                                                    message: error_msg,
                                                },
                                            )),
                                        );
                                    }
                                }
                            } else if let Some(task_status::Status::Successful(
                                successful_task,
                            )) = task_status.status
                            {
                                // update task metrics for successful task
                                if let Err(err) =
                                    running_stage.update_task_metrics(operator_metrics)
                                {
                                    warn!(job_id, stage_id = running_stage.stage_id, error = %err, "error updating task metrics");
                                }

                                if let Some(executor) = executor {
                                    locations.append(&mut partition_to_location(
                                        &job_id,
                                        partitions.iter().map(|p| *p as usize).collect(),
                                        stage_id,
                                        executor,
                                        successful_task.partitions,
                                    ))
                                };
                            } else {
                                warn!(
                                    task_identity,
                                    job_id, "task status is invalid for updating",
                                );
                            }
                        }
                    }
                    let is_final_successful = running_stage.is_successful()
                        && !reset_running_stages.contains_key(&stage_id);
                    if is_final_successful {
                        successful_stages.insert(stage_id);
                        // if this stage is final successful, we want to combine the stage metrics to plan's metric set and print out the plan
                        if let Some(stage_metrics) = running_stage.stage_metrics.as_ref()
                        {
                            print_stage_metrics(
                                self.job_id.as_str(),
                                stage_id,
                                running_stage.plan.as_ref(),
                                stage_metrics,
                            );
                        }
                    }

                    let output_links = running_stage.output_links.clone();
                    resolved_stages.extend(
                        ExecutionGraph::update_stage_output_links(
                            self.job_id.as_str(),
                            stage_id,
                            is_final_successful,
                            locations,
                            output_links,
                            &mut self.stages,
                            &mut self.output_locations,
                        )?
                        .into_iter(),
                    );
                } else if let ExecutionStage::UnResolved(unsolved_stage) = stage {
                    for task_status in stage_task_statuses.into_iter() {
                        let task_stage_attempt_num =
                            task_status.stage_attempt_num as usize;
                        let partitions = &task_status.partitions;
                        let task_identity = format!(
                            "TID {} {}/{}.{}/{:?}",
                            task_status.task_id,
                            job_id,
                            stage_id,
                            task_stage_attempt_num,
                            partitions
                        );
                        let mut should_ignore = true;
                        // handle delayed failed tasks if the stage's next attempt is still in UnResolved status.
                        if let Some(task_status::Status::Failed(failed_task)) =
                            task_status.status
                        {
                            if unsolved_stage.stage_attempt_num - task_stage_attempt_num
                                == 1
                            {
                                let failed_reason = failed_task.failed_reason;
                                match failed_reason {
                                    Some(FailedReason::ExecutionError(
                                        ExecutionError { error },
                                    )) => {
                                        should_ignore = false;

                                        if let Some(err) = error {
                                            failed_stages.insert(stage_id, Arc::new(err));
                                        } else {
                                            failed_stages.insert(
                                                stage_id,
                                                Arc::new(
                                                    execution_error::Error::Internal(
                                                        execution_error::Internal {
                                                            message:
                                                                "Unknown execution error"
                                                                    .to_string(),
                                                        },
                                                    ),
                                                ),
                                            );
                                        }
                                    }
                                    Some(FailedReason::FetchPartitionError(
                                        fetch_partiton_error,
                                    )) if failed_stages.is_empty()
                                        && current_running_stages.contains(
                                            &(fetch_partiton_error.map_stage_id as usize),
                                        )
                                        && !unsolved_stage
                                            .last_attempt_failure_reasons
                                            .contains(
                                                &fetch_partiton_error.executor_id,
                                            ) =>
                                    {
                                        should_ignore = false;
                                        unsolved_stage
                                            .last_attempt_failure_reasons
                                            .insert(
                                                fetch_partiton_error.executor_id.clone(),
                                            );
                                        let map_stage_id =
                                            fetch_partiton_error.map_stage_id as usize;
                                        let executor_id =
                                            fetch_partiton_error.executor_id;
                                        let removed_map_partitions = unsolved_stage
                                            .remove_input_partitions(
                                                map_stage_id,
                                                &executor_id,
                                            )?;

                                        let missing_inputs = reset_running_stages
                                            .entry(map_stage_id)
                                            .or_default();
                                        missing_inputs.extend(removed_map_partitions);
                                        warn!(job_id, map_stage_id, stage_id, task_identity, "resetting running stage, error fetching partition from parent stage");

                                        // If the previous other task updates had already mark the map stage success, need to remove it.
                                        if successful_stages.contains(&map_stage_id) {
                                            successful_stages.remove(&map_stage_id);
                                        }
                                        if resolved_stages.contains(&stage_id) {
                                            resolved_stages.remove(&stage_id);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        if should_ignore {
                            warn!(task_identity, job_id, stage_id, "ignoring task status updates on stage, stage is unresolved");
                        }
                    }
                } else {
                    warn!(
                        job_id,
                        stage_id,
                        partitions = ?stage_task_statuses.into_iter().map(|task_status| task_status.partitions).collect::<Vec<_>>(),
                        "cannot update tasks statuses on stage, stage is not running",
                    );
                }
            } else {
                return Err(BallistaError::Internal(format!(
                    "Invalid stage ID {stage_id} for job {}",
                    job_id
                )));
            }
        }

        // Update failed stage attempts back to self
        for (stage_id, attempts) in failed_stage_attempts_tmp.iter() {
            self.failed_stage_attempts
                .insert(*stage_id, HashSet::from_iter(attempts.iter().copied()));
        }

        for (stage_id, missing_parts) in &resubmit_successful_stages {
            if let Some(stage) = self.stages.get_mut(stage_id) {
                if let ExecutionStage::Successful(success_stage) = stage {
                    for partition in missing_parts {
                        if *partition > success_stage.partitions {
                            return Err(BallistaError::Internal(format!(
                                "Invalid partition ID {} in map stage {}",
                                *partition, stage_id
                            )));
                        }
                        let task_info = &mut success_stage.task_infos[*partition];
                        // Update the task info to failed
                        task_info.task_status = task_status::Status::Failed(FailedTask {
                            retryable: true,
                            count_to_failures: false,
                            failed_reason: Some(FailedReason::ResultLost(ResultLost {
                                message: "FetchPartitionError in parent stage".to_owned(),
                            })),
                        });
                    }
                } else {
                    warn!(
                        job_id,
                        stage_id, "cannot resubmit stage, stage is not successful"
                    );
                }
            } else {
                return Err(BallistaError::Internal(format!(
                    "Invalid stage ID {stage_id} for job {job_id}",
                )));
            }
        }

        for (stage_id, missing_parts) in &reset_running_stages {
            if let Some(stage) = self.stages.get_mut(stage_id) {
                if let ExecutionStage::Running(running_stage) = stage {
                    for partition in missing_parts {
                        if *partition > running_stage.partitions {
                            return Err(BallistaError::Internal(format!(
                                "Invalid partition ID {} in map stage {}",
                                *partition, stage_id
                            )));
                        }
                    }

                    running_stage.reset_task_info(missing_parts.iter().copied());
                } else {
                    warn!(
                        job_id,
                        stage_id,
                        "cannot reset running tasks on stage, stage is not running"
                    );
                }
            } else {
                return Err(BallistaError::Internal(format!(
                    "Invalid stage ID {stage_id} for job {job_id}",
                )));
            }
        }

        self.processing_stages_update(UpdatedStages {
            resolved_stages,
            successful_stages,
            failed_stages,
            rollback_running_stages,
            resubmit_successful_stages: resubmit_successful_stages
                .keys()
                .cloned()
                .collect(),
        })
    }

    /// Processing stage status update after task status changing
    fn processing_stages_update(
        &mut self,
        updated_stages: UpdatedStages,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        let job_id = self.job_id.to_owned();
        let mut has_resolved = false;

        for stage_id in updated_stages.resolved_stages {
            self.resolve_stage(stage_id)?;
            has_resolved = true;
        }

        for stage_id in updated_stages.successful_stages {
            self.succeed_stage(stage_id);
        }

        let mut events = vec![];
        // Only handle the rollback logic when there are no failed stages
        if updated_stages.failed_stages.is_empty() {
            let mut running_tasks_to_cancel = vec![];
            for (stage_id, failure_reasons) in updated_stages.rollback_running_stages {
                let tasks = self.rollback_running_stage(stage_id, failure_reasons)?;
                running_tasks_to_cancel.extend(tasks);
            }

            for stage_id in updated_stages.resubmit_successful_stages {
                self.rerun_successful_stage(stage_id);
            }

            if !running_tasks_to_cancel.is_empty() {
                events.push(QueryStageSchedulerEvent::CancelTasks(
                    running_tasks_to_cancel,
                ));
            }
        }

        if !updated_stages.failed_stages.is_empty() {
            info!(job_id, "job failed");
            let error = match updated_stages.failed_stages.iter().last() {
                Some((stage_id, err)) => {
                    info!(
                        job_id, stage_id, error = %err,
                        "job failed with failed stage"
                    );
                    err.clone()
                }
                _ => {
                    info!(job_id, "job failed with unknown error");
                    Arc::new(execution_error::Error::Internal(
                        execution_error::Internal {
                            message: "Unknown error".to_string(),
                        },
                    ))
                }
            };
            self.fail_job(error.clone());
            events.push(QueryStageSchedulerEvent::JobRunningFailed {
                job_id,
                queued_at: self.queued_at,
                failed_at: timestamp_millis(),
                error,
            });
        } else if self.is_successful() {
            // If this ExecutionGraph is successful, finish it
            info!(job_id, "job successful, finalizing output partitions");
            self.succeed_job()?;
            events.push(QueryStageSchedulerEvent::JobFinished {
                job_id,
                queued_at: self.queued_at,
                completed_at: timestamp_millis(),
            });
        } else if has_resolved {
            events.push(QueryStageSchedulerEvent::JobUpdated(job_id))
        }
        Ok(events)
    }

    /// Return a Vec of resolvable stage ids
    fn update_stage_output_links(
        job_id: &str,
        stage_id: usize,
        is_completed: bool,
        locations: Vec<PartitionLocation>,
        output_links: Vec<usize>,
        stages: &mut HashMap<usize, ExecutionStage>,
        output_locations: &mut Vec<PartitionLocation>,
    ) -> Result<Vec<usize>> {
        let mut resolved_stages = vec![];

        if output_links.is_empty() {
            // If `output_links` is empty, then this is a final stage
            output_locations.extend(locations);
        } else {
            for link in output_links.iter() {
                // If this is an intermediate stage, we need to push its `PartitionLocation`s to the parent stage
                if let Some(linked_stage) = stages.get_mut(link) {
                    if let ExecutionStage::UnResolved(linked_unresolved_stage) =
                        linked_stage
                    {
                        linked_unresolved_stage
                            .add_input_partitions(stage_id, locations.clone())?;

                        // If all tasks for this stage are complete, mark the input complete in the parent stage
                        if is_completed {
                            linked_unresolved_stage.complete_input(stage_id);
                        }

                        // If all input partitions are ready, we can resolve any UnresolvedShuffleExec in the parent stage plan
                        if linked_unresolved_stage.resolvable() {
                            resolved_stages.push(linked_unresolved_stage.stage_id);
                        }
                    } else {
                        return Err(BallistaError::Internal(format!(
                        "Error updating job {job_id}: The stage {link} as the output link of stage {stage_id}  should be unresolved"
                    )));
                    }
                } else {
                    return Err(BallistaError::Internal(format!(
                    "Error updating job {job_id}: Invalid output link {stage_id} for stage {link}"
                )));
                }
            }
        }

        Ok(resolved_stages)
    }

    /// Return all the currently running stage ids
    pub fn running_stages(&self) -> Vec<usize> {
        self.stages
            .iter()
            .filter_map(|(stage_id, stage)| {
                if let ExecutionStage::Running(_running) = stage {
                    Some(*stage_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    // Return all currently running tasks along with the executor ID on which they are assigned
    pub fn running_tasks(&self) -> Vec<RunningTaskInfo> {
        let mut tasks = Vec::default();

        for (_, stage) in self.stages().iter() {
            if let ExecutionStage::Running(stage) = stage {
                tasks.extend(stage.running_tasks().into_iter().map(
                    |(task_id, stage_id, partition_id, executor_id)| RunningTaskInfo {
                        task_id,
                        job_id: self.job_id.clone(),
                        stage_id,
                        partition_id,
                        executor_id,
                    },
                ))
            }
        }

        tasks.shrink_to_fit();
        tasks
    }

    // Total number of tasks in this plan that are ready for scheduling
    pub fn available_tasks(&self) -> usize {
        self.stages
            .values()
            .map(|stage| {
                if let ExecutionStage::Running(stage) = stage {
                    stage.available_tasks()
                } else {
                    0
                }
            })
            .sum()
    }

    /// Get next task that can be assigned to the given executor.
    /// This method should only be called when the resulting task is immediately
    /// being launched as the status will be set to Running and it will not be
    /// available to the scheduler.
    /// If the task is not launched the status must be reset to allow the task to
    /// be scheduled elsewhere.
    pub fn pop_next_task(
        &mut self,
        executor_id: &str,
        num_tasks: usize,
    ) -> Result<Option<TaskDescription>> {
        if matches!(
            self.status,
            JobStatus {
                status: Some(job_status::Status::Failed(_)),
                ..
            }
        ) {
            warn!(
                job_id = self.job_id,
                executor_id, "Call pop_next_task on failed Job"
            );
            return Ok(None);
        }

        let find_candidate = self.stages.iter().any(|(_stage_id, stage)| {
            if let ExecutionStage::Running(stage) = stage {
                stage.available_tasks() > 0
            } else {
                false
            }
        });

        if find_candidate {
            let task_id = self.next_task_id();
            let next_task = self.stages.iter_mut().find(|(_stage_id, stage)| {
                    if let ExecutionStage::Running(stage) = stage {
                        stage.available_tasks() > 0
                    } else {
                        false
                    }
                }).map(|(stage_id, stage)| {
                    if let ExecutionStage::Running(stage) = stage {
let mut partitions = vec![];
                        let task_info = TaskInfo {
                            task_id,
                            scheduled_time: timestamp_millis() as u128,
                            // Those times will be updated when the task finish
                            launch_time: 0,
                            start_exec_time: 0,
                            end_exec_time: 0,
                            finish_time: 0,
                            task_status: task_status::Status::Running(RunningTask {
                                executor_id: executor_id.to_owned()
                            }),
                        };
for (partition, status) in stage.task_infos
                            .iter_mut()
                            .enumerate()
                            .filter(|(_,status)| status.is_none())
                            .take(num_tasks) {
                            *status = Some(task_info.clone());
                            partitions.push(partition);
                        }
                        if partitions.is_empty() {
                            return Err(BallistaError::Internal(format!("Error getting next task for job {}: Stage {stage_id} is ready but has no pending tasks", self.job_id)));
                        }
                        Ok(TaskDescription {
                            session_id: self.session_id.clone(),
                            partitions: TaskPartitions {
                                job_id: self.job_id.clone(),
                                stage_id: *stage_id,
                                partitions
                            },
                            stage_attempt_num: stage.stage_attempt_num,
                            task_id,
                            plan: stage.plan.clone(),
                            output_partitioning: stage.output_partitioning.clone(),
                            resolved_at: stage.resolved_at,
                        })
                    } else {
                        Err(BallistaError::General(format!("Stage {stage_id} is not a running stage")))
                    }
                }).transpose()?;

            // If no available tasks found in the running stage,
            // try to find a resolved stage and convert it to the running stage
            if next_task.is_none() {
                if self.revive() {
                    return self.pop_next_task(executor_id, num_tasks);
                }
            } else {
                return Ok(next_task);
            }
        } else if self.revive() {
            return self.pop_next_task(executor_id, num_tasks);
        }

        Ok(None)
    }

    pub fn update_status(&mut self, new_status: JobStatus) {
        self.status = new_status
    }

    /// Reset running and successful stages on a given executor
    /// This will first check the unresolved/resolved/running stages and reset the running tasks and successful tasks.
    /// Then it will check the successful stage and whether there are running parent stages need to read shuffle from it.
    /// If yes, reset the successful tasks and roll back the resolved shuffle recursively.
    ///
    /// Returns the reset stage ids and running tasks should be killed
    pub fn reset_stages_on_lost_executor(&mut self, executor_id: &str) {
        warn!(
            job_id = self.job_id,
            executor_id, "resetting stages for lost executor"
        );

        self.stages.iter_mut().for_each(|(stage_id, stage)| {
            if let ExecutionStage::Running(stage) = stage {
                let reset = stage.reset_tasks(executor_id);
                if reset > 0 {
                    warn!(
                        num_tasks = reset,
                        job_id = self.job_id,
                        stage_id,
                        executor_id,
                        "resetting running tasks for lost executor"
                    );
                }
            }
        })
    }

    /// Convert unresolved stage to be resolved
    pub fn resolve_stage(&mut self, stage_id: usize) -> Result<()> {
        if let Some(ExecutionStage::UnResolved(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Resolved(stage.to_resolved()?));
        } else {
            warn!(
                job_id = self.job_id,
                stage_id, "failed to find unresolved stage to resolve",
            );
        }

        Ok(())
    }

    /// Convert running stage to be successful
    pub fn succeed_stage(&mut self, stage_id: usize) {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Successful(stage.to_successful()));
            self.clear_stage_failure(stage_id);
        } else {
            warn!(
                job_id = self.job_id,
                stage_id, "failed to succeed stage, stage is not running",
            )
        }
    }

    /// Convert running stage to be failed
    pub fn fail_stage(&mut self, stage_id: usize, err_msg: String) {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Failed(stage.to_failed(err_msg)));
        } else {
            info!(
                job_id = self.job_id,
                stage_id, "failed to fail stage, stage is not running"
            )
        }
    }

    /// Convert running stage to be unresolved,
    /// Returns a Vec of RunningTaskInfo for running tasks in this stage.
    pub fn rollback_running_stage(
        &mut self,
        stage_id: usize,
        failure_reasons: HashSet<String>,
    ) -> Result<Vec<RunningTaskInfo>> {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            let running_tasks = stage
                .running_tasks()
                .into_iter()
                .map(
                    |(task_id, stage_id, partition_id, executor_id)| RunningTaskInfo {
                        task_id,
                        job_id: self.job_id.clone(),
                        stage_id,
                        partition_id,
                        executor_id,
                    },
                )
                .collect();
            self.stages.insert(
                stage_id,
                ExecutionStage::UnResolved(stage.to_unresolved(failure_reasons)?),
            );
            return Ok(running_tasks);
        } else {
            info!(
                job_id = self.job_id,
                stage_id, "failed to rollback running stage, stage is not running",
            );
        }

        Ok(Vec::default())
    }

    /// Convert resolved stage to be unresolved
    pub fn rollback_resolved_stage(&mut self, stage_id: usize) -> Result<()> {
        if let Some(ExecutionStage::Resolved(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::UnResolved(stage.to_unresolved()?));
        } else {
            info!(
                job_id = self.job_id,
                stage_id, "failed to rollback resolved stage, stage is not resolved",
            );
        }

        Ok(())
    }

    /// Convert successful stage to be running
    pub fn rerun_successful_stage(&mut self, stage_id: usize) {
        if let Some(ExecutionStage::Successful(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Running(stage.to_running()));
        } else {
            info!(
                job_id = self.job_id,
                stage_id, "failed to re-run successful stage, stage is not successful",
            )
        }
    }

    /// fail job with error message
    pub fn fail_job(&mut self, reason: Arc<execution_error::Error>) {
        self.end_time = timestamp_millis();
        self.status = JobStatus {
            job_id: self.job_id.clone(),
            job_name: self.job_name.clone(),
            status: Some(Status::Failed(FailedJob {
                error: Some(ExecutionError {
                    error: Some(reason.as_ref().clone()),
                }),
                queued_at: self.queued_at,
                started_at: self.start_time,
                ended_at: self.end_time,
            })),
        };
    }

    /// Mark the job success
    pub fn succeed_job(&mut self) -> Result<()> {
        if !self
            .stages
            .values()
            .all(|s| matches!(s, ExecutionStage::Successful(_)))
        {
            return Err(BallistaError::Internal(format!(
                "Attempt to finalize an incomplete job {}",
                self.job_id
            )));
        }

        let partition_location = self
            .output_locations()
            .iter()
            .map(|l| l.try_into())
            .collect::<Result<Vec<_>>>()?;
        let (completed_stages, total_task_duration_ms) =
            ExecutionGraph::calculate_completed_stages_and_total_duration(&self.stages);
        let stage_metrics = ExecutionGraph::calculate_stage_metrics(&self.stages)?;
        self.end_time = timestamp_millis();
        self.status = JobStatus {
            job_id: self.job_id.clone(),
            job_name: self.job_name.clone(),
            status: Some(job_status::Status::Successful(SuccessfulJob {
                partition_location,
                queued_at: self.queued_at,
                started_at: self.start_time,
                ended_at: self.end_time,
                circuit_breaker_tripped: self.circuit_breaker_tripped,
                circuit_breaker_tripped_labels: self
                    .circuit_breaker_tripped_labels
                    .iter()
                    .cloned()
                    .collect(),
                warnings: self.warnings.clone(),
                stage_count: self.stages.len() as u32,
                total_task_duration_ms,
                completed_stages: completed_stages as u64,
                stage_metrics,
            })),
        };

        Ok(())
    }

    /// Clear the stage failure count for this stage if the stage is finally success
    fn clear_stage_failure(&mut self, stage_id: usize) {
        self.failed_stage_attempts.remove(&stage_id);
    }

    pub(crate) async fn decode_execution_graph<
        T: 'static + AsLogicalPlan,
        U: 'static + AsExecutionPlan,
    >(
        proto: protobuf::ExecutionGraph,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
        object_store: Option<Arc<dyn ObjectStore>>,
        clients: Arc<Cache<String, LimitedBallistaClient>>,
    ) -> Result<ExecutionGraph> {
        let status = proto.status.ok_or_else(|| {
            BallistaError::Internal("Invalid Execution Graph".to_owned())
        })?;
        let mut stages: HashMap<usize, ExecutionStage> = HashMap::new();
        for graph_stage in proto.stages {
            let stage_type = graph_stage.stage_type.expect("Unexpected empty stage");

            let execution_stage = match stage_type {
                StageType::UnresolvedStage(stage) => {
                    let stage: UnresolvedStage = UnresolvedStage::decode(
                        stage,
                        codec,
                        session_ctx,
                        object_store.clone(),
                        clients.clone(),
                    )?;
                    (stage.stage_id, ExecutionStage::UnResolved(stage))
                }
                StageType::ResolvedStage(stage) => {
                    let stage: ResolvedStage = ResolvedStage::decode(
                        stage,
                        codec,
                        session_ctx,
                        object_store.clone(),
                        clients.clone(),
                    )?;
                    (stage.stage_id, ExecutionStage::Resolved(stage))
                }
                StageType::SuccessfulStage(stage) => {
                    let stage: SuccessfulStage = SuccessfulStage::decode(
                        stage,
                        codec,
                        session_ctx,
                        object_store.clone(),
                        clients.clone(),
                    )?;
                    (stage.stage_id, ExecutionStage::Successful(stage))
                }
                StageType::FailedStage(stage) => {
                    let stage: FailedStage =
                        FailedStage::decode(stage, codec, session_ctx)?;
                    (stage.stage_id, ExecutionStage::Failed(stage))
                }
            };

            stages.insert(execution_stage.0, execution_stage.1);
        }

        let output_locations: Vec<PartitionLocation> = proto
            .output_locations
            .iter()
            .map(|loc| loc.try_into())
            .collect::<Result<Vec<_>>>()?;

        let failed_stage_attempts = proto
            .failed_attempts
            .into_iter()
            .map(|attempt| {
                (
                    attempt.stage_id as usize,
                    HashSet::from_iter(
                        attempt
                            .stage_attempt_num
                            .into_iter()
                            .map(|num| num as usize),
                    ),
                )
            })
            .collect();

        Ok(ExecutionGraph {
            scheduler_id: (!proto.scheduler_id.is_empty()).then_some(proto.scheduler_id),
            job_id: proto.job_id,
            job_name: proto.job_name,
            session_id: proto.session_id,
            status,
            queued_at: proto.queued_at,
            start_time: proto.start_time,
            end_time: proto.end_time,
            stages,
            output_partitions: proto.output_partitions as usize,
            output_locations,
            task_id_gen: proto.task_id_gen as usize,
            failed_stage_attempts,
            circuit_breaker_tripped: proto.circuit_breaker_tripped,
            circuit_breaker_tripped_labels: HashSet::from_iter(
                proto.circuit_breaker_tripped_labels,
            ),
            warnings: proto.warnings,
        })
    }

    /// Running stages will not be persisted so that will not be encoded.
    /// Running stages will be convert back to the resolved stages to be encoded and persisted
    pub(crate) fn encode_execution_graph<
        T: 'static + AsLogicalPlan,
        U: 'static + AsExecutionPlan,
    >(
        graph: ExecutionGraph,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::ExecutionGraph> {
        let stages = graph
            .stages()
            .values()
            .map(|stage| {
                let stage_type = match stage {
                    ExecutionStage::UnResolved(stage) => {
                        StageType::UnresolvedStage(UnresolvedStage::encode(stage, codec)?)
                    }
                    ExecutionStage::Resolved(stage) => {
                        StageType::ResolvedStage(ResolvedStage::encode(stage, codec)?)
                    }
                    ExecutionStage::Running(stage) => StageType::ResolvedStage(
                        ResolvedStage::encode(&stage.to_resolved(), codec)?,
                    ),
                    ExecutionStage::Successful(stage) => {
                        StageType::SuccessfulStage(SuccessfulStage::encode(stage, codec)?)
                    }
                    ExecutionStage::Failed(stage) => {
                        StageType::FailedStage(FailedStage::encode(stage, codec)?)
                    }
                };
                Ok(protobuf::ExecutionGraphStage {
                    stage_type: Some(stage_type),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let output_locations: Vec<protobuf::PartitionLocation> = graph
            .output_locations()
            .iter()
            .map(|loc| loc.try_into())
            .collect::<Result<Vec<_>>>()?;

        let failed_attempts: Vec<protobuf::StageAttempts> = graph
            .failed_stage_attempts
            .into_iter()
            .map(|(stage_id, attempts)| {
                let stage_attempt_num = attempts
                    .into_iter()
                    .map(|num| num as u32)
                    .collect::<Vec<_>>();
                protobuf::StageAttempts {
                    stage_id: stage_id as u32,
                    stage_attempt_num,
                }
            })
            .collect::<Vec<_>>();

        Ok(protobuf::ExecutionGraph {
            job_id: graph.job_id,
            job_name: graph.job_name,
            session_id: graph.session_id,
            status: Some(graph.status),
            queued_at: graph.queued_at,
            start_time: graph.start_time,
            end_time: graph.end_time,
            output_partitions: graph.output_partitions as u64,
            output_locations,
            scheduler_id: graph.scheduler_id.unwrap_or_default(),
            task_id_gen: graph.task_id_gen as u32,
            failed_attempts,
            circuit_breaker_tripped: graph.circuit_breaker_tripped,
            circuit_breaker_tripped_labels: graph
                .circuit_breaker_tripped_labels
                .iter()
                .cloned()
                .collect(),
            warnings: graph.warnings,
            stages,
        })
    }

    pub fn trip_stage(
        &mut self,
        stage_id: usize,
        labels: Vec<String>,
        preempt: bool,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        self.circuit_breaker_tripped = true;
        self.circuit_breaker_tripped_labels.extend(labels);

        if !preempt {
            info!(
                job_id = self.job_id,
                stage_id, "Will not preempt running tasks"
            );
            return Ok(vec![]);
        }

        let task_id = self.next_task_id();
        let stage = if let Some(stage) = self.stages.get_mut(&stage_id) {
            stage
        } else {
            return Ok(vec![]);
        };

        let running_stage = if let ExecutionStage::Running(stage) = stage {
            stage
        } else {
            return Ok(vec![]);
        };

        let current_time = timestamp_millis();

        let mut num_stopped = 0;
        let mut partitions = vec![];

        for i in 0..running_stage.task_infos.len() {
            if running_stage.task_infos[i].is_none() {
                num_stopped += 1;
                partitions.push(i as u32);
                // Set the task info to running only to have it immediately set to successful
                // by the update_task_status_internal method below.
                // If we don't do it this way update_task_status_internal will panic.
                running_stage.task_infos[i] = Some(TaskInfo {
                    task_id,
                    scheduled_time: current_time as u128,
                    launch_time: current_time as u128,
                    start_exec_time: current_time as u128,
                    end_exec_time: current_time as u128,
                    finish_time: current_time as u128,
                    task_status: task_status::Status::Running(RunningTask {
                        executor_id: "<circuit-breaker>".to_owned(),
                    }),
                });
            }
        }

        info!(
            job_id = self.job_id,
            stage_id,
            "Preempted {} tasks due to tripped circuit breaker (labels: {:?})",
            num_stopped,
            self.circuit_breaker_tripped_labels,
        );

        let task_status = TaskStatus {
            task_id: task_id as u32,
            job_id: self.job_id.clone(),
            stage_id: running_stage.stage_id as u32,
            stage_attempt_num: running_stage.stage_attempt_num as u32,
            partitions,
            launch_time: current_time,
            start_exec_time: current_time,
            end_exec_time: current_time,
            metrics: vec![],
            status: Some(task_status::Status::Successful(SuccessfulTask {
                executor_id: "<circuit-breaker>".to_owned(),
                partitions: vec![],
            })),
        };

        self.update_task_status_internal(None, vec![task_status], 4, 4)
    }
}

impl Debug for ExecutionGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let stages = self
            .stages
            .values()
            .map(|stage| format!("{stage:?}"))
            .collect::<Vec<String>>()
            .join("");
        write!(f, "ExecutionGraph[job_id={}, session_id={}, available_tasks={}, is_successful={}, circuit_breaker_tripped={}]\n{}",
        self.job_id, self.session_id, self.available_tasks(), self.is_successful(), self.circuit_breaker_tripped, stages)
    }
}

/// Utility for building a set of `ExecutionStage`s from
/// a list of `ShuffleWriterExec`.
///
/// This will infer the dependency structure for the stages
/// so that we can construct a DAG from the stages.
struct ExecutionStageBuilder {
    /// Stage ID which is currently being visited
    current_stage_id: usize,
    /// Map from stage ID -> List of child stage IDs
    stage_dependencies: HashMap<usize, Vec<usize>>,
    /// Map from Stage ID -> output link
    output_links: HashMap<usize, Vec<usize>>,
    object_store: Option<Arc<dyn ObjectStore>>,
    clients: Arc<Cache<String, LimitedBallistaClient>>,
    shuffle_reader_options: Arc<ShuffleReaderExecOptions>,
}

impl ExecutionStageBuilder {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        clients: Arc<Cache<String, LimitedBallistaClient>>,
        shuffle_reader_options: Arc<ShuffleReaderExecOptions>,
    ) -> Self {
        Self {
            current_stage_id: 0,
            stage_dependencies: HashMap::new(),
            output_links: HashMap::new(),
            object_store: Some(object_store),
            clients,
            shuffle_reader_options,
        }
    }

    pub fn build(
        mut self,
        stages: Vec<Arc<ShuffleWriterExec>>,
    ) -> Result<HashMap<usize, ExecutionStage>> {
        let mut execution_stages: HashMap<usize, ExecutionStage> = HashMap::new();
        // First, build the dependency graph
        for stage in &stages {
            accept(stage.as_ref(), &mut self)?;
        }

        // Now, create the execution stages
        for stage in stages {
            let partitioning = stage.shuffle_output_partitioning().cloned();
            let stage_id = stage.stage_id();
            let output_links = self.output_links.remove(&stage_id).unwrap_or_default();

            let child_stages = self
                .stage_dependencies
                .remove(&stage_id)
                .unwrap_or_default();

            let stage = if child_stages.is_empty() {
                ExecutionStage::Resolved(ResolvedStage::new(
                    stage_id,
                    0,
                    stage,
                    partitioning,
                    output_links,
                    HashMap::new(),
                    HashSet::new(),
                    self.object_store.clone(),
                    self.clients.clone(),
                    self.shuffle_reader_options.clone(),
                ))
            } else {
                ExecutionStage::UnResolved(UnresolvedStage::new(
                    stage_id,
                    stage,
                    partitioning,
                    output_links,
                    child_stages,
                    self.object_store.clone(),
                    self.clients.clone(),
                    self.shuffle_reader_options.clone(),
                ))
            };
            execution_stages.insert(stage_id, stage);
        }

        Ok(execution_stages)
    }
}

impl ExecutionPlanVisitor for ExecutionStageBuilder {
    type Error = BallistaError;

    fn pre_visit(
        &mut self,
        plan: &dyn ExecutionPlan,
    ) -> std::result::Result<bool, Self::Error> {
        if let Some(shuffle_write) = plan.as_any().downcast_ref::<ShuffleWriterExec>() {
            self.current_stage_id = shuffle_write.stage_id();
        } else if let Some(unresolved_shuffle) =
            plan.as_any().downcast_ref::<UnresolvedShuffleExec>()
        {
            if let Some(output_links) =
                self.output_links.get_mut(&unresolved_shuffle.stage_id)
            {
                if !output_links.contains(&self.current_stage_id) {
                    output_links.push(self.current_stage_id);
                }
            } else {
                self.output_links
                    .insert(unresolved_shuffle.stage_id, vec![self.current_stage_id]);
            }

            if let Some(deps) = self.stage_dependencies.get_mut(&self.current_stage_id) {
                if !deps.contains(&unresolved_shuffle.stage_id) {
                    deps.push(unresolved_shuffle.stage_id);
                }
            } else {
                self.stage_dependencies
                    .insert(self.current_stage_id, vec![unresolved_shuffle.stage_id]);
            }
        }
        Ok(true)
    }
}

#[derive(Clone)]
pub struct TaskPartitions {
    pub job_id: String,
    pub stage_id: usize,
    pub partitions: Vec<usize>,
}

/// Represents the basic unit of work for the Ballista executor. Will execute
/// one partition of one stage on one task slot.
#[derive(Clone)]
pub struct TaskDescription {
    pub session_id: String,
    pub partitions: TaskPartitions,
    pub stage_attempt_num: usize,
    pub task_id: usize,
    pub plan: Arc<dyn ExecutionPlan>,
    pub output_partitioning: Option<Partitioning>,
    pub resolved_at: u64,
}

impl TaskDescription {
    /// Total number of partitions executed as part of this task
    pub fn concurrency(&self) -> usize {
        self.partitions.partitions.len()
    }
}

impl Debug for TaskDescription {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);
        write!(
            f,
            "TaskDescription[session_id: {},job: {}, stage: {}.{}, partitions: {:?} task_id: {}, resolved_at: {}]\n{}",
            self.session_id,
            self.partitions.job_id,
            self.partitions.stage_id,
            self.stage_attempt_num,
            self.partitions.partitions,
            self.task_id,
            self.resolved_at,
            plan
        )
    }
}

fn partition_to_location(
    job_id: &str,
    map_partitions: Vec<usize>,
    stage_id: usize,
    executor: &ExecutorMetadata,
    shuffles: Vec<ShuffleWritePartition>,
) -> Vec<PartitionLocation> {
    shuffles
        .into_iter()
        .map(|shuffle| PartitionLocation {
            job_id: job_id.to_string(),
            stage_id,
            map_partitions: map_partitions.clone(),
            output_partition: shuffle.output_partition as usize,
            executor_meta: executor.clone(),
            partition_stats: PartitionStats::new(
                Some(shuffle.num_rows),
                Some(shuffle.num_batches),
                Some(shuffle.num_bytes),
            ),
            path: shuffle.path,
        })
        .collect()
}

struct StageMetricsBuilder<'a> {
    stage_id: usize,
    plan: &'a dyn ExecutionPlan,
    metrics: VecDeque<&'a MetricsSet>,
    plan_metrics: Vec<PlanMetrics>,
}

impl<'a> StageMetricsBuilder<'a> {
    pub fn new(
        stage_id: usize,
        plan: &'a dyn ExecutionPlan,
        metrics: &'a [MetricsSet],
    ) -> Self {
        Self {
            stage_id,
            plan,
            metrics: VecDeque::from_iter(metrics),
            plan_metrics: vec![],
        }
    }

    pub fn build(mut self) -> Result<StageMetrics> {
        visit_execution_plan(self.plan, &mut self)?;

        Ok(StageMetrics {
            stage_id: self.stage_id as i64,
            partition_id: 0,
            plan_metrics: self.plan_metrics,
        })
    }
}

impl<'a> ExecutionPlanVisitor for StageMetricsBuilder<'a> {
    type Error = BallistaError;

    fn pre_visit(
        &mut self,
        plan: &dyn ExecutionPlan,
    ) -> std::result::Result<bool, Self::Error> {
        let plan_name = stringify!(plan.as_any());
        let metrics = to_metric(self.metrics.pop_front());

        self.plan_metrics.push(PlanMetrics {
            plan_name: plan_name.to_owned(),
            metrics,
        });

        Ok(!plan.children().is_empty())
    }
}

fn to_metric(metrics: Option<&MetricsSet>) -> Vec<Metric> {
    if let Some(metrics) = metrics {
        metrics
            .iter()
            .map(|m| match m.value() {
                MetricValue::OutputRows(count) => Metric {
                    value: count.value() as i64,
                    name: "output_rows".to_string(),
                    metric_type: MetricType::Count as i32,
                    labels: vec![],
                },
                MetricValue::ElapsedCompute(time) => Metric {
                    value: time.value() as i64,
                    name: "elapsed_compute".to_string(),
                    metric_type: MetricType::Time as i32,
                    labels: vec![],
                },
                MetricValue::SpillCount(count) => Metric {
                    value: count.value() as i64,
                    name: "spill_count".to_string(),
                    metric_type: MetricType::Count as i32,
                    labels: vec![],
                },
                MetricValue::SpilledBytes(count) => Metric {
                    value: count.value() as i64,
                    name: "spill_bytes".to_string(),
                    metric_type: MetricType::Count as i32,
                    labels: vec![],
                },
                MetricValue::CurrentMemoryUsage(gauge) => Metric {
                    value: gauge.value() as i64,
                    name: "current_memory_usage".to_string(),
                    metric_type: MetricType::Gauge as i32,
                    labels: vec![],
                },
                MetricValue::Count { name, count } => Metric {
                    value: count.value() as i64,
                    name: name.to_string(),
                    metric_type: MetricType::Count as i32,
                    labels: vec![],
                },
                MetricValue::Gauge { name, gauge } => Metric {
                    value: gauge.value() as i64,
                    name: name.to_string(),
                    metric_type: MetricType::Gauge as i32,
                    labels: vec![],
                },
                MetricValue::Time { name, time } => Metric {
                    value: time.value() as i64,
                    name: name.to_string(),
                    metric_type: MetricType::Time as i32,
                    labels: vec![],
                },
                MetricValue::StartTimestamp(timestamp) => Metric {
                    value: timestamp
                        .value()
                        .map(|dt| dt.timestamp_millis())
                        .unwrap_or_default(),
                    name: "start_timestamp".to_string(),
                    metric_type: MetricType::Timestamp as i32,
                    labels: vec![],
                },
                MetricValue::EndTimestamp(timestamp) => Metric {
                    value: timestamp
                        .value()
                        .map(|dt| dt.timestamp_millis())
                        .unwrap_or_default(),
                    name: "end_timestamp".to_string(),
                    metric_type: MetricType::Timestamp as i32,
                    labels: vec![],
                },
            })
            .collect()
    } else {
        vec![]
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use crate::scheduler_server::event::QueryStageSchedulerEvent;
    use ballista_core::error::Result;
    use ballista_core::serde::protobuf::{
        self, failed_task, job_status, ExecutionError, FailedTask, FetchPartitionError,
        IoError, JobStatus, TaskKilled,
    };

    use crate::state::execution_graph::ExecutionGraph;
    use crate::test_utils::{
        mock_completed_task, mock_executor, mock_failed_task, test_aggregation_plan,
        test_coalesce_plan, test_join_plan, test_two_aggregations_plan,
        test_union_all_plan, test_union_plan,
    };

    #[tokio::test]
    async fn test_drain_tasks() -> Result<()> {
        let mut agg_graph = test_aggregation_plan(4).await;

        println!("Graph: {agg_graph:?}");

        drain_tasks(&mut agg_graph)?;

        assert!(
            agg_graph.is_successful(),
            "Failed to complete aggregation plan"
        );

        let mut coalesce_graph = test_coalesce_plan(4).await;

        drain_tasks(&mut coalesce_graph)?;

        assert!(
            coalesce_graph.is_successful(),
            "Failed to complete coalesce plan"
        );

        let mut join_graph = test_join_plan(4).await;

        drain_tasks(&mut join_graph)?;

        println!("{join_graph:?}");

        assert!(join_graph.is_successful(), "Failed to complete join plan");

        let mut union_all_graph = test_union_all_plan(4).await;

        drain_tasks(&mut union_all_graph)?;

        println!("{union_all_graph:?}");

        assert!(
            union_all_graph.is_successful(),
            "Failed to complete union plan"
        );

        let mut union_graph = test_union_plan(4).await;

        drain_tasks(&mut union_graph)?;

        println!("{union_graph:?}");

        assert!(union_graph.is_successful(), "Failed to complete union plan");

        Ok(())
    }

    #[tokio::test]
    async fn test_finalize() -> Result<()> {
        let mut agg_graph = test_aggregation_plan(4).await;

        drain_tasks(&mut agg_graph)?;

        let status = agg_graph.status;

        assert!(matches!(
            status,
            protobuf::JobStatus {
                status: Some(job_status::Status::Successful(_)),
                ..
            }
        ));

        let outputs = agg_graph.output_locations;

        assert_eq!(outputs.len(), agg_graph.output_partitions);

        for location in outputs {
            assert_eq!(location.executor_meta.host, "localhost2".to_owned());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_do_not_retry_killed_task() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut agg_graph = test_aggregation_plan(4).await;
        // Call revive to move the leaf Resolved stages to Running
        agg_graph.revive();

        // Complete the first stage
        if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // 1st task in the second stage
        let task1 = agg_graph.pop_next_task(&executor2.id, 1)?.unwrap();
        let task_status1 = mock_completed_task(task1, &executor2.id);

        // 2rd task in the second stage
        let task2 = agg_graph.pop_next_task(&executor2.id, 1)?.unwrap();
        let task_status2 = mock_failed_task(
            task2,
            FailedTask {
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::TaskKilled(TaskKilled {})),
            },
        );

        agg_graph.update_task_status(
            &executor2,
            vec![task_status1, task_status2],
            4,
            4,
        )?;

        assert_eq!(agg_graph.available_tasks(), 2);
        drain_tasks(&mut agg_graph)?;
        assert_eq!(agg_graph.available_tasks(), 0);

        assert!(
            !agg_graph.is_successful(),
            "Expected the agg graph can not complete"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_max_task_failed_count() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut agg_graph = test_aggregation_plan(2).await;
        // Call revive to move the leaf Resolved stages to Running
        agg_graph.revive();

        // Complete the first stage
        if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // 1st task in the second stage
        let task1 = agg_graph.pop_next_task(&executor2.id, 1)?.unwrap();
        let task_status1 = mock_completed_task(task1, &executor2.id);

        // 2rd task in the second stage, failed due to IOError
        let task2 = agg_graph.pop_next_task(&executor2.id, 1)?.unwrap();
        let task_status2 = mock_failed_task(
            task2.clone(),
            FailedTask {
                retryable: true,
                count_to_failures: true,
                failed_reason: Some(failed_task::FailedReason::IoError(IoError {
                    message: "IOError".to_string(),
                })),
            },
        );

        agg_graph.update_task_status(
            &executor2,
            vec![task_status1, task_status2],
            4,
            4,
        )?;

        assert_eq!(agg_graph.available_tasks(), 1);

        // 2rd task's attempts
        for _attempt in 1..5 {
            if let Some(task2_attempt) = agg_graph.pop_next_task(&executor2.id, 1)? {
                assert_eq!(
                    task2_attempt.partitions.partitions,
                    task2.partitions.partitions
                );
                let task_status = mock_failed_task(
                    task2_attempt.clone(),
                    FailedTask {
                        retryable: true,
                        count_to_failures: true,
                        failed_reason: Some(failed_task::FailedReason::IoError(
                            IoError {
                                message: "IOError".to_string(),
                            },
                        )),
                    },
                );
                agg_graph.update_task_status(&executor2, vec![task_status], 4, 4)?;
            }
        }

        assert!(
            matches!(
                agg_graph.status,
                JobStatus {
                    status: Some(job_status::Status::Failed(_)),
                    ..
                }
            ),
            "Expected job status to be Failed but was {:?}",
            agg_graph.status
        );

        let failure_reason = format!("{:?}", agg_graph.status);
        assert!(
            failure_reason.contains(
                "Stage 2 had 4 task failures, fail the stage, most recent failure reason"
            ),
            "Unexpected failure reason {}",
            failure_reason
        );
        assert!(
            failure_reason.contains("IOError"),
            "Unexpected failure reason {}",
            failure_reason
        );
        assert!(!agg_graph.is_successful());

        Ok(())
    }

    #[tokio::test]
    async fn test_normal_fetch_failure() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut agg_graph = test_aggregation_plan(4).await;
        // Call revive to move the leaf Resolved stages to Running
        agg_graph.revive();

        // Complete the Stage 1
        if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // 1st task in the Stage 2
        let task1 = agg_graph.pop_next_task(&executor2.id, 1)?.unwrap();
        let task_status1 = mock_completed_task(task1, &executor2.id);

        // 2nd task in the Stage 2, failed due to FetchPartitionError
        let task2 = agg_graph.pop_next_task(&executor2.id, 1)?.unwrap();
        let task_status2 = mock_failed_task(
            task2,
            FailedTask {
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor1.id.clone(),
                        map_stage_id: 1,
                        map_partitions: vec![0],
                        message: "FetchPartitionError".to_string(),
                    },
                )),
            },
        );

        let mut running_task_count = 0;
        while let Some(_task) = agg_graph.pop_next_task(&executor2.id, 1)? {
            running_task_count += 1;
        }
        assert_eq!(running_task_count, 2);

        let stage_events = agg_graph.update_task_status(
            &executor2,
            vec![task_status1, task_status2],
            4,
            4,
        )?;

        assert_eq!(stage_events.len(), 1);
        assert!(matches!(
            stage_events[0],
            QueryStageSchedulerEvent::CancelTasks(_)
        ));

        // Stage 1 is running
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 1);
        assert_eq!(agg_graph.available_tasks(), 1);

        drain_tasks(&mut agg_graph)?;
        assert!(agg_graph.is_successful(), "Failed to complete agg plan");
        Ok(())
    }

    #[tokio::test]
    async fn test_many_fetch_failures_in_one_stage() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let executor3 = mock_executor("executor-id3".to_string());
        let mut agg_graph = test_two_aggregations_plan(8).await;

        agg_graph.revive();
        assert_eq!(agg_graph.stages.len(), 3);

        // Complete the Stage 1
        if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // Complete the Stage 2, 5 tasks run on executor_2 and 3 tasks run on executor_1
        for _i in 0..5 {
            if let Some(task) = agg_graph.pop_next_task(&executor2.id, 1)? {
                let task_status = mock_completed_task(task, &executor2.id);
                agg_graph.update_task_status(&executor2, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(agg_graph.available_tasks(), 3);
        for _i in 0..3 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
                let task_status = mock_completed_task(task, &executor1.id);
                agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
            }
        }

        // Run Stage 3, 6 tasks failed due to FetchPartitionError on different map partitions on executor_2
        let mut many_fetch_failure_status = vec![];
        for part in 2..8 {
            if let Some(task) = agg_graph.pop_next_task(&executor3.id, 1)? {
                let task_status = mock_failed_task(
                    task,
                    FailedTask {
                        retryable: false,
                        count_to_failures: false,
                        failed_reason: Some(
                            failed_task::FailedReason::FetchPartitionError(
                                FetchPartitionError {
                                    executor_id: executor2.id.clone(),
                                    map_stage_id: 2,
                                    map_partitions: vec![part],
                                    message: "FetchPartitionError".to_string(),
                                },
                            ),
                        ),
                    },
                );
                many_fetch_failure_status.push(task_status);
            }
        }
        assert_eq!(many_fetch_failure_status.len(), 6);
        agg_graph.update_task_status(&executor3, many_fetch_failure_status, 4, 4)?;

        // The Running stage should be Stage 2 now
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 5);

        drain_tasks(&mut agg_graph)?;
        assert!(agg_graph.is_successful(), "Failed to complete agg plan");
        Ok(())
    }

    #[tokio::test]
    async fn test_many_consecutive_stage_fetch_failures() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut agg_graph = test_aggregation_plan(4).await;
        // Call revive to move the leaf Resolved stages to Running
        agg_graph.revive();

        for attempt in 0..6 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
                let task_status = mock_completed_task(task, &executor1.id);
                agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
            }

            // 1rd task in the Stage 2, failed due to FetchPartitionError
            if let Some(task1) = agg_graph.pop_next_task(&executor2.id, 1)? {
                let task_status1 = mock_failed_task(
                    task1.clone(),
                    FailedTask {
                        retryable: false,
                        count_to_failures: false,
                        failed_reason: Some(
                            failed_task::FailedReason::FetchPartitionError(
                                FetchPartitionError {
                                    executor_id: executor1.id.clone(),
                                    map_stage_id: 1,
                                    map_partitions: vec![0],
                                    message: "FetchPartitionError".to_string(),
                                },
                            ),
                        ),
                    },
                );

                let stage_events =
                    agg_graph.update_task_status(&executor2, vec![task_status1], 4, 4)?;

                if attempt < 3 {
                    // No JobRunningFailed stage events
                    assert_eq!(stage_events.len(), 0);
                    // Stage 1 is running
                    let running_stage = agg_graph.running_stages();
                    assert_eq!(running_stage.len(), 1);
                    assert_eq!(running_stage[0], 1);
                    assert_eq!(agg_graph.available_tasks(), 1);
                } else {
                    // Job is failed after exceeds the max_stage_failures
                    assert_eq!(stage_events.len(), 1);
                    assert!(matches!(
                        stage_events[0],
                        QueryStageSchedulerEvent::JobRunningFailed { .. }
                    ));
                    // Stage 2 is still running
                    let running_stage = agg_graph.running_stages();
                    assert_eq!(running_stage.len(), 1);
                    assert_eq!(running_stage[0], 2);
                }
            }
        }

        drain_tasks(&mut agg_graph)?;
        assert!(!agg_graph.is_successful(), "Expect to fail the agg plan");

        let failure_reason = format!("{:?}", agg_graph.status);
        assert!(failure_reason
            .contains("Stage 2 has failed 4 times, most recent failure reason"));
        assert!(failure_reason.contains("FetchPartitionError"));

        Ok(())
    }

    #[tokio::test]
    async fn test_long_delayed_fetch_failures() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let executor3 = mock_executor("executor-id3".to_string());
        let mut agg_graph = test_two_aggregations_plan(8).await;

        agg_graph.revive();
        assert_eq!(agg_graph.stages.len(), 3);

        // Complete the Stage 1
        if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // Complete the Stage 2, 5 tasks run on executor_2, 2 tasks run on executor_1, 1 task runs on executor_3
        for _i in 0..5 {
            if let Some(task) = agg_graph.pop_next_task(&executor2.id, 1)? {
                let task_status = mock_completed_task(task, &executor2.id);
                agg_graph.update_task_status(&executor2, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(agg_graph.available_tasks(), 3);

        for _i in 0..2 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
                let task_status = mock_completed_task(task, &executor1.id);
                agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
            }
        }

        if let Some(task) = agg_graph.pop_next_task(&executor3.id, 1)? {
            let task_status = mock_completed_task(task, &executor3.id);
            agg_graph.update_task_status(&executor3, vec![task_status], 4, 4)?;
        }
        assert_eq!(agg_graph.available_tasks(), 0);

        //Run Stage 3
        // 1st task scheduled
        let task_1 = agg_graph.pop_next_task(&executor3.id, 1)?.unwrap();
        // 2nd task scheduled
        let task_2 = agg_graph.pop_next_task(&executor3.id, 1)?.unwrap();
        // 3rd task scheduled
        let task_3 = agg_graph.pop_next_task(&executor3.id, 1)?.unwrap();
        // 4th task scheduled
        let task_4 = agg_graph.pop_next_task(&executor3.id, 1)?.unwrap();
        // 5th task scheduled
        let task_5 = agg_graph.pop_next_task(&executor3.id, 1)?.unwrap();

        // Stage 3, 1st task failed due to FetchPartitionError(executor2)
        let task_status_1 = mock_failed_task(
            task_1,
            FailedTask {
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor2.id.clone(),
                        map_stage_id: 2,
                        map_partitions: vec![0],
                        message: "FetchPartitionError".to_string(),
                    },
                )),
            },
        );
        agg_graph.update_task_status(&executor3, vec![task_status_1], 4, 4)?;

        // The Running stage is Stage 2 now
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 5);

        // Stage 3, 2nd task failed due to FetchPartitionError(executor2)
        let task_status_2 = mock_failed_task(
            task_2,
            FailedTask {
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor2.id.clone(),
                        map_stage_id: 2,
                        map_partitions: vec![0],
                        message: "FetchPartitionError".to_string(),
                    },
                )),
            },
        );
        // This task update should be ignored
        agg_graph.update_task_status(&executor3, vec![task_status_2], 4, 4)?;
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 5);

        // Stage 3, 3rd task failed due to FetchPartitionError(executor1)
        let task_status_3 = mock_failed_task(
            task_3,
            FailedTask {
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor1.id.clone(),
                        map_stage_id: 2,
                        map_partitions: vec![0],
                        message: "FetchPartitionError".to_string(),
                    },
                )),
            },
        );
        // This task update should be handled because it has a different failure reason
        agg_graph.update_task_status(&executor3, vec![task_status_3], 4, 4)?;
        // Running stage is still Stage 2, but available tasks changed to 7
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 7);

        // Finish 4 tasks in Stage 2, to make some progress
        for _i in 0..4 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
                let task_status = mock_completed_task(task, &executor1.id);
                agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 3);

        // Stage 3, 4th task failed due to FetchPartitionError(executor1)
        let task_status_4 = mock_failed_task(
            task_4,
            FailedTask {
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor1.id.clone(),
                        map_stage_id: 2,
                        map_partitions: vec![1],
                        message: "FetchPartitionError".to_string(),
                    },
                )),
            },
        );
        // This task update should be ignored because the same failure reason is already handled
        agg_graph.update_task_status(&executor3, vec![task_status_4], 4, 4)?;
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 3);

        // Finish the other 3 tasks in Stage 2
        for _i in 0..3 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
                let task_status = mock_completed_task(task, &executor1.id);
                agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(agg_graph.available_tasks(), 0);

        // Stage 3, the very long delayed 5th task failed due to FetchPartitionError(executor3)
        // Although the failure reason is new, but this task should be ignored
        // Because its map stage's new attempt is finished and this stage's new attempt is running
        let task_status_5 = mock_failed_task(
            task_5,
            FailedTask {
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor3.id.clone(),
                        map_stage_id: 2,
                        map_partitions: vec![1],
                        message: "FetchPartitionError".to_string(),
                    },
                )),
            },
        );
        agg_graph.update_task_status(&executor3, vec![task_status_5], 4, 4)?;
        // Stage 3's new attempt is running
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 3);
        assert_eq!(agg_graph.available_tasks(), 8);

        // There is one failed stage attempts: Stage 3. Stage 2 does not count to failed attempts
        assert_eq!(agg_graph.failed_stage_attempts.len(), 1);
        assert_eq!(
            agg_graph.failed_stage_attempts.get(&3).cloned(),
            Some(HashSet::from([0]))
        );
        drain_tasks(&mut agg_graph)?;
        assert!(agg_graph.is_successful(), "Failed to complete agg plan");
        // Failed stage attempts are cleaned
        assert_eq!(agg_graph.failed_stage_attempts.len(), 0);

        Ok(())
    }

    #[tokio::test]
    // This test case covers a race condition in delayed fetch failure handling:
    // TaskStatus of input stage's new attempt come together with the parent stage's delayed FetchFailure
    async fn test_long_delayed_fetch_failures_race_condition() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let executor3 = mock_executor("executor-id3".to_string());
        let mut agg_graph = test_two_aggregations_plan(8).await;

        agg_graph.revive();
        assert_eq!(agg_graph.stages.len(), 3);

        // Complete the Stage 1
        if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // Complete the Stage 2, 5 tasks run on executor_2, 3 tasks run on executor_1
        for _i in 0..5 {
            if let Some(task) = agg_graph.pop_next_task(&executor2.id, 1)? {
                let task_status = mock_completed_task(task, &executor2.id);
                agg_graph.update_task_status(&executor2, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(agg_graph.available_tasks(), 3);

        for _i in 0..3 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
                let task_status = mock_completed_task(task, &executor1.id);
                agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(agg_graph.available_tasks(), 0);

        // Run Stage 3
        // 1st task scheduled
        let task_1 = agg_graph.pop_next_task(&executor3.id, 1)?.unwrap();
        // 2nd task scheduled
        let task_2 = agg_graph.pop_next_task(&executor3.id, 1)?.unwrap();

        // Stage 3, 1st task failed due to FetchPartitionError(executor2)
        let task_status_1 = mock_failed_task(
            task_1.clone(),
            FailedTask {
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor2.id.clone(),
                        map_stage_id: 2,
                        map_partitions: task_1
                            .partitions
                            .partitions
                            .iter()
                            .map(|p| *p as u32)
                            .collect(),
                        message: "FetchPartitionError".to_string(),
                    },
                )),
            },
        );
        agg_graph.update_task_status(&executor3, vec![task_status_1], 4, 4)?;

        // The Running stage is Stage 2 now
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 5);

        // Complete the 5 tasks in Stage 2's new attempts
        let mut task_status_vec = vec![];
        for _i in 0..5 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
                task_status_vec.push(mock_completed_task(task, &executor1.id))
            }
        }

        // Stage 3, 2nd task failed due to FetchPartitionError(executor1)
        let task_status_2 = mock_failed_task(
            task_2,
            FailedTask {
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor1.id.clone(),
                        map_stage_id: 2,
                        map_partitions: vec![1],
                        message: "FetchPartitionError".to_string(),
                    },
                )),
            },
        );
        task_status_vec.push(task_status_2);

        // TaskStatus of Stage 2 come together with Stage 3 delayed FetchFailure update.
        // The successful tasks from Stage 2 would try to succeed the Stage2 and the delayed fetch failure try to reset the TaskInfo
        agg_graph.update_task_status(&executor3, task_status_vec, 4, 4)?;
        //The Running stage is still Stage 2, 3 new pending tasks added due to FetchPartitionError(executor1)
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 3);

        drain_tasks(&mut agg_graph)?;
        assert!(agg_graph.is_successful(), "Failed to complete agg plan");

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_failures_in_different_stages() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let executor3 = mock_executor("executor-id3".to_string());
        let mut agg_graph = test_two_aggregations_plan(8).await;

        agg_graph.revive();
        assert_eq!(agg_graph.stages.len(), 3);

        // Complete the Stage 1
        if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // Complete the Stage 2, 5 tasks run on executor_2, 3 tasks run on executor_1
        for _i in 0..5 {
            if let Some(task) = agg_graph.pop_next_task(&executor2.id, 1)? {
                let task_status = mock_completed_task(task, &executor2.id);
                agg_graph.update_task_status(&executor2, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(agg_graph.available_tasks(), 3);
        for _i in 0..3 {
            if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
                let task_status = mock_completed_task(task, &executor1.id);
                agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
            }
        }
        assert_eq!(agg_graph.available_tasks(), 0);

        // Run Stage 3
        // 1rd task in the Stage 3, failed due to FetchPartitionError(executor1)
        if let Some(task1) = agg_graph.pop_next_task(&executor3.id, 1)? {
            let task_status1 = mock_failed_task(
                task1.clone(),
                FailedTask {
                    retryable: false,
                    count_to_failures: false,
                    failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                        FetchPartitionError {
                            executor_id: executor1.id.clone(),
                            map_stage_id: 2,
                            map_partitions: task1
                                .partitions
                                .partitions
                                .iter()
                                .map(|p| *p as u32)
                                .collect(),
                            message: "FetchPartitionError".to_string(),
                        },
                    )),
                },
            );

            let _stage_events =
                agg_graph.update_task_status(&executor3, vec![task_status1], 4, 4)?;
        }
        // The Running stage is Stage 2 now
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 2);
        assert_eq!(agg_graph.available_tasks(), 3);

        // 1rd task in the Stage 2's new attempt, failed due to FetchPartitionError(executor1)
        if let Some(task1) = agg_graph.pop_next_task(&executor3.id, 1)? {
            let task_status1 = mock_failed_task(
                task1.clone(),
                FailedTask {
                    retryable: false,
                    count_to_failures: false,
                    failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                        FetchPartitionError {
                            executor_id: executor1.id.clone(),
                            map_stage_id: 1,
                            map_partitions: task1
                                .partitions
                                .partitions
                                .iter()
                                .map(|p| *p as u32)
                                .collect(),
                            message: "FetchPartitionError".to_string(),
                        },
                    )),
                },
            );
            let _stage_events =
                agg_graph.update_task_status(&executor3, vec![task_status1], 4, 4)?;
        }
        // The Running stage is Stage 1 now
        let running_stage = agg_graph.running_stages();
        assert_eq!(running_stage.len(), 1);
        assert_eq!(running_stage[0], 1);
        assert_eq!(agg_graph.available_tasks(), 1);

        println!("GRAPH: {:#?}", agg_graph);
        // There are two failed stage attempts: Stage 2 and Stage 3
        assert_eq!(agg_graph.failed_stage_attempts.len(), 2);
        assert_eq!(
            agg_graph.failed_stage_attempts.get(&2).cloned(),
            Some(HashSet::from([1]))
        );
        assert_eq!(
            agg_graph.failed_stage_attempts.get(&3).cloned(),
            Some(HashSet::from([0]))
        );

        drain_tasks(&mut agg_graph)?;
        assert!(agg_graph.is_successful(), "Failed to complete agg plan");
        assert_eq!(agg_graph.failed_stage_attempts.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_failure_with_normal_task_failure() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut agg_graph = test_aggregation_plan(4).await;
        // Call revive to move the leaf Resolved stages to Running
        agg_graph.revive();

        // Complete the Stage 1
        if let Some(task) = agg_graph.pop_next_task(&executor1.id, 1)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 4, 4)?;
        }

        // 1st task in the Stage 2
        let task1 = agg_graph.pop_next_task(&executor2.id, 1)?.unwrap();
        let task_status1 = mock_completed_task(task1, &executor2.id);

        // 2nd task in the Stage 2, failed due to FetchPartitionError
        let task2 = agg_graph.pop_next_task(&executor2.id, 1)?.unwrap();
        let task_status2 = mock_failed_task(
            task2.clone(),
            FailedTask {
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::FetchPartitionError(
                    FetchPartitionError {
                        executor_id: executor1.id.clone(),
                        map_stage_id: 1,
                        map_partitions: task2
                            .partitions
                            .partitions
                            .iter()
                            .map(|p| *p as u32)
                            .collect(),
                        message: "FetchPartitionError".to_string(),
                    },
                )),
            },
        );

        // 3rd task in the Stage 2, failed due to ExecutionError
        let task3 = agg_graph.pop_next_task(&executor2.id, 1)?.unwrap();
        let task_status3 = mock_failed_task(
            task3,
            FailedTask {
                retryable: false,
                count_to_failures: false,
                failed_reason: Some(failed_task::FailedReason::ExecutionError(
                    ExecutionError { error: None },
                )),
            },
        );

        let stage_events = agg_graph.update_task_status(
            &executor2,
            vec![task_status1, task_status2, task_status3],
            4,
            4,
        )?;

        assert_eq!(stage_events.len(), 1);
        assert!(matches!(
            stage_events[0],
            QueryStageSchedulerEvent::JobRunningFailed { .. }
        ));

        drain_tasks(&mut agg_graph)?;
        assert!(!agg_graph.is_successful(), "Expect to fail the agg plan");

        let failure_reason = format!("{:?}", agg_graph.status);
        assert!(failure_reason.contains("ExecutionError"));

        Ok(())
    }

    // #[tokio::test]
    // async fn test_shuffle_files_should_cleaned_after_fetch_failure() -> Result<()> {
    //     todo!()
    // }

    fn drain_tasks(graph: &mut ExecutionGraph) -> Result<()> {
        let executor = mock_executor("executor-id1".to_string());
        while let Some(task) = graph.pop_next_task(&executor.id, 1)? {
            let task_status = mock_completed_task(task, &executor.id);
            graph.update_task_status(&executor, vec![task_status], 1, 1)?;
        }

        Ok(())
    }
}
