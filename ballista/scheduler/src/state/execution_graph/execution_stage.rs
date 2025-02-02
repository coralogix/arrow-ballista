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

use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::iter::FromIterator;
use std::sync::Arc;

use ballista_core::client::LimitedBallistaClient;
use ballista_core::execution_plans::ShuffleReaderExecOptions;
use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, Metric, Partitioning};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::logical_plan::AsLogicalPlan;
use moka::future::Cache;
use object_store::ObjectStore;
use tracing::{debug, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf::{
    self, task_info, GraphStageInput, OperatorMetricsSet, SuccessfulTask, TaskStatus,
};
use ballista_core::serde::protobuf::{task_status, RunningTask};
use ballista_core::serde::scheduler::to_proto::hash_partitioning_to_proto;
use ballista_core::serde::scheduler::PartitionLocation;
use ballista_core::serde::BallistaCodec;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use datafusion_proto::physical_plan::AsExecutionPlan;

use crate::display::DisplayableBallistaExecutionPlan;
use crate::scheduler_server::timestamp_millis;

/// A stage in the ExecutionGraph,
/// represents a set of tasks (one per each `partition`) which can be executed concurrently.
/// For a stage, there are five states. And the state machine is as follows:
///
/// UnResolvedStage           FailedStage
///       ↓            ↙           ↑
///  ResolvedStage     →     RunningStage
///                                ↓
///                         SuccessfulStage
#[derive(Clone)]
pub(crate) enum ExecutionStage {
    UnResolved(UnresolvedStage),
    Resolved(ResolvedStage),
    Running(RunningStage),
    Successful(SuccessfulStage),
    Failed(FailedStage),
}

impl Debug for ExecutionStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionStage::UnResolved(unresolved_stage) => unresolved_stage.fmt(f),
            ExecutionStage::Resolved(resolved_stage) => resolved_stage.fmt(f),
            ExecutionStage::Running(running_stage) => running_stage.fmt(f),
            ExecutionStage::Successful(successful_stage) => successful_stage.fmt(f),
            ExecutionStage::Failed(failed_stage) => failed_stage.fmt(f),
        }
    }
}

impl ExecutionStage {
    /// Get the name of the variant
    pub(crate) fn variant_name(&self) -> &str {
        match self {
            ExecutionStage::UnResolved(_) => "Unresolved",
            ExecutionStage::Resolved(_) => "Resolved",
            ExecutionStage::Running(_) => "Running",
            ExecutionStage::Successful(_) => "Successful",
            ExecutionStage::Failed(_) => "Failed",
        }
    }

    /// Get the query plan for this query stage
    pub(crate) fn plan(&self) -> &dyn ExecutionPlan {
        match self {
            ExecutionStage::UnResolved(stage) => stage.plan.as_ref(),
            ExecutionStage::Resolved(stage) => stage.plan.as_ref(),
            ExecutionStage::Running(stage) => stage.plan.as_ref(),
            ExecutionStage::Successful(stage) => stage.plan.as_ref(),
            ExecutionStage::Failed(stage) => stage.plan.as_ref(),
        }
    }

    pub(crate) fn stage_metrics(&self) -> Option<&Vec<MetricsSet>> {
        match self {
            ExecutionStage::UnResolved(_) | ExecutionStage::Resolved(_) => None,
            ExecutionStage::Running(RunningStage { stage_metrics, .. }) => {
                stage_metrics.as_ref()
            }
            ExecutionStage::Successful(SuccessfulStage { stage_metrics, .. }) => {
                Some(stage_metrics)
            }
            ExecutionStage::Failed(FailedStage { stage_metrics, .. }) => {
                stage_metrics.as_ref()
            }
        }
    }
}

/// For a stage whose input stages are not all completed, we say it's a unresolved stage
#[derive(Clone)]
pub(crate) struct UnresolvedStage {
    /// Stage ID
    pub(crate) stage_id: usize,
    /// Stage Attempt number
    pub(crate) stage_attempt_num: usize,
    /// Output partitioning for this stage.
    pub(crate) output_partitioning: Option<Partitioning>,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(crate) output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    /// This stage can only be resolved an executed once all child stages are completed.
    pub(crate) inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// Record last attempt's failure reasons to avoid duplicate resubmits
    pub(crate) last_attempt_failure_reasons: HashSet<String>,

    pub(crate) object_store: Option<Arc<dyn ObjectStore>>,

    pub(crate) clients: Arc<Cache<String, LimitedBallistaClient>>,

    pub(crate) shuffle_reader_options: Arc<ShuffleReaderExecOptions>,
}

/// For a stage, if it has no inputs or all of its input stages are completed,
/// then we call it as a resolved stage
#[derive(Clone)]
pub(crate) struct ResolvedStage {
    /// Stage ID
    pub(crate) stage_id: usize,
    /// Stage Attempt number
    pub(crate) stage_attempt_num: usize,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub(crate) partitions: usize,
    /// Output partitioning for this stage.
    pub(crate) output_partitioning: Option<Partitioning>,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(crate) output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub(crate) inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// Record last attempt's failure reasons to avoid duplicate resubmits
    pub(crate) last_attempt_failure_reasons: HashSet<String>,
    /// Timestamp when then stage went into resolved state
    pub(crate) resolved_at: u64,

    pub(crate) object_store: Option<Arc<dyn ObjectStore>>,
    pub(crate) clients: Arc<Cache<String, LimitedBallistaClient>>,
    pub(crate) shuffle_reader_options: Arc<ShuffleReaderExecOptions>,
}

/// Different from the resolved stage, a running stage will
/// 1. save the execution plan as encoded one to avoid serialization cost for creating task definition
/// 2. manage the task statuses
/// 3. manage the stage-level combined metrics
/// Running stages will only be maintained in memory and will not saved to the backend storage
#[derive(Clone)]
pub(crate) struct RunningStage {
    /// Stage ID
    pub(crate) stage_id: usize,
    /// Stage Attempt number
    pub(crate) stage_attempt_num: usize,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub(crate) partitions: usize,
    /// Output partitioning for this stage.
    pub(crate) output_partitioning: Option<Partitioning>,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(crate) output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub(crate) inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// TaskInfo of each already scheduled task. If info is None, the partition has not yet been scheduled.
    /// The index of the Vec is the task's partition id
    pub(crate) task_infos: Vec<Option<TaskInfo>>,
    /// Track the number of task failures for this stage
    pub(crate) task_failures: usize,
    /// Combined metrics of the already finished tasks in the stage, If it is None, no task is finished yet.
    pub(crate) stage_metrics: Option<Vec<MetricsSet>>,
    /// Timestamp when then stage went into resolved state
    pub(crate) resolved_at: u64,
    pub(crate) object_store: Option<Arc<dyn ObjectStore>>,
    pub(crate) clients: Arc<Cache<String, LimitedBallistaClient>>,
    pub(crate) shuffle_reader_options: Arc<ShuffleReaderExecOptions>,
}

/// If a stage finishes successfully, its task statuses and metrics will be finalized
#[derive(Clone)]
pub(crate) struct SuccessfulStage {
    /// Stage ID
    pub(crate) stage_id: usize,
    /// Stage Attempt number
    pub(crate) stage_attempt_num: usize,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub(crate) partitions: usize,
    /// Output partitioning for this stage.
    pub(crate) output_partitioning: Option<Partitioning>,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(crate) output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub(crate) inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// TaskInfo of each already successful task.
    /// The index of the Vec is the task's partition id
    pub(crate) task_infos: Vec<TaskInfo>,
    /// Combined metrics of the already finished tasks in the stage.
    pub(crate) stage_metrics: Vec<MetricsSet>,
    pub(crate) object_store: Option<Arc<dyn ObjectStore>>,
    pub(crate) clients: Arc<Cache<String, LimitedBallistaClient>>,
    pub(crate) shuffle_reader_options: Arc<ShuffleReaderExecOptions>,
}

/// If a stage fails, it will be with an error message
#[derive(Clone)]
pub(crate) struct FailedStage {
    /// Stage ID
    pub(crate) stage_id: usize,
    /// Stage Attempt number
    pub(crate) stage_attempt_num: usize,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub(crate) partitions: usize,
    /// Output partitioning for this stage.
    pub(crate) output_partitioning: Option<Partitioning>,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(crate) output_links: Vec<usize>,
    /// `ExecutionPlan` for this stage
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// TaskInfo of each already scheduled tasks. If info is None, the partition has not yet been scheduled
    /// The index of the Vec is the task's partition id
    pub(crate) task_infos: Vec<Option<TaskInfo>>,
    /// Combined metrics of the already finished tasks in the stage, If it is None, no task is finished yet.
    pub(crate) stage_metrics: Option<Vec<MetricsSet>>,
    /// Error message
    pub(crate) error_message: String,
}

#[derive(Clone)]
pub(crate) struct TaskInfo {
    /// Task ID
    pub(super) task_id: usize,
    /// Task scheduled time
    pub(super) scheduled_time: u128,
    /// Task launch time
    pub(super) launch_time: u128,
    /// Start execution time
    pub(super) start_exec_time: u128,
    /// Finish execution time
    pub(super) end_exec_time: u128,
    /// Task finish time
    pub(super) finish_time: u128,
    /// Task Status
    pub(super) task_status: task_status::Status,
}

impl TaskInfo {
    pub fn is_finished(&self) -> bool {
        matches!(self.task_status, task_status::Status::Successful(_))
    }

    pub fn execution_time(&self) -> u128 {
        self.end_exec_time - self.start_exec_time
    }
}

impl UnresolvedStage {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_partitioning: Option<Partitioning>,
        output_links: Vec<usize>,
        child_stage_ids: Vec<usize>,
        object_store: Option<Arc<dyn ObjectStore>>,
        clients: Arc<Cache<String, LimitedBallistaClient>>,
        shuffle_reader_options: Arc<ShuffleReaderExecOptions>,
    ) -> Self {
        let mut inputs: HashMap<usize, StageOutput> = HashMap::new();
        for input_stage_id in child_stage_ids {
            inputs.insert(input_stage_id, StageOutput::new());
        }

        Self {
            stage_id,
            stage_attempt_num: 0,
            output_partitioning,
            output_links,
            inputs,
            plan,
            last_attempt_failure_reasons: Default::default(),
            object_store,
            clients,
            shuffle_reader_options,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_with_inputs(
        stage_id: usize,
        stage_attempt_num: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_partitioning: Option<Partitioning>,
        output_links: Vec<usize>,
        inputs: HashMap<usize, StageOutput>,
        last_attempt_failure_reasons: HashSet<String>,
        object_store: Option<Arc<dyn ObjectStore>>,
        clients: Arc<Cache<String, LimitedBallistaClient>>,
        shuffle_reader_options: Arc<ShuffleReaderExecOptions>,
    ) -> Self {
        Self {
            stage_id,
            stage_attempt_num,
            output_partitioning,
            output_links,
            inputs,
            plan,
            last_attempt_failure_reasons,
            object_store,
            clients,
            shuffle_reader_options,
        }
    }

    /// Add input partitions published from an input stage.
    pub(super) fn add_input_partitions(
        &mut self,
        stage_id: usize,
        locations: Vec<PartitionLocation>,
    ) -> Result<()> {
        if let Some(stage_inputs) = self.inputs.get_mut(&stage_id) {
            for partition in locations {
                stage_inputs.add_partition(partition);
            }
        } else {
            return Err(BallistaError::Internal(format!("Error adding input partitions to stage {}, {} is not a valid child stage ID", self.stage_id, stage_id)));
        }

        Ok(())
    }

    /// Remove input partitions from an input stage on a given executor.
    /// Return the HashSet of removed map partition ids
    pub(super) fn remove_input_partitions(
        &mut self,
        input_stage_id: usize,
        executor_id: &str,
    ) -> Result<HashSet<usize>> {
        if let Some(stage_output) = self.inputs.get_mut(&input_stage_id) {
            let mut bad_map_partitions = HashSet::new();
            stage_output
                .partition_locations
                .iter_mut()
                .for_each(|(_partition, locs)| {
                    locs.iter().for_each(|loc| {
                        if loc.executor_meta.id == executor_id {
                            bad_map_partitions.extend(&loc.map_partitions);
                        }
                    });

                    locs.retain(|loc| loc.executor_meta.id != executor_id);
                });
            stage_output.complete = false;
            Ok(bad_map_partitions)
        } else {
            Err(BallistaError::Internal(format!("Error remove input partition for Stage {}, {} is not a valid child stage ID", self.stage_id, input_stage_id)))
        }
    }

    /// Marks the input stage ID as complete.
    pub(super) fn complete_input(&mut self, stage_id: usize) {
        if let Some(input) = self.inputs.get_mut(&stage_id) {
            input.complete = true;
        }
    }

    /// Returns true if all inputs are complete and we can resolve all
    /// UnresolvedShuffleExec operators to ShuffleReadExec
    pub(super) fn resolvable(&self) -> bool {
        self.inputs.iter().all(|(_, input)| input.is_complete())
    }

    /// Change to the resolved state
    pub(super) fn to_resolved(&self) -> Result<ResolvedStage> {
        let input_locations = self
            .inputs
            .iter()
            .map(|(stage, input)| (*stage, input.partition_locations.clone()))
            .collect();
        let plan = crate::planner::remove_unresolved_shuffles(
            self.plan.clone(),
            &input_locations,
            self.object_store.clone(),
            self.clients.clone(),
            self.shuffle_reader_options.clone(),
        )?;

        // Optimize join order and aggregate based on new resolved statistics
        let optimize_join = JoinSelection::new();
        let config = SessionConfig::default();
        let plan = optimize_join.optimize(plan, config.options())?;
        let optimize_aggregate = AggregateStatistics::new();
        let plan =
            optimize_aggregate.optimize(plan, SessionConfig::default().options())?;

        Ok(ResolvedStage::new(
            self.stage_id,
            self.stage_attempt_num,
            plan,
            self.output_partitioning.clone(),
            self.output_links.clone(),
            self.inputs.clone(),
            self.last_attempt_failure_reasons.clone(),
            self.object_store.clone(),
            self.clients.clone(),
            self.shuffle_reader_options.clone(),
        ))
    }

    pub(super) fn decode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: protobuf::UnResolvedStage,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
        object_store: Option<Arc<dyn ObjectStore>>,
        clients: Arc<Cache<String, LimitedBallistaClient>>,
    ) -> Result<UnresolvedStage> {
        let plan_proto = U::try_decode(&stage.plan)?;
        let plan = plan_proto.try_into_physical_plan(
            session_ctx,
            session_ctx.runtime_env().as_ref(),
            codec.physical_extension_codec(),
        )?;

        let output_partitioning: Option<Partitioning> = parse_protobuf_hash_partitioning(
            stage.output_partitioning.as_ref(),
            session_ctx,
            plan.schema().as_ref(),
        )?;

        let inputs = decode_inputs(stage.inputs)?;
        let shuffle_reader_options = stage
            .shuffle_reader_options
            .as_ref()
            .ok_or_else(|| {
                BallistaError::General("ShuffleReaderExecOptions is missing".to_owned())
            })?
            .into();
        Ok(UnresolvedStage {
            stage_id: stage.stage_id as usize,
            stage_attempt_num: stage.stage_attempt_num as usize,
            output_partitioning,
            output_links: stage.output_links.into_iter().map(|l| l as usize).collect(),
            plan,
            inputs,
            last_attempt_failure_reasons: HashSet::from_iter(
                stage.last_attempt_failure_reasons,
            ),
            object_store,
            clients,
            shuffle_reader_options: Arc::new(shuffle_reader_options),
        })
    }

    pub(super) fn encode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: &UnresolvedStage,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::UnResolvedStage> {
        let mut plan: Vec<u8> = vec![];
        U::try_from_physical_plan(stage.plan.clone(), codec.physical_extension_codec())
            .and_then(|proto| proto.try_encode(&mut plan))?;

        let inputs = encode_inputs(&stage.inputs)?;

        let output_partitioning =
            hash_partitioning_to_proto(stage.output_partitioning.as_ref())?;

        Ok(protobuf::UnResolvedStage {
            stage_id: stage.stage_id as u32,
            stage_attempt_num: stage.stage_attempt_num as u32,
            output_partitioning,
            output_links: stage.output_links.iter().map(|l| *l as u32).collect(),
            inputs,
            plan,
            last_attempt_failure_reasons: Vec::from_iter(
                stage.last_attempt_failure_reasons.clone(),
            ),
            shuffle_reader_options: Some(stage.shuffle_reader_options.as_ref().into()),
        })
    }
}

impl Debug for UnresolvedStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);

        write!(
            f,
            "=========UnResolvedStage[stage_id={}.{}, children={}]=========\nInputs{:?}\n{}",
            self.stage_id,
            self.stage_attempt_num,
            self.inputs.len(),
            self.inputs,
            plan
        )
    }
}

impl ResolvedStage {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        stage_id: usize,
        stage_attempt_num: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_partitioning: Option<Partitioning>,
        output_links: Vec<usize>,
        inputs: HashMap<usize, StageOutput>,
        last_attempt_failure_reasons: HashSet<String>,
        object_store: Option<Arc<dyn ObjectStore>>,
        clients: Arc<Cache<String, LimitedBallistaClient>>,
        shuffle_reader_options: Arc<ShuffleReaderExecOptions>,
    ) -> Self {
        let partitions = plan.output_partitioning().partition_count();

        Self {
            stage_id,
            stage_attempt_num,
            partitions,
            output_partitioning,
            output_links,
            inputs,
            plan,
            last_attempt_failure_reasons,
            resolved_at: timestamp_millis(),
            object_store,
            clients,
            shuffle_reader_options,
        }
    }

    /// Change to the running state
    pub(super) fn to_running(&self) -> RunningStage {
        RunningStage::new(
            self.stage_id,
            self.stage_attempt_num,
            self.plan.clone(),
            self.partitions,
            self.output_partitioning.clone(),
            self.output_links.clone(),
            self.inputs.clone(),
            self.resolved_at,
            self.object_store.clone(),
            self.clients.clone(),
            self.shuffle_reader_options.clone(),
        )
    }

    /// Change to the unresolved state
    pub(super) fn to_unresolved(&self) -> Result<UnresolvedStage> {
        let new_plan = crate::planner::rollback_resolved_shuffles(self.plan.clone())?;

        let unresolved = UnresolvedStage::new_with_inputs(
            self.stage_id,
            self.stage_attempt_num,
            new_plan,
            self.output_partitioning.clone(),
            self.output_links.clone(),
            self.inputs.clone(),
            self.last_attempt_failure_reasons.clone(),
            self.object_store.clone(),
            self.clients.clone(),
            self.shuffle_reader_options.clone(),
        );
        Ok(unresolved)
    }

    pub(super) fn decode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: protobuf::ResolvedStage,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
        object_store: Option<Arc<dyn ObjectStore>>,
        clients: Arc<Cache<String, LimitedBallistaClient>>,
    ) -> Result<ResolvedStage> {
        let plan_proto = U::try_decode(&stage.plan)?;
        let plan = plan_proto.try_into_physical_plan(
            session_ctx,
            session_ctx.runtime_env().as_ref(),
            codec.physical_extension_codec(),
        )?;

        let output_partitioning: Option<Partitioning> = parse_protobuf_hash_partitioning(
            stage.output_partitioning.as_ref(),
            session_ctx,
            plan.schema().as_ref(),
        )?;

        let inputs = decode_inputs(stage.inputs)?;
        let shuffle_reader_options = stage
            .shuffle_reader_options
            .as_ref()
            .ok_or_else(|| {
                BallistaError::General(
                    "ShuffleReaderExecNodeOptions is missing".to_owned(),
                )
            })?
            .into();
        Ok(ResolvedStage {
            stage_id: stage.stage_id as usize,
            stage_attempt_num: stage.stage_attempt_num as usize,
            partitions: stage.partitions as usize,
            output_partitioning,
            output_links: stage.output_links.into_iter().map(|l| l as usize).collect(),
            inputs,
            plan,
            last_attempt_failure_reasons: HashSet::from_iter(
                stage.last_attempt_failure_reasons,
            ),
            resolved_at: stage.resolved_at,
            object_store,
            clients,
            shuffle_reader_options: Arc::new(shuffle_reader_options),
        })
    }

    pub(super) fn encode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: &ResolvedStage,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::ResolvedStage> {
        let mut plan: Vec<u8> = vec![];
        U::try_from_physical_plan(stage.plan.clone(), codec.physical_extension_codec())
            .and_then(|proto| proto.try_encode(&mut plan))?;

        let output_partitioning =
            hash_partitioning_to_proto(stage.output_partitioning.as_ref())?;

        let inputs = encode_inputs(&stage.inputs)?;
        let shuffle_reader_options = stage.shuffle_reader_options.as_ref().into();
        Ok(protobuf::ResolvedStage {
            stage_id: stage.stage_id as u32,
            stage_attempt_num: stage.stage_attempt_num as u32,
            partitions: stage.partitions as u32,
            output_partitioning,
            output_links: stage.output_links.iter().map(|l| *l as u32).collect(),
            inputs,
            plan,
            last_attempt_failure_reasons: Vec::from_iter(
                stage.last_attempt_failure_reasons.clone(),
            ),
            resolved_at: stage.resolved_at,
            shuffle_reader_options: Some(shuffle_reader_options),
        })
    }
}

impl Debug for ResolvedStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);

        write!(
            f,
            "=========ResolvedStage[stage_id={}.{}, partitions={}]=========\n{}",
            self.stage_id, self.stage_attempt_num, self.partitions, plan
        )
    }
}

impl RunningStage {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        stage_id: usize,
        stage_attempt_num: usize,
        plan: Arc<dyn ExecutionPlan>,
        partitions: usize,
        output_partitioning: Option<Partitioning>,
        output_links: Vec<usize>,
        inputs: HashMap<usize, StageOutput>,
        resolved_at: u64,
        object_store: Option<Arc<dyn ObjectStore>>,
        clients: Arc<Cache<String, LimitedBallistaClient>>,
        shuffle_reader_options: Arc<ShuffleReaderExecOptions>,
    ) -> Self {
        Self {
            stage_id,
            stage_attempt_num,
            partitions,
            output_partitioning,
            output_links,
            inputs,
            plan,
            task_infos: vec![None; partitions],
            task_failures: 0,
            stage_metrics: None,
            resolved_at,
            object_store,
            clients,
            shuffle_reader_options,
        }
    }

    pub(super) fn to_successful(&self) -> SuccessfulStage {
        let task_infos = self
            .task_infos
            .iter()
            .enumerate()
            .map(|(partition_id, info)| {
                info.clone().unwrap_or_else(|| {
                    panic!(
                        "TaskInfo for task {}.{}/{} should not be none",
                        self.stage_id, self.stage_attempt_num, partition_id
                    )
                })
            })
            .collect();
        let stage_metrics = self.stage_metrics.clone().unwrap_or_else(|| {
            warn!("The metrics for stage {} should not be none", self.stage_id);
            vec![]
        });
        SuccessfulStage {
            stage_id: self.stage_id,
            stage_attempt_num: self.stage_attempt_num,
            partitions: self.partitions,
            output_partitioning: self.output_partitioning.clone(),
            output_links: self.output_links.clone(),
            inputs: self.inputs.clone(),
            plan: self.plan.clone(),
            task_infos,
            stage_metrics,
            object_store: self.object_store.clone(),
            clients: self.clients.clone(),
            shuffle_reader_options: self.shuffle_reader_options.clone(),
        }
    }

    pub(super) fn to_failed(&self, error_message: String) -> FailedStage {
        FailedStage {
            stage_id: self.stage_id,
            stage_attempt_num: self.stage_attempt_num,
            partitions: self.partitions,
            output_partitioning: self.output_partitioning.clone(),
            output_links: self.output_links.clone(),
            plan: self.plan.clone(),
            task_infos: self.task_infos.clone(),
            stage_metrics: self.stage_metrics.clone(),
            error_message,
        }
    }

    /// Change to the resolved state and bump the stage attempt number
    pub(super) fn to_resolved(&self) -> ResolvedStage {
        ResolvedStage::new(
            self.stage_id,
            self.stage_attempt_num + 1,
            self.plan.clone(),
            self.output_partitioning.clone(),
            self.output_links.clone(),
            self.inputs.clone(),
            HashSet::new(),
            self.object_store.clone(),
            self.clients.clone(),
            self.shuffle_reader_options.clone(),
        )
    }

    /// Change to the unresolved state and bump the stage attempt number
    pub(super) fn to_unresolved(
        &self,
        failure_reasons: HashSet<String>,
    ) -> Result<UnresolvedStage> {
        let new_plan = crate::planner::rollback_resolved_shuffles(self.plan.clone())?;

        let unresolved = UnresolvedStage::new_with_inputs(
            self.stage_id,
            self.stage_attempt_num + 1,
            new_plan,
            self.output_partitioning.clone(),
            self.output_links.clone(),
            self.inputs.clone(),
            failure_reasons,
            self.object_store.clone(),
            self.clients.clone(),
            self.shuffle_reader_options.clone(),
        );
        Ok(unresolved)
    }

    /// Returns `true` if all tasks for this stage are successful
    pub(super) fn is_successful(&self) -> bool {
        self.task_infos.iter().all(|info| {
            matches!(
                info,
                Some(TaskInfo {
                    task_status: task_status::Status::Successful(_),
                    ..
                })
            )
        })
    }

    /// Returns the number of successful tasks
    pub(super) fn successful_tasks(&self) -> usize {
        self.task_infos
            .iter()
            .filter(|info| {
                matches!(
                    info,
                    Some(TaskInfo {
                        task_status: task_status::Status::Successful(_),
                        ..
                    })
                )
            })
            .count()
    }

    /// Returns the number of scheduled tasks
    pub(super) fn scheduled_tasks(&self) -> usize {
        self.task_infos.iter().filter(|s| s.is_some()).count()
    }

    /// Returns a vector of currently running tasks in this stage
    pub(super) fn running_tasks(&self) -> Vec<(usize, usize, usize, String)> {
        self.task_infos
            .iter()
            .enumerate()
            .filter_map(|(partition, info)| match info {
                Some(TaskInfo {task_id,
                         task_status: task_status::Status::Running(RunningTask { executor_id }), ..}) => {
                    Some((*task_id, self.stage_id, partition, executor_id.clone()))
                }
                _ => None,
            })
            .collect()
    }

    /// Returns the number of tasks in this stage which are available for scheduling.
    /// If the stage is not yet resolved, then this will return `0`, otherwise it will
    /// return the number of tasks where the task info is not yet set.
    pub(super) fn available_tasks(&self) -> usize {
        self.task_infos.iter().filter(|s| s.is_none()).count()
    }

    /// Update the TaskInfo for task partition
    pub(super) fn update_task_info(&mut self, status: &TaskStatus) -> bool {
        let task_status = status.status.as_ref().unwrap();
        for partition_id in &status.partitions {
            debug!("Updating TaskInfo for partition {}", partition_id);
            let task_info = self.task_infos[*partition_id as usize].as_ref().unwrap();
            let task_id = task_info.task_id;

            let scheduled_time = task_info.scheduled_time;

            let updated_task_info = TaskInfo {
                task_id,
                scheduled_time,
                launch_time: status.launch_time as u128,
                start_exec_time: status.start_exec_time as u128,
                end_exec_time: status.end_exec_time as u128,
                finish_time: timestamp_millis() as u128,
                task_status: task_status.clone(),
            };
            self.task_infos[*partition_id as usize] = Some(updated_task_info);
        }

        true
    }

    /// update and combine the task metrics to the stage metrics
    pub(super) fn update_task_metrics(
        &mut self,
        metrics: Vec<OperatorMetricsSet>,
    ) -> Result<()> {
        // For some cases, task metrics not set, especially for testings.
        if metrics.is_empty() {
            return Ok(());
        }

        let new_metrics_set = if let Some(combined_metrics) = &mut self.stage_metrics {
            if metrics.len() != combined_metrics.len() {
                return Err(BallistaError::Internal(format!("Error updating task metrics to stage {}, task metrics array size {} does not equal \
                with the stage metrics array size {}", self.stage_id, metrics.len(), combined_metrics.len())));
            }
            let metrics_values_array = metrics
                .into_iter()
                .map(|ms| {
                    ms.metrics
                        .into_iter()
                        .map(|m| m.try_into())
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;

            combined_metrics
                .iter_mut()
                .zip(metrics_values_array)
                .map(|(first, second)| Self::combine_metrics_set(first, second))
                .collect()
        } else {
            metrics
                .into_iter()
                .map(|ms| ms.try_into())
                .collect::<Result<Vec<_>>>()?
        };
        self.stage_metrics = Some(new_metrics_set);

        Ok(())
    }

    pub(super) fn combine_metrics_set(
        first: &mut MetricsSet,
        second: Vec<MetricValue>,
    ) -> MetricsSet {
        for metric_value in second {
            // TODO recheck the lable logic
            let new_metric = Arc::new(Metric::new(metric_value, None));
            first.push(new_metric);
        }
        first.aggregate_by_name()
    }

    /// Reset the task info for the given task ID. This should be called when a task failed and need to be
    /// re-scheduled.
    pub fn reset_task_info(&mut self, partitions: impl Iterator<Item = usize>) {
        for partition in partitions {
            self.task_infos[partition] = None;
        }
    }

    /// Reset the running and completed tasks on a given executor
    /// Returns the number of running tasks that were reset
    pub fn reset_tasks(&mut self, executor: &str) -> usize {
        let mut reset = 0;
        for task in self.task_infos.iter_mut() {
            match task {
                Some(TaskInfo {
                    task_status: task_status::Status::Running(RunningTask { executor_id }),
                    ..
                }) if *executor == *executor_id => {
                    *task = None;
                    reset += 1;
                }
                Some(TaskInfo {
                    task_status:
                        task_status::Status::Successful(SuccessfulTask {
                            executor_id,
                            partitions: _,
                        }),
                    ..
                }) if *executor == *executor_id => {
                    *task = None;
                    reset += 1;
                }
                _ => {}
            }
        }
        reset
    }

    /// Remove input partitions from an input stage on a given executor.
    /// Return the HashSet of removed map partition ids
    pub(super) fn remove_input_partitions(
        &mut self,
        input_stage_id: usize,
        executor_id: &str,
    ) -> Result<HashSet<usize>> {
        if let Some(stage_output) = self.inputs.get_mut(&input_stage_id) {
            let mut bad_map_partitions = HashSet::new();
            stage_output
                .partition_locations
                .iter_mut()
                .for_each(|(_partition, locs)| {
                    locs.iter().for_each(|loc| {
                        if loc.executor_meta.id == executor_id {
                            bad_map_partitions.extend(&loc.map_partitions);
                        }
                    });

                    locs.retain(|loc| loc.executor_meta.id != executor_id);
                });
            stage_output.complete = false;
            Ok(bad_map_partitions)
        } else {
            Err(BallistaError::Internal(format!("Error remove input partition for Stage {}, {} is not a valid child stage ID", self.stage_id, input_stage_id)))
        }
    }
}

impl Debug for RunningStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);

        write!(
            f,
            "=========RunningStage[stage_id={}.{}, partitions={}, successful_tasks={}, scheduled_tasks={}, available_tasks={}]=========\n{}",
            self.stage_id,
            self.stage_attempt_num,
            self.partitions,
            self.successful_tasks(),
            self.scheduled_tasks(),
            self.available_tasks(),
            plan
        )
    }
}

impl SuccessfulStage {
    /// Change to the running state and bump the stage attempt number
    pub fn to_running(&self) -> RunningStage {
        let mut task_infos: Vec<Option<TaskInfo>> = Vec::new();
        for task in self.task_infos.iter() {
            match task {
                TaskInfo {
                    task_status: task_status::Status::Successful(_),
                    ..
                } => task_infos.push(Some(task.clone())),
                _ => task_infos.push(None),
            }
        }
        let stage_metrics = if self.stage_metrics.is_empty() {
            None
        } else {
            Some(self.stage_metrics.clone())
        };
        RunningStage {
            stage_id: self.stage_id,
            stage_attempt_num: self.stage_attempt_num + 1,
            partitions: self.partitions,
            output_partitioning: self.output_partitioning.clone(),
            output_links: self.output_links.clone(),
            inputs: self.inputs.clone(),
            plan: self.plan.clone(),
            task_failures: 0,
            task_infos,
            stage_metrics,
            resolved_at: timestamp_millis(),
            object_store: self.object_store.clone(),
            clients: self.clients.clone(),
            shuffle_reader_options: self.shuffle_reader_options.clone(),
        }
    }

    pub(super) fn decode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: protobuf::SuccessfulStage,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
        object_store: Option<Arc<dyn ObjectStore>>,
        clients: Arc<Cache<String, LimitedBallistaClient>>,
    ) -> Result<SuccessfulStage> {
        let plan_proto = U::try_decode(&stage.plan)?;
        let plan = plan_proto.try_into_physical_plan(
            session_ctx,
            session_ctx.runtime_env().as_ref(),
            codec.physical_extension_codec(),
        )?;

        let output_partitioning: Option<Partitioning> = parse_protobuf_hash_partitioning(
            stage.output_partitioning.as_ref(),
            session_ctx,
            plan.schema().as_ref(),
        )?;

        let inputs = decode_inputs(stage.inputs)?;
        assert_eq!(
            stage.task_infos.len(),
            stage.partitions as usize,
            "protobuf::SuccessfulStage task_infos len not equal to partitions."
        );
        let task_infos = stage.task_infos.into_iter().map(decode_taskinfo).collect();
        let stage_metrics = stage
            .stage_metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>>>()?;
        let shuffle_reader_options = stage
            .shuffle_reader_options
            .as_ref()
            .ok_or_else(|| {
                BallistaError::General("ShuffleReaderExecOptions is missing".to_owned())
            })?
            .into();
        Ok(SuccessfulStage {
            stage_id: stage.stage_id as usize,
            stage_attempt_num: stage.stage_attempt_num as usize,
            partitions: stage.partitions as usize,
            output_partitioning,
            output_links: stage.output_links.into_iter().map(|l| l as usize).collect(),
            inputs,
            plan,
            task_infos,
            stage_metrics,
            object_store,
            clients,
            shuffle_reader_options: Arc::new(shuffle_reader_options),
        })
    }

    pub(super) fn encode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: &SuccessfulStage,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::SuccessfulStage> {
        let stage_id = stage.stage_id;

        let mut plan: Vec<u8> = vec![];
        U::try_from_physical_plan(stage.plan.clone(), codec.physical_extension_codec())
            .and_then(|proto| proto.try_encode(&mut plan))?;

        let output_partitioning =
            hash_partitioning_to_proto(stage.output_partitioning.as_ref())?;

        let inputs = encode_inputs(&stage.inputs)?;
        let task_infos = stage
            .task_infos
            .iter()
            .enumerate()
            .map(|(partition, task_info)| encode_taskinfo(task_info, partition))
            .collect();

        let stage_metrics = stage
            .stage_metrics
            .iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>>>()?;
        let shuffle_reader_options = stage.shuffle_reader_options.as_ref().into();
        Ok(protobuf::SuccessfulStage {
            stage_id: stage_id as u32,
            stage_attempt_num: stage.stage_attempt_num as u32,
            partitions: stage.partitions as u32,
            output_partitioning,
            output_links: stage.output_links.iter().map(|l| *l as u32).collect(),
            inputs,
            plan,
            task_infos,
            stage_metrics,
            shuffle_reader_options: Some(shuffle_reader_options),
        })
    }
}

impl Debug for SuccessfulStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableBallistaExecutionPlan::new(
            self.plan.as_ref(),
            &self.stage_metrics,
        )
        .indent();

        write!(
            f,
            "=========SuccessfulStage[stage_id={}.{}, partitions={}]=========\n{}",
            self.stage_id, self.stage_attempt_num, self.partitions, plan
        )
    }
}

impl FailedStage {
    /// Returns the number of successful tasks
    pub(super) fn successful_tasks(&self) -> usize {
        self.task_infos
            .iter()
            .filter(|info| {
                matches!(
                    info,
                    Some(TaskInfo {
                        task_status: task_status::Status::Successful(_),
                        ..
                    })
                )
            })
            .count()
    }
    /// Returns the number of scheduled tasks
    pub(super) fn scheduled_tasks(&self) -> usize {
        self.task_infos.iter().filter(|s| s.is_some()).count()
    }

    /// Returns the number of tasks in this stage which are available for scheduling.
    /// If the stage is not yet resolved, then this will return `0`, otherwise it will
    /// return the number of tasks where the task status is not yet set.
    pub(super) fn available_tasks(&self) -> usize {
        self.task_infos.iter().filter(|s| s.is_none()).count()
    }

    pub(super) fn decode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: protobuf::FailedStage,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
    ) -> Result<FailedStage> {
        let plan_proto = U::try_decode(&stage.plan)?;
        let plan = plan_proto.try_into_physical_plan(
            session_ctx,
            session_ctx.runtime_env().as_ref(),
            codec.physical_extension_codec(),
        )?;

        let output_partitioning: Option<Partitioning> = parse_protobuf_hash_partitioning(
            stage.output_partitioning.as_ref(),
            session_ctx,
            plan.schema().as_ref(),
        )?;

        let mut task_infos: Vec<Option<TaskInfo>> = vec![None; stage.partitions as usize];
        for info in stage.task_infos {
            task_infos[info.partition_id as usize] = Some(decode_taskinfo(info.clone()));
        }

        let stage_metrics = if stage.stage_metrics.is_empty() {
            None
        } else {
            let ms = stage
                .stage_metrics
                .into_iter()
                .map(|m| m.try_into())
                .collect::<Result<Vec<_>>>()?;
            Some(ms)
        };

        Ok(FailedStage {
            stage_id: stage.stage_id as usize,
            stage_attempt_num: stage.stage_attempt_num as usize,
            partitions: stage.partitions as usize,
            output_partitioning,
            output_links: stage.output_links.into_iter().map(|l| l as usize).collect(),
            plan,
            task_infos,
            stage_metrics,
            error_message: stage.error_message,
        })
    }

    pub(super) fn encode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: &FailedStage,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::FailedStage> {
        let stage_id = stage.stage_id;

        let mut plan: Vec<u8> = vec![];
        U::try_from_physical_plan(stage.plan.clone(), codec.physical_extension_codec())
            .and_then(|proto| proto.try_encode(&mut plan))?;

        let output_partitioning =
            hash_partitioning_to_proto(stage.output_partitioning.as_ref())?;

        let task_infos: Vec<protobuf::TaskInfo> = stage
            .task_infos
            .iter()
            .enumerate()
            .filter_map(|(partition, task_info)| {
                task_info
                    .as_ref()
                    .map(|info| encode_taskinfo(info, partition))
            })
            .collect();

        let stage_metrics = match stage.stage_metrics.as_ref() {
            Some(metrics) => metrics
                .iter()
                .map(|m| m.try_into())
                .collect::<Result<Vec<_>>>()?,
            _ => Vec::default(),
        };

        Ok(protobuf::FailedStage {
            stage_id: stage_id as u32,
            stage_attempt_num: stage.stage_attempt_num as u32,
            partitions: stage.partitions as u32,
            output_partitioning,
            output_links: stage.output_links.iter().map(|l| *l as u32).collect(),
            plan,
            task_infos,
            stage_metrics,
            error_message: stage.error_message.clone(),
        })
    }
}

impl Debug for FailedStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);

        write!(
            f,
            "=========FailedStage[stage_id={}.{}, partitions={}, successful_tasks={}, scheduled_tasks={}, available_tasks={}, error_message={}]=========\n{}",
            self.stage_id,
            self.stage_attempt_num,
            self.partitions,
            self.successful_tasks(),
            self.scheduled_tasks(),
            self.available_tasks(),
            self.error_message,
            plan
        )
    }
}

/// This data structure collects the partition locations for an `ExecutionStage`.
/// Each `ExecutionStage` will hold a `StageOutput`s for each of its child stages.
/// When all tasks for the child stage are complete, it will mark the `StageOutput`
/// as complete.
#[derive(Clone, Debug, Default)]
pub(crate) struct StageOutput {
    /// Map from partition -> partition locations
    pub partition_locations: HashMap<usize, Vec<PartitionLocation>>,
    /// Flag indicating whether all tasks are complete
    pub complete: bool,
}

impl StageOutput {
    pub(super) fn new() -> Self {
        Self {
            partition_locations: HashMap::new(),
            complete: false,
        }
    }

    /// Add a `PartitionLocation` to the `StageOutput`
    pub(super) fn add_partition(&mut self, partition_location: PartitionLocation) {
        if let Some(parts) = self
            .partition_locations
            .get_mut(&partition_location.output_partition)
        {
            parts.push(partition_location)
        } else {
            self.partition_locations.insert(
                partition_location.output_partition,
                vec![partition_location],
            );
        }
    }

    pub(super) fn is_complete(&self) -> bool {
        self.complete
    }
}

fn decode_inputs(
    stage_inputs: Vec<GraphStageInput>,
) -> Result<HashMap<usize, StageOutput>> {
    let mut inputs: HashMap<usize, StageOutput> = HashMap::new();
    for input in stage_inputs {
        let stage_id = input.stage_id as usize;

        let outputs = input
            .partition_locations
            .into_iter()
            .map(|loc| {
                let partition = loc.partition as usize;
                let locations = loc
                    .partition_location
                    .iter()
                    .map(|l| l.try_into())
                    .collect::<Result<Vec<_>>>()?;
                Ok((partition, locations))
            })
            .collect::<Result<HashMap<usize, Vec<PartitionLocation>>>>()?;

        inputs.insert(
            stage_id,
            StageOutput {
                partition_locations: outputs,
                complete: input.complete,
            },
        );
    }
    Ok(inputs)
}

fn encode_inputs(
    stage_inputs: &HashMap<usize, StageOutput>,
) -> Result<Vec<GraphStageInput>> {
    let mut inputs: Vec<protobuf::GraphStageInput> = vec![];
    for (stage_id, output) in stage_inputs.iter() {
        inputs.push(protobuf::GraphStageInput {
            stage_id: *stage_id as u32,
            partition_locations: output
                .partition_locations
                .iter()
                .map(|(partition, locations)| {
                    Ok(protobuf::TaskInputPartitions {
                        partition: *partition as u32,
                        partition_location: locations
                            .iter()
                            .map(|l| l.try_into())
                            .collect::<Result<Vec<_>>>()?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            complete: output.complete,
        });
    }
    Ok(inputs)
}

fn decode_taskinfo(task_info: protobuf::TaskInfo) -> TaskInfo {
    let task_info_status = match task_info.status {
        Some(task_info::Status::Running(running)) => {
            task_status::Status::Running(running)
        }
        Some(task_info::Status::Failed(failed)) => task_status::Status::Failed(failed),
        Some(task_info::Status::Successful(success)) => {
            task_status::Status::Successful(success)
        }
        _ => panic!(
            "protobuf::TaskInfo status for task {} should not be none",
            task_info.task_id
        ),
    };
    TaskInfo {
        task_id: task_info.task_id as usize,
        scheduled_time: task_info.scheduled_time as u128,
        launch_time: task_info.launch_time as u128,
        start_exec_time: task_info.start_exec_time as u128,
        end_exec_time: task_info.end_exec_time as u128,
        finish_time: task_info.finish_time as u128,
        task_status: task_info_status,
    }
}

fn encode_taskinfo(task_info: &TaskInfo, partition_id: usize) -> protobuf::TaskInfo {
    let task_info_status = match &task_info.task_status {
        task_status::Status::Running(running) => {
            task_info::Status::Running(running.clone())
        }
        task_status::Status::Failed(failed) => task_info::Status::Failed(failed.clone()),
        task_status::Status::Successful(success) => {
            task_info::Status::Successful(success.clone())
        }
    };
    protobuf::TaskInfo {
        task_id: task_info.task_id as u32,
        partition_id: partition_id as u32,
        scheduled_time: task_info.scheduled_time as u64,
        launch_time: task_info.launch_time as u64,
        start_exec_time: task_info.start_exec_time as u64,
        end_exec_time: task_info.end_exec_time as u64,
        finish_time: task_info.finish_time as u64,
        status: Some(task_info_status),
    }
}
