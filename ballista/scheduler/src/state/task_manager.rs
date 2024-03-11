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

use crate::scheduler_server::event::QueryStageSchedulerEvent;

use crate::state::execution_graph::{ExecutionGraph, RunningTaskInfo, TaskDescription};
use crate::state::executor_manager::{ExecutorManager, ExecutorReservation};

use ballista_core::client::LimitedBallistaClient;
use ballista_core::error::BallistaError;
use ballista_core::error::Result;
use ballista_core::execution_plans::ShuffleReaderExecOptions;
use datafusion::config::{ConfigEntry, ConfigOptions};
use futures::future::try_join_all;
use moka::future::Cache;
use object_store::ObjectStore;

use crate::cluster::JobState;
use ballista_core::serde::protobuf::{
    self, execution_error, job_status, ExecutionError, FailedJob, JobOverview, JobStatus,
    KeyValuePair, QueuedJob, TaskDefinition, TaskStatus,
};
use ballista_core::serde::scheduler::to_proto::hash_partitioning_to_proto;
use ballista_core::serde::scheduler::ExecutorMetadata;
use ballista_core::serde::BallistaCodec;
use dashmap::DashMap;
use datafusion::physical_plan::ExecutionPlan;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;

use datafusion::prelude::SessionContext;
use itertools::Itertools;
use lazy_static::lazy_static;
use prometheus::{register_histogram, Histogram};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use tokio::sync::{watch, RwLock, RwLockWriteGuard};

use crate::scheduler_server::timestamp_millis;
use ballista_core::event_loop::EventSender;
use ballista_core::physical_optimizer::OptimizeTaskGroup;
use tracing::trace;

lazy_static! {
    static ref TASK_IDELE_TIME: Histogram = register_histogram!(
        "scheduler_task_idle_time",
        "Time tasks are idle before being scheduler",
        vec![0.1, 1., 2., 3., 4., 5., 10., 20., 30., 60., 120., 240.]
    )
    .unwrap();
}

#[derive(Clone, Debug)]
struct ActiveJob {
    id: String,
    pass: f64,
    scheduled_tasks: usize,
    available_tasks: usize,
}

impl ActiveJob {
    pub fn update_pass(&mut self, scheduled_task_slots: usize) {
        self.scheduled_tasks = scheduled_task_slots;
        self.pass += scheduled_task_slots as f64 / self.available_tasks as f64;
    }
}

impl PartialOrd for ActiveJob {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ActiveJob {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_pass = ((self.scheduled_tasks as f64) / (self.available_tasks as f64)).floor();
        let other_pass = ((other.scheduled_tasks as f64) / (other.available_tasks as f64)).floor();

        if self_pass >= 1.0 && other_pass >= 1.0 {
            std::cmp::Ordering::Equal
        } else if self_pass >= 1.0 {
            std::cmp::Ordering::Less
        } else if other_pass >= 1.0 {
            std::cmp::Ordering::Greater
        } else {
            self.pass.partial_cmp(&other.pass).unwrap_or(std::cmp::Ordering::Equal)
                .then(other.available_tasks.cmp(&self.available_tasks))
        }
    }
}

impl PartialEq<Self> for ActiveJob {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.pass == other.pass
            && self.scheduled_tasks == other.scheduled_tasks
            && self.available_tasks == other.available_tasks
    }
}

impl Eq for ActiveJob {}

struct TaskQueue {
    inner: BinaryHeap<ActiveJob>,
    jobs: ActiveJobCache,
    global_pass: f64,
    global_scheduled_tasks: usize,
    global_available_tasks: usize,
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            inner: BinaryHeap::new(),
            jobs: Arc::new(DashMap::new()),
            global_pass: 0.0,
            global_scheduled_tasks: 0,
            global_available_tasks: 0,
        }
    }

    pub fn submit(&mut self, graph: ExecutionGraph, tokens: usize) {
        let job_id = graph.job_id().to_owned();
        let active_job = ActiveJob {
            id: job_id.clone(),
            // progress: self.global_progress,
            pass: 0.0,
            scheduled_tasks: 0,
            available_tasks: tokens,
        };

        self.global_available_tasks += tokens;

        self.inner.push(active_job);

        self.jobs
            .insert(job_id, JobInfoCache::new(graph));
    }

    pub fn pop(&mut self) -> Option<(ActiveJob, JobInfoCache)> {
        self.inner.pop().and_then(|job| self.jobs.get(&job.id)
            .map(|info_ref| (job, info_ref.value().clone())))
    }

    pub fn push_job(&mut self, job: ActiveJob) {
        self.inner.push(job);
    }

    /*
    pub fn push(&mut self, job_id: String, tokens: usize) {
        let active_job = ActiveJob {
            id: job_id.clone(),
            pass: self.global_pass,
            tokens,
        };
        self.inner.push(active_job);
    }
    */

    pub fn update_global_pass(&mut self, scheduled_task_slots: usize) {
        self.global_scheduled_tasks += scheduled_task_slots;
        self.global_pass = self.global_scheduled_tasks as f64 / self.global_available_tasks as f64;
    }

    pub fn jobs(&self) -> &ActiveJobCache {
        &self.jobs
    }

    pub fn get_job(&self, job_id: &str) -> Option<JobInfoCache> {
        self.jobs.get(job_id).map(|info| info.clone())
    }

    pub fn remove(&self, job_id: &str) -> Option<JobInfoCache> {
        self.jobs.remove(job_id).map(|(_, job)| job)
    }

    pub fn size(&self) -> usize {
        self.jobs.len()
    }

    pub fn pending_tasks(&self) -> usize {
        let mut count = 0;
        for job in self.jobs.iter() {
            count += job.pending_tasks.load(Ordering::Acquire);
        }

        count
    }
}

type ActiveJobCache = Arc<DashMap<String, JobInfoCache>>;
/*
#[derive(Default)]
struct ActiveJobQueue {
    queue: SegQueue<String>,
    task_queue: TaskQueue,
    jobs: ActiveJobCache,
}

impl ActiveJobQueue {
    pub fn pop(&self) -> Option<ActiveJobRef> {
        loop {
            if let Some(job_id) = self.queue.pop() {
                if let Some(job_info) = self.jobs.get(&job_id) {
                    return Some(ActiveJobRef {
                        queue: &self.queue,
                        job: job_info.clone(),
                        job_id,
                    });
                } else {
                    continue;
                }
            } else {
                return None;
            }
        }
    }

    pub fn pending_tasks(&self) -> usize {
        let mut count = 0;
        for job in self.jobs.iter() {
            count += job.pending_tasks.load(Ordering::Acquire);
        }

        count
    }

    pub fn push(&self, job_id: String, graph: ExecutionGraph) {
        self.jobs.insert(job_id.clone(), JobInfoCache::new(graph));
        self.queue.push(job_id);
    }

    pub fn jobs(&self) -> &ActiveJobCache {
        &self.jobs
    }

    pub fn get_job(&self, job_id: &str) -> Option<JobInfoCache> {
        self.jobs.get(job_id).map(|info| info.clone())
    }

    pub fn remove(&self, job_id: &str) -> Option<JobInfoCache> {
        self.jobs.remove(job_id).map(|(_, job)| job)
    }

    pub fn size(&self) -> usize {
        self.jobs.len()
    }
}
*/

/*
struct ActiveJobRef<'a> {
    queue: &'a SegQueue<String>,
    job: JobInfoCache,
    job_id: String,
}

impl<'a> Deref for ActiveJobRef<'a> {
    type Target = JobInfoCache;

    fn deref(&self) -> &Self::Target {
        &self.job
    }
}

impl<'a> Drop for ActiveJobRef<'a> {
    fn drop(&mut self) {
        self.queue.push(std::mem::take(&mut self.job_id));
    }
}
*/

// TODO move to configuration file
/// Default max failure attempts for task level retry
pub const TASK_MAX_FAILURES: usize = 4;
/// Default max failure attempts for stage level retry
pub const STAGE_MAX_FAILURES: usize = 4;

#[async_trait::async_trait]
pub trait TaskLauncher<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>:
Send + Sync + 'static
{
    fn prepare_task_definition(
        &self,
        ctx: Arc<SessionContext>,
        task: &TaskDescription,
    ) -> Result<TaskDefinition>;

    async fn launch_tasks(
        &self,
        executor: &ExecutorMetadata,
        tasks: &[TaskDescription],
        executor_manager: &ExecutorManager,
    ) -> Result<()>;
}

struct DefaultTaskLauncher<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    scheduler_id: String,
    state: Arc<dyn JobState>,
    codec: BallistaCodec<T, U>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> DefaultTaskLauncher<T, U> {
    pub fn new(
        scheduler_id: String,
        state: Arc<dyn JobState>,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        Self {
            scheduler_id,
            state,
            codec,
        }
    }
}

#[async_trait::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskLauncher<T, U>
for DefaultTaskLauncher<T, U>
{
    fn prepare_task_definition(
        &self,
        ctx: Arc<SessionContext>,
        task: &TaskDescription,
    ) -> Result<TaskDefinition> {
        let job_id = task.partitions.job_id.clone();
        let stage_id = task.partitions.stage_id;

        debug!(job_id, stage_id, "Preparing task definition for {:?}", task);

        let props = ctx
            .state()
            .config_options()
            .entries()
            .into_iter()
            .filter_map(|ConfigEntry { key, value, .. }| {
                value.map(|value| KeyValuePair { key, value })
            })
            .collect();

        let optimizer = OptimizeTaskGroup::new(task.partitions.partitions.clone());

        let group_plan =
            optimizer.optimize(task.plan.clone(), &ConfigOptions::default())?;

        let mut plan: Vec<u8> = vec![];
        let plan_proto =
            U::try_from_physical_plan(group_plan, self.codec.physical_extension_codec())?;
        plan_proto.try_encode(&mut plan)?;

        let output_partitioning =
            hash_partitioning_to_proto(task.output_partitioning.as_ref())?;

        Ok(TaskDefinition {
            task_id: task.task_id as u32,
            job_id,
            stage_id: stage_id as u32,
            stage_attempt_num: task.stage_attempt_num as u32,
            partitions: task
                .partitions
                .partitions
                .iter()
                .map(|p| *p as u32)
                .collect(),
            plan,
            output_partitioning,
            session_id: task.session_id.clone(),
            launch_time: timestamp_millis(),
            props,
        })
    }

    async fn launch_tasks(
        &self,
        executor: &ExecutorMetadata,
        tasks: &[TaskDescription],
        executor_manager: &ExecutorManager,
    ) -> Result<()> {
        if log::max_level() >= log::Level::Info {
            let tasks_ids: Vec<String> = tasks
                .iter()
                .map(|task| {
                    format!(
                        "{}/{}/{:?}",
                        task.partitions.job_id,
                        task.partitions.stage_id,
                        task.partitions.partitions
                    )
                })
                .collect();
            info!(
                "Launching tasks on executor {:?} for {:?}",
                executor.id, tasks_ids
            );
        }

        let tasks = tasks.iter().map(|task_def| async {
            let ctx = self.state.get_session(&task_def.session_id).await?;
            self.prepare_task_definition(ctx, task_def)
        });

        let tasks: Vec<TaskDefinition> = try_join_all(tasks).await?;

        let mut client = executor_manager.get_client(&executor.id).await?;
        client
            .launch_task(protobuf::LaunchTaskParams {
                tasks,
                scheduler_id: self.scheduler_id.clone(),
            })
            .await?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct TaskManager<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    state: Arc<dyn JobState>,
    scheduler_id: String,
    // Cache for active jobs curated by this scheduler
    active_job_queue: Arc<RwLock<TaskQueue>>,
    launcher: Arc<dyn TaskLauncher<T, U>>,
    drained: Arc<watch::Sender<()>>,
    check_drained: watch::Receiver<()>,
    object_store: Option<Arc<dyn ObjectStore>>,
    clients: Arc<Cache<String, LimitedBallistaClient>>,
    shuffle_reader_options: Arc<ShuffleReaderExecOptions>,
}

struct ExecutionGraphWriteGuard<'a> {
    inner: RwLockWriteGuard<'a, ExecutionGraph>,
    pending_tasks: Arc<AtomicUsize>,
}

impl<'a> Deref for ExecutionGraphWriteGuard<'a> {
    type Target = ExecutionGraph;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<'a> DerefMut for ExecutionGraphWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.deref_mut()
    }
}

impl<'a> Drop for ExecutionGraphWriteGuard<'a> {
    fn drop(&mut self) {
        let tasks = self.inner.available_tasks();
        self.pending_tasks.store(tasks, Ordering::Release);
    }
}

#[derive(Clone)]
struct JobInfoCache {
    // Cache for active execution graphs curated by this scheduler
    execution_graph: Arc<RwLock<ExecutionGraph>>,
    // Cache for encoded execution stage plan to avoid duplicated encoding for multiple tasks
    #[allow(dead_code)]
    encoded_stage_plans: HashMap<usize, Vec<u8>>,
    // Number of current pending tasks for this job
    pending_tasks: Arc<AtomicUsize>,
}

impl JobInfoCache {
    fn new(graph: ExecutionGraph) -> Self {
        let pending_tasks = Arc::new(AtomicUsize::new(graph.available_tasks()));

        Self {
            execution_graph: Arc::new(RwLock::new(graph)),
            encoded_stage_plans: HashMap::new(),
            pending_tasks,
        }
    }

    pub async fn graph_mut(&self) -> ExecutionGraphWriteGuard {
        let guard = self.execution_graph.write().await;

        ExecutionGraphWriteGuard {
            inner: guard,
            pending_tasks: self.pending_tasks.clone(),
        }
    }
}

#[derive(Clone)]
pub struct UpdatedStages {
    pub resolved_stages: HashSet<usize>,
    pub successful_stages: HashSet<usize>,
    pub failed_stages: HashMap<usize, Arc<execution_error::Error>>,
    pub rollback_running_stages: HashMap<usize, HashSet<String>>,
    pub resubmit_successful_stages: HashSet<usize>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskManager<T, U> {
    pub fn new(
        state: Arc<dyn JobState>,
        codec: BallistaCodec<T, U>,
        scheduler_id: String,
        object_store: Option<Arc<dyn ObjectStore>>,
        clients: Arc<Cache<String, LimitedBallistaClient>>,
        shuffle_reader_options: Arc<ShuffleReaderExecOptions>,
    ) -> Self {
        let launcher =
            DefaultTaskLauncher::new(scheduler_id.clone(), state.clone(), codec);

        Self::with_launcher(
            state,
            scheduler_id,
            Arc::new(launcher),
            object_store,
            clients,
            shuffle_reader_options,
        )
    }

    #[allow(dead_code)]
    pub(crate) fn with_launcher(
        state: Arc<dyn JobState>,
        scheduler_id: String,
        launcher: Arc<dyn TaskLauncher<T, U>>,
        object_store: Option<Arc<dyn ObjectStore>>,
        clients: Arc<Cache<String, LimitedBallistaClient>>,
        shuffle_reader_options: Arc<ShuffleReaderExecOptions>,
    ) -> Self {
        let (drained, check_drained) = watch::channel(());

        Self {
            state,
            scheduler_id,
            active_job_queue: Arc::new(RwLock::new(TaskQueue::new())),
            launcher,
            drained: Arc::new(drained),
            check_drained,
            object_store,
            clients,
            shuffle_reader_options,
        }
    }

    /// Return the number of current pending tasks for active jobs
    /// on this scheduler
    pub async fn get_pending_task_count(&self) -> usize {
        self.active_job_queue.read().await.pending_tasks()
    }

    /// Return the count of current active jobs on this scheduler instance.
    pub async fn get_active_job_count(&self) -> usize {
        self.active_job_queue.read().await.size()
    }

    /// Enqueue a job for scheduling
    pub async fn queue_job(
        &self,
        job_id: &str,
        job_name: &str,
        queued_at: u64,
    ) -> Result<()> {
        self.state.accept_job(job_id, job_name, queued_at).await
    }

    /// Generate an ExecutionGraph for the job and save it to the persistent state.
    /// By default, this job will be curated by the scheduler which receives it.
    /// Then we will also save it to the active execution graph
    pub async fn submit_job(
        &self,
        job_id: &str,
        job_name: &str,
        session_id: &str,
        plan: Arc<dyn ExecutionPlan>,
        queued_at: u64,
        warnings: Vec<String>,
    ) -> Result<()> {
        let mut graph = ExecutionGraph::new(
            &self.scheduler_id,
            job_id,
            job_name,
            session_id,
            plan,
            queued_at,
            self.object_store.clone(),
            warnings,
            self.clients.clone(),
            self.shuffle_reader_options.clone(),
        )?;
        info!(
            job_id,
            job_name, session_id, "submitting execution graph: {:?}", graph
        );

        self.state.submit_job(job_id, &graph).await?;

        graph.revive();
        // self.active_job_queue.push(job_id.to_owned(), graph.clone());

        let tokens = graph.available_tasks();
        let mut guard = self.active_job_queue.write().await;
        guard.submit(graph, tokens);

        Ok(())
    }

    /// Get a list of active job ids
    pub async fn get_jobs(&self) -> Result<Vec<JobOverview>> {
        let job_ids = self.state.get_jobs().await?;

        let mut jobs = Vec::with_capacity(job_ids.len());

        for job_id in &job_ids {
            if let Some(cached) = self.get_active_execution_graph(job_id).await {
                let graph = cached.read().await;
                jobs.push(graph.deref().into());
            } else if let Some(graph) = self.state.get_execution_graph(job_id).await? {
                jobs.push((&graph).into());
            }
        }

        Ok(jobs)
    }

    /// Get the status of a job. First look in the active cache.
    /// If no one found, then in the Active/Completed jobs, and then in Failed jobs
    pub async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        if let Some(graph) = self.get_active_execution_graph(job_id).await {
            let guard = graph.read().await;

            Ok(Some(guard.status()))
        } else {
            self.state.get_job_status(job_id).await
        }
    }

    /// Get the execution graph of of a job. First look in the active cache.
    /// If no one found, then in the Active/Completed jobs.
    pub(crate) async fn get_job_execution_graph(
        &self,
        job_id: &str,
    ) -> Result<Option<Arc<ExecutionGraph>>> {
        if let Some(cached) = self.get_active_execution_graph(job_id).await {
            let guard = cached.read().await;

            Ok(Some(Arc::new(guard.deref().clone())))
        } else {
            let graph = self.state.get_execution_graph(job_id).await?;
            Ok(graph.map(Arc::new))
        }
    }

    /// Update given task statuses in the respective job and return a tuple containing:
    /// 1. A list of QueryStageSchedulerEvent to publish.
    /// 2. A list of reservations that can now be offered.
    pub(crate) async fn update_task_statuses(
        &self,
        executor: &ExecutorMetadata,
        task_status: Vec<TaskStatus>,
        tx_event: EventSender<QueryStageSchedulerEvent>,
    ) -> Result<()> {
        let mut job_updates: HashMap<String, Vec<TaskStatus>> = HashMap::new();
        for status in task_status {
            trace!("Task Update\n{:?}", status);
            let job_id = status.job_id.clone();
            let job_task_statuses = job_updates.entry(job_id).or_default();
            job_task_statuses.push(status);
        }

        let guard = self.active_job_queue.read().await;

        for (job_id, statuses) in job_updates {
            let num_tasks = statuses.len();
            debug!(job_id, num_tasks, "updating task statuses");


            let job_events = if let Some(job) = guard.get_job(&job_id) {
                let mut graph = job.graph_mut().await;

                graph.update_task_status(
                    executor,
                    statuses,
                    TASK_MAX_FAILURES,
                    STAGE_MAX_FAILURES,
                )?
            } else {
                // TODO Deal with curator changed case
                warn!(job_id, "job not found in active job cache");
                vec![]
            };

            for event in job_events {
                tx_event.post_event(event);
            }
        }

        Ok(())
    }

    /// Take a list of executor reservations and fill them with tasks that are ready
    /// to be scheduled.
    ///
    /// Here we use the following  algorithm:
    ///
    /// 1. For each free reservation, try to assign a task from one of the active jobs
    /// 2. If we cannot find a task in all active jobs, then add the reservation to the list of unassigned reservations
    ///
    /// Finally, we return:
    /// 1. A list of assignments which is a (Executor ID, Task) tuple
    /// 2. A list of unassigned reservations which we could not find tasks for
    /// 3. The number of pending tasks across active jobs
    pub async fn fill_reservations(
        &self,
        reservations: &[ExecutorReservation],
    ) -> Result<(
        Vec<(String, TaskDescription)>,
        Vec<ExecutorReservation>,
        usize,
    )> {
        let num_reservations = reservations.len();

        let mut free_reservations: HashMap<&String, Vec<&ExecutorReservation>> =
            reservations
                .iter()
                .group_by(|res| &res.executor_id)
                .into_iter()
                .map(|(executor_id, group)| (executor_id, group.collect()))
                .collect();

        let mut assignments: Vec<(String, TaskDescription)> = vec![];
        let mut pending_tasks = 0usize;
        let mut assign_tasks = 0usize;

        for _ in 0..self.get_active_job_count().await {
            let mut task_queue = self.active_job_queue.write().await;

            if let Some((mut active_job, job_info)) = task_queue.pop() {
                let mut graph = job_info.graph_mut().await;
                let mut scheduled_tasks_this_job = 0;

                for (exec_id, slots) in free_reservations.iter_mut() {
                    if slots.is_empty() {
                        continue;
                    }

                    if graph.is_failed() {
                        break;
                    }

                    match graph.pop_next_task(exec_id, slots.len()) {
                        Ok(Some(task)) => {
                            TASK_IDELE_TIME.observe(
                                timestamp_millis().saturating_sub(task.resolved_at) as f64
                                    / 1_000.,
                            );

                            assign_tasks += task.concurrency();
                            scheduled_tasks_this_job += task.concurrency();
                            slots.truncate(slots.len() - task.concurrency());
                            assignments.push(((*exec_id).clone(), task));
                        }
                        Ok(None) => {
                            // no more tasks to assign
                            break;
                        }
                        res @ Err(_) => {
                            // push job back into queue with updated remaining tasks
                            task_queue.push_job(active_job.clone());
                            error!(job_id = &graph.job_id(), "failed to pop next task: {:?}", res);
                            res?;
                        }
                    }
                }

                active_job.update_pass(scheduled_tasks_this_job);
                // task_queue.push(active_job.id.clone(), graph.available_tasks());
                task_queue.push_job(active_job.clone());

                if assign_tasks >= num_reservations {
                    pending_tasks += graph.available_tasks();
                    break;
                }
            } else {
                break;
            }
        }

        let mut unassigned = vec![];
        for (_, slots) in free_reservations {
            unassigned.extend(slots.into_iter().cloned());
        }

        Ok((assignments, unassigned, pending_tasks))
    }

    /// Mark a job to success. This will create a key under the CompletedJobs keyspace
    /// and remove the job from ActiveJobs
    pub(crate) async fn succeed_job(&self, job_id: &str) -> Result<()> {
        debug!(job_id, "completing job");
        if let Some(graph) = self.get_active_execution_graph(job_id).await {
            let graph = graph.read().await.clone();

            if graph.is_successful() {
                self.state.save_job(job_id, &graph).await?;

                // After state is saved, remove job from active cache
                let _ = self.remove_active_execution_graph(job_id).await;
            } else {
                error!(job_id, "cannot complete job, not finished");
                return Ok(());
            }
        } else {
            warn!(job_id, "cannot not complete job, not found in active cache");
        }

        Ok(())
    }

    /// Cancel the job and return a Vec of running tasks need to cancel
    pub(crate) async fn cancel_job(
        &self,
        job_id: &str,
    ) -> Result<(Vec<RunningTaskInfo>, usize)> {
        self.abort_job(
            job_id,
            Arc::new(execution_error::Error::Cancelled(
                execution_error::Cancelled {},
            )),
        )
            .await
    }

    /// Abort the job and return a Vec of running tasks need to cancel
    pub(crate) async fn abort_job(
        &self,
        job_id: &str,
        reason: Arc<execution_error::Error>,
    ) -> Result<(Vec<RunningTaskInfo>, usize)> {
        let (tasks_to_cancel, pending_tasks) =
            if let Some(job) = self.active_job_queue.read().await.get_job(job_id) {
                let mut guard = job.graph_mut().await;

                let pending_tasks = guard.available_tasks();
                let running_tasks = guard.running_tasks();
                info!(
                    job_id,
                    tasks = running_tasks.len(),
                    "cancelling running tasks"
                );

                guard.fail_job(reason);

                self.state.save_job(job_id, &guard).await?;

                // After state is saved, remove job from active cache
                let _ = self.remove_active_execution_graph(job_id).await;

                (running_tasks, pending_tasks)
            } else {
                // TODO listen the job state update event and fix task cancelling
                warn!(
                    job_id,
                    "cannot cancel tasks for job, not found in active cache"
                );
                (vec![], 0)
            };

        Ok((tasks_to_cancel, pending_tasks))
    }

    /// Mark a unscheduled job as failed. This will create a key under the FailedJobs keyspace
    /// and remove the job from ActiveJobs or QueuedJobs
    pub async fn fail_unscheduled_job(
        &self,
        job_id: &str,
        job_name: &str,
        queued_at: u64,
        job_error: Arc<BallistaError>,
    ) -> Result<()> {
        self.state
            .fail_unscheduled_job(job_id, job_name, queued_at, job_error)
            .await
    }

    pub async fn update_job(&self, job_id: &str) -> Result<usize> {
        debug!(job_id, "updating job");

        let mut guard = self.active_job_queue.write().await;
        if let Some(job) = guard.get_job(job_id) {
            let mut graph = job.graph_mut().await;

            let curr_available_tasks = graph.available_tasks();

            graph.revive();

            debug!(job_id, "saving job with status {:?}", graph.status());

            self.state.save_job(job_id, &graph).await?;

            let new_tasks = graph.available_tasks() - curr_available_tasks;

            guard.update_global_pass(new_tasks);

            Ok(new_tasks)
        } else {
            warn!(job_id, "cannot update, not found in active cache");

            Ok(0)
        }
    }

    /// return a Vec of running tasks need to cancel
    pub async fn executor_lost(&self, executor_id: &str) {
        for pairs in self.active_job_queue.read().await.jobs().iter() {
            let (_, job_info) = pairs.pair();
            let mut graph = job_info.graph_mut().await;
            graph.reset_stages_on_lost_executor(executor_id);
        }
    }

    /// Retrieve the number of available tasks for the given job. The value returned
    /// is strictly a point-in-time snapshot
    pub async fn get_available_task_count(&self, job_id: &str) -> Result<usize> {
        if let Some(graph) = self.get_active_execution_graph(job_id).await {
            let available_tasks = graph.read().await.available_tasks();
            Ok(available_tasks)
        } else {
            warn!(
                job_id,
                "cannot get available task count for job, not found in active cache"
            );
            Ok(0)
        }
    }

    pub async fn prepare_task_definition(
        &self,
        task: TaskDescription,
    ) -> Result<TaskDefinition> {
        let ctx = self.state.get_session(&task.session_id).await?;
        self.launcher.prepare_task_definition(ctx, &task)
    }

    /// Launch the given tasks on the specified executor
    pub(crate) async fn launch_tasks(
        &self,
        executor: &ExecutorMetadata,
        tasks: &[TaskDescription],
        executor_manager: &ExecutorManager,
    ) -> Result<()> {
        self.launcher
            .launch_tasks(executor, tasks, executor_manager)
            .await
    }

    /// Get the `ExecutionGraph` for the given job ID from cache
    pub(crate) async fn get_active_execution_graph(
        &self,
        job_id: &str,
    ) -> Option<Arc<RwLock<ExecutionGraph>>> {
        self.active_job_queue.read().await
            .get_job(job_id)
            .map(|cached| cached.execution_graph)
    }

    /// Remove the `ExecutionGraph` for the given job ID from cache
    pub(crate) async fn remove_active_execution_graph(
        &self,
        job_id: &str,
    ) -> Option<Arc<RwLock<ExecutionGraph>>> {
        let removed = self
            .active_job_queue
            .read().await
            .remove(job_id)
            .map(|value| value.execution_graph);

        if self.get_active_job_count().await == 0 {
            self.drained.send_replace(());
        }

        removed
    }

    /// Generate a new random Job ID
    pub fn generate_job_id(&self) -> String {
        let mut rng = thread_rng();
        std::iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(7)
            .collect()
    }

    /// Clean up a failed job in FailedJobs Keyspace by delayed clean_up_interval seconds
    pub(crate) fn clean_up_job_delayed(&self, job_id: String, clean_up_interval: u64) {
        if clean_up_interval == 0 {
            info!(job_id, "clean_up_interval was 0, ignoring");
            return;
        }

        let state = self.state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(clean_up_interval)).await;
            if let Err(err) = state.remove_job(&job_id).await {
                error!(job_id, error = %err, "failed to remove job");
            }
        });
    }

    pub async fn wait_drained(&self) {
        let mut check_drained = self.check_drained.clone();

        loop {
            if self.get_active_job_count().await == 0 {
                break;
            }

            if check_drained.changed().await.is_err() {
                break;
            };
        }
    }

    pub async fn trip_circuit_breaker(
        &self,
        job_id: String,
        stage_id: usize,
        labels: Vec<String>,
        preempt_stage: bool,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        if let Some(job) = self.active_job_queue.read().await.get_job(&job_id) {
            let mut graph = job.graph_mut().await;
            graph.trip_stage(stage_id, labels, preempt_stage)
        } else {
            Ok(vec![])
        }
    }
}

pub trait JobOverviewExt {
    fn is_running(&self) -> bool;
    fn queued(job_id: String, job_name: String, queued_at: u64) -> JobOverview;
    fn failed(
        job_id: String,
        job_name: String,
        queued_at: u64,
        reason: Arc<BallistaError>,
    ) -> JobOverview;
}

impl JobOverviewExt for JobOverview {
    fn is_running(&self) -> bool {
        matches!(
            self.status,
            Some(JobStatus {
                status: Some(job_status::Status::Running(_)),
                ..
            })
        )
    }

    fn queued(job_id: String, job_name: String, queued_at: u64) -> Self {
        Self {
            job_id: job_id.clone(),
            job_name: job_name.clone(),
            status: Some(JobStatus {
                job_id,
                job_name,
                status: Some(job_status::Status::Queued(QueuedJob { queued_at })),
            }),
            queued_at,
            start_time: 0,
            end_time: 0,
            num_stages: 0,
            completed_stages: 0,
            total_task_duration_ms: 0,
        }
    }

    fn failed(
        job_id: String,
        job_name: String,
        queued_at: u64,
        reason: Arc<BallistaError>,
    ) -> JobOverview {
        let status = JobStatus {
            job_id: job_id.clone(),
            job_name: job_name.clone(),
            status: Some(job_status::Status::Failed(FailedJob {
                error: Some(ExecutionError {
                    error: Some(reason.as_ref().into()),
                }),
                queued_at,
                started_at: 0,
                ended_at: timestamp_millis(),
            })),
        };

        Self {
            job_id,
            job_name,
            status: Some(status),
            queued_at,
            start_time: 0,
            end_time: 0,
            num_stages: 0,
            completed_stages: 0,
            total_task_duration_ms: 0,
        }
    }
}

impl From<&ExecutionGraph> for JobOverview {
    fn from(value: &ExecutionGraph) -> Self {
        let (completed_stages, total_task_duration_ms) =
            ExecutionGraph::calculate_completed_stages_and_total_duration(value.stages());

        Self {
            job_id: value.job_id().to_string(),
            job_name: value.job_name().to_string(),
            status: Some(value.status()),
            queued_at: value.queued_at(),
            start_time: value.start_time(),
            end_time: value.end_time(),
            num_stages: value.stage_count() as u32,
            completed_stages: completed_stages as u32,
            total_task_duration_ms,
        }
    }
}
