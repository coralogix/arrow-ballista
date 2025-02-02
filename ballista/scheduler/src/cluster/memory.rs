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

use crate::cluster::{
    ClusterState, JobState, JobStateEvent, JobStateEventStream, JobStatus,
};
use crate::config::TaskDistribution;
use crate::state::execution_graph::ExecutionGraph;
use crate::state::executor_manager::ExecutorReservation;
use async_trait::async_trait;
use ballista_core::config::BallistaConfig;
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf::{
    executor_status, AvailableTaskSlots, ExecutionError, ExecutorHeartbeat,
    ExecutorStatus, ExecutorTaskSlots, FailedJob, QueuedJob,
};
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata};
use dashmap::DashMap;
use datafusion::prelude::SessionContext;

use crate::cluster::event::ClusterEventSender;
use crate::scheduler_server::{timestamp_millis, timestamp_secs, SessionBuilder};
use crate::state::session_manager::create_datafusion_context;
use ballista_core::serde::protobuf::job_status::Status;
use itertools::Itertools;
use log::warn;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::ops::DerefMut;

use datafusion::config::Extensions;
use std::sync::Arc;
use tracing::debug;

use super::{reserve_slots_bias, reserve_slots_round_robin};

#[derive(Default)]
pub struct InMemoryClusterState {
    /// Current available task slots for each executor
    task_slots: Mutex<ExecutorTaskSlots>,
    /// Current executors
    executors: DashMap<String, ExecutorMetadata>,
    /// Last heartbeat received for each executor
    heartbeats: DashMap<String, ExecutorHeartbeat>,
}

#[async_trait]
impl ClusterState for InMemoryClusterState {
    async fn reserve_slots(
        &self,
        num_slots: u32,
        distribution: TaskDistribution,
        executors: Option<HashSet<String>>,
    ) -> Result<Vec<ExecutorReservation>> {
        let mut guard = self.task_slots.lock();

        let mut available_slots: Vec<&mut AvailableTaskSlots> = guard
            .task_slots
            .iter_mut()
            .filter_map(|data| {
                (data.slots > 0
                    && executors
                        .as_ref()
                        .map(|executors| executors.contains(&data.executor_id))
                        .unwrap_or(true))
                .then_some(data)
            })
            .collect();

        available_slots.sort_by(|a, b| Ord::cmp(&b.slots, &a.slots));

        let reservations = match distribution {
            TaskDistribution::Bias => reserve_slots_bias(available_slots, num_slots),
            TaskDistribution::RoundRobin => {
                reserve_slots_round_robin(available_slots, num_slots)
            }
        };

        Ok(reservations)
    }

    async fn reserve_slots_exact(
        &self,
        num_slots: u32,
        distribution: TaskDistribution,
        executors: Option<HashSet<String>>,
    ) -> Result<Vec<ExecutorReservation>> {
        let mut guard = self.task_slots.lock();

        let rollback = guard.clone();

        let mut available_slots: Vec<&mut AvailableTaskSlots> = guard
            .task_slots
            .iter_mut()
            .filter_map(|data| {
                (data.slots > 0
                    && executors
                        .as_ref()
                        .map(|executors| executors.contains(&data.executor_id))
                        .unwrap_or(true))
                .then_some(data)
            })
            .collect();

        available_slots.sort_by(|a, b| Ord::cmp(&b.slots, &a.slots));

        let reservations = match distribution {
            TaskDistribution::Bias => reserve_slots_bias(available_slots, num_slots),
            TaskDistribution::RoundRobin => {
                reserve_slots_round_robin(available_slots, num_slots)
            }
        };

        if reservations.len() as u32 != num_slots {
            *guard = rollback;
            Ok(vec![])
        } else {
            Ok(reservations)
        }
    }

    async fn cancel_reservations(
        &self,
        reservations: Vec<ExecutorReservation>,
    ) -> Result<()> {
        let mut increments = HashMap::new();
        for ExecutorReservation { executor_id, .. } in reservations {
            if let Some(inc) = increments.get_mut(&executor_id) {
                *inc += 1;
            } else {
                increments.insert(executor_id, 1usize);
            }
        }

        let mut guard = self.task_slots.lock();

        for executor_slots in guard.task_slots.iter_mut() {
            if let Some(slots) = increments.get(&executor_slots.executor_id) {
                executor_slots.slots += *slots as u32;
            }
        }

        Ok(())
    }

    async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        mut spec: ExecutorData,
        reserve: bool,
    ) -> Result<Vec<ExecutorReservation>> {
        let executor_id = metadata.id.clone();

        self.save_executor_metadata(metadata).await?;
        self.save_executor_heartbeat(ExecutorHeartbeat {
            executor_id: executor_id.clone(),
            timestamp: timestamp_secs(),
            metrics: vec![],
            status: Some(ExecutorStatus {
                status: Some(executor_status::Status::Active(String::default())),
            }),
        })
        .await?;

        let mut guard = self.task_slots.lock();

        // Check to see if we already have task slots for executor. If so, remove them.
        if let Some((idx, _)) = guard
            .task_slots
            .iter()
            .find_position(|slots| slots.executor_id == executor_id)
        {
            guard.task_slots.swap_remove(idx);
        }

        if reserve {
            let slots = std::mem::take(&mut spec.available_task_slots) as usize;
            let reservations = (0..slots)
                .map(|_| ExecutorReservation::new_free(executor_id.clone()))
                .collect();

            guard.task_slots.push(AvailableTaskSlots {
                executor_id,
                slots: 0,
            });

            Ok(reservations)
        } else {
            guard.task_slots.push(AvailableTaskSlots {
                executor_id,
                slots: spec.available_task_slots,
            });

            Ok(vec![])
        }
    }

    async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> Result<()> {
        self.executors.insert(metadata.id.clone(), metadata);
        Ok(())
    }

    async fn get_executor_metadata(&self, executor_id: &str) -> Result<ExecutorMetadata> {
        self.executors
            .get(executor_id)
            .map(|pair| pair.value().clone())
            .ok_or_else(|| {
                BallistaError::Internal(format!(
                    "Not executor with ID {executor_id} found"
                ))
            })
    }

    async fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) -> Result<()> {
        let executor_id = heartbeat.executor_id.clone();
        if let Some(mut last) = self.heartbeats.get_mut(&executor_id) {
            let _ = std::mem::replace(last.deref_mut(), heartbeat);
        } else {
            self.heartbeats.insert(executor_id, heartbeat);
        }

        Ok(())
    }

    async fn remove_executor(&self, executor_id: &str) -> Result<()> {
        {
            let mut guard = self.task_slots.lock();

            if let Some((idx, _)) = guard
                .task_slots
                .iter()
                .find_position(|slots| slots.executor_id == executor_id)
            {
                guard.task_slots.swap_remove(idx);
            }
        }

        self.heartbeats.remove(executor_id);

        Ok(())
    }

    fn executor_heartbeats(&self) -> HashMap<String, ExecutorHeartbeat> {
        self.heartbeats
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect()
    }

    fn get_executor_heartbeat(&self, executor_id: &str) -> Option<ExecutorHeartbeat> {
        self.heartbeats.get(executor_id).map(|r| r.value().clone())
    }
}

/// Implementation of `JobState` which keeps all state in memory. If using `InMemoryJobState`
/// no job state will be shared between schedulers
pub struct InMemoryJobState {
    scheduler: String,
    /// Jobs which have either completed successfully or failed
    completed_jobs: DashMap<String, JobStatus>,
    /// In-memory store of queued jobs. Map from Job ID -> (Job Name, queued_at timestamp)
    queued_jobs: DashMap<String, (String, u64)>,
    /// In-memory store of running job statuses. Map from Job ID -> JobStatus
    running_jobs: DashMap<String, ExecutionGraph>,
    /// Active ballista sessions
    sessions: DashMap<String, Arc<SessionContext>>,
    /// `SessionBuilder` for building DataFusion `SessionContext` from `BallistaConfig`
    session_builder: SessionBuilder,
    /// Sender of job events
    job_event_sender: ClusterEventSender<JobStateEvent>,
}

impl InMemoryJobState {
    pub fn new(scheduler: impl Into<String>, session_builder: SessionBuilder) -> Self {
        Self {
            scheduler: scheduler.into(),
            completed_jobs: Default::default(),
            queued_jobs: Default::default(),
            running_jobs: Default::default(),
            sessions: Default::default(),
            session_builder,
            job_event_sender: ClusterEventSender::new(100),
        }
    }
}

#[async_trait]
impl JobState for InMemoryJobState {
    async fn submit_job(&self, job_id: &str, graph: &ExecutionGraph) -> Result<()> {
        if self.queued_jobs.get(job_id).is_some() {
            let job_id_owned = job_id.to_owned();
            self.running_jobs
                .insert(job_id_owned.clone(), graph.clone());
            self.queued_jobs.remove(job_id);

            self.job_event_sender.send(&JobStateEvent::JobAcquired {
                job_id: job_id_owned,
                owner: self.scheduler.clone(),
            });

            Ok(())
        } else {
            Err(BallistaError::Internal(format!(
                "Failed to submit job {job_id}, not found in queued jobs"
            )))
        }
    }

    async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        if let Some((job_name, queued_at)) = self.queued_jobs.get(job_id).as_deref() {
            return Ok(Some(JobStatus {
                job_id: job_id.to_string(),
                job_name: job_name.clone(),
                status: Some(Status::Queued(QueuedJob {
                    queued_at: *queued_at,
                })),
            }));
        }

        if let Some(graph) = self.running_jobs.get(job_id).as_deref() {
            return Ok(Some(graph.status()));
        }

        if let Some(status) = self.completed_jobs.get(job_id).as_deref() {
            return Ok(Some(status.clone()));
        }

        Ok(None)
    }

    async fn get_execution_graph(&self, job_id: &str) -> Result<Option<ExecutionGraph>> {
        Ok(self.running_jobs.get(job_id).map(|graph| graph.clone()))
    }

    async fn try_acquire_job(&self, _job_id: &str) -> Result<Option<ExecutionGraph>> {
        // Always return None. The only state stored here are for completed jobs
        // which cannot be acquired
        Ok(None)
    }

    async fn save_job(&self, job_id: &str, graph: &ExecutionGraph) -> Result<()> {
        let status = graph.status();

        debug!("saving state for job {job_id} with status {:?}", status);

        // If job is either successful or failed, save to completed jobs
        if matches!(
            status.status,
            Some(Status::Successful(_)) | Some(Status::Failed(_))
        ) {
            // Once job is completed, store completed execution graph
            self.completed_jobs.insert(job_id.to_string(), status);
            self.running_jobs.remove(job_id);
        } else if let Some(old_status) =
            self.running_jobs.insert(job_id.to_string(), graph.clone())
        {
            self.job_event_sender.send(&JobStateEvent::JobUpdated {
                job_id: job_id.to_string(),
                status: old_status.status(),
            })
        }

        Ok(())
    }

    async fn get_session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        self.sessions
            .get(session_id)
            .map(|sess| sess.clone())
            .ok_or_else(|| {
                BallistaError::General(format!("No session for {session_id} found"))
            })
    }

    async fn create_session(
        &self,
        config: &BallistaConfig,
        extensions: Extensions,
    ) -> Result<Arc<SessionContext>> {
        let session =
            create_datafusion_context(None, config, extensions, self.session_builder);
        self.sessions.insert(session.session_id(), session.clone());

        Ok(session)
    }

    async fn update_session(
        &self,
        session_id: &str,
        config: &BallistaConfig,
        extensions: Extensions,
    ) -> Result<Arc<SessionContext>> {
        let session = create_datafusion_context(
            Some(session_id.to_string()),
            config,
            extensions,
            self.session_builder,
        );
        self.sessions
            .insert(session_id.to_string(), session.clone());

        Ok(session)
    }

    async fn job_state_events(&self) -> Result<JobStateEventStream> {
        Ok(Box::pin(self.job_event_sender.subscribe()))
    }

    async fn remove_job(&self, job_id: &str) -> Result<()> {
        if self.completed_jobs.remove(job_id).is_none() {
            warn!("Tried to delete non-existent job {job_id} from state");
        }
        Ok(())
    }

    async fn get_jobs(&self) -> Result<HashSet<String>> {
        let mut result =
            HashSet::with_capacity(self.completed_jobs.len() + self.running_jobs.len());

        for job in self.completed_jobs.iter() {
            result.insert(job.key().clone());
        }

        for job in self.running_jobs.iter() {
            result.insert(job.key().clone());
        }

        Ok(result)
    }

    async fn get_job_statuses(&self) -> Result<Vec<(String, JobStatus)>> {
        Ok(self
            .completed_jobs
            .iter()
            .map(|pair| (pair.key().clone(), pair.value().clone()))
            .collect())
    }

    async fn accept_job(
        &self,
        job_id: &str,
        job_name: &str,
        queued_at: u64,
    ) -> Result<()> {
        self.queued_jobs
            .insert(job_id.to_string(), (job_name.to_string(), queued_at));

        Ok(())
    }

    async fn fail_unscheduled_job(
        &self,
        job_id: &str,
        job_name: &str,
        queued_at: u64,
        reason: Arc<BallistaError>,
    ) -> Result<()> {
        if self.queued_jobs.remove(job_id).is_some() {
            let job_id = job_id.to_string();
            self.completed_jobs.insert(
                job_id.clone(),
                JobStatus {
                    job_id,
                    job_name: job_name.to_string(),
                    status: Some(Status::Failed(FailedJob {
                        error: Some(ExecutionError {
                            error: Some(reason.as_ref().into()),
                        }),
                        queued_at,
                        started_at: 0,
                        ended_at: timestamp_millis(),
                    })),
                },
            );

            Ok(())
        } else {
            Err(BallistaError::Internal(format!(
                "Could not fail unscheduler job {job_id}, job not found in queued jobs"
            )))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::cluster::memory::{InMemoryClusterState, InMemoryJobState};
    use crate::cluster::test::{
        test_executor_registration, test_fuzz_reservations, test_job_lifecycle,
        test_job_planning_failure, test_reservation,
    };
    use crate::cluster::TaskDistribution;
    use crate::test_utils::{
        test_aggregation_plan, test_join_plan, test_two_aggregations_plan,
    };
    use ballista_core::error::Result;
    use ballista_core::utils::default_session_builder;

    #[tokio::test]
    async fn test_in_memory_registration() -> Result<()> {
        test_executor_registration(InMemoryClusterState::default()).await
    }

    #[tokio::test]
    async fn test_in_memory_reserve() -> Result<()> {
        test_reservation(InMemoryClusterState::default(), TaskDistribution::Bias).await?;
        test_reservation(
            InMemoryClusterState::default(),
            TaskDistribution::RoundRobin,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_in_memory_fuzz_reserve() -> Result<()> {
        test_fuzz_reservations(
            InMemoryClusterState::default(),
            10,
            TaskDistribution::Bias,
            10,
            10,
        )
        .await?;
        test_fuzz_reservations(
            InMemoryClusterState::default(),
            10,
            TaskDistribution::RoundRobin,
            10,
            10,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_in_memory_job_lifecycle() -> Result<()> {
        test_job_lifecycle(
            InMemoryJobState::new("", default_session_builder),
            test_aggregation_plan(4).await,
        )
        .await?;
        test_job_lifecycle(
            InMemoryJobState::new("", default_session_builder),
            test_two_aggregations_plan(4).await,
        )
        .await?;
        test_job_lifecycle(
            InMemoryJobState::new("", default_session_builder),
            test_join_plan(4).await,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_in_memory_job_planning_failure() -> Result<()> {
        test_job_planning_failure(
            InMemoryJobState::new("", default_session_builder),
            test_aggregation_plan(4).await,
        )
        .await?;
        test_job_planning_failure(
            InMemoryJobState::new("", default_session_builder),
            test_two_aggregations_plan(4).await,
        )
        .await?;
        test_job_planning_failure(
            InMemoryJobState::new("", default_session_builder),
            test_join_plan(4).await,
        )
        .await?;

        Ok(())
    }
}
