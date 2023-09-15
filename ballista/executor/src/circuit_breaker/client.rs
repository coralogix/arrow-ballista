use anyhow::Error;
use ballista_core::{
    error::BallistaError,
    serde::protobuf::{self, CircuitBreakerUpdateRequest},
};
use dashmap::DashMap;
use std::{
    collections::{HashMap, HashSet},
    ops::Add,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
    time::Instant,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::{info, warn};

use crate::{
    circuit_breaker::stream::CircuitBreakerUpdate,
    scheduler_client_registry::SchedulerClientRegistry,
};

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct CircuitBreakerStageKey {
    pub job_id: String,
    pub stage_id: u32,
}

impl From<CircuitBreakerStageKey> for protobuf::CircuitBreakerStageKey {
    fn from(val: CircuitBreakerStageKey) -> Self {
        protobuf::CircuitBreakerStageKey {
            job_id: val.job_id,
            stage_id: val.stage_id,
        }
    }
}

impl From<protobuf::CircuitBreakerStageKey> for CircuitBreakerStageKey {
    fn from(key: protobuf::CircuitBreakerStageKey) -> Self {
        Self {
            job_id: key.job_id,
            stage_id: key.stage_id,
        }
    }
}

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct CircuitBreakerTaskKey {
    pub stage_key: CircuitBreakerStageKey,
    pub attempt_num: u32,
    pub partition: u32,
    pub task_id: String,
}

impl From<CircuitBreakerTaskKey> for protobuf::CircuitBreakerTaskKey {
    fn from(val: CircuitBreakerTaskKey) -> Self {
        protobuf::CircuitBreakerTaskKey {
            stage_key: Some(val.stage_key.into()),
            attempt_num: val.attempt_num,
            partition: val.partition,
            task_id: val.task_id,
        }
    }
}

impl From<protobuf::CircuitBreakerTaskKey> for CircuitBreakerTaskKey {
    fn from(key: protobuf::CircuitBreakerTaskKey) -> Self {
        Self {
            stage_key: key.stage_key.unwrap().into(),
            attempt_num: key.attempt_num,
            partition: key.partition,
            task_id: key.task_id,
        }
    }
}

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct CircuitBreakerMetadataExtension {
    pub job_id: String,
    pub stage_id: u32,
    pub attempt_number: u32,
}

pub struct CircuitBreakerClientConfig {
    pub send_interval: Duration,
    pub cache_cleanup_frequency: Duration,
    pub cache_ttl: Duration,
}

impl Default for CircuitBreakerClientConfig {
    fn default() -> Self {
        Self {
            send_interval: Duration::from_secs(1),
            cache_cleanup_frequency: Duration::from_secs(15),
            cache_ttl: Duration::from_secs(60),
        }
    }
}

pub struct CircuitBreakerClient {
    update_sender: Sender<ClientUpdate>,
    state_per_stage: Arc<DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>>,
}

struct CircuitBreakerStageState {
    circuit_breaker: Arc<AtomicBool>,
    active_tasks: u32,
}

impl CircuitBreakerStageState {
    fn trip(&self) {
        self.circuit_breaker.store(true, Ordering::Release);
    }
}

#[derive(Debug)]
struct SchedulerRegistration {
    task_id: String,
    scheduler_id: String,
}

#[derive(Debug)]
struct SchedulerDeregistration {
    task_id: String,
}

#[derive(Debug)]
struct CircuitBreakerRegistration {
    key: CircuitBreakerStageKey,
}

#[derive(Debug)]
struct CircuitBreakerDeregistration {
    key: CircuitBreakerStageKey,
}

#[derive(Debug)]
enum ClientUpdate {
    Create(CircuitBreakerRegistration),
    Update(CircuitBreakerUpdate),
    Delete(CircuitBreakerDeregistration),
    SchedulerRegistration(SchedulerRegistration),
    SchedulerDeregistration(SchedulerDeregistration),
}

impl CircuitBreakerClient {
    pub fn new(
        get_scheduler: Arc<dyn SchedulerClientRegistry>,
        config: CircuitBreakerClientConfig,
    ) -> Self {
        let (update_sender, update_receiver) = channel(99);

        let state_per_stage = Arc::new(DashMap::new());

        tokio::spawn(Self::run_sender_daemon(
            update_receiver,
            config.send_interval,
            config.cache_cleanup_frequency,
            config.cache_ttl,
            get_scheduler,
            state_per_stage.clone(),
        ));

        Self {
            update_sender,
            state_per_stage,
        }
    }

    pub fn register(
        &self,
        key: CircuitBreakerStageKey,
    ) -> Result<Arc<AtomicBool>, Error> {
        let circuit_breaker = if let Some(state) = self.state_per_stage.get(&key) {
            state.circuit_breaker.clone()
        } else {
            let circuit_breaker = Arc::new(AtomicBool::new(false));

            let state = CircuitBreakerStageState {
                circuit_breaker: circuit_breaker.clone(),
                active_tasks: 0,
            };

            self.state_per_stage.insert(key.clone(), state);

            circuit_breaker
        };

        let registration = CircuitBreakerRegistration { key };

        let update = ClientUpdate::Create(registration);

        self.update_sender
            .try_send(update)
            .map_err(|e| e.into())
            .map(|_| circuit_breaker)
    }

    pub fn deregister(&self, key: CircuitBreakerStageKey) -> Result<(), Error> {
        let deregistration = CircuitBreakerDeregistration { key };

        let update = ClientUpdate::Delete(deregistration);
        self.update_sender.try_send(update).map_err(|e| e.into())
    }

    pub fn send_update(&self, update: CircuitBreakerUpdate) -> Result<(), Error> {
        let update = ClientUpdate::Update(update);
        self.update_sender.try_send(update).map_err(|e| e.into())
    }

    pub fn register_scheduler(
        &self,
        task_id: String,
        scheduler_id: String,
    ) -> Result<(), BallistaError> {
        info!(
            "Registering scheduler {} for task {}",
            scheduler_id, task_id
        );

        let update = ClientUpdate::SchedulerRegistration(SchedulerRegistration {
            task_id,
            scheduler_id,
        });

        self.update_sender.try_send(update).map_err(|e| {
            BallistaError::Internal(format!(
                "Failed to send scheduler registration: {}",
                e
            ))
        })
    }

    pub fn deregister_scheduler(&self, task_id: String) -> Result<(), BallistaError> {
        info!("Deregistering scheduler for task {}", task_id);

        let update =
            ClientUpdate::SchedulerDeregistration(SchedulerDeregistration { task_id });

        self.update_sender.try_send(update).map_err(|e| {
            BallistaError::Internal(format!(
                "Failed to send scheduler deregistration: {}",
                e
            ))
        })
    }

    async fn run_sender_daemon(
        update_receiver: Receiver<ClientUpdate>,
        send_interval: Duration,
        cache_cleanup_frequency: Duration,
        cache_ttl: Duration,
        get_scheduler: Arc<dyn SchedulerClientRegistry>,
        state_per_stage: Arc<DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>>,
    ) {
        let mut scheduler_ids = HashMap::new();
        let mut inactive_stages = HashMap::new();
        let mut last_cleanup = Instant::now();

        let updates_stream =
            ReceiverStream::new(update_receiver).chunks_timeout(1000, send_interval);

        tokio::pin!(updates_stream);

        while let Some(combined_received) = updates_stream.next().await {
            let mut updates = Vec::new();
            let mut scheduler_deregistrations = Vec::new();
            let mut deregistrations = Vec::new();

            for update in combined_received {
                match update {
                    ClientUpdate::Create(register) => {
                        if let Some(mut state) = state_per_stage.get_mut(&register.key) {
                            state.active_tasks += 1;
                            inactive_stages.remove(&register.key);
                        } else {
                            warn!("No state found for task {:?}", register.key);
                        }
                    }
                    ClientUpdate::Delete(deregistration) => {
                        deregistrations.push(deregistration);
                    }
                    ClientUpdate::Update(update) => {
                        updates.push(update);
                    }
                    ClientUpdate::SchedulerRegistration(registration) => {
                        scheduler_ids
                            .insert(registration.task_id, registration.scheduler_id);
                    }

                    ClientUpdate::SchedulerDeregistration(deregistration) => {
                        scheduler_deregistrations.push(deregistration);
                    }
                }
            }

            let mut updates_per_scheduler = HashMap::new();
            let mut seen_keys = HashSet::new();

            for update in updates.into_iter().rev() {
                // Per request only one update per task is sent
                // This is why we go from newest to oldest
                if seen_keys.insert(update.key.clone()) {
                    let scheduler_id: &String = match scheduler_ids
                        .get(&update.key.task_id)
                    {
                        Some(scheduler_id) => scheduler_id,
                        None => {
                            warn!("No scheduler found for task {}", update.key.task_id);
                            continue;
                        }
                    };

                    updates_per_scheduler
                        .entry(scheduler_id.clone())
                        .or_insert_with(Vec::new)
                        .push(update);
                }
            }

            for (scheduler_id, updates) in updates_per_scheduler {
                let mut request_updates = Vec::with_capacity(updates.len());

                for update in updates {
                    let key = update.key.into();

                    request_updates.push(protobuf::CircuitBreakerUpdate {
                        key: Some(key),
                        percent: update.percent,
                    })
                }

                let mut scheduler = match get_scheduler
                    .get_or_create_scheduler_client(&scheduler_id)
                    .await
                {
                    Ok(scheduler) => scheduler,
                    Err(e) => {
                        warn!("Failed to get scheduler {}: {}", scheduler_id, e);
                        continue;
                    }
                };

                if request_updates.is_empty() {
                    continue;
                }

                let request = CircuitBreakerUpdateRequest {
                    updates: request_updates,
                };

                match scheduler.send_circuit_breaker_update(request).await {
                    Err(e) => warn!(
                        "Failed to send circuit breaker update to scheduler {}: {}",
                        scheduler_id, e
                    ),
                    Ok(response) => {
                        let commands = response.into_inner().commands;

                        for command in commands {
                            if let Some(key_proto) = command.key {
                                let key = key_proto.into();

                                if let Some(state) = state_per_stage.get_mut(&key) {
                                    state.trip();
                                } else {
                                    warn!("No state found for task {:?}", key);
                                }
                            }
                        }
                    }
                };
            }

            for deregistration in scheduler_deregistrations {
                scheduler_ids.remove(&deregistration.task_id);
            }

            for deregistration in deregistrations {
                if let Some(mut state) = state_per_stage.get_mut(&deregistration.key) {
                    state.active_tasks -= 1;

                    if state.active_tasks == 0 {
                        inactive_stages
                            .insert(deregistration.key.clone(), Instant::now());
                    }
                }
            }

            if last_cleanup.add(cache_cleanup_frequency) < Instant::now() {
                let mut inactive_stages = inactive_stages.drain().collect::<Vec<_>>();

                inactive_stages.sort_by_key(|(_, last_seen)| *last_seen);

                let mut to_remove = Vec::new();

                for (key, last_seen) in inactive_stages {
                    if last_seen.add(cache_ttl) < Instant::now() {
                        to_remove.push(key);
                    }
                }

                for key in to_remove {
                    state_per_stage.remove(&key);
                }

                last_cleanup = Instant::now();
            }
        }
    }
}
