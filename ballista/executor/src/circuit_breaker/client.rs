use anyhow::Error;
use ballista_core::{
    circuit_breaker::model::{CircuitBreakerStageKey, CircuitBreakerTaskKey},
    error::BallistaError,
    serde::protobuf::{self, CircuitBreakerUpdateRequest, CircuitBreakerUpdateResponse},
};
use dashmap::DashMap;
use futures::TryFutureExt;
use lazy_static::lazy_static;
use prometheus::{
    register_histogram, register_int_counter, register_int_gauge, Histogram, IntCounter,
    IntGauge,
};
use std::{
    collections::HashMap,
    ops::Add,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
    time::Instant,
};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};
use tracing::{info, warn};

use crate::{
    circuit_breaker::stream::CircuitBreakerUpdate,
    scheduler_client_registry::SchedulerClientRegistry,
};

use super::stream::CircuitBreakerLabelsRegistration;

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct CircuitBreakerMetadataExtension {
    pub job_id: String,
    pub stage_id: u32,
    pub attempt_number: u32,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct CircuitBreakerClientConfig {
    pub send_interval: Duration,
    pub cache_cleanup_frequency: Duration,
    pub cache_ttl: Duration,
    pub channel_size: usize,
    pub max_batch_size: usize,
}

impl Default for CircuitBreakerClientConfig {
    fn default() -> Self {
        Self {
            send_interval: Duration::from_secs(1),
            cache_cleanup_frequency: Duration::from_secs(15),
            cache_ttl: Duration::from_secs(60),
            channel_size: 1000,
            max_batch_size: 1000,
        }
    }
}

pub struct CircuitBreakerClient {
    update_sender: Sender<ClientUpdate>,
    state_per_stage: Arc<DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>>,
}

struct CircuitBreakerStageState {
    circuit_breaker: Arc<AtomicBool>,
    last_updated: Instant,
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
enum ClientUpdate {
    LabelsRegistration(CircuitBreakerLabelsRegistration),
    Update(CircuitBreakerUpdate),
    SchedulerRegistration(SchedulerRegistration),
    SchedulerDeregistration(SchedulerDeregistration),
}

lazy_static! {
    static ref BATCH_SIZE: Histogram = register_histogram!(
        "ballista_circuit_breaker_client_batch_size",
        "Number of updates in a batch sent to the scheduler (in percent of max batch size)",
        Vec::from_iter((0..=100).step_by(5).map(|x| x as f64 / 100.0))
    )
    .unwrap();
    static ref UPDATE_LATENCY_SECONDS: Histogram = register_histogram!(
        "ballista_circuit_breaker_client_update_latency",
        "Latency of sending updates to the schedulers in seconds",
        vec![0.01, 0.03, 0.05, 0.1, 0.3, 0.5, 1.0, 3.0]
    )
    .unwrap();
    static ref SENT_LABEL_REGISTRATIONS: IntCounter = register_int_counter!(
        "ballista_circuit_breaker_client_sent_label_registrations",
        "Number of registrations sent to the schedulers"
    )
    .unwrap();
    static ref SENT_UPDATES: IntCounter = register_int_counter!(
        "ballista_circuit_breaker_client_sent_updates",
        "Number of updates sent to the schedulers"
    )
    .unwrap();
    static ref CACHE_SIZE: IntGauge = register_int_gauge!(
        "ballista_circuit_breaker_client_cache_size",
        "Number active stages in cache"
    )
    .unwrap();
    static ref SCHEDULER_LOOKUP_SIZE: IntGauge = register_int_gauge!(
        "ballista_circuit_breaker_client_scheduler_lookup_size",
        "Number of schedulers in lookup"
    )
    .unwrap();
    static ref UPDATE_CHANNEL_CAPACITY: IntGauge = register_int_gauge!(
        "ballista_circuit_breaker_client_update_channel_capacity",
        "Capacity of the update channel"
    )
    .unwrap();
    static ref DROPPED_UPDATES: IntCounter = register_int_counter!(
        "ballista_circuit_breaker_client_dropped_updates",
        "Number of updates dropped because the channel was full"
    )
    .unwrap();
}

impl CircuitBreakerClient {
    pub fn new(
        get_scheduler: Arc<dyn SchedulerClientRegistry>,
        config: CircuitBreakerClientConfig,
    ) -> Self {
        let (update_sender, update_receiver) = channel(config.channel_size);

        let executor_id = uuid::Uuid::new_v4().to_string();

        let state_per_stage = Arc::new(DashMap::new());

        tokio::spawn(Self::run_sender_daemon(
            update_receiver,
            config,
            get_scheduler,
            state_per_stage.clone(),
            executor_id,
        ));

        Self {
            update_sender,
            state_per_stage,
        }
    }

    pub fn register(
        &self,
        key: CircuitBreakerTaskKey,
        labels: Vec<String>,
    ) -> Result<Arc<AtomicBool>, Error> {
        let state = self
            .state_per_stage
            .entry(key.stage_key.clone())
            .or_insert_with(|| CircuitBreakerStageState {
                circuit_breaker: Arc::new(AtomicBool::new(false)),
                last_updated: Instant::now(),
            });

        let registration = CircuitBreakerLabelsRegistration { key, labels };

        self.send_update_internal(ClientUpdate::LabelsRegistration(registration))
            .map(|_| state.circuit_breaker.clone())
    }

    pub fn send_update(&self, update: CircuitBreakerUpdate) -> Result<(), Error> {
        self.send_update_internal(ClientUpdate::Update(update))
    }

    fn send_update_internal(&self, update: ClientUpdate) -> Result<(), Error> {
        let res = self.update_sender.try_send(update).map_err(|e| e.into());
        UPDATE_CHANNEL_CAPACITY.set(self.update_sender.capacity() as i64);
        res
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

        self.send_update_internal(update).map_err(|e| {
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

        self.send_update_internal(update).map_err(|e| {
            BallistaError::Internal(format!(
                "Failed to send scheduler deregistration: {}",
                e
            ))
        })
    }

    async fn run_sender_daemon(
        mut update_receiver: Receiver<ClientUpdate>,
        config: CircuitBreakerClientConfig,
        get_scheduler: Arc<dyn SchedulerClientRegistry>,
        state_per_stage: Arc<DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>>,
        executor_id: String,
    ) {
        let mut last_cleanup = Instant::now();
        let mut task_scheduler_lookup = HashMap::new();
        let mut per_scheduler_state = HashMap::new();
        let mut last_update_request = None;

        while let Some(update) = update_receiver.recv().await {
            Self::handle_update(
                update,
                &mut per_scheduler_state,
                &mut task_scheduler_lookup,
                &config,
                &executor_id,
                state_per_stage.clone(),
                get_scheduler.clone(),
                &mut last_update_request,
            )
            .await;

            let now = Instant::now();

            if last_cleanup.add(config.cache_cleanup_frequency) < now {
                // Only cleaned up with a delay so multiple tasks of the same stage share the tripped state.
                // This will make sure new tasks that are received after the trip signal is received will be stopped immediately.
                Self::delete_old_stage_states(&config, &state_per_stage, now);

                // We only delete from the per scheduler state if we still receive updates for a task registered with that scheduler.
                // So we should eventually delete old per scheduler states from the map to not leak memory.
                Self::delete_old_per_scheduler_states(
                    &config,
                    &mut per_scheduler_state,
                    now,
                );

                last_cleanup = now;

                // Not related to cleanup, we just don't want to calculate the length for every update.
                SCHEDULER_LOOKUP_SIZE.set(task_scheduler_lookup.len() as i64);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_update(
        received: ClientUpdate,
        updates_per_scheduler: &mut HashMap<String, PerSchedulerState>,
        task_scheduler_lookup: &mut HashMap<String, String>,
        config: &CircuitBreakerClientConfig,
        executor_id: &str,
        state_per_stage: Arc<DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>>,
        scheduler_lookup: Arc<dyn SchedulerClientRegistry>,
        last_update_request: &mut Option<JoinHandle<()>>,
    ) {
        let now = Instant::now();

        match received {
            ClientUpdate::LabelsRegistration(reg) => {
                if let Some(mut state) = state_per_stage.get_mut(&reg.key.stage_key) {
                    state.last_updated = now;
                }

                if let Some(scheduler_id) = task_scheduler_lookup.get(&reg.key.task_id) {
                    updates_per_scheduler
                        .entry(scheduler_id.clone())
                        .or_default()
                        .label_registrations
                        .push(reg);
                } else {
                    // This happens when the task has completed but the update was still in flight.
                    info!("No scheduler found for task {}", reg.key.task_id);
                }
            }
            ClientUpdate::Update(update) => {
                if let Some(mut state) = state_per_stage.get_mut(&update.key.stage_key) {
                    state.last_updated = now;
                }

                if let Some(scheduler_id) = task_scheduler_lookup.get(&update.key.task_id)
                {
                    let mut per_scheduler_state = updates_per_scheduler
                        .remove(scheduler_id)
                        .unwrap_or_default();

                    if let Some(entry) = per_scheduler_state.updates.get_mut(&update.key)
                    {
                        *entry = update.percent.max(*entry);
                    } else if per_scheduler_state.updates.len() >= config.max_batch_size {
                        DROPPED_UPDATES.inc();
                    } else {
                        per_scheduler_state
                            .updates
                            .insert(update.key, update.percent);
                    }

                    let no_inflight_update = last_update_request
                        .as_ref()
                        .map(|j| j.is_finished())
                        .unwrap_or(true);

                    let should_send_update =
                        per_scheduler_state.last_sent.add(config.send_interval) < now;

                    if no_inflight_update && should_send_update {
                        per_scheduler_state.last_sent = now;

                        *last_update_request = Self::process_batch(
                            scheduler_id,
                            &mut per_scheduler_state.updates,
                            &mut per_scheduler_state.label_registrations,
                            state_per_stage,
                            scheduler_lookup,
                            executor_id,
                            config,
                        )
                        .await;
                    } else {
                        // We don't want to send an update yet (or an update is still in flight),
                        // so we put the state back into the map.
                        updates_per_scheduler
                            .insert(scheduler_id.clone(), per_scheduler_state);
                    }
                } else {
                    // This happens when the task has completed but the update was still in flight.
                    info!("No scheduler found for task {}", update.key.task_id);
                }
            }
            ClientUpdate::SchedulerRegistration(registration) => {
                task_scheduler_lookup
                    .insert(registration.task_id, registration.scheduler_id);
            }

            ClientUpdate::SchedulerDeregistration(deregistration) => {
                task_scheduler_lookup.remove(&deregistration.task_id);
            }
        }
    }

    async fn process_batch(
        scheduler_id: &str,
        updates: &mut HashMap<CircuitBreakerTaskKey, f64>,
        label_registrations: &mut Vec<CircuitBreakerLabelsRegistration>,
        state_per_stage: Arc<DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>>,
        get_scheduler: Arc<dyn SchedulerClientRegistry>,
        executor_id: &str,
        config: &CircuitBreakerClientConfig,
    ) -> Option<JoinHandle<()>> {
        let update_protos = updates
            .drain()
            .map(|(key, percent)| protobuf::CircuitBreakerUpdate {
                key: Some(key.into()),
                percent,
            })
            .collect::<Vec<_>>();

        let label_registration_protos = label_registrations
            .drain(..)
            .map(|r| protobuf::CircuitBreakerLabelsRegistration {
                key: Some(r.key.into()),
                labels: r.labels,
            })
            .collect::<Vec<_>>();

        let request = CircuitBreakerUpdateRequest {
            updates: update_protos,
            label_registrations: label_registration_protos,
            executor_id: executor_id.to_owned(),
        };

        let scheduler_id = scheduler_id.to_owned();
        let state_per_stage = state_per_stage.clone();
        let max_batch_size = config.max_batch_size;

        // fork the actual request so we can continue collecting the local state
        // for the next request even if the request takes long to process for some reason.
        Some(tokio::spawn(Self::send_request(
            scheduler_id,
            state_per_stage,
            max_batch_size,
            request,
            get_scheduler.clone(),
        )))
    }

    async fn send_request(
        scheduler_id: String,
        state_per_stage: Arc<DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>>,
        max_batch_size: usize,
        request: CircuitBreakerUpdateRequest,
        get_scheduler: Arc<dyn SchedulerClientRegistry>,
    ) {
        let mut scheduler = match get_scheduler
            .get_or_create_scheduler_client(&scheduler_id)
            .await
        {
            Ok(scheduler) => scheduler,
            Err(e) => {
                warn!("Failed to get scheduler {}: {}", scheduler_id, e);
                return;
            }
        };

        let request_time = Instant::now();
        let updates_len = request.updates.len();
        let label_registrations_len = request.label_registrations.len();

        scheduler
            .send_circuit_breaker_update(request)
            .map_ok_or_else(
                move |e| {
                    warn!(
                        "Failed to send circuit breaker update to scheduler {}: {}",
                        scheduler_id, e
                    )
                },
                move |response| {
                    let request_latency = request_time.elapsed();

                    SENT_UPDATES.inc_by(updates_len as u64);
                    BATCH_SIZE.observe(updates_len as f64 / max_batch_size as f64);
                    SENT_LABEL_REGISTRATIONS.inc_by(label_registrations_len as u64);
                    UPDATE_LATENCY_SECONDS.observe(request_latency.as_secs_f64());

                    Self::handle_response(response.into_inner(), &state_per_stage);
                },
            )
            .await;
    }

    fn handle_response(
        response: CircuitBreakerUpdateResponse,
        state_per_stage: &DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>,
    ) {
        for command in response.commands {
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

    fn delete_old_stage_states(
        config: &CircuitBreakerClientConfig,
        state_per_stage: &DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>,
        timestamp: Instant,
    ) {
        let mut keys_to_delete = Vec::new();

        for entry in state_per_stage.iter() {
            if entry.value().last_updated.add(config.cache_ttl) < timestamp {
                keys_to_delete.push(entry.key().clone());
            }
        }

        for key in keys_to_delete.iter() {
            state_per_stage.remove(key);
        }

        CACHE_SIZE.set(state_per_stage.len() as i64);
    }

    fn delete_old_per_scheduler_states(
        config: &CircuitBreakerClientConfig,
        updates_per_scheduler: &mut HashMap<String, PerSchedulerState>,
        timestamp: Instant,
    ) {
        let mut keys_to_delete = Vec::new();

        for (scheduler_id, state) in updates_per_scheduler.iter() {
            if state.last_sent.add(config.cache_ttl) < timestamp {
                keys_to_delete.push(scheduler_id.clone());
            }
        }

        for key in keys_to_delete.iter() {
            updates_per_scheduler.remove(key);
        }
    }
}

struct PerSchedulerState {
    updates: HashMap<CircuitBreakerTaskKey, f64>,
    label_registrations: Vec<CircuitBreakerLabelsRegistration>,
    last_sent: Instant,
}

impl Default for PerSchedulerState {
    fn default() -> Self {
        Self {
            updates: HashMap::new(),
            label_registrations: Vec::new(),
            last_sent: Instant::now(),
        }
    }
}
