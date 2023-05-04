use anyhow::Error;
use ballista_core::{
    error::BallistaError,
    serde::protobuf::{self, CircuitBreakerUpdateRequest},
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::{info, warn};

use crate::{
    circuit_breaker::stream::CircuitBreakerUpdate,
    scheduler_client_registry::SchedulerClientRegistry,
};

pub struct CircuitBreakerClient {
    update_sender: Sender<ClientUpdate>,
}

struct CircuitBreakerTaskState {
    circuit_breaker: Arc<AtomicBool>,
}

#[derive(Debug)]
struct SchedulerRegistration {
    task_id: String,
    scheduler_id: String,
}

#[derive(Debug)]
struct SchedulerUnregistration {
    task_id: String,
}

#[derive(Debug)]
struct CircuitBreakerRegistration {
    task_id: String,
    circuit_breaker: Arc<AtomicBool>,
}

#[derive(Debug)]
enum ClientUpdate {
    Create(CircuitBreakerRegistration),
    Update(CircuitBreakerUpdate),
    SchedulerRegistration(SchedulerRegistration),
    SchedulerUnregistration(SchedulerUnregistration),
}

impl CircuitBreakerClient {
    pub fn new(
        send_interval: Duration,
        get_scheduler: Arc<dyn SchedulerClientRegistry>,
    ) -> Self {
        let (update_sender, update_receiver) = channel(99);

        tokio::spawn(Self::run_daemon(
            update_receiver,
            send_interval,
            get_scheduler,
        ));

        Self { update_sender }
    }

    pub fn register(
        &self,
        task_id: String,
        circuit_breaker: Arc<AtomicBool>,
    ) -> Result<(), Error> {
        let registration = CircuitBreakerRegistration {
            task_id,
            circuit_breaker,
        };

        let update = ClientUpdate::Create(registration);
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

    pub fn unregister_scheduler(&self, task_id: String) -> Result<(), BallistaError> {
        info!("Unregistering scheduler for task {}", task_id);

        let update =
            ClientUpdate::SchedulerUnregistration(SchedulerUnregistration { task_id });

        self.update_sender.try_send(update).map_err(|e| {
            BallistaError::Internal(format!(
                "Failed to send scheduler unregistration: {}",
                e
            ))
        })
    }

    async fn run_daemon(
        update_receiver: Receiver<ClientUpdate>,
        send_interval: Duration,
        get_scheduler: Arc<dyn SchedulerClientRegistry>,
    ) {
        let mut state_per_task = HashMap::new();
        let mut scheduler_ids = HashMap::new();

        let updates_stream =
            ReceiverStream::new(update_receiver).chunks_timeout(1000, send_interval);

        tokio::pin!(updates_stream);

        while let Some(combined_received) = updates_stream.next().await {
            let mut updates = Vec::new();
            let mut scheduler_unregistrations = Vec::new();

            for update in combined_received {
                match update {
                    ClientUpdate::Create(register) => {
                        let state = CircuitBreakerTaskState {
                            circuit_breaker: register.circuit_breaker,
                        };

                        state_per_task.insert(register.task_id, state);
                    }
                    ClientUpdate::Update(update) => {
                        updates.push(update);
                    }
                    ClientUpdate::SchedulerRegistration(registration) => {
                        scheduler_ids
                            .insert(registration.task_id, registration.scheduler_id);
                    }

                    ClientUpdate::SchedulerUnregistration(unregistration) => {
                        scheduler_unregistrations.push(unregistration);
                    }
                }
            }

            let mut updates_per_scheduler = HashMap::new();
            let mut seen_task_ids = HashSet::new();

            for update in updates.into_iter().rev() {
                // Per request only one update per task is sent
                // This is why we go from newest to oldest
                if seen_task_ids.insert(update.task_id.clone()) {
                    let scheduler_id: &String = match scheduler_ids.get(&update.task_id) {
                        Some(scheduler_id) => scheduler_id,
                        None => {
                            warn!("No scheduler found for task {}", update.task_id);
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
                    request_updates.push(protobuf::CircuitBreakerUpdate {
                        task_id: update.task_id,
                        partition: update.partition,
                        percent: update.percent,
                    })
                }

                let scheduler = match get_scheduler
                    .get_or_create_scheduler_client(&scheduler_id)
                    .await
                {
                    Ok(scheduler) => scheduler,
                    Err(e) => {
                        warn!("Failed to get scheduler {}: {}", scheduler_id, e);
                        continue;
                    }
                };

                let request = CircuitBreakerUpdateRequest {
                    updates: request_updates,
                };

                match scheduler
                    .lock()
                    .await
                    .send_circuit_breaker_update(request)
                    .await
                {
                    Err(e) => warn!(
                        "Failed to send circuit breaker update to scheduler {}: {}",
                        scheduler_id, e
                    ),
                    Ok(response) => {
                        let commands = response.into_inner().commands;

                        for command in commands {
                            if let Some(state) = state_per_task.get(&command.task_id) {
                                state.circuit_breaker.store(true, Ordering::SeqCst);
                            } else {
                                warn!("No state found for task {}", command.task_id);
                            }
                        }
                    }
                };
            }

            for unregistration in scheduler_unregistrations {
                scheduler_ids.remove(&unregistration.task_id);
            }
        }
    }
}
