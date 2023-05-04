use ballista_core::{
    error::BallistaError,
    serde::protobuf::{self, ShortCircuitUpdateRequest},
};
use std::{
    collections::HashMap,
    convert::TryInto,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::warn;

use crate::{
    scheduler_client::SchedulerClientRegistry,
    short_circuit::short_circuit_stream::ShortCircuitUpdate,
};
use datafusion::error::{DataFusionError, Result};

use super::short_circuit_stream::ShortCircuitConfig;

pub struct ShortCircuitClient {
    updates_sender: Sender<Update>,
}

struct SchedulerRegistration {
    task_id: String,
    scheduler_id: String,
}

struct SchedulerUnregistration {
    task_id: String,
}

struct ShortCircuitRegister {
    task_id: String,
    short_circuit: Arc<AtomicBool>,
}

enum Update {
    ShortCircuitRegister(ShortCircuitRegister),
    ShortCircuit(ShortCircuitUpdate),
    SchedulerRegistration(SchedulerRegistration),
    SchedulerUnregistration(SchedulerUnregistration),
}

struct ShortCircuitTaskState {
    short_circuit: Arc<AtomicBool>,
}

impl ShortCircuitClient {
    pub fn new(
        send_interval_ms: u64,
        get_scheduler: Arc<dyn SchedulerClientRegistry>,
    ) -> Self {
        let (updates_sender, updates_receiver) = channel(99);

        tokio::spawn(Self::run_daemon(
            updates_receiver,
            send_interval_ms,
            get_scheduler,
        ));

        Self { updates_sender }
    }

    async fn run_daemon(
        updates_receiver: Receiver<Update>,
        send_interval_ms: u64,
        get_scheduler: Arc<dyn SchedulerClientRegistry>,
    ) {
        let mut state_per_task = HashMap::new();

        let mut scheduler_ids = HashMap::new();

        let updates_stream = ReceiverStream::new(updates_receiver)
            .chunks_timeout(100, Duration::from_millis(send_interval_ms));

        tokio::pin!(updates_stream);

        while let Some(combined_received) = updates_stream.next().await {
            let mut updates_per_scheduler = HashMap::new();

            for update in combined_received {
                match update {
                    Update::ShortCircuitRegister(register) => {
                        let state = ShortCircuitTaskState {
                            short_circuit: register.short_circuit,
                        };

                        state_per_task.insert(register.task_id, state);
                    }
                    Update::ShortCircuit(update) => {
                        let scheduler_id: &String = match scheduler_ids
                            .get(&update.task_id)
                        {
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
                    Update::SchedulerRegistration(registration) => {
                        scheduler_ids.insert(
                            registration.task_id.clone(),
                            registration.scheduler_id.clone(),
                        );
                    }
                    Update::SchedulerUnregistration(unregistration) => {
                        scheduler_ids.remove(&unregistration.task_id);
                    }
                }
            }

            for (scheduler_id, updates) in updates_per_scheduler {
                let mut request_updates = Vec::with_capacity(updates.len());

                for update in updates {
                    request_updates.push(protobuf::ShortCircuitUpdate {
                        id: update.task_id,
                        count: update.count,
                        partition: update.partition.try_into().unwrap(),
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

                let request = ShortCircuitUpdateRequest {
                    updates: request_updates,
                };

                match scheduler
                    .lock()
                    .await
                    .send_short_circuit_update(request)
                    .await
                {
                    Err(e) => warn!(
                        "Failed to send short circuit update to scheduler {}: {}",
                        scheduler_id, e
                    ),
                    Ok(response) => {
                        let commands = response.into_inner().commands;

                        for command in commands {
                            if let Some(state) = state_per_task.get(&command.id) {
                                state.short_circuit.store(true, Ordering::SeqCst);
                            } else {
                                warn!("No state found for task {}", command.id);
                            }
                        }
                    }
                };
            }
        }
    }

    pub fn register_scheduler(
        &self,
        task_id: String,
        scheduler_id: String,
    ) -> Result<(), BallistaError> {
        let update = Update::SchedulerRegistration(SchedulerRegistration {
            task_id,
            scheduler_id,
        });

        self.updates_sender.try_send(update).map_err(|e| {
            BallistaError::Internal(format!(
                "Failed to send scheduler registration: {}",
                e
            ))
        })
    }

    pub fn unregister_scheduler(&self, task_id: String) -> Result<(), BallistaError> {
        let update = Update::SchedulerUnregistration(SchedulerUnregistration { task_id });

        self.updates_sender.try_send(update).map_err(|e| {
            BallistaError::Internal(format!(
                "Failed to send scheduler unregistration: {}",
                e
            ))
        })
    }

    pub fn register_stream(
        &self,
        task_id: String,
    ) -> Result<ShortCircuitConfig, DataFusionError> {
        let short_circuit = Arc::new(AtomicBool::new(false));

        let update = Update::ShortCircuitRegister(ShortCircuitRegister {
            task_id: task_id.clone(),
            short_circuit: short_circuit.clone(),
        });

        self.updates_sender.try_send(update).map_err(|e| {
            DataFusionError::Internal(format!(
                "Failed to send short circuit registration: {}",
                e
            ))
        })?;

        Ok(ShortCircuitConfig { short_circuit })
    }

    pub fn send_update(&self, update: ShortCircuitUpdate) -> Result<(), BallistaError> {
        let update = Update::ShortCircuit(update);

        self.updates_sender.try_send(update).map_err(|e| {
            BallistaError::Internal(format!("Failed to send short circuit update: {}", e))
        })
    }
}
