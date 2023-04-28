use ballista_core::serde::protobuf::{
    scheduler_grpc_client::SchedulerGrpcClient, ShortCircuitRegisterRequest,
    ShortCircuitUpdateRequest,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::SystemTime,
};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex, RwLock,
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::transport::Channel;
use tracing::{info, warn};

use crate::short_circuit::short_circuit_stream::{
    ShortCircuitConfig, ShortCircuitUpdate,
};
use datafusion::error::{DataFusionError, Result};

type SchedulerLookup =
    Arc<RwLock<HashMap<String, Arc<Mutex<SchedulerGrpcClient<Channel>>>>>>;

pub struct ShortCircuitClient {
    create_sender: Sender<RegisterShortCircuit>,
    updates_sender: Sender<ShortCircuitUpdate>,
    scheduler_lookup: SchedulerLookup,
}

struct ShortCircuitTaskState {
    short_circuit: Arc<AtomicBool>,
    buffered_num_rows: u64,
    buffered_num_bytes: u64,
    last_sent: u64,
}

#[derive(Debug)]
struct RegisterShortCircuit {
    task_id: String,
    short_circuit: Arc<AtomicBool>,
    row_count_limit: Option<u64>,
    bytes_count_limit: Option<u64>,
}

#[derive(Debug)]
enum DaemonUpdate {
    Create(RegisterShortCircuit),
    Update(ShortCircuitUpdate),
}

impl ShortCircuitClient {
    pub fn new(send_interval_ms: u64) -> Self {
        let (create_sender, create_receiver) = channel(99);
        let (updates_sender, updates_receiver) = channel(99);

        let scheduler_lookup = Arc::new(RwLock::new(HashMap::new()));

        tokio::spawn(Self::run_daemon(
            create_receiver,
            updates_receiver,
            scheduler_lookup.clone(),
            send_interval_ms,
        ));

        Self {
            create_sender,
            updates_sender,
            scheduler_lookup,
        }
    }

    async fn run_daemon(
        create_receiver: Receiver<RegisterShortCircuit>,
        updates_receiver: Receiver<ShortCircuitUpdate>,
        scheduler_lookup: SchedulerLookup,
        send_interval_ms: u64,
    ) {
        let mut state_per_task = HashMap::new();

        let create_stream =
            ReceiverStream::new(create_receiver).map(DaemonUpdate::Create);

        let updates_stream =
            ReceiverStream::new(updates_receiver).map(DaemonUpdate::Update);

        let mut combined_stream = create_stream.merge(updates_stream);

        while let Some(combined_received) = combined_stream.next().await {
            match combined_received {
                DaemonUpdate::Create(create) => {
                    let task_id = create.task_id.clone();
                    let config = ShortCircuitTaskState {
                        short_circuit: create.short_circuit,
                        buffered_num_rows: 0,
                        buffered_num_bytes: 0,
                        last_sent: 0,
                    };

                    state_per_task.insert(task_id.clone(), config);

                    if let Some(scheduler) = scheduler_lookup.read().await.get(&task_id) {
                        let mut scheduler = scheduler.lock().await;
                        let register = ShortCircuitRegisterRequest {
                            task_identity: task_id.clone(),
                            row_count_limit: create.row_count_limit,
                            byte_count_limit: create.bytes_count_limit,
                        };

                        info!("Registering short circuit: {:?}", register);
                        if let Err(e) = scheduler.register_short_circuit(register).await {
                            warn!("Failed to register short circuit: {:?}", e);
                        }
                    }
                }
                DaemonUpdate::Update(update) => {
                    let task_id = update.task_id.clone();

                    if let Some(scheduler) = scheduler_lookup.read().await.get(&task_id) {
                        let mut scheduler = scheduler.lock().await;
                        let mut delete_state = false;
                        if let Some(state) = state_per_task.get_mut(&task_id) {
                            if state.last_sent + send_interval_ms
                                > Self::timestamp_millis()
                            {
                                state.buffered_num_rows += update.num_rows;
                                state.buffered_num_bytes += update.num_bytes;
                            } else {
                                let update = ShortCircuitUpdateRequest {
                                    task_identity: task_id.clone(),
                                    row_count: update.num_rows + state.buffered_num_rows,
                                    byte_count: update.num_bytes
                                        + state.buffered_num_bytes,
                                };

                                state.buffered_num_rows = 0;
                                state.buffered_num_bytes = 0;

                                info!("Sending short circuit update: {:?}", update);

                                match scheduler.send_short_circuit_update(update).await {
                                    Err(e) => {
                                        warn!("Failed to update short circuit: {:?}", e);
                                    }
                                    Ok(cmd) => {
                                        let short_circuit =
                                            cmd.into_inner().short_circuit;
                                        if short_circuit {
                                            info!(
                                                "Short circuit triggered for task {}",
                                                task_id
                                            );

                                            state
                                                .short_circuit
                                                .store(short_circuit, Ordering::SeqCst);

                                            // Delete state if short circuit is triggered
                                            // The short circuit atomic bool is wrapped in an Arc
                                            // so it will remain readable by the stream
                                            delete_state = true;
                                        }
                                    }
                                }
                                state.last_sent = Self::timestamp_millis();
                            }
                        }

                        if delete_state {
                            state_per_task.remove(&task_id);
                        }
                    }
                }
            }
        }
    }

    fn timestamp_millis() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    pub fn register_limit(
        &self,
        task_id: String,
        row_count_limit: Option<u64>,
        bytes_count_limit: Option<u64>,
    ) -> Result<ShortCircuitConfig> {
        let short_circuit = Arc::new(AtomicBool::new(false));

        let create = RegisterShortCircuit {
            task_id,
            short_circuit: short_circuit.clone(),
            row_count_limit,
            bytes_count_limit,
        };

        self.create_sender.try_send(create).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to send register request to short circuit daemon: {}",
                e
            ))
        })?;

        Ok(ShortCircuitConfig {
            send_update: self.updates_sender.clone(),
            short_circuit,
        })
    }

    pub async fn register_scheduler(
        &self,
        task_id: String,
        scheduler: Arc<Mutex<SchedulerGrpcClient<Channel>>>,
    ) {
        info!("Registering scheduler lookup for task {}", task_id);
        let mut scheduler_lookup = self.scheduler_lookup.write().await;
        scheduler_lookup.insert(task_id, scheduler);
    }

    pub async fn unregister_scheduler(&self, task_id: &str) {
        info!("Unregistering scheduler lookup for task {}", task_id);
        let mut scheduler_lookup = self.scheduler_lookup.write().await;
        scheduler_lookup.remove(task_id);
    }
}

struct Registration {
    client: Arc<ShortCircuitClient>,
    task_id: String,
}
impl Drop for Registration {
    fn drop(&mut self) {
        let client = self.client.clone();
        let task_id = self.task_id.clone();
        tokio::spawn(async move {
            client.unregister_scheduler(&task_id).await;
        });
    }
}
