use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

use crate::global_limit_stream::{GlobalLimitConfig, GlobalLimitUpdate};
use datafusion::error::{DataFusionError, Result};

pub struct GlobalLimitDaemon {
    create_sender: mpsc::Sender<RegisterGlobalLimit>,
    updates_sender: mpsc::Sender<GlobalLimitUpdate>,
}

struct GlobalLimitState {
    short_circuit: Arc<AtomicBool>,
}

struct RegisterGlobalLimit {
    task_id: String,
    short_circuit: Arc<AtomicBool>,
}

enum DaemonUpdate {
    Create(RegisterGlobalLimit),
    Update(GlobalLimitUpdate),
}

impl GlobalLimitDaemon {
    pub fn new() -> Self {
        let (create_sender, create_receiver) = mpsc::channel(99);
        let (updates_sender, updates_receiver) = mpsc::channel(99);

        tokio::spawn(Self::run_daemon(create_receiver, updates_receiver));

        Self {
            create_sender,
            updates_sender,
        }
    }

    async fn run_daemon(
        create_receiver: mpsc::Receiver<RegisterGlobalLimit>,
        updates_receiver: mpsc::Receiver<GlobalLimitUpdate>,
    ) {
        let mut state_per_task = HashMap::new();

        let create_stream =
            ReceiverStream::new(create_receiver).map(|c| DaemonUpdate::Create(c));

        let updates_stream =
            ReceiverStream::new(updates_receiver).map(|u| DaemonUpdate::Update(u));

        let mut combined_stream = create_stream.merge(updates_stream);

        while let Some(combined_received) = combined_stream.next().await {
            match combined_received {
                DaemonUpdate::Create(create) => {
                    let task_id = create.task_id.clone();
                    let config = GlobalLimitState {
                        short_circuit: create.short_circuit,
                    };
                    state_per_task.insert(task_id, config);
                }
                DaemonUpdate::Update(update) => {
                    let task_id = update.task_id.clone();
                    // TODO: Send update to (correct) scheduler
                    // TODO: Flip short circuit flag if response indicates limit was reached
                    state_per_task.get(&task_id).map(|state| {
                        state.short_circuit.store(true, Ordering::SeqCst);
                    });
                }
            }
        }
    }

    pub fn register_limit(&self, task_id: String) -> Result<GlobalLimitConfig> {
        let short_circuit = Arc::new(AtomicBool::new(false));

        let create = RegisterGlobalLimit {
            task_id: task_id.clone(),
            short_circuit: short_circuit.clone(),
        };

        self.create_sender.try_send(create).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to send create to global limit daemon: {}",
                e
            ))
        })?;

        Ok(GlobalLimitConfig {
            send_update: self.updates_sender.clone(),
            short_circuit,
        })
    }
}
