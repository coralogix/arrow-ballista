use std::collections::HashMap;
use std::sync::Arc;

use regex::Regex;
use tokio::{
    sync::RwLock,
    time::{sleep, Duration},
};
use tracing::{info, warn};

use crate::scheduler_server::timestamp_millis;

pub struct ShortCircuitController {
    state: Arc<RwLock<HashMap<String, State>>>,
}

struct State {
    row_count_limit: Option<u64>,
    byte_count_limit: Option<u64>,
    row_count: u64,
    byte_count: u64,
    last_update: u64,
}

struct TaskIdentity {
    job_id: String,
    stage_id: String,
    stage_attempt_num: u64,
}

impl TaskIdentity {
    fn to_key(&self) -> String {
        format!(
            "{}/{}/{}",
            self.job_id, self.stage_id, self.stage_attempt_num
        )
    }
}

impl Default for ShortCircuitController {
    fn default() -> Self {
        let state = Arc::new(RwLock::new(HashMap::new()));

        tokio::spawn(Self::run_daemon(state.clone()));

        Self { state }
    }
}

impl ShortCircuitController {
    async fn run_daemon(state: Arc<RwLock<HashMap<String, State>>>) {
        loop {
            sleep(Duration::from_secs(1)).await;

            let mut state = state.write().await;

            state.retain(|task_id, state| {
                let now = timestamp_millis();
                // Some amount of time that is long enough to not cause issues
                let should_retain = state.last_update + 60_000 > now;
                if !should_retain {
                    info!("Removing short circuit state for task {}", task_id);
                }
                should_retain
            });
        }
    }

    pub async fn register_short_circuit(
        &self,
        task_identity: String,
        row_count_limit: Option<u64>,
        byte_count_limit: Option<u64>,
    ) -> Result<(), String> {
        info!(
            "Registering short circuit for task {} with row count limit {:?} and byte count limit {:?}",
            task_identity, row_count_limit, byte_count_limit
        );

        let mut state = self.state.write().await;

        let task_identity = Self::parse_task_identity(task_identity)?;

        state.insert(
            task_identity.to_key(),
            State {
                row_count_limit,
                byte_count_limit,
                row_count: 0,
                byte_count: 0,
                last_update: timestamp_millis(),
            },
        );

        Ok(())
    }

    pub async fn update(
        &self,
        task_identity: String,
        row_count: u64,
        byte_count: u64,
    ) -> Result<bool, String> {
        let mut state = self.state.write().await;

        let task_identity = Self::parse_task_identity(task_identity)?;

        let state = match state.get_mut(&task_identity.to_key()) {
            Some(state) => state,
            // If the registration hasn't happened yet
            None => {
                warn!(
                    "Short circuit update received for unregistered task {}",
                    task_identity.to_key()
                );
                return Ok(false);
            }
        };

        state.row_count += row_count;
        state.byte_count += byte_count;
        state.last_update = timestamp_millis();

        let should_short_circuit = state
            .row_count_limit
            .iter()
            .any(|limit| state.row_count >= *limit)
            || state
                .byte_count_limit
                .iter()
                .any(|limit| state.byte_count >= *limit);

        if should_short_circuit {
            info!(
                "Short circuiting task {} due to global limit reached",
                task_identity.to_key()
            );
        }

        Ok(should_short_circuit)
    }

    fn parse_task_identity(task_identity: String) -> Result<TaskIdentity, String> {
        // Ballista uses the same task identity format consistently, but it has to use the String defined by Datafusion.
        // So we need to parse it here.
        let regex = Regex::new(r"TID (\d+) ([^/]*)/(\d+)\.(\d+)/\[([\d,\s]+)\]").unwrap();
        let captures = regex
            .captures(&task_identity)
            .ok_or_else(|| format!("Invalid task identity: {}", task_identity))?;
        // let _task_id = captures.get(1).unwrap().as_str();
        let job_id = captures.get(2).unwrap().as_str().to_owned();
        let stage_id = captures.get(3).unwrap().as_str().to_owned();
        let stage_attempt_num_str = captures.get(4).unwrap().as_str();
        // let _stage_partitions = captures.get(5).unwrap().as_str();

        let stage_attempt_num = stage_attempt_num_str.parse::<u64>().unwrap();

        Ok(TaskIdentity {
            job_id,
            stage_id,
            stage_attempt_num,
        })
    }
}
