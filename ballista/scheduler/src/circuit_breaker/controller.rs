use std::sync::Arc;

use dashmap::DashMap;
use regex::Regex;
use tracing::{info, warn};

#[derive(Clone)]
pub struct CircuitBreakerController {
    job_states: Arc<DashMap<String, JobState>>,
}

struct JobState {
    stage_states: Arc<DashMap<String, StageState>>,
}

struct StageState {
    attempt_states: Arc<DashMap<u64, AttemptState>>,
}

struct AttemptState {
    partition_states: Arc<DashMap<u32, PartitionState>>,
}

struct PartitionState {
    percent: f64,
}

struct TaskIdentity {
    job_id: String,
    stage_id: String,
    stage_attempt_num: u64,
}

impl Default for CircuitBreakerController {
    fn default() -> Self {
        let job_states = Arc::new(DashMap::new());
        Self { job_states }
    }
}

impl CircuitBreakerController {
    pub fn is_tripped_for(&self, job_id: &str) -> bool {
        let stage_states = match self.job_states.get(&job_id.to_owned()) {
            Some(state) => state.stage_states.clone(),
            // If the registration hasn't happened yet
            None => {
                return false;
            }
        };

        let is_tripped = stage_states.iter().any(|stage_state| {
            stage_state
                .value()
                .attempt_states
                .iter()
                .any(|attempt_state| {
                    attempt_state
                        .value()
                        .partition_states
                        .iter()
                        .fold(0.0, |a, b| a + b.percent)
                        >= 1.0
                })
        });

        is_tripped
    }

    pub fn create(&self, job_id: &str) {
        info!("Creating circuit breaker for job {}", job_id);
        self.job_states.insert(
            job_id.to_owned(),
            JobState {
                stage_states: Arc::new(DashMap::new()),
            },
        );
    }

    pub fn delete(&self, job_id: &str) {
        info!("Deleting circuit breaker for job {}", job_id);
        self.job_states.remove(job_id);
    }

    pub async fn update(
        &self,
        task_identity: String,
        partition: u32,
        percent: f64,
    ) -> Result<bool, String> {
        let task_info = Self::parse_task_identity(task_identity.clone())?;

        let stage_states = match self.job_states.get(&task_info.job_id) {
            Some(state) => state.stage_states.clone(),
            // If the registration hasn't happened yet
            None => {
                warn!(
                    "Circuit breaker update received for unregistered job {}",
                    task_info.job_id
                );
                return Ok(false);
            }
        };

        let attempt_states = match stage_states.get(&task_info.stage_id) {
            Some(state) => state.attempt_states.clone(),
            None => {
                let attempt_states = Arc::new(DashMap::new());
                stage_states.insert(
                    task_info.stage_id.clone(),
                    StageState {
                        attempt_states: attempt_states.clone(),
                    },
                );
                attempt_states
            }
        };

        let partition_states = match attempt_states.get(&task_info.stage_attempt_num) {
            Some(state) => state.partition_states.clone(),
            None => {
                let partition_states = Arc::new(DashMap::new());
                attempt_states.insert(
                    task_info.stage_attempt_num,
                    AttemptState {
                        partition_states: partition_states.clone(),
                    },
                );
                partition_states
            }
        };

        match partition_states.get_mut(&partition) {
            Some(mut state) => {
                state.percent = percent;
            }
            None => {
                partition_states.insert(partition, PartitionState { percent });
            }
        };

        let should_circuit_breaker = partition_states
            .iter()
            .map(|s| s.percent)
            .fold(0.0, |a, b| a + b)
            >= 1.0;

        if should_circuit_breaker {
            info!(
                "Sending circuit breaker signal to task {}, global limit reached",
                task_identity
            );
        }

        Ok(should_circuit_breaker)
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
