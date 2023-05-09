use std::collections::HashMap;

use ballista_core::serde::protobuf::CircuitBreakerKey;
use parking_lot::RwLock;
use tracing::{info, warn};

pub struct CircuitBreakerController {
    job_states: RwLock<HashMap<String, JobState>>,
}

struct JobState {
    stage_states: HashMap<u32, StageState>,
}

struct StageState {
    attempt_states: HashMap<u32, AttemptState>,
}

struct AttemptState {
    node_states: HashMap<String, NodeState>,
}

struct NodeState {
    partition_states: HashMap<u32, PartitionState>,
}

struct PartitionState {
    percent: f64,
}

impl Default for CircuitBreakerController {
    fn default() -> Self {
        let job_states = RwLock::new(HashMap::new());
        Self { job_states }
    }
}

impl CircuitBreakerController {
    pub fn is_tripped_for(&self, job_id: &str) -> bool {
        let job_states = self.job_states.read();

        let stage_states = match job_states.get(job_id) {
            Some(state) => &state.stage_states,
            // If the registration hasn't happened yet
            None => {
                return false;
            }
        };

        let is_tripped = stage_states.values().any(|stage_state| {
            stage_state.attempt_states.values().any(|attempt_state| {
                attempt_state.node_states.values().any(|node_state| {
                    node_state
                        .partition_states
                        .values()
                        .fold(0.0, |a, b| a + b.percent)
                        >= 1.0
                })
            })
        });

        is_tripped
    }

    pub fn create(&self, job_id: &str) {
        info!("Creating circuit breaker for job {}", job_id);

        let mut job_states = self.job_states.write();

        job_states.insert(
            job_id.to_owned(),
            JobState {
                stage_states: HashMap::new(),
            },
        );
    }

    pub fn delete(&self, job_id: &str) {
        info!("Deleting circuit breaker for job {}", job_id);
        let mut job_states = self.job_states.write();
        job_states.remove(job_id);
    }

    pub fn update(&self, key: CircuitBreakerKey, percent: f64) -> Result<bool, String> {
        let mut job_states = self.job_states.write();
        let stage_states = match job_states.get_mut(&key.job_id) {
            Some(state) => &mut state.stage_states,
            // If the registration hasn't happened yet
            None => {
                warn!(
                    "Circuit breaker update received for unregistered job {}",
                    key.job_id
                );
                return Ok(false);
            }
        };

        let attempt_states = match stage_states.get_mut(&key.stage_id) {
            Some(state) => &mut state.attempt_states,
            None => {
                let attempt_states = HashMap::new();
                &mut stage_states
                    .entry(key.stage_id)
                    .or_insert_with(|| StageState { attempt_states })
                    .attempt_states
            }
        };

        let node_states = match attempt_states.get_mut(&key.attempt_num) {
            Some(state) => &mut state.node_states,
            None => {
                let node_states = HashMap::new();
                &mut attempt_states
                    .entry(key.attempt_num)
                    .or_insert_with(|| AttemptState { node_states })
                    .node_states
            }
        };

        let partition_states = match node_states.get_mut(&key.node_id) {
            Some(state) => &mut state.partition_states,
            None => {
                let partition_states = HashMap::new();
                &mut node_states.entry(key.node_id)
                    .or_insert_with(|| NodeState { partition_states })
                    .partition_states
            }
        };

        match partition_states.get_mut(&key.partition) {
            Some(mut state) => {
                state.percent = percent;
            }
            None => {
                partition_states.insert(key.partition, PartitionState { percent });
            }
        };

        let should_circuit_breaker = partition_states
            .values()
            .map(|s| s.percent)
            .fold(0.0, |a, b| a + b)
            >= 1.0;

        if should_circuit_breaker {
            info!(
                "Sending circuit breaker signal to task {}, global limit reached",
                key.task_id
            );
        }

        Ok(should_circuit_breaker)
    }
}
