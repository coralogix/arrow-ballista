use std::sync::Arc;

use ballista_core::serde::protobuf::CircuitBreakerKey;
use dashmap::DashMap;
use tracing::{info, warn};

#[derive(Clone)]
pub struct CircuitBreakerController {
    job_states: Arc<DashMap<String, JobState>>,
}

struct JobState {
    stage_states: Arc<DashMap<u32, StageState>>,
}

struct StageState {
    attempt_states: Arc<DashMap<u32, AttemptState>>,
}

struct AttemptState {
    node_states: Arc<DashMap<String, NodeState>>,
}

struct NodeState {
    partition_states: Arc<DashMap<u32, PartitionState>>,
}

struct PartitionState {
    percent: f64,
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
                    attempt_state.value().node_states.iter().any(|node_state| {
                        node_state
                            .value()
                            .partition_states
                            .iter()
                            .fold(0.0, |a, b| a + b.percent)
                            >= 1.0
                    })
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
        key: CircuitBreakerKey,
        percent: f64,
    ) -> Result<bool, String> {
        let stage_states = match self.job_states.get(&key.job_id) {
            Some(state) => state.stage_states.clone(),
            // If the registration hasn't happened yet
            None => {
                warn!(
                    "Circuit breaker update received for unregistered job {}",
                    key.job_id
                );
                return Ok(false);
            }
        };

        let attempt_states = match stage_states.get(&key.stage_id) {
            Some(state) => state.attempt_states.clone(),
            None => {
                let attempt_states = Arc::new(DashMap::new());
                stage_states.insert(
                    key.stage_id,
                    StageState {
                        attempt_states: attempt_states.clone(),
                    },
                );
                attempt_states
            }
        };

        let node_states = match attempt_states.get(&key.attempt_num) {
            Some(state) => state.node_states.clone(),
            None => {
                let node_states = Arc::new(DashMap::new());
                attempt_states.insert(
                    key.attempt_num,
                    AttemptState {
                        node_states: node_states.clone(),
                    },
                );
                node_states
            }
        };

        let partition_states = match node_states.get(&key.node_id) {
            Some(state) => state.partition_states.clone(),
            None => {
                let partition_states = Arc::new(DashMap::new());
                node_states.insert(
                    key.node_id,
                    NodeState {
                        partition_states: partition_states.clone(),
                    },
                );
                partition_states
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
            .iter()
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
