use std::collections::HashMap;

use ballista_core::serde::protobuf::CircuitBreakerTaskKey;
use parking_lot::RwLock;
use tracing::{debug, info};

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
    pub fn create(&self, job_id: &str) {
        info!(job_id, "creating circuit breaker");

        let mut job_states = self.job_states.write();

        job_states.insert(
            job_id.to_owned(),
            JobState {
                stage_states: HashMap::new(),
            },
        );
    }

    pub fn delete(&self, job_id: &str) {
        info!(job_id, "deleting circuit breaker");
        let mut job_states = self.job_states.write();
        job_states.remove(job_id);
    }

    pub fn update(
        &self,
        key: CircuitBreakerTaskKey,
        percent: f64,
    ) -> Result<bool, String> {
        let mut job_states = self.job_states.write();

        let stage_key = if let Some(stage_key) = key.stage_key {
            stage_key
        } else {
            return Err("received circuit breaker update without stage key".to_owned());
        };

        let stage_states = match job_states.get_mut(&stage_key.job_id) {
            Some(state) => &mut state.stage_states,
            None => {
                debug!(
                    job_id = stage_key.job_id,
                    "received circuit breaker update for unregistered job",
                );
                return Ok(false);
            }
        };

        let partition_states = &mut stage_states
            .entry(stage_key.stage_id)
            .or_insert_with(|| StageState {
                attempt_states: HashMap::new(),
            })
            .attempt_states
            .entry(key.attempt_num)
            .or_insert_with(|| AttemptState {
                partition_states: HashMap::new(),
            })
            .partition_states;

        partition_states
            .entry(key.partition)
            .or_insert_with(|| PartitionState { percent })
            .percent = percent;

        let should_trip =
            partition_states.values().map(|s| s.percent).sum::<f64>() >= 1.0;

        Ok(should_trip)
    }
}
