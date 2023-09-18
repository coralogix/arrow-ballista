use std::collections::HashMap;

use ballista_core::circuit_breaker::model::{
    CircuitBreakerStageKey, CircuitBreakerTaskKey,
};
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
    executor_trip_state: HashMap<String, bool>,
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
        let mut job_states = self.job_states.write();
        job_states.remove(job_id);

        info!(job_id, "deleted circuit breaker",);
    }

    pub fn update(
        &self,
        key: CircuitBreakerTaskKey,
        percent: f64,
        executor_id: String,
    ) -> Result<bool, String> {
        let mut job_states = self.job_states.write();

        let stage_key = key.stage_key.clone();

        let job_state = match job_states.get_mut(&stage_key.job_id) {
            Some(state) => state,
            None => {
                debug!(
                    job_id = stage_key.job_id,
                    "received circuit breaker update for unregistered job",
                );
                return Ok(false);
            }
        };

        let stage_states = &mut job_state.stage_states;

        let stage_state =
            &mut stage_states
                .entry(stage_key.stage_id)
                .or_insert_with(|| StageState {
                    attempt_states: HashMap::new(),
                });

        let attempt_states = &mut stage_state.attempt_states;

        let attempt_state = attempt_states
            .entry(key.stage_key.attempt_num)
            .or_insert_with(|| {
                let mut executor_trip_state = HashMap::new();
                executor_trip_state.insert(executor_id.clone(), false);
                AttemptState {
                    partition_states: HashMap::new(),
                    executor_trip_state,
                }
            });

        attempt_state
            .executor_trip_state
            .entry(executor_id.clone())
            .or_insert_with(|| false);

        let partition_states = &mut attempt_state.partition_states;

        partition_states
            .entry(key.partition)
            .or_insert_with(|| PartitionState { percent })
            .percent = percent;

        let sum_percentage = partition_states.values().map(|s| s.percent).sum::<f64>();

        let should_trip = sum_percentage >= 1.0;

        Ok(should_trip)
    }

    pub fn get_tripped_stages(&self, executor_id: &str) -> Vec<CircuitBreakerStageKey> {
        self.job_states
            .read()
            .iter()
            .flat_map(|(job_id, job_state)| {
                job_state
                    .stage_states
                    .iter()
                    .flat_map(|(stage_num, stage_state)| {
                        stage_state.attempt_states.iter().flat_map(
                            |(attempt_num, attempt_state)| {
                                attempt_state
                                    .executor_trip_state
                                    .get(executor_id)
                                    .filter(|tripped| **tripped)
                                    .map(|_| CircuitBreakerStageKey {
                                        job_id: job_id.clone(),
                                        stage_id: *stage_num,
                                        attempt_num: *attempt_num,
                                    })
                            },
                        )
                    })
            })
            .collect::<Vec<_>>()
    }
}
