use std::collections::HashMap;

use ballista_core::circuit_breaker::model::{
    CircuitBreakerStageKey, CircuitBreakerTaskKey,
};
use dashmap::{DashMap, DashSet};
use parking_lot::RwLock;
use tracing::{debug, info};

pub struct CircuitBreakerController {
    job_states: RwLock<HashMap<String, JobState>>,
    executor_messages: DashMap<String, DashSet<CircuitBreakerStageKey>>,
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
    executor: String,
}

impl Default for CircuitBreakerController {
    fn default() -> Self {
        let job_states = RwLock::new(HashMap::new());
        let executor_messages = DashMap::new();

        Self {
            job_states,
            executor_messages,
        }
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

        self.executor_messages.retain(|_, v| {
            v.retain(|stage_key| stage_key.job_id != job_id);
            !v.is_empty()
        });

        info!(
            job_id,
            message = "deleted circuit breaker",
            executor_messages_len = self.executor_messages.len(),
        );
    }

    pub fn update(
        &self,
        key: CircuitBreakerTaskKey,
        percent: f64,
        executor_id: String,
    ) -> Result<bool, String> {
        let mut job_states = self.job_states.write();

        let stage_key = key.stage_key.clone();

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

        let attempt_states = &mut stage_states
            .entry(stage_key.stage_id)
            .or_insert_with(|| StageState {
                attempt_states: HashMap::new(),
            })
            .attempt_states;

        let partition_states = &mut attempt_states
            .entry(key.stage_key.attempt_num)
            .or_insert_with(|| AttemptState {
                partition_states: HashMap::new(),
            })
            .partition_states;

        partition_states
            .entry(key.partition)
            .or_insert_with(|| PartitionState {
                percent,
                executor: executor_id,
            })
            .percent = percent;

        let should_trip =
            partition_states.values().map(|s| s.percent).sum::<f64>() >= 1.0;

        if should_trip {
            for executor_id in attempt_states
                .values()
                .flat_map(|p| p.partition_states.values())
                .map(|p| p.executor.clone())
            {
                self.executor_messages
                    .entry(executor_id)
                    .or_insert_with(DashSet::new)
                    .insert(stage_key.clone());
            }
        }

        Ok(should_trip)
    }

    pub fn get_tripped_stages(&self, executor_id: &str) -> Vec<CircuitBreakerStageKey> {
        let mut tripped_stages = Vec::new();
        if let Some((_, stages)) = self.executor_messages.remove(executor_id) {
            for stage in stages.iter() {
                tripped_stages.push(stage.clone());
            }
        }
        tripped_stages
    }
}
