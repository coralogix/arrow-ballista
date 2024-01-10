use crate::serde::protobuf;

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct CircuitBreakerStateKey {
    pub job_id: String,
    pub shared_state_id: String,
    pub stage_id: u32,
    pub attempt_num: u32,
}

impl From<CircuitBreakerStateKey> for protobuf::CircuitBreakerStateKey {
    fn from(val: CircuitBreakerStateKey) -> Self {
        protobuf::CircuitBreakerStateKey {
            job_id: val.job_id,
            stage_id: val.stage_id,
            attempt_num: val.attempt_num,
            shared_state_id: val.shared_state_id,
        }
    }
}

impl From<protobuf::CircuitBreakerStateKey> for CircuitBreakerStateKey {
    fn from(key: protobuf::CircuitBreakerStateKey) -> Self {
        Self {
            job_id: key.job_id,
            stage_id: key.stage_id,
            attempt_num: key.attempt_num,
            shared_state_id: key.shared_state_id,
        }
    }
}

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct CircuitBreakerTaskKey {
    pub state_key: CircuitBreakerStateKey,
    pub partition: u32,
    pub task_id: String,
}

impl From<CircuitBreakerTaskKey> for protobuf::CircuitBreakerTaskKey {
    fn from(val: CircuitBreakerTaskKey) -> Self {
        protobuf::CircuitBreakerTaskKey {
            state_key: Some(val.state_key.into()),
            partition: val.partition,
            task_id: val.task_id,
        }
    }
}

impl TryFrom<protobuf::CircuitBreakerTaskKey> for CircuitBreakerTaskKey {
    type Error = String;
    fn try_from(key: protobuf::CircuitBreakerTaskKey) -> Result<Self, String> {
        Ok(Self {
            state_key: key
                .state_key
                .ok_or_else(|| {
                    "Circuit breaker task key contains no stage key".to_owned()
                })?
                .into(),
            partition: key.partition,
            task_id: key.task_id,
        })
    }
}
