use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use tokio::sync::RwLock;
use tracing::{info, warn};

use super::plan_visitor::{DefaultPlanVisitor, PlanVisitor};

pub struct ShortCircuitController {
    visitor: Arc<dyn PlanVisitor>,
    state: Arc<RwLock<HashMap<String, State>>>,
    job_registrations: Arc<RwLock<HashMap<String, ShortCircuitRegistration>>>,
}

struct State {
    counts: Vec<u64>,
    limit: u64,
}

impl Default for ShortCircuitController {
    fn default() -> Self {
        let state = Arc::new(RwLock::new(HashMap::new()));

        Self {
            visitor: Arc::new(DefaultPlanVisitor::default()),
            state,
            job_registrations: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl ShortCircuitController {
    pub fn new(visitor: Arc<dyn PlanVisitor>) -> Self {
        let state = Arc::new(RwLock::new(HashMap::new()));
        let job_registrations = Arc::new(RwLock::new(HashMap::new()));

        Self {
            visitor,
            state,
            job_registrations
        }
    }

    pub async fn register(
        &self,
        job_id: &str,
        plan: Arc<dyn ExecutionPlan>,
    ) {
        info!("Registering short circuit nodes for job {}", job_id);
        let visited = self.visitor.visit_all(plan.as_ref());

        let mut state = self.state.write().await;
        let mut ids = Vec::with_capacity(visited.len());

        for result in visited {
            info!("Registering short circuit node {}", result.id);
            state.insert(
                result.id.clone(),
                State {
                    counts: vec![],
                    limit: result.limit,
                },
            );

            ids.push(result.id.clone());
        }

        let registration = ShortCircuitRegistration { ids };

        let mut job_registrations = self.job_registrations.write().await;
        job_registrations.insert(job_id.to_owned(), registration);
    }

    pub async fn unregister(&self, job_id: &str) {
        info!("Unregistering short circuit nodes for job {}", job_id);
        let mut state = self.state.write().await;

        let job_registrations = self.job_registrations.read().await;
        let registration = job_registrations.get(job_id).unwrap();

        for id in &registration.ids {
            info!("Unregistering short circuit node {}", id);
            state.remove(id);
        }
    }

    pub async fn update(
        &self,
        id: String,
        partition: usize,
        count: u64,
    ) -> Result<bool, String> {
        let mut state = self.state.write().await;

        let state = match state.get_mut(&id) {
            Some(state) => state,
            // If the registration hasn't happened yet
            None => {
                warn!("Short circuit update received for unregistered node {}", id);
                return Ok(false);
            }
        };

        if state.counts.len() <= partition {
            state.counts.resize(partition + 1, 0);
        }

        state.counts[partition] += count;

        let should_short_circuit = state.counts.iter().sum::<u64>() >= state.limit;

        if should_short_circuit {
            info!(
                "Short circuiting node {} partition {} due to global limit reached",
                id, partition
            );
        }

        Ok(should_short_circuit)
    }
}

pub struct ShortCircuitRegistration {
    ids: Vec<String>,
}
