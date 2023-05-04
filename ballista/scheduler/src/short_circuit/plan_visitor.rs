use datafusion::physical_plan::ExecutionPlan;

pub trait PlanVisitor
where
    Self: Sync + Send,
{
    fn visit(&self, plan: &dyn ExecutionPlan) -> Option<PlanVisitorResult>;
}

impl dyn PlanVisitor {
    pub fn visit_all(&self, plan: &dyn ExecutionPlan) -> Vec<PlanVisitorResult> {
        let mut results = vec![];

        for child in plan.children() {
            results.extend(self.visit_all(child.as_ref()));
        }

        if let Some(result) = self.visit(plan) {
            results.push(result);
        }

        results
    }
}

pub struct PlanVisitorResult {
    pub id: String,
    pub limit: u64,
}

#[derive(Default)]
pub struct DefaultPlanVisitor {}

impl PlanVisitor for DefaultPlanVisitor {
    fn visit(&self, _plan: &dyn ExecutionPlan) -> Option<PlanVisitorResult> {
        None
    }
}
