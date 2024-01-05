use dashmap::DashMap;

#[derive(Debug, Clone)]
pub struct WarningCollector {
		warnings: DashMap<String, Vec<String>>
}

impl WarningCollector {
		pub fn new() -> Self {
				Self {
						warnings: DashMap::new()
				}
		}

		pub fn add_warning(&self, job_id:String, warning: String) {
				self.warnings.entry(job_id).or_insert(vec![]).push(warning);
		}

		pub fn warnings(&self, job_id:String) -> Vec<String> {
				self.warnings.entry(job_id).or_insert(vec![]).iter().map(|s| s.clone()).collect()
		}
}