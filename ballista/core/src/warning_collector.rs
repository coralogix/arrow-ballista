use dashmap::DashSet;

#[derive(Debug, Clone, Default)]
pub struct WarningCollector {
    warnings: DashSet<String>,
}

impl WarningCollector {
    pub fn add_warning(&self, warning: String) {
        self.warnings.insert(warning);
    }

    pub fn warnings(&self) -> Vec<String> {
        self.warnings.iter().map(|s| s.clone()).collect()
    }
}
