use dashmap::DashSet;

#[derive(Debug, Clone)]
pub struct WarningCollector {
    warnings: DashSet<String>,
}

impl WarningCollector {
    pub fn new() -> Self {
        Self {
            warnings: DashSet::new(),
        }
    }

    pub fn add_warning(&self, warning: String) {
        self.warnings.insert(warning);
    }

    pub fn warnings(&self) -> Vec<String> {
        self.warnings.iter().map(|s| s.clone()).collect()
    }
}
