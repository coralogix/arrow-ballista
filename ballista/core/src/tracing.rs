use std::collections::HashMap;

use datafusion::config::{ConfigExtension, ExtensionOptions};
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::Context;
use datafusion::error::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Clone, Deserialize, Default, Debug)]
pub struct TraceExtension(HashMap<String, String>);

impl TraceExtension {
    pub fn new(context: &Context) -> Self {
        let mut trace_extension = Self::default();

        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(context, &mut trace_extension)
        });

        trace_extension
    }
}

impl From<&TraceExtension> for Context {
    fn from(val: &TraceExtension) -> Self {
        opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(val))
    }
}

impl ConfigExtension for TraceExtension {
    const PREFIX: &'static str = "cgx_trace";
}

impl ExtensionOptions for TraceExtension {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        self.0.insert(key.to_string(), value.to_string());
        Ok(())
    }

    fn entries(&self) -> Vec<datafusion::config::ConfigEntry> {
        self.0
            .iter()
            .map(|(key, value)| datafusion::config::ConfigEntry {
                key: format!("{}.{}", TraceExtension::PREFIX, key),
                value: Some(value.clone()),
                description: "",
            })
            .collect()
    }
}

impl Injector for TraceExtension {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.into(), value);
    }
}

impl Extractor for TraceExtension {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|x| &**x)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|x| &**x).collect()
    }
}
