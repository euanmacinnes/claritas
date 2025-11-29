use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capability {
    pub name: String,
}

pub fn default_capabilities() -> Vec<Capability> {
    vec![Capability { name: "meta.ping".into() }, Capability { name: "meta.capabilities".into() }]
}
