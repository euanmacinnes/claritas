use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExperimentSpec {
    pub name: String,
}

pub fn run_experiment(_spec: &ExperimentSpec) {
    // TODO: implement A/B runner
}
