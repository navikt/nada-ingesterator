#[allow(unused_imports)]
mod prelude {
    pub use kube::CustomResource;
    pub use schemars::JsonSchema;
    pub use serde::{Serialize, Deserialize};
    pub use std::collections::BTreeMap;
    pub use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
}

use self::prelude::*;

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(group = "data.nav.no", version = "v1", kind = "Ingest", plural = "ingests")]
#[kube(namespaced)]
#[kube(status = "IngestStatus")]
pub struct IngestSpec {
    /// Nais environment to target: `prod-gcp`, `dev-fss`, etc.
    pub environment: String,
    /// Nais team to target, corresponds to namespace.
    pub team: String,
    /// Nais Postgres instance to target.
    pub postgres: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct IngestStatus {
    /// List of status conditions
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<IngestStatusConditions>>,
    /// Latest observed generation
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "observedGeneration")]
    pub observed_generation: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct IngestStatusConditions {
    /// Human-readable message indicating details about the condition’s last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The status of the condition, either True, False or Unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Unique identifier of a condition.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    pub r#type: Option<String>,
}

