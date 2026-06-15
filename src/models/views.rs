//! Views and materialized views

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Represents a View
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct View {
    pub name: String,
    pub schema: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recursive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<ViewColumn>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_option: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_barrier: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// A column in a view or materialized view: a bare name or a mapping
/// with a name and comment
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ViewColumn {
    Name(String),
    Detailed {
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        comment: Option<String>,
    },
}

/// Represents a Materialized View
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedView {
    pub name: String,
    pub schema: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<ViewColumn>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_access_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_parameters: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tablespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexes: Option<Vec<super::Index>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}
