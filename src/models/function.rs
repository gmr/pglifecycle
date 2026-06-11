//! Functions and procedures

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Represents a Function
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Function {
    pub name: String,
    pub schema: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Vec<FunctionParameter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub returns: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transform_types: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub immutable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volatile: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leak_proof: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub called_on_null_input: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub support: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a single parameter for a function
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionParameter {
    pub mode: String,
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
}

/// Represents a Procedure
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Procedure {
    pub name: String,
    pub schema: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Vec<FunctionParameter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transform_types: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}
