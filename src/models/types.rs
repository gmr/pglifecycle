//! User defined data types

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Represents a user defined data type
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Type {
    pub name: String,
    pub schema: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receive: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub typmod_in: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub typmod_out: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub analyze: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub internal_length: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub passed_by_value: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alignment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub like_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub element: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collatable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<TypeColumn>>,
    #[serde(rename = "enum", skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtype: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtype_opclass: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtype_diff: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a column in a composite type
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeColumn {
    pub name: String,
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
}
