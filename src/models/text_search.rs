//! Text search configurations, dictionaries, parsers, and templates

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Represents a complex object for text search
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextSearch {
    pub schema: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configurations: Option<Vec<TextSearchConfig>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dictionaries: Option<Vec<TextSearchDict>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parsers: Option<Vec<TextSearchParser>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub templates: Option<Vec<TextSearchTemplate>>,
}

/// Represents a configuration object for Text Search
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextSearchConfig {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parser: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a dictionary object for Text Search
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextSearchDict {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a parser object for Text Search
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextSearchParser {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gettoken_function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lextypes_function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headline_function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a template for Text Search
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextSearchTemplate {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lexize_function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub init_function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}
