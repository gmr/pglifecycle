//! Aggregates, casts, collations, conversions, domains, event triggers,
//! extensions, FDWs, languages, operators, publications, schemas,
//! sequences, servers, subscriptions, tablespaces, and user mappings

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Represents the implementation of an aggregate for a data type
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Aggregate {
    pub name: String,
    pub schema: String,
    pub owner: String,
    pub arguments: Vec<Argument>,
    pub sfunc: String,
    pub state_data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_data_size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ffunc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalfunc_extra: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalfunc_modify: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub combinefunc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serialfunc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deserialfunc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_condition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msfunc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minvfunc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mstate_data_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mstate_data_size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mffunc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mfinalfunc_extra: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mfinalfunc_modify: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minitial_condition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_operator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hypothetical: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents an argument to an aggregate
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Argument {
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Represents a cast between two data types
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cast {
    pub schema: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inout: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignment: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub implicit: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a Collation
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Collation {
    pub name: String,
    pub schema: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locale: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lc_collate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lc_ctype: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deterministic: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub copy_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a Conversion
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Conversion {
    pub name: String,
    pub schema: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding_to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a Domain
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Domain {
    pub name: String,
    pub schema: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_constraints: Option<Vec<DomainConstraint>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a Check Constraint in a Domain
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainConstraint {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
}

/// Represents an event trigger
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventTrigger {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<EventTriggerFilter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// An event trigger filter
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventTriggerFilter {
    pub tags: Vec<String>,
}

/// Represents an extension
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Extension {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cascade: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a Foreign Data Wrapper
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignDataWrapper {
    pub name: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handler: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a Procedural Language
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Language {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replace: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trusted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handler: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inline_handler: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents an operator used to compare values
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Operator {
    pub name: String,
    pub schema: String,
    pub owner: String,
    pub function: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left_arg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right_arg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commutator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub negator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restrict: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub join: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hashes: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merges: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a Publication
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Publication {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub all_tables: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a schema/namespace
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    pub name: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a sequence
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sequence {
    pub name: String,
    pub schema: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub increment_by: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_value: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_value: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_with: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cycle: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owned_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a foreign server
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Server {
    pub name: String,
    pub foreign_data_wrapper: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub server_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a logical replication subscription
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    pub name: String,
    pub connection: String,
    pub publications: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a tablespace
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tablespace {
    pub name: String,
    pub owner: String,
    pub location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a user mapping
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserMapping {
    pub name: String,
    pub servers: Vec<UserMappingServer>,
}

/// Represents a server for a user mapping
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserMappingServer {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Map<String, Value>>,
}
