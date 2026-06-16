//! Tables and their child objects (columns, constraints, indexes,
//! triggers, partitioning)

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Represents a table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub name: String,
    pub schema: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unlogged: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parents: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub like_table: Option<LikeTable>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<Column>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexes: Option<Vec<Index>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<ConstraintColumns>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_constraints: Option<Vec<CheckConstraint>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_constraints: Option<Vec<ConstraintColumns>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub foreign_keys: Option<Vec<ForeignKey>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triggers: Option<Vec<Trigger>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<TablePartitionBehavior>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions: Option<Vec<TablePartition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_parameters: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tablespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_tablespace: Option<String>,
    /// Foreign server backing a foreign table (CREATE FOREIGN TABLE ...
    /// SERVER); its presence marks the table as foreign
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    /// Foreign table OPTIONS (key 'value', ...); an open map, as the
    /// keys depend on the foreign data wrapper
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a column in a table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_constraint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generated: Option<ColumnGenerated>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents configuration of a generated column
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnGenerated {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence_behavior: Option<String>,
}

/// Represents a Check Constraint in a Table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckConstraint {
    pub name: String,
    pub expression: String,
}

/// Constraint columns for primary keys and unique constraints. The YAML
/// form may be a single column name, a list of column names, or a
/// mapping with `columns` and optional `include` (see table.yml)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConstraintColumns {
    Name(String),
    Columns(Vec<String>),
    Detailed {
        columns: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        include: Option<Vec<String>>,
    },
}

/// Represents a Foreign Key on a Table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub references: ForeignKeyReference,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_delete: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_update: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deferrable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initially_deferred: Option<bool>,
}

/// Represents the table a Foreign Key references
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKeyReference {
    pub name: String,
    pub columns: Vec<String>,
}

/// Represents an Index on a table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Index {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recurse: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<IndexColumn>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include: Option<Vec<String>>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
    pub where_clause: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_parameters: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tablespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a column in an index on a table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexColumn {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub opclass: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_placement: Option<String>,
}

/// Represents the settings for creating a table using LIKE
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LikeTable {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_comments: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_constraints: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_defaults: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_generated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_identity: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_indexes: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_statistics: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_storage: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_all: Option<bool>,
}

/// Defines a table partition
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePartition {
    pub name: String,
    pub schema: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_values_in: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_values_from: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_values_to: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_values_with: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Defines how a table is partitioned
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePartitionBehavior {
    #[serde(rename = "type")]
    pub partition_type: String,
    pub columns: Vec<TablePartitionColumn>,
}

/// A table partition column: either a bare column name or a mapping
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TablePartitionColumn {
    Name(String),
    Detailed {
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        expression: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        collation: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        opclass: Option<String>,
    },
}

/// Table Triggers
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trigger {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_each: Option<String>,
    /// A CONSTRAINT TRIGGER (always AFTER ROW; may be deferrable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deferrable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initially_deferred: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}
