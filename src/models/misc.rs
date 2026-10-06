//! Access methods, aggregates, casts, collations, conversions, domains,
//! event triggers, extensions, FDWs, languages, operators, operator
//! classes and families, publications, schemas, sequences, servers,
//! subscriptions, tablespaces, and user mappings

use std::collections::BTreeSet;

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
    /// The aggregated arguments of an ordered-set aggregate, after
    /// ORDER BY; `arguments` then holds its direct arguments
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_by: Option<Vec<Argument>>,
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
    /// Security labels (`SECURITY LABEL FOR provider ON ...`), as
    /// a label for each provider
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_labels: Option<super::SecurityLabels>,
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
    /// ICU tailoring rules (PostgreSQL 16+)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rules: Option<String>,
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
    /// Security labels (`SECURITY LABEL FOR provider ON ...`), as
    /// a label for each provider
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_labels: Option<super::SecurityLabels>,
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
    /// `true` renders `NOT VALID`: the values in the columns of the
    /// domain were never checked, and only new ones are. Only a CHECK
    /// has it, and only ALTER DOMAIN can add it, thus the build writes
    /// such a CHECK as its own entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_valid: Option<bool>,
}

impl Domain {
    /// The name that PostgreSQL makes for a NOT NULL of the domain with
    /// no name: `<domain>_not_null`, or `<domain>_not_null1`, `2` and
    /// so on when another constraint of the domain has that name
    pub fn not_null_name(&self) -> String {
        let used = self
            .check_constraints
            .iter()
            .flatten()
            .filter(|c| !c.is_not_null())
            .filter_map(|c| c.name.clone())
            .collect();
        crate::utils::choose_constraint_name(
            &self.name, None, "not_null", &used,
        )
    }

    /// The domain with a name for each CHECK that has no name: the name
    /// that PostgreSQL gives it, `<domain>_check`, or `<domain>_check1`,
    /// `2` and so on when another constraint of the domain has that
    /// name. Build makes the constraints with a name before the CHECKs
    /// with no name, and these in their order, thus PostgreSQL gives
    /// them these names. PostgreSQL also looks at the names of the
    /// constraints of the other objects in the schema, which the domain
    /// does not know.
    pub fn with_check_names(&self) -> Domain {
        let mut domain = self.clone();
        let Some(constraints) = &mut domain.check_constraints else {
            return domain;
        };
        let mut used: BTreeSet<String> =
            constraints.iter().filter_map(|c| c.name.clone()).collect();
        for check in constraints
            .iter_mut()
            .filter(|c| c.name.is_none() && c.expression.is_some())
        {
            let name = crate::utils::choose_constraint_name(
                &domain.name,
                None,
                "check",
                &used,
            );
            used.insert(name.clone());
            check.name = Some(name);
        }
        domain
    }
}

impl DomainConstraint {
    /// Whether the constraint is a NOT NULL
    pub fn is_not_null(&self) -> bool {
        self.nullable == Some(false) && self.expression.is_none()
    }
}

/// Represents an event trigger
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventTrigger {
    pub name: String,
    /// The role that owns the trigger. It must be a superuser. Absent,
    /// deploy does not set or compare the owner
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<EventTriggerFilter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    /// When the trigger fires: DISABLED, REPLICA or ALWAYS (ALTER EVENT
    /// TRIGGER). Absent is the default, ORIGIN.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<String>,
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
    /// Security labels (`SECURITY LABEL FOR provider ON ...`), as
    /// a label for each provider
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_labels: Option<super::SecurityLabels>,
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

/// An access method (CREATE ACCESS METHOD). It has no schema and no
/// owner.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessMethod {
    pub name: String,
    /// TABLE or INDEX
    #[serde(rename = "type")]
    pub method_type: String,
    pub handler: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// An operator family (CREATE OPERATOR FAMILY). The operators and
/// support functions are the ones added to the family with ALTER
/// OPERATOR FAMILY ... ADD, not the ones of its operator classes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorFamily {
    pub name: String,
    pub schema: String,
    pub owner: String,
    /// The index access method (USING)
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operators: Option<Vec<OperatorClassOperator>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub functions: Option<Vec<OperatorClassFunction>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// An operator class (CREATE OPERATOR CLASS)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorClass {
    pub name: String,
    pub schema: String,
    pub owner: String,
    /// The index access method (USING)
    pub method: String,
    /// The column data type (FOR TYPE)
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<bool>,
    /// The operator family, schema-qualified. Without one, PostgreSQL
    /// makes a family with the name of the class.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    /// The data type stored in the index (STORAGE)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operators: Option<Vec<OperatorClassOperator>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub functions: Option<Vec<OperatorClassFunction>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// An operator of an operator class or family: `OPERATOR strategy
/// name (left, right) [FOR ORDER BY sort_family]`
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorClassOperator {
    pub strategy: u32,
    /// The operator, schema-qualified when it is not in pg_catalog
    pub name: String,
    /// The left and right operand types
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Vec<String>>,
    /// The btree operator family that sorts the result of an ordering
    /// operator (FOR ORDER BY)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_by: Option<String>,
}

/// A support function of an operator class or family: `FUNCTION
/// number (left, right) function(arguments)`
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorClassFunction {
    pub support: u32,
    /// The left and right operand types the function is for
    #[serde(skip_serializing_if = "Option::is_none")]
    pub types: Option<Vec<String>>,
    /// The function and its argument types
    pub function: String,
}

/// Extended statistics on a table or materialized view (CREATE
/// STATISTICS). A top-level object, not a table child: its name is
/// schema-qualified, and its owner need not own the table.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Statistics {
    pub name: String,
    pub schema: String,
    pub owner: String,
    /// The table or materialized view, schema-qualified
    pub table: String,
    /// ndistinct, dependencies or mcv; absent for all of them
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kinds: Option<Vec<String>>,
    /// Each column or parenthesized expression, as written
    pub elements: Vec<String>,
    /// ALTER STATISTICS ... SET STATISTICS
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a Publication
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Publication {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<PublicationTable>>,
    /// FOR TABLES IN SCHEMA: every table in these schemas, now and
    /// later
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schemas: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub all_tables: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Security labels (`SECURITY LABEL FOR provider ON ...`), as
    /// a label for each provider
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_labels: Option<super::SecurityLabels>,
}

/// The operations that a publication publishes by default, in the
/// order that pg_dump writes them
const PUBLISH_ALL: [&str; 4] = ["insert", "update", "delete", "truncate"];

impl Publication {
    /// The value that PostgreSQL uses for a parameter that is not given
    pub fn parameter_default(key: &str) -> Option<Value> {
        match key {
            "publish" => Some(Value::Array(
                PUBLISH_ALL.iter().map(|op| Value::from(*op)).collect(),
            )),
            "publish_via_partition_root" => Some(Value::Bool(false)),
            "publish_generated_columns" => Some(Value::from("none")),
            _ => None,
        }
    }

    /// The same publication in the form deploy compares. The tables,
    /// their columns, the schemas and the operations are sets, so each
    /// is in a fixed order. A table name is written as PostgreSQL
    /// resolves it, and a parameter at its default is absent. A row
    /// filter loses the parentheses that enclose all of it, which
    /// pg_dump adds; other than that, it is compared as text.
    pub fn canonical(&self) -> Publication {
        let mut tables: Vec<PublicationTable> = self
            .tables
            .iter()
            .flatten()
            .map(PublicationTable::canonical)
            .collect();
        tables.sort_by(|a, b| a.name().cmp(b.name()));
        let mut schemas = self.schemas.clone().unwrap_or_default();
        schemas.sort();
        schemas.dedup();
        let mut parameters = Map::new();
        for (key, value) in self.parameters.iter().flatten() {
            let value = match (key.as_str(), value) {
                ("publish", Value::Array(operations)) => {
                    let operations: Vec<String> = operations
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_lowercase)
                        .collect();
                    Value::Array(
                        PUBLISH_ALL
                            .iter()
                            .filter(|op| operations.iter().any(|o| o == *op))
                            .map(|op| Value::from(*op))
                            .collect(),
                    )
                }
                (_, Value::String(value)) => Value::from(value.to_lowercase()),
                (_, value) => value.clone(),
            };
            if Some(&value) != Self::parameter_default(key).as_ref() {
                parameters.insert(key.clone(), value);
            }
        }
        Publication {
            name: self.name.clone(),
            tables: (!tables.is_empty()).then_some(tables),
            schemas: (!schemas.is_empty()).then_some(schemas),
            all_tables: self.all_tables.filter(|all| *all),
            parameters: (!parameters.is_empty()).then_some(parameters),
            comment: self.comment.clone(),
            security_labels: None,
        }
    }
}

/// A table in a publication: its qualified name, or the name with the
/// columns and row filter the publication limits it to
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PublicationTable {
    Name(String),
    Filtered(FilteredPublicationTable),
}

impl PublicationTable {
    pub fn name(&self) -> &str {
        match self {
            PublicationTable::Name(name) => name,
            PublicationTable::Filtered(table) => &table.name,
        }
    }

    /// The table as [`Publication::canonical`] compares it: the name as
    /// PostgreSQL resolves it, the columns in name order, and the row
    /// filter without the parentheses that enclose all of it
    fn canonical(&self) -> PublicationTable {
        let name = canonical_relation(self.name());
        let PublicationTable::Filtered(table) = self else {
            return PublicationTable::Name(name);
        };
        let columns = table.columns.clone().filter(|c| !c.is_empty()).map(
            |mut columns| {
                columns.sort();
                columns.dedup();
                columns
            },
        );
        let row_filter = table
            .row_filter
            .as_deref()
            .map(|f| crate::utils::strip_outer_parens(f).to_string());
        if columns.is_none() && row_filter.is_none() {
            return PublicationTable::Name(name);
        }
        PublicationTable::Filtered(FilteredPublicationTable {
            name,
            columns,
            row_filter,
        })
    }
}

/// A qualified relation name as PostgreSQL resolves it: a name that is
/// not quoted folds to lowercase, and each part is quoted only when it
/// must be, so `"test"."replicated"` and `TEST.replicated` are the same
pub(crate) fn canonical_relation(name: &str) -> String {
    let mut parts = vec![String::new()];
    let mut quoted = false;
    let mut chars = name.trim().chars().peekable();
    while let Some(c) = chars.next() {
        let part = parts.last_mut().expect("one part at least");
        match c {
            '"' if quoted && chars.peek() == Some(&'"') => {
                chars.next();
                part.push('"');
            }
            '"' => quoted = !quoted,
            '.' if !quoted => parts.push(String::new()),
            c if quoted => part.push(c),
            c if c.is_whitespace() => {}
            c => part.push(c.to_ascii_lowercase()),
        }
    }
    parts
        .iter()
        .map(|part| crate::utils::quote_ident(part))
        .collect::<Vec<_>>()
        .join(".")
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilteredPublicationTable {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    /// The row filter, a WHERE expression
    #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
    pub row_filter: Option<String>,
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
    /// Security labels (`SECURITY LABEL FOR provider ON ...`), as
    /// a label for each provider
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_labels: Option<super::SecurityLabels>,
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
    /// Security labels (`SECURITY LABEL FOR provider ON ...`), as
    /// a label for each provider
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_labels: Option<super::SecurityLabels>,
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
    /// Security labels (`SECURITY LABEL FOR provider ON ...`), as
    /// a label for each provider
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_labels: Option<super::SecurityLabels>,
}

/// The subscription options that the catalog does not keep, as only
/// CREATE SUBSCRIPTION reads them, and `enabled`, which pg_dump does
/// not write: a subscription restores disabled
const SUBSCRIPTION_CREATE_ONLY: [&str; 4] =
    ["connect", "copy_data", "create_slot", "enabled"];

impl Subscription {
    /// The value that PostgreSQL 18 uses for an option that is not
    /// given. The slot name is the subscription name, so it is not here.
    pub fn parameter_default(key: &str) -> Option<Value> {
        match key {
            "binary" | "disable_on_error" | "failover" | "run_as_owner"
            | "two_phase" => Some(Value::Bool(false)),
            "password_required" => Some(Value::Bool(true)),
            "origin" => Some(Value::from("any")),
            "streaming" => Some(Value::from("parallel")),
            "synchronous_commit" => Some(Value::from("off")),
            _ => None,
        }
    }

    /// The name of the replication slot, or `None` for `slot_name =
    /// NONE`. The slot has the name of the subscription unless the
    /// project gives another one.
    pub fn slot_name(&self) -> Option<&str> {
        match self.parameters.as_ref().and_then(|p| p.get("slot_name")) {
            Some(Value::String(slot)) if slot.eq_ignore_ascii_case("none") => {
                None
            }
            Some(Value::String(slot)) => Some(slot),
            _ => Some(&self.name),
        }
    }

    /// The same subscription in the form deploy compares: the
    /// publications are a set, so they are in name order, and an option
    /// at its default, an option that the catalog does not keep, and a
    /// slot name that is the subscription name are absent. Streaming
    /// and synchronous commit given as a boolean are `on` or `off`, as
    /// pg_dump writes them.
    pub fn canonical(&self) -> Subscription {
        let mut parameters = Map::new();
        for (key, value) in self.parameters.iter().flatten() {
            if SUBSCRIPTION_CREATE_ONLY.contains(&key.as_str()) {
                continue;
            }
            let value = match (key.as_str(), value) {
                ("streaming" | "synchronous_commit", Value::Bool(on)) => {
                    Value::from(if *on { "on" } else { "off" })
                }
                // NONE is a keyword; any other slot name keeps its case
                ("slot_name", Value::String(slot))
                    if slot.eq_ignore_ascii_case("none") =>
                {
                    Value::from("NONE")
                }
                ("slot_name", value) => value.clone(),
                (_, Value::String(value)) => Value::from(value.to_lowercase()),
                (_, value) => value.clone(),
            };
            if Some(&value) != Self::parameter_default(key).as_ref() {
                parameters.insert(key.clone(), value);
            }
        }
        if parameters.get("slot_name").and_then(Value::as_str)
            == Some(self.name.as_str())
        {
            parameters.shift_remove("slot_name");
        }
        let mut publications = self.publications.clone();
        publications.sort();
        publications.dedup();
        Subscription {
            name: self.name.clone(),
            connection: self.connection.clone(),
            publications,
            parameters: (!parameters.is_empty()).then_some(parameters),
            comment: self.comment.clone(),
            security_labels: None,
        }
    }
}

/// Represents a transform, which converts a data type for a
/// procedural language. It has no owner and no schema: the project
/// files it under the schema of its type.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transform {
    pub schema: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub language: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_sql: Option<String>,
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
    /// Security labels (`SECURITY LABEL FOR provider ON ...`), as
    /// a label for each provider
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_labels: Option<super::SecurityLabels>,
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
