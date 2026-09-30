//! Functions and procedures

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The settings that PostgreSQL keeps as a list of names, which pg_dump
/// writes as one string constant for each element. This is
/// `variable_is_guc_list_quote` in PostgreSQL 18's
/// src/bin/pg_dump/dumputils.c: the settings marked `GUC_LIST_QUOTE`.
/// A routine keeps such a setting as a list. As one string, the value
/// is one name.
pub const LIST_SETTINGS: &[&str] = &[
    "local_preload_libraries",
    "oauth_validator_libraries",
    "output_plugin_libraries",
    "search_path",
    "session_preload_libraries",
    "shared_preload_libraries",
    "temp_tablespaces",
    "unix_socket_directories",
];

/// Whether PostgreSQL keeps the setting `name` as a list of names. A
/// setting name has no case.
pub fn is_list_setting(name: &str) -> bool {
    LIST_SETTINGS.iter().any(|s| s.eq_ignore_ascii_case(name))
}

/// The settings of a routine in the form that deploy compares, which
/// is the form that pull writes. A setting name has no case, pull reads
/// a value as text, and a list of one element is that element.
pub fn canonical_settings(
    settings: &Map<String, Value>,
) -> Map<String, Value> {
    settings
        .iter()
        .map(|(name, value)| {
            let value = match value {
                Value::Number(_) | Value::Bool(_) => {
                    Value::String(value.to_string())
                }
                Value::Array(items) if items.len() == 1 => items[0].clone(),
                other => other.clone(),
            };
            (name.to_lowercase(), value)
        })
        .collect()
}

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
    /// A SQL-standard body, `BEGIN ATOMIC ... END`, in place of
    /// `definition`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_body: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

impl Function {
    /// The identity signature (`name(mode name type, ...)`) used to
    /// match `COMMENT ON FUNCTION` and ACL targets; mirrors
    /// `pg_get_function_identity_arguments` — defaults are omitted,
    /// `IN` modes are implicit, `OUT` parameters are kept (Postgres
    /// includes them in the identity), and `RETURNS TABLE` columns are
    /// excluded (they never reach `parameters`, but guard anyway)
    pub fn identity(&self) -> String {
        let args: Vec<String> = self
            .parameters
            .iter()
            .flatten()
            .filter(|p| p.mode != "TABLE")
            .map(|p| {
                let mut parts: Vec<&str> = Vec::new();
                if p.mode != "IN" {
                    parts.push(&p.mode);
                }
                if let Some(name) = &p.name {
                    parts.push(name);
                }
                parts.push(&p.data_type);
                parts.join(" ")
            })
            .collect();
        format!("{}({})", self.name, args.join(", "))
    }
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
    /// A SQL-standard body, `BEGIN ATOMIC ... END`, in place of
    /// `definition`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_body: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

impl Procedure {
    /// The same routine as a function with no return type, which the
    /// build renders with PROCEDURE in place of FUNCTION
    pub fn as_function(&self) -> Function {
        Function {
            name: self.name.clone(),
            schema: self.schema.clone(),
            owner: self.owner.clone(),
            sql: self.sql.clone(),
            parameters: self.parameters.clone(),
            returns: None,
            language: self.language.clone(),
            transform_types: self.transform_types.clone(),
            window: None,
            immutable: None,
            stable: None,
            volatile: None,
            leak_proof: None,
            called_on_null_input: None,
            strict: None,
            security: self.security.clone(),
            parallel: None,
            cost: None,
            rows: None,
            support: None,
            configuration: self.configuration.clone(),
            definition: self.definition.clone(),
            sql_body: self.sql_body.clone(),
            object_file: self.object_file.clone(),
            link_symbol: self.link_symbol.clone(),
            comment: self.comment.clone(),
        }
    }

    /// The procedure a parsed routine describes, dropping what only a
    /// function can have
    pub fn from_function(function: Function) -> Procedure {
        Procedure {
            name: function.name,
            schema: function.schema,
            owner: function.owner,
            sql: function.sql,
            parameters: function.parameters,
            language: function.language,
            transform_types: function.transform_types,
            security: function.security,
            configuration: function.configuration,
            definition: function.definition,
            sql_body: function.sql_body,
            object_file: function.object_file,
            link_symbol: function.link_symbol,
            comment: function.comment,
        }
    }

    /// The identity signature, as [`Function::identity`] computes it
    pub fn identity(&self) -> String {
        self.as_function().identity()
    }

    /// The same procedure in the form deploy compares, where each field
    /// that a file can write in more than one way has the form that
    /// pull writes. PostgreSQL keeps no typmod in a parameter type,
    /// folds the language name to lower case, and `INVOKER` is the
    /// default security. pull reads a default as text, and an empty
    /// parameter list as absent. The settings compare as
    /// [`canonical_settings`] gives them.
    pub fn canonical(&self) -> Procedure {
        let text = |value: &Value| match value {
            Value::Number(_) | Value::Bool(_) => {
                Value::String(value.to_string())
            }
            other => other.clone(),
        };
        let identity_type = crate::deploy::identity_type;
        Procedure {
            parameters: self
                .parameters
                .as_ref()
                .filter(|parameters| !parameters.is_empty())
                .map(|parameters| {
                    parameters
                        .iter()
                        .map(|p| FunctionParameter {
                            data_type: identity_type(&p.data_type),
                            default: p.default.as_ref().map(text),
                            ..p.clone()
                        })
                        .collect()
                }),
            language: self.language.as_ref().map(|l| l.to_lowercase()),
            transform_types: self
                .transform_types
                .as_ref()
                .map(|types| types.iter().map(|t| identity_type(t)).collect()),
            security: self
                .security
                .clone()
                .filter(|s| !s.eq_ignore_ascii_case("INVOKER")),
            configuration: self.configuration.as_ref().map(canonical_settings),
            ..self.clone()
        }
    }
}
