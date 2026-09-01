//! Groups, roles, users, and their ACLs

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Represents role grant/revoke ACLs
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Acls {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub databases: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domains: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub foreign_data_wrappers: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub foreign_servers: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub functions: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<Membership>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub languages: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub large_objects: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub roles: Option<Vec<Membership>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schemata: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequences: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tablespaces: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub types: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub views: Option<Map<String, Value>>,
}

/// One entry in a role's `roles` or `groups` ACL list: a membership in
/// that role, written as the bare role name when the membership
/// behaves the way PostgreSQL would grant it by default, and as a
/// mapping when it does not
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Membership {
    Name(String),
    Detailed(MembershipOptions),
}

/// A membership and the options that make it differ from a plain
/// `GRANT role TO member`. `admin` carries WITH ADMIN OPTION;
/// `inherit` and `set` need PostgreSQL 16 or later to emit
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipOptions {
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admin: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherit: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub set: Option<bool>,
}

impl Membership {
    /// The role granted, without its options
    pub fn role(&self) -> &str {
        match self {
            Self::Name(role) => role,
            Self::Detailed(options) => &options.role,
        }
    }

    /// The `WITH ...` option list for a GRANT, in a canonical order so
    /// two spellings of the same membership render identically
    pub fn options_sql(&self) -> Option<String> {
        let Self::Detailed(options) = self else {
            return None;
        };
        let mut parts = Vec::new();
        if options.admin == Some(true) {
            parts.push(String::from("ADMIN OPTION"));
        }
        for (keyword, value) in
            [("INHERIT", options.inherit), ("SET", options.set)]
        {
            if let Some(value) = value {
                parts.push(format!(
                    "{keyword} {}",
                    if value { "TRUE" } else { "FALSE" }
                ));
            }
        }
        (!parts.is_empty()).then(|| parts.join(", "))
    }
}

/// Represents a group
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Group {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environments: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grants: Option<Acls>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revocations: Option<Acls>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<GroupOptions>,
}

/// Options for a group
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_db: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_role: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherit: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub superuser: Option<bool>,
}

/// Represents a role
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Role {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environments: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grants: Option<Acls>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revocations: Option<Acls>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<RoleOptions>,
    /// `ALTER ROLE ... SET` parameters, as the schema's array of
    /// `{ name: value }` objects (role.yml / user.yml `settings`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<Vec<Map<String, Value>>>,
}

/// Options for a role or user
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bypass_rls: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_db: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_role: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherit: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub login: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub superuser: Option<bool>,
}

/// Represents a user
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct User {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environments: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid_until: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grants: Option<Acls>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revocations: Option<Acls>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<RoleOptions>,
    /// `ALTER ROLE ... SET` parameters, as the schema's array of
    /// `{ name: value }` objects (role.yml / user.yml `settings`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<Vec<Map<String, Value>>>,
}
