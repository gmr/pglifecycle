//! Project loading and validation (ports project.py)

mod load;
pub(crate) use load::{
    aggregate_signature, operator_signature, parameter_signature,
    routine_base_name, split_sql_name, tag_signature,
};
mod type_names;
pub mod validate;

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde_json::{Map, Value};

use crate::models;

/// The complete project including all database objects
#[derive(Debug)]
pub struct Project {
    pub name: String,
    pub superuser: String,
    pub default_schema: String,
    pub path: PathBuf,
    pub settings: DatabaseSettings,
    pub inventory: Vec<models::Item>,
}

/// The settings and the comment of the database, from `project.yaml`.
/// Each list has one `{ name: value }` object for each setting, as
/// `role.yml` `settings` has
#[derive(Debug, Default)]
pub struct DatabaseSettings {
    /// `comment`: `COMMENT ON DATABASE`
    pub comment: Option<String>,
    /// `security_labels`: `SECURITY LABEL ON DATABASE`
    pub security_labels: Option<models::SecurityLabels>,
    /// `connection_limit`: `ALTER DATABASE ... CONNECTION LIMIT`
    pub connection_limit: Option<i64>,
    /// `is_template`: `ALTER DATABASE ... IS_TEMPLATE`
    pub is_template: Option<bool>,
    /// `settings`: `ALTER DATABASE ... SET`
    pub database: Vec<Map<String, Value>>,
    /// `role_settings`: `ALTER ROLE ... IN DATABASE ... SET`, by role
    /// name
    pub roles: BTreeMap<String, Vec<Map<String, Value>>>,
}

/// Load the project from the specified project directory
pub fn load(path: &std::path::Path) -> Result<Project, String> {
    let _task = crate::progress::spinner("Loading project");
    load::Loader::new(path).load()
}
