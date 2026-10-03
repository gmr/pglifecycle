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

/// The settings of the database, from `project.yaml`. Each list has
/// one `{ name: value }` object for each setting, as `role.yml`
/// `settings` has
#[derive(Debug, Default)]
pub struct DatabaseSettings {
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
