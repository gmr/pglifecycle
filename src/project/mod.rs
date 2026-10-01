//! Project loading and validation (ports project.py)

mod load;
pub(crate) use load::{
    aggregate_signature, generated_name, operator_signature,
    parameter_signature, split_sql_name, tag_signature,
};
pub mod validate;

use std::path::PathBuf;

use crate::models;

/// The complete project including all database objects
#[derive(Debug)]
pub struct Project {
    pub name: String,
    pub encoding: String,
    pub stdstrings: bool,
    pub superuser: String,
    pub default_schema: String,
    pub path: PathBuf,
    pub inventory: Vec<models::Item>,
}

/// Load the project from the specified project directory
pub fn load(path: &std::path::Path) -> Result<Project, String> {
    let _task = crate::progress::spinner("Loading project");
    load::Loader::new(path).load()
}
