//! Project loading and validation (ports project.py)

mod load;
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
    load::Loader::new(path).load()
}
