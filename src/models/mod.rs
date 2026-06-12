//! Models for database objects (ports models.py)
//!
//! Field order matches the Python dataclasses; serde serializes in
//! declaration order, which sets the key order of emitted YAML. Optional
//! fields skip serialization when None so emitted files only contain
//! what was defined. `deny_unknown_fields` mirrors dataclass(**defn)
//! raising on unexpected keys.

mod function;
mod misc;
mod roles;
mod table;
mod text_search;
mod types;
mod views;

use std::collections::BTreeSet;

pub use function::*;
pub use misc::*;
pub use roles::*;
pub use table::*;
pub use text_search::*;
pub use types::*;
pub use views::*;

/// One database object definition of any supported type
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(untagged)]
pub enum Definition {
    Aggregate(Aggregate),
    Cast(Cast),
    Collation(Collation),
    Conversion(Conversion),
    Domain(Domain),
    EventTrigger(EventTrigger),
    Extension(Extension),
    ForeignDataWrapper(ForeignDataWrapper),
    Function(Function),
    Group(Group),
    Language(Language),
    MaterializedView(MaterializedView),
    Operator(Operator),
    Publication(Publication),
    Role(Role),
    Schema(Schema),
    Sequence(Sequence),
    Server(Server),
    Subscription(Subscription),
    Table(Table),
    Tablespace(Tablespace),
    TextSearch(TextSearch),
    Type(Type),
    User(User),
    UserMapping(UserMapping),
    View(View),
}

impl Definition {
    /// The object name (`name` field; casts derive one from their types)
    pub fn name(&self) -> String {
        match self {
            Definition::Aggregate(d) => d.name.clone(),
            Definition::Cast(d) => format!(
                "({} AS {})",
                d.source_type.as_deref().unwrap_or_default(),
                d.target_type.as_deref().unwrap_or_default()
            ),
            Definition::Collation(d) => d.name.clone(),
            Definition::Conversion(d) => d.name.clone(),
            Definition::Domain(d) => d.name.clone(),
            Definition::EventTrigger(d) => d.name.clone(),
            Definition::Extension(d) => d.name.clone(),
            Definition::ForeignDataWrapper(d) => d.name.clone(),
            Definition::Function(d) => d.name.clone(),
            Definition::Group(d) => d.name.clone(),
            Definition::Language(d) => d.name.clone(),
            Definition::MaterializedView(d) => d.name.clone(),
            Definition::Operator(d) => d.name.clone(),
            Definition::Publication(d) => d.name.clone(),
            Definition::Role(d) => d.name.clone(),
            Definition::Schema(d) => d.name.clone(),
            Definition::Sequence(d) => d.name.clone(),
            Definition::Server(d) => d.name.clone(),
            Definition::Subscription(d) => d.name.clone(),
            Definition::Table(d) => d.name.clone(),
            Definition::Tablespace(d) => d.name.clone(),
            Definition::TextSearch(d) => d.schema.clone(),
            Definition::Type(d) => d.name.clone(),
            Definition::User(d) => d.name.clone(),
            Definition::UserMapping(d) => d.name.clone(),
            Definition::View(d) => d.name.clone(),
        }
    }

    /// The owning role, where the type has one
    pub fn owner(&self) -> Option<&str> {
        match self {
            Definition::Aggregate(d) => Some(&d.owner),
            Definition::Cast(d) => Some(&d.owner),
            Definition::Collation(d) => Some(&d.owner),
            Definition::Conversion(d) => Some(&d.owner),
            Definition::Domain(d) => Some(&d.owner),
            Definition::ForeignDataWrapper(d) => Some(&d.owner),
            Definition::Function(d) => Some(&d.owner),
            Definition::MaterializedView(d) => Some(&d.owner),
            Definition::Operator(d) => Some(&d.owner),
            Definition::Schema(d) => Some(&d.owner),
            Definition::Sequence(d) => Some(&d.owner),
            Definition::Table(d) => Some(&d.owner),
            Definition::Tablespace(d) => Some(&d.owner),
            Definition::Type(d) => Some(&d.owner),
            Definition::View(d) => Some(&d.owner),
            _ => None,
        }
    }

    /// The object comment, where the type has one
    pub fn comment(&self) -> Option<&str> {
        match self {
            Definition::Aggregate(d) => d.comment.as_deref(),
            Definition::Cast(d) => d.comment.as_deref(),
            Definition::Collation(d) => d.comment.as_deref(),
            Definition::Conversion(d) => d.comment.as_deref(),
            Definition::Domain(d) => d.comment.as_deref(),
            Definition::EventTrigger(d) => d.comment.as_deref(),
            Definition::Extension(d) => d.comment.as_deref(),
            Definition::ForeignDataWrapper(d) => d.comment.as_deref(),
            Definition::Function(d) => d.comment.as_deref(),
            Definition::Group(d) => d.comment.as_deref(),
            Definition::Language(d) => d.comment.as_deref(),
            Definition::MaterializedView(d) => d.comment.as_deref(),
            Definition::Operator(d) => d.comment.as_deref(),
            Definition::Publication(d) => d.comment.as_deref(),
            Definition::Role(d) => d.comment.as_deref(),
            Definition::Schema(d) => d.comment.as_deref(),
            Definition::Sequence(d) => d.comment.as_deref(),
            Definition::Server(d) => d.comment.as_deref(),
            Definition::Subscription(d) => d.comment.as_deref(),
            Definition::Table(d) => d.comment.as_deref(),
            Definition::Tablespace(d) => d.comment.as_deref(),
            Definition::Type(d) => d.comment.as_deref(),
            Definition::User(d) => d.comment.as_deref(),
            Definition::View(d) => d.comment.as_deref(),
            Definition::TextSearch(_) | Definition::UserMapping(_) => None,
        }
    }

    /// The tablespace assignment, where the type has one
    pub fn tablespace(&self) -> Option<&str> {
        match self {
            Definition::MaterializedView(d) => d.tablespace.as_deref(),
            Definition::Table(d) => d.tablespace.as_deref(),
            _ => None,
        }
    }

    /// The schema the object belongs to, where the type has one
    pub fn schema(&self) -> Option<&str> {
        match self {
            Definition::Aggregate(d) => Some(&d.schema),
            Definition::Cast(d) => Some(&d.schema),
            Definition::Collation(d) => Some(&d.schema),
            Definition::Conversion(d) => Some(&d.schema),
            Definition::Domain(d) => Some(&d.schema),
            Definition::Extension(d) => d.schema.as_deref(),
            Definition::Function(d) => Some(&d.schema),
            Definition::MaterializedView(d) => Some(&d.schema),
            Definition::Operator(d) => Some(&d.schema),
            Definition::Sequence(d) => Some(&d.schema),
            Definition::Table(d) => Some(&d.schema),
            Definition::TextSearch(d) => Some(&d.schema),
            Definition::Type(d) => Some(&d.schema),
            Definition::View(d) => Some(&d.schema),
            _ => None,
        }
    }
}

/// An item in the project inventory
#[derive(Clone, Debug)]
pub struct Item {
    pub id: usize,
    pub desc: crate::constants::ObjectType,
    pub definition: Definition,
    pub dependencies: BTreeSet<usize>,
}
