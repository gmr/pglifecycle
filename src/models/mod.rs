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
