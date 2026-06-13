//! Shared constants (ports constants.py and project.py classifications)

/// Project subdirectories, one per object-type path in the managed layout
/// (constants.PATHS in the Python implementation, deduplicated — foreign
/// tables share the tables directory)
pub const PROJECT_DIRS: &[&str] = &[
    "aggregates",
    "casts",
    "collations",
    "conversions",
    "dml",
    "domains",
    "event_triggers",
    "functions",
    "groups",
    "materialized_views",
    "operators",
    "procedures",
    "publications",
    "roles",
    "schemata",
    "sequences",
    "servers",
    "subscriptions",
    "tables",
    "tablespaces",
    "text_search",
    "types",
    "user_mappings",
    "users",
    "views",
];

/// Database object types tracked in the project inventory
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ObjectType {
    Aggregate,
    Cast,
    Collation,
    Conversion,
    Domain,
    EventTrigger,
    Extension,
    ForeignDataWrapper,
    Function,
    Group,
    MaterializedView,
    Operator,
    ProceduralLanguage,
    Publication,
    Role,
    Schema,
    Sequence,
    Server,
    Subscription,
    Table,
    Tablespace,
    TextSearch,
    Type,
    User,
    UserMapping,
    View,
}

impl ObjectType {
    /// The pg_dump-style description (constants.py values)
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Aggregate => "AGGREGATE",
            Self::Cast => "CAST",
            Self::Collation => "COLLATION",
            Self::Conversion => "CONVERSION",
            Self::Domain => "DOMAIN",
            Self::EventTrigger => "EVENT TRIGGER",
            Self::Extension => "EXTENSION",
            Self::ForeignDataWrapper => "FOREIGN DATA WRAPPER",
            Self::Function => "FUNCTION",
            Self::Group => "GROUP",
            Self::MaterializedView => "MATERIALIZED VIEW",
            Self::Operator => "OPERATOR",
            Self::ProceduralLanguage => "PROCEDURAL LANGUAGE",
            Self::Publication => "PUBLICATION",
            Self::Role => "ROLE",
            Self::Schema => "SCHEMA",
            Self::Sequence => "SEQUENCE",
            Self::Server => "SERVER",
            Self::Subscription => "SUBSCRIPTION",
            Self::Table => "TABLE",
            Self::Tablespace => "TABLESPACE",
            Self::TextSearch => "TEXT SEARCH",
            Self::Type => "TYPE",
            Self::User => "USER",
            Self::UserMapping => "USER MAPPING",
            Self::View => "VIEW",
        }
    }

    /// Project subdirectory holding this object type's files
    pub fn path(&self) -> Option<&'static str> {
        Some(match self {
            Self::Aggregate => "aggregates",
            Self::Cast => "casts",
            Self::Collation => "collations",
            Self::Conversion => "conversions",
            Self::Domain => "domains",
            Self::EventTrigger => "event_triggers",
            Self::Function => "functions",
            Self::Group => "groups",
            Self::MaterializedView => "materialized_views",
            Self::Operator => "operators",
            Self::Publication => "publications",
            Self::Role => "roles",
            Self::Schema => "schemata",
            Self::Sequence => "sequences",
            Self::Server => "servers",
            Self::Subscription => "subscriptions",
            Self::Table => "tables",
            Self::Tablespace => "tablespaces",
            Self::TextSearch => "text_search",
            Self::Type => "types",
            Self::User => "users",
            Self::UserMapping => "user_mappings",
            Self::View => "views",
            Self::Extension
            | Self::ForeignDataWrapper
            | Self::ProceduralLanguage => return None,
        })
    }

    /// The JSON-Schema file stem for this object type
    pub fn schema_file(&self) -> String {
        self.as_str().to_lowercase().replace(' ', "_")
    }

    /// Map a plural container/dependency key to its object type
    /// (constants.OBJ_KEYS)
    pub fn from_plural_key(key: &str) -> Option<Self> {
        Some(match key {
            "casts" => Self::Cast,
            "conversions" => Self::Conversion,
            "domains" => Self::Domain,
            "extensions" => Self::Extension,
            "foreign data wrappers" => Self::ForeignDataWrapper,
            "functions" => Self::Function,
            "groups" => Self::Group,
            "languages" => Self::ProceduralLanguage,
            "operators" => Self::Operator,
            "roles" => Self::Role,
            "sequences" => Self::Sequence,
            "schemata" => Self::Schema,
            "tables" => Self::Table,
            "tablespaces" => Self::Tablespace,
            "text_search" => Self::TextSearch,
            "types" => Self::Type,
            "views" => Self::View,
            _ => return None,
        })
    }

    /// The plural key used by this type's per-schema container files
    pub fn plural_key(&self) -> &'static str {
        match self {
            Self::Cast => "casts",
            Self::Conversion => "conversions",
            Self::Operator => "operators",
            Self::TextSearch => "text_search",
            Self::Type => "types",
            other => unreachable!("no container key for {other:?}"),
        }
    }

    /// Object types read from per-schema container files
    /// (project.py _PER_SCHEMA_FILES)
    pub fn is_per_schema_file(&self) -> bool {
        matches!(
            self,
            Self::Cast
                | Self::Conversion
                | Self::Operator
                | Self::TextSearch
                | Self::Type
        )
    }

    /// Object types without an `owner` field (project.py _OWNERLESS)
    pub fn is_ownerless(&self) -> bool {
        matches!(
            self,
            Self::EventTrigger
                | Self::Group
                | Self::Publication
                | Self::Role
                | Self::Server
                | Self::Subscription
                | Self::TextSearch
                | Self::User
                | Self::UserMapping
        )
    }

    /// Object types without a `schema` field (project.py _SCHEMALESS)
    pub fn is_schemaless(&self) -> bool {
        matches!(
            self,
            Self::EventTrigger
                | Self::Group
                | Self::Publication
                | Self::Role
                | Self::Schema
                | Self::Server
                | Self::Subscription
                | Self::Tablespace
                | Self::User
                | Self::UserMapping
        )
    }
}

/// The order object files are read in (project.py _READ_ORDER)
pub const READ_ORDER: &[ObjectType] = &[
    ObjectType::Schema,
    ObjectType::Operator,
    ObjectType::Aggregate,
    ObjectType::Collation,
    ObjectType::Conversion,
    ObjectType::Type,
    ObjectType::Domain,
    ObjectType::Tablespace,
    ObjectType::Table,
    ObjectType::Sequence,
    ObjectType::Function,
    ObjectType::View,
    ObjectType::MaterializedView,
    ObjectType::Cast,
    ObjectType::TextSearch,
    ObjectType::Server,
    ObjectType::EventTrigger,
    ObjectType::Publication,
    ObjectType::Subscription,
];

pub const DEPENDENCIES: &str = "dependencies";
