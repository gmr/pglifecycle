//! Tables and their child objects (columns, constraints, indexes,
//! triggers, partitioning)

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A flag whose only non-default value is `true`, with an explicit
/// `false` as absent. Pull records only `true`, so the two then
/// compare equal.
fn true_only(value: Option<bool>) -> Option<bool> {
    value.filter(|value| *value)
}

/// Represents a table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub name: String,
    pub schema: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unlogged: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parents: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub like_table: Option<LikeTable>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<Column>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_defaults: Option<Vec<ColumnDefault>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexes: Option<Vec<Index>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<ConstraintColumns>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_constraints: Option<Vec<CheckConstraint>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_null_constraints: Option<Vec<NotNullConstraint>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_constraints: Option<Vec<ConstraintColumns>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub foreign_keys: Option<Vec<ForeignKey>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude_constraints: Option<Vec<ExcludeConstraint>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triggers: Option<Vec<Trigger>>,
    /// Whether row-level security is enabled and forced. Absent means
    /// the project does not manage it: deploy then leaves the table's
    /// row security, and its policies unless `policies` is given, as
    /// the database has them. Pull always writes it for a table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_level_security: Option<RowLevelSecurity>,
    /// What logical replication records to identify an updated or
    /// deleted row. Absent is DEFAULT, the primary key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica_identity: Option<ReplicaIdentity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policies: Option<Vec<Policy>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<TablePartitionBehavior>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions: Option<Vec<TablePartition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_parameters: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tablespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_tablespace: Option<String>,
    /// Foreign server backing a foreign table (CREATE FOREIGN TABLE ...
    /// SERVER); its presence marks the table as foreign
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    /// Foreign table OPTIONS (key 'value', ...); an open map, as the
    /// keys depend on the foreign data wrapper
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

impl Table {
    /// The same table with each *valid* table-level NOT NULL on one of
    /// its own columns moved onto that column.
    ///
    /// The two are one constraint written two ways. pg_dump writes a
    /// valid NOT NULL on a local column inline on the column, and the
    /// table-level form only for an inherited column, or for a NOT
    /// VALID one, which it has to add with ALTER TABLE. Clearing
    /// `not_valid` in a pulled project leaves the table-level form on a
    /// local column, which then never compares equal to what the
    /// database reports once validated, so deploy dropped and re-added
    /// it on every run. Comparing canonical forms removes that.
    pub fn with_canonical_not_nulls(&self) -> Table {
        let mut table = self.clone();
        let Some(not_nulls) = table.not_null_constraints.take() else {
            return table;
        };
        let mut kept = Vec::new();
        for not_null in not_nulls {
            let column = table
                .columns
                .iter_mut()
                .flatten()
                .find(|c| c.name == not_null.column);
            match column {
                Some(column) if not_null.not_valid != Some(true) => {
                    column.nullable = Some(false);
                    if not_null.name.is_some() || not_null.no_inherit.is_some()
                    {
                        column.not_null_constraint = Some(ColumnNotNull {
                            name: not_null.name,
                            no_inherit: not_null.no_inherit,
                        });
                    }
                }
                _ => kept.push(not_null),
            }
        }
        table.not_null_constraints = (!kept.is_empty()).then_some(kept);
        table
    }

    /// This table, the database side of a comparison, without the row
    /// security state and policies that `repo` does not manage.
    ///
    /// A project written before row security was modeled has neither
    /// field. Reading that as "disabled, no policies" would make deploy
    /// strip every table's protections, so absent means unmanaged
    /// instead. A `row_level_security` state manages the policies too,
    /// and there an absent list means none.
    pub fn without_unmanaged_security(&self, repo: &Table) -> Table {
        let mut table = self.clone();
        if repo.row_level_security.is_none() {
            table.row_level_security = None;
            if repo.policies.is_none() {
                table.policies = None;
            }
        }
        table
    }

    /// The same table in the form deploy compares: the NOT NULLs moved
    /// as [`Self::with_canonical_not_nulls`] moves them, and every value
    /// written at its default read as absent. A file may state a
    /// default (`forced: false`, `command: ALL`), which pull never
    /// writes, and the two have to compare equal.
    pub fn canonical(&self) -> Table {
        let mut table = self.with_canonical_not_nulls();
        for column in table.columns.iter_mut().flatten() {
            if let Some(options) = column
                .generated
                .as_mut()
                .and_then(|g| g.sequence_options.as_mut())
            {
                options.cycle = true_only(options.cycle);
            }
            if let Some(generated) = column.generated.as_mut() {
                generated.sequence_options = generated
                    .sequence_options
                    .take()
                    .filter(|options| *options != SequenceOptions::default());
            }
        }
        for check in table.check_constraints.iter_mut().flatten() {
            check.not_valid = true_only(check.not_valid);
        }
        for not_null in table.not_null_constraints.iter_mut().flatten() {
            not_null.not_valid = true_only(not_null.not_valid);
        }
        for foreign_key in table.foreign_keys.iter_mut().flatten() {
            foreign_key.not_valid = true_only(foreign_key.not_valid);
        }
        if let Some(state) = table.row_level_security.as_mut() {
            state.forced = true_only(state.forced);
        }
        table.replica_identity = match table.replica_identity.take() {
            Some(ReplicaIdentity::Mode(mode))
                if mode.eq_ignore_ascii_case("default") =>
            {
                None
            }
            Some(ReplicaIdentity::Mode(mode)) => {
                Some(ReplicaIdentity::Mode(mode.to_uppercase()))
            }
            other => other,
        };
        // pg_dump always writes the method, and btree is the default
        for exclude in table.exclude_constraints.iter_mut().flatten() {
            exclude.method.get_or_insert_with(|| String::from("btree"));
        }
        table.with_canonical_policies()
    }

    /// The same table with each policy canonical, in name order, and an
    /// empty list as none: policies are matched by name, so neither the
    /// order nor an empty list is a difference
    pub fn with_canonical_policies(&self) -> Table {
        let mut table = self.clone();
        if let Some(policies) = &mut table.policies {
            *policies = policies.iter().map(Policy::canonical).collect();
            policies.sort_by(|a, b| a.name.cmp(&b.name));
        }
        table.policies = table.policies.filter(|p| !p.is_empty());
        table
    }
}

/// Represents a column in a table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
    /// The name and NO INHERIT flag of the column's NOT NULL
    /// constraint (PostgreSQL 18+), present only when either differs
    /// from the default. `nullable` remains authoritative for whether
    /// the constraint exists at all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_null_constraint: Option<ColumnNotNull>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_constraint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generated: Option<ColumnGenerated>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// A default on a column the table does not declare locally: an
/// inheritance child carries defaults for columns it inherits, and
/// pg_dump writes them as a separate `ALTER TABLE ONLY child ALTER
/// COLUMN col SET DEFAULT ...` because there is no local column entry
/// to hold one
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnDefault {
    pub column: String,
    pub default: Value,
}

/// The parts of a column's NOT NULL constraint that `nullable: false`
/// cannot express. PostgreSQL 18 made NOT NULL a named constraint, and
/// pg_dump writes the name only when it is not the one PostgreSQL
/// generates, `<table>_<column>_not_null`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnNotNull {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no_inherit: Option<bool>,
}

/// Represents configuration of a generated column
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnGenerated {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    /// Whether the expression is materialized. PostgreSQL 18 made
    /// `VIRTUAL` the default, and pg_dump omits the keyword for a
    /// virtual column, so this has to be recorded rather than assumed.
    ///
    /// Absent means `Stored`: project files written before this field
    /// existed carry no keyword and have always rendered `STORED`, so
    /// reading absence as virtual would silently change what they
    /// build. `pull` writes the field on every generated column.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<GeneratedKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence_behavior: Option<String>,
    /// The options of an identity column's own sequence. pg_dump writes
    /// them in `ALTER TABLE ... ADD GENERATED ... AS IDENTITY (...)`,
    /// and they are what makes one identity column differ from another:
    /// without them an identity that starts at 100 or cycles rebuilt as
    /// a plain one that starts at 1.
    ///
    /// Separate from `sequence`, which older project files use to name
    /// a sequence managed as its own object. The build never renders
    /// that name, so rendering it as `SEQUENCE NAME` would create the
    /// sequence a second time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence_options: Option<SequenceOptions>,
}

/// An identity column's sequence options. Each is absent when it holds
/// PostgreSQL's default, which is also what a hand-written identity
/// omits, so the two compare equal instead of forcing a rebuild.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SequenceOptions {
    /// Kept only when it is not the `<table>_<column>_seq` PostgreSQL
    /// generates in the table's schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_with: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub increment_by: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_value: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_value: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cycle: Option<bool>,
}

/// How a generated column's expression is materialized
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GeneratedKind {
    Stored,
    Virtual,
}

impl GeneratedKind {
    /// The keyword `CREATE TABLE` expects after the expression
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stored => "STORED",
            Self::Virtual => "VIRTUAL",
        }
    }
}

/// Represents a Check Constraint in a Table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckConstraint {
    pub name: String,
    pub expression: String,
    /// `false` renders `NOT ENFORCED` (PostgreSQL 18+). PostgreSQL
    /// records a not-enforced constraint as not validated as well, and
    /// pg_dump writes only the `NOT ENFORCED` clause for it, so this
    /// one field covers both. Absent means enforced, the default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enforced: Option<bool>,
    /// `true` renders `NOT VALID`: rows already in the table were never
    /// checked, and only new ones are. See [`ForeignKey::not_valid`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_valid: Option<bool>,
}

/// A table-level `NOT NULL <column>` constraint (PostgreSQL 18+).
/// pg_dump emits this form only for a column the table inherits rather
/// than declares, since there is no local column entry to carry it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotNullConstraint {
    /// Omitted when the constraint carries the name PostgreSQL
    /// generates by default (`<table>_<column>_not_null`), which is
    /// also when pg_dump omits it
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub column: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no_inherit: Option<bool>,
    /// `true` renders `NOT VALID`; see [`ForeignKey::not_valid`]. Only
    /// the table-level form carries it: a column's own `NOT NULL NOT
    /// VALID` is a syntax error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_valid: Option<bool>,
}

/// Constraint columns for primary keys and unique constraints. The YAML
/// form may be a single column name, a list of column names, or a
/// mapping with `columns` and optional `include` (see table.yml)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConstraintColumns {
    Name(String),
    Columns(Vec<String>),
    Detailed {
        /// The constraint name, kept only when it differs from the one
        /// PostgreSQL generates, which is also when pg_dump writes it.
        /// Without it a named unique or primary key constraint rebuilt
        /// under a generated name, and `deploy` could not reconcile it
        /// in place, because it matches a constraint by name.
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        columns: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        include: Option<Vec<String>>,
        /// `UNIQUE NULLS NOT DISTINCT`, where one null equals another
        /// (PostgreSQL 15+). PostgreSQL rejects the clause on a
        /// primary key, whose columns cannot be null.
        #[serde(skip_serializing_if = "Option::is_none")]
        nulls_not_distinct: Option<bool>,
        /// `WITHOUT OVERLAPS` on the last column, which makes the
        /// constraint temporal (PostgreSQL 18+). The grammar attaches
        /// it to the column list rather than to a named column, so it
        /// is a flag: it always applies to the last column, which must
        /// be a range or multirange. Legal on a primary key and on a
        /// unique constraint.
        #[serde(skip_serializing_if = "Option::is_none")]
        without_overlaps: Option<bool>,
    },
}

/// Represents a Foreign Key on a Table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub references: ForeignKeyReference,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_delete: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_update: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deferrable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initially_deferred: Option<bool>,
    /// The referencing side's range column in a temporal foreign key,
    /// `FOREIGN KEY (parent, PERIOD valid_at)` (PostgreSQL 18+)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period: Option<String>,
    /// `false` renders `NOT ENFORCED`; see [`CheckConstraint::enforced`]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enforced: Option<bool>,
    /// `true` renders `NOT VALID`. A constraint keeps that state only
    /// when added with ALTER TABLE: in CREATE TABLE, PostgreSQL checks
    /// the (empty) table and records it valid, so the build emits a
    /// not-valid constraint as its own entry. pg_dump writes only `NOT
    /// ENFORCED` for a constraint that is not enforced, since that
    /// implies not validated, so the two do not appear together.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_valid: Option<bool>,
}

/// An exclusion constraint (EXCLUDE USING method (element WITH
/// operator, ...)): no two rows may have every element compare true
/// under its operator
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExcludeConstraint {
    pub name: String,
    /// The index access method; PostgreSQL's default is btree, and
    /// pg_dump always writes it
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    pub elements: Vec<ExcludeElement>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include: Option<Vec<String>>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
    pub where_clause: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deferrable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initially_deferred: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// One element of an exclusion constraint: an index column (a column
/// or an expression, with its collation, operator class and order)
/// and the operator it is compared with
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExcludeElement {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub opclass: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_placement: Option<String>,
    pub operator: String,
}

impl ExcludeElement {
    /// The index column part of the element
    pub fn column(&self) -> IndexColumn {
        IndexColumn {
            name: self.name.clone(),
            expression: self.expression.clone(),
            collation: self.collation.clone(),
            opclass: self.opclass.clone(),
            direction: self.direction.clone(),
            null_placement: self.null_placement.clone(),
        }
    }
}

/// Represents the table a Foreign Key references
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKeyReference {
    pub name: String,
    pub columns: Vec<String>,
    /// The referenced side's range column in a temporal foreign key,
    /// `REFERENCES t (id, PERIOD valid_at)` (PostgreSQL 18+)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period: Option<String>,
}

/// Represents an Index on a table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Index {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recurse: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<IndexColumn>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include: Option<Vec<String>>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
    pub where_clause: Option<String>,
    /// `NULLS NOT DISTINCT` on a unique index (PostgreSQL 15+)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nulls_not_distinct: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_parameters: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tablespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Represents a column in an index on a table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexColumn {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub opclass: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_placement: Option<String>,
}

/// Represents the settings for creating a table using LIKE
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LikeTable {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_comments: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_constraints: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_defaults: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_generated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_identity: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_indexes: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_statistics: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_storage: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_all: Option<bool>,
}

/// Defines a table partition
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePartition {
    pub name: String,
    pub schema: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_values_in: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_values_from: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_values_to: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_values_with: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Defines how a table is partitioned
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePartitionBehavior {
    #[serde(rename = "type")]
    pub partition_type: String,
    pub columns: Vec<TablePartitionColumn>,
}

/// A table partition column: either a bare column name or a mapping
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TablePartitionColumn {
    Name(String),
    Detailed {
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        expression: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        collation: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        opclass: Option<String>,
    },
}

/// A table's replica identity other than DEFAULT: `FULL` (the whole
/// row), `NOTHING`, or a unique index
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ReplicaIdentity {
    Mode(String),
    Index { index: String },
}

/// A table's row-level security state
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowLevelSecurity {
    pub enabled: bool,
    /// FORCE ROW LEVEL SECURITY: the policies apply to the table owner
    /// too
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forced: Option<bool>,
}

/// A row-level security policy (CREATE POLICY). Each field that has a
/// default keeps only a value that differs from it, so a hand-written
/// `command: ALL` or `roles: [PUBLIC]` compares equal to the policy
/// pulled from the database, where pg_dump omits both.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub name: String,
    /// AS RESTRICTIVE; the default is PERMISSIVE
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restrictive: Option<bool>,
    /// SELECT, INSERT, UPDATE or DELETE; the default is ALL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// The roles the policy applies to; the default is PUBLIC
    #[serde(skip_serializing_if = "Option::is_none")]
    pub roles: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub using: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with_check: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

impl Policy {
    /// The same policy with each field at its default as absent, and
    /// the command and PUBLIC in upper case, as pull reads them
    pub fn canonical(&self) -> Policy {
        let roles = self.roles.as_ref().map(|roles| {
            roles
                .iter()
                .map(|role| {
                    if role.eq_ignore_ascii_case("public") {
                        String::from("PUBLIC")
                    } else {
                        role.clone()
                    }
                })
                .collect::<Vec<_>>()
        });
        Policy {
            restrictive: true_only(self.restrictive),
            command: self
                .command
                .as_ref()
                .map(|command| command.to_uppercase())
                .filter(|command| command != "ALL"),
            roles: roles.filter(|roles| roles != &["PUBLIC"]),
            ..self.clone()
        }
    }
}

/// Table Triggers
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trigger {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub for_each: Option<String>,
    /// A CONSTRAINT TRIGGER (always AFTER ROW; may be deferrable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deferrable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initially_deferred: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A value written at its default compares equal to the absent
    /// value pull records, so deploy does not see a change on every
    /// run; the file itself keeps what it says
    #[test]
    fn written_defaults_are_canonically_absent() {
        let table: Table = serde_json::from_value(serde_json::json!({
            "name": "t", "schema": "s", "owner": "o",
            "columns": [{"name": "id", "data_type": "integer",
                         "generated": {"sequence_behavior": "ALWAYS",
                                       "sequence_options": {"cycle": false}}}],
            "check_constraints": [
                {"name": "c", "expression": "a > 0", "not_valid": false}
            ],
            "row_level_security": {"enabled": true, "forced": false},
            "policies": [
                {"name": "p", "restrictive": false, "command": "all",
                 "roles": ["public"]},
                {"name": "o", "command": "select",
                 "roles": ["alice", "public"]},
            ],
        }))
        .unwrap();
        assert_eq!(
            table.row_level_security.as_ref().unwrap().forced,
            Some(false)
        );
        let canonical = table.canonical();
        let generated =
            canonical.columns.as_ref().unwrap()[0].generated.as_ref();
        assert_eq!(generated.unwrap().sequence_options, None);
        assert_eq!(canonical.check_constraints.unwrap()[0].not_valid, None);
        assert_eq!(canonical.row_level_security.unwrap().forced, None);
        let policies = canonical.policies.unwrap();
        assert_eq!(policies[0].name, "o");
        assert_eq!(policies[0].command.as_deref(), Some("SELECT"));
        assert_eq!(
            policies[0].roles,
            Some(vec![String::from("alice"), String::from("PUBLIC")])
        );
        assert_eq!(
            (
                &policies[1].restrictive,
                &policies[1].command,
                &policies[1].roles
            ),
            (&None, &None, &None)
        );
    }

    /// A project written before row security was modeled leaves the
    /// database's state and policies alone; a stated state manages the
    /// policies too
    #[test]
    fn absent_row_security_is_unmanaged() {
        let table = |value: serde_json::Value| -> Table {
            let mut base = serde_json::json!(
                {"name": "t", "schema": "s", "owner": "o"}
            );
            base.as_object_mut()
                .unwrap()
                .extend(value.as_object().unwrap().clone());
            serde_json::from_value(base).unwrap()
        };
        let db = table(serde_json::json!({
            "row_level_security": {"enabled": true},
            "policies": [{"name": "p"}],
        }));
        let stale = table(serde_json::json!({}));
        let stripped = db.without_unmanaged_security(&stale);
        assert_eq!(stripped.row_level_security, None);
        assert_eq!(stripped.policies, None);
        let policies_only = table(serde_json::json!({"policies": []}));
        let stripped = db.without_unmanaged_security(&policies_only);
        assert_eq!(stripped.row_level_security, None);
        assert!(stripped.policies.is_some());
        let managed = table(
            serde_json::json!({"row_level_security": {"enabled": false}}),
        );
        assert_eq!(db.without_unmanaged_security(&managed), db);
    }
}
