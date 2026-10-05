//! Tables and their child objects (columns, constraints, indexes,
//! triggers, partitioning)

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A flag whose only non-default value is `true`, with an explicit
/// `false` as absent. Pull records only `true`, so the two then
/// compare equal.
fn true_only(value: Option<bool>) -> Option<bool> {
    value.filter(|value| *value)
}

/// Storage parameters with each value as the text of the value that
/// PostgreSQL reads. PostgreSQL keeps each value as text, and pull
/// writes that text, but a project can give a YAML number or boolean.
///
/// - A boolean, and the words true, false, on, off, yes and no in any
///   case, are `true` or `false`. PostgreSQL reads each of these words
///   as a boolean, and an option with more values (such as
///   `vacuum_index_cleanup`) reads them as a boolean too.
/// - A number is the text of its value, so `0.1` and `0.10` are equal.
///   1 and 0 are numbers here, although a boolean option also accepts
///   them.
/// - Other text stays as it is.
///
/// An empty map is none: the build writes no `WITH ()` for it.
pub(crate) fn canonical_storage_parameters(
    parameters: Option<Map<String, Value>>,
) -> Option<Map<String, Value>> {
    let value = |value: Value| {
        let text = match value {
            Value::String(text) => text,
            other => other.to_string(),
        };
        let text = match text.to_ascii_lowercase().as_str() {
            "true" | "on" | "yes" => String::from("true"),
            "false" | "off" | "no" => String::from("false"),
            _ => match text.parse::<f64>() {
                Ok(number) if number.is_finite() => number.to_string(),
                _ => text,
            },
        };
        Value::String(text)
    };
    parameters.filter(|p| !p.is_empty()).map(|parameters| {
        parameters
            .into_iter()
            .map(|(key, parameter)| (key, value(parameter)))
            .collect()
    })
}

/// A collation as PostgreSQL finds it (see
/// [`crate::deploy::canonical_collation`])
fn canonical_collation(collation: &mut Option<String>) {
    if let Some(name) = collation {
        *name = crate::deploy::canonical_collation(name);
    }
}

/// An expression with the type of each cast in the form that
/// PostgreSQL writes (see [`crate::deploy::canonical_casts`])
fn canonical_expression(expression: &mut Option<String>) {
    if let Some(text) = expression {
        *text = crate::deploy::canonical_casts(text);
    }
}

/// A column default with the type of each cast in the form that
/// PostgreSQL writes. A default that is not a string has no cast. A
/// NULL default on a column of a built-in type is in the form that
/// PostgreSQL stores (see [`crate::deploy::stored_null_default`]).
fn canonical_default(data_type: &str, default: &mut Option<Value>) {
    if let Some(Value::String(text)) = default
        && let Some(stored) = crate::deploy::stored_null_default(
            data_type,
            text,
            &crate::deploy::UserTypes::new(),
        )
    {
        *default = stored.map(Value::String);
    }
    if let Some(Value::String(text)) = default {
        *text = crate::deploy::canonical_casts(text);
    }
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
    /// Comments on the table's primary key, unique, check, foreign key
    /// and NOT NULL constraints, by constraint name. An exclusion
    /// constraint keeps its comment on itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint_comments:
        Option<std::collections::BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triggers: Option<Vec<Trigger>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rules: Option<Vec<Rule>>,
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
    /// The name of each NOT NULL constraint of the table, by column:
    /// the name that the model records, else the name that PostgreSQL
    /// gives it. PostgreSQL first keeps the names that were given, then
    /// names the others in the order of the columns, and adds a number
    /// to a name that is in use (`AddRelationNotNullConstraints`). Thus
    /// two long columns can get `<cut>_not_null` and `<cut>_not_null1`.
    /// PostgreSQL also adds a number for a name that a constraint of
    /// another table in the schema has; that case is not known here.
    pub fn not_null_names(&self) -> BTreeMap<String, String> {
        let columns = self
            .columns
            .iter()
            .flatten()
            .filter(|c| c.nullable == Some(false))
            .map(|c| {
                let name = c.not_null_constraint.as_ref();
                (&c.name, name.and_then(|n| n.name.as_ref()))
            });
        let table_level = self
            .not_null_constraints
            .iter()
            .flatten()
            .map(|c| (&c.column, c.name.as_ref()));
        let mut names = BTreeMap::new();
        let mut unnamed = Vec::new();
        for (column, name) in columns.chain(table_level) {
            match name {
                Some(name) => {
                    names.insert(column.clone(), name.clone());
                }
                None => unnamed.push(column),
            }
        }
        let mut used: BTreeSet<String> = names.values().cloned().collect();
        for column in unnamed {
            let name = crate::utils::choose_constraint_name(
                &self.name,
                Some(column),
                "not_null",
                &used,
            );
            used.insert(name.clone());
            names.insert(column.clone(), name);
        }
        names
    }

    /// Whether PostgreSQL makes the column NOT NULL, also when the
    /// column does not say it: a column of the primary key, or an
    /// identity column. PostgreSQL 18 gives the column a NOT NULL
    /// constraint of the name that [`Self::not_null_names`] gives, and
    /// refuses DROP NOT NULL on it.
    pub fn is_always_not_null(&self, column: &Column) -> bool {
        let in_key = self
            .primary_key
            .as_ref()
            .is_some_and(|key| key.columns().contains(&column.name));
        let identity = column.generated.as_ref().is_some_and(|g| {
            g.expression.is_none()
                && (g.sequence_behavior.is_some() || g.sequence.is_some())
        });
        in_key || identity
    }

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

    /// The same table with the CHECK of each column moved to the
    /// CHECKs of the table.
    ///
    /// PostgreSQL keeps a CHECK on a column as a CHECK of the table, and
    /// pg_dump and pull write it so. The name is
    /// `<table>_<column>_check`, with a number when a CHECK of the table
    /// has the name. Build writes the CHECK with this name (deviation
    /// 60), because PostgreSQL takes the name of a CHECK with no name
    /// from the columns of its expression and in the order of the
    /// constraints.
    pub fn with_table_checks(&self) -> Table {
        let mut table = self.clone();
        let mut used: BTreeSet<String> = table
            .check_constraints
            .iter()
            .flatten()
            .map(|c| c.name.clone())
            .collect();
        let mut moved = Vec::new();
        for column in table.columns.iter_mut().flatten() {
            let Some(expression) = column.check_constraint.take() else {
                continue;
            };
            let name = crate::utils::choose_constraint_name(
                &table.name,
                Some(&column.name),
                "check",
                &used,
            );
            used.insert(name.clone());
            moved.push(CheckConstraint {
                name,
                expression,
                enforced: None,
                not_valid: None,
            });
        }
        if !moved.is_empty() {
            table
                .check_constraints
                .get_or_insert_default()
                .extend(moved);
        }
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
    /// writes, and the two have to compare equal. A value that
    /// PostgreSQL keeps in another form than the project can write it
    /// (a storage parameter, a collation, the type of a cast in an index
    /// or an exclusion constraint expression, in a WHERE clause, a CHECK
    /// constraint, a default, a generated column expression, a policy
    /// expression or a trigger WHEN condition) is in the form that
    /// PostgreSQL reads. A column that PostgreSQL makes NOT NULL (see
    /// [`Self::is_always_not_null`]) is NOT NULL. A CHECK on a column is
    /// a CHECK of the table (see [`Self::with_table_checks`]), and the
    /// CHECKs are in name order.
    pub fn canonical(&self) -> Table {
        let mut table = self.with_canonical_not_nulls().with_table_checks();
        let not_null: Vec<bool> = table
            .columns
            .iter()
            .flatten()
            .map(|c| table.is_always_not_null(c))
            .collect();
        for (column, not_null) in
            table.columns.iter_mut().flatten().zip(not_null)
        {
            if not_null {
                column.nullable = Some(false);
            }
            canonical_collation(&mut column.collation);
            canonical_default(&column.data_type, &mut column.default);
            if let Some(options) = column
                .generated
                .as_mut()
                .and_then(|g| g.sequence_options.as_mut())
            {
                options.cycle = true_only(options.cycle);
            }
            if let Some(generated) = column.generated.as_mut() {
                canonical_expression(&mut generated.expression);
                generated.sequence_options = generated
                    .sequence_options
                    .take()
                    .filter(|options| *options != SequenceOptions::default());
            }
        }
        for check in table.check_constraints.iter_mut().flatten() {
            check.not_valid = true_only(check.not_valid);
            check.expression =
                crate::deploy::canonical_check(&check.expression);
        }
        if let Some(checks) = &mut table.check_constraints {
            checks.sort_by(|a, b| a.name.cmp(&b.name));
        }
        for column_default in table.column_defaults.iter_mut().flatten() {
            if let Value::String(text) = &mut column_default.default {
                *text = crate::deploy::canonical_casts(text);
            }
        }
        for trigger in table.triggers.iter_mut().flatten() {
            canonical_expression(&mut trigger.condition);
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
            canonical_expression(&mut exclude.where_clause);
            for element in &mut exclude.elements {
                canonical_collation(&mut element.collation);
                if let Some(expression) = &mut element.expression {
                    *expression = crate::deploy::canonical_casts(expression);
                }
            }
        }
        table.storage_parameters =
            canonical_storage_parameters(table.storage_parameters.take());
        if let Some(indexes) = &mut table.indexes {
            *indexes = indexes.iter().map(Index::canonical).collect();
        }
        for column in table.partition.iter_mut().flat_map(|p| &mut p.columns) {
            if let TablePartitionColumn::Detailed { collation, .. } = column {
                canonical_collation(collation);
            }
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
    /// SET STORAGE: PLAIN, EXTERNAL, EXTENDED or MAIN, when it differs
    /// from the type's default
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<String>,
    /// SET COMPRESSION: pglz or lz4, when set for the column
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<String>,
    /// SET STATISTICS: the column's statistics target
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statistics: Option<i64>,
    /// SET (...): attribute options such as n_distinct
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Map<String, Value>>,
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
/// pg_dump writes the name only when it is not
/// `<table>_<column>_not_null`. The model also records no name when
/// PostgreSQL generated it: cut to 63 bytes, or with a number that
/// PostgreSQL added (see [`Table::not_null_names`]).
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
    /// Kept only when it is not the `<table>_<column>_seq`, cut to 63
    /// bytes, that PostgreSQL generates in the table's schema
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
    /// generates by default (`<table>_<column>_not_null`, cut to 63
    /// bytes, or with a number that PostgreSQL added; see
    /// [`Table::not_null_names`])
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

impl ConstraintColumns {
    /// The columns of the constraint
    pub fn columns(&self) -> &[String] {
        match self {
            Self::Name(column) => std::slice::from_ref(column),
            Self::Columns(columns) | Self::Detailed { columns, .. } => columns,
        }
    }
}

/// Represents a Foreign Key on a Table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKey {
    /// Empty when the project leaves it out: the loader then sets the
    /// name that PostgreSQL generates (see `project::load`)
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    pub columns: Vec<String>,
    pub references: ForeignKeyReference,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_delete: Option<String>,
    /// The columns that `ON DELETE SET NULL` or `SET DEFAULT` sets, when
    /// not all of them (PostgreSQL 15+). A composite key that shares a
    /// column that is not null, such as a tenant id, needs this.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_delete_columns: Option<Vec<String>>,
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

impl Index {
    /// The same index in the form deploy compares: the storage
    /// parameters and the collations in the form that PostgreSQL
    /// reads, and the type of each cast in an expression and in the
    /// WHERE clause in the form that PostgreSQL writes. A value at its
    /// default is absent, and the method is btree when it is absent,
    /// as pg_dump writes them: `unique`, `nulls_not_distinct` and
    /// `recurse` at their defaults, no storage parameters, the order
    /// `ASC`, and the NULL placement of the order (`LAST` with `ASC`,
    /// `FIRST` with `DESC`).
    pub fn canonical(&self) -> Index {
        let mut index = self.clone();
        index.unique = true_only(index.unique);
        index.nulls_not_distinct = true_only(index.nulls_not_distinct);
        index.recurse = index.recurse.filter(|recurse| !recurse);
        index.method.get_or_insert_with(|| String::from("btree"));
        canonical_expression(&mut index.where_clause);
        index.storage_parameters =
            canonical_storage_parameters(index.storage_parameters.take());
        for column in index.columns.iter_mut().flatten() {
            let descending = column.direction.as_deref() == Some("DESC");
            if !descending {
                column.direction = None;
            }
            let default_placement = if descending { "FIRST" } else { "LAST" };
            if column.null_placement.as_deref() == Some(default_placement) {
                column.null_placement = None;
            }
            canonical_collation(&mut column.collation);
            if let Some(expression) = &mut column.expression {
                *expression = crate::deploy::canonical_casts(expression);
            }
        }
        index
    }
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
    /// The partition is a table of its own in the project, created on
    /// its own and attached with ATTACH PARTITION, because it has
    /// indexes, constraints or other properties of its own that a
    /// partition modeled by its bounds alone cannot hold
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attached: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

impl Table {
    /// Whether this table, a partition, has anything of its own beyond
    /// what its parent gives it, and so has to stay a table of its own
    /// in the project rather than fold into its parent's `partitions`
    pub fn has_own_partition_properties(&self) -> bool {
        let columns = self.columns.iter().flatten().any(|c| {
            c.default.is_some()
                || c.comment.is_some()
                || c.storage.is_some()
                || c.compression.is_some()
                || c.statistics.is_some()
                || c.options.is_some()
                || c.generated.is_some()
        });
        columns
            || self.indexes.is_some()
            || self.primary_key.is_some()
            || self.unique_constraints.is_some()
            || self.foreign_keys.is_some()
            || self.check_constraints.is_some()
            || self.exclude_constraints.is_some()
            || self.constraint_comments.is_some()
            || self.triggers.is_some()
            || self.rules.is_some()
            || self.row_level_security.is_some()
            || self.policies.is_some()
            || self.replica_identity.is_some()
            || self.partition.is_some()
            || self.partitions.is_some()
            || self.storage_parameters.is_some()
            || self.tablespace.is_some()
            || self.access_method.is_some()
    }
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
    /// The same policy with each field at its default as absent, the
    /// command and PUBLIC in upper case, as pull reads them, and the
    /// type of each cast in USING and WITH CHECK in the form that
    /// PostgreSQL writes
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
            using: self.using.as_deref().map(crate::deploy::canonical_casts),
            with_check: self
                .with_check
                .as_deref()
                .map(crate::deploy::canonical_casts),
            ..self.clone()
        }
    }
}

/// A rewrite rule on a table or view (CREATE RULE)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub name: String,
    /// SELECT, INSERT, UPDATE or DELETE
    pub event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    /// DO INSTEAD; the default is DO ALSO
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instead: Option<bool>,
    /// The commands the rule runs; absent for NOTHING
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commands: Option<Vec<String>>,
    /// DISABLED, REPLICA or ALWAYS (ALTER TABLE ... RULE); absent is
    /// the default, ORIGIN
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
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
    /// The columns of `UPDATE OF`: the UPDATE event fires only when
    /// one of them is a target of the update
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_columns: Option<Vec<String>>,
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

    /// PostgreSQL makes a column of the primary key and an identity
    /// column NOT NULL, and pull records `nullable: false` for each. A
    /// project without it compares equal, so deploy plans no DROP NOT
    /// NULL, which PostgreSQL refuses on a primary key column
    #[test]
    fn primary_key_and_identity_columns_are_canonically_not_null() {
        let table = |nullable: serde_json::Value| -> Table {
            let mut value = serde_json::json!({
                "name": "t", "schema": "s", "owner": "o",
                "columns": [
                    {"name": "a", "data_type": "integer"},
                    {"name": "b", "data_type": "integer"},
                    {"name": "i", "data_type": "integer",
                     "generated": {"sequence_behavior": "ALWAYS"}},
                    {"name": "v", "data_type": "text"},
                ],
                "primary_key": {"name": "k", "columns": ["a", "b"]},
            });
            for column in 0..3 {
                value["columns"][column]["nullable"] = nullable.clone();
            }
            serde_json::from_value(value).unwrap()
        };
        let written = table(serde_json::Value::Null);
        let pulled = table(serde_json::json!(false));
        assert_ne!(written, pulled);
        assert_eq!(written.canonical(), pulled.canonical());
        let canonical = written.canonical();
        assert_eq!(canonical.columns.as_ref().unwrap()[3].nullable, None);
        assert_eq!(
            canonical.not_null_names().into_keys().collect::<Vec<_>>(),
            ["a", "b", "i"]
        );
        assert_eq!(canonical.not_null_names()["a"], "t_a_not_null");
    }

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

    /// A table with `value` added to its fields, and one index
    fn with_fields(value: serde_json::Value) -> Table {
        let mut base = serde_json::json!({
            "name": "t", "schema": "s", "owner": "o",
            "columns": [{"name": "label", "data_type": "text"}],
            "indexes": [{"name": "i", "columns": [{"name": "label"}]}],
        });
        base.as_object_mut()
            .unwrap()
            .extend(value.as_object().unwrap().clone());
        serde_json::from_value(base).unwrap()
    }

    /// The same table with `parameters` as the storage parameters of
    /// the table and of its index
    fn with_parameters(parameters: serde_json::Value) -> Table {
        let mut table = with_fields(serde_json::json!({
            "storage_parameters": parameters.clone(),
        }));
        table.indexes.as_mut().unwrap()[0].storage_parameters =
            Some(serde_json::from_value(parameters).unwrap());
        table
    }

    /// PostgreSQL keeps a storage parameter as text, and pull writes
    /// the text. A number or a boolean in the project compares equal
    /// to that text, and a boolean compares by its value.
    #[test]
    fn storage_parameters_compare_by_value() {
        let same = |a, b| {
            assert_eq!(
                with_parameters(a).canonical(),
                with_parameters(b).canonical()
            )
        };
        same(
            serde_json::json!({"fillfactor": 90, "autovacuum_enabled": false}),
            serde_json::json!({"fillfactor": "90", "autovacuum_enabled": "false"}),
        );
        same(
            serde_json::json!({"autovacuum_enabled": false}),
            serde_json::json!({"autovacuum_enabled": "OFF"}),
        );
        same(
            serde_json::json!({"autovacuum_enabled": true}),
            serde_json::json!({"autovacuum_enabled": "on"}),
        );
        same(
            serde_json::json!({"vacuum_index_cleanup": "yes"}),
            serde_json::json!({"vacuum_index_cleanup": "True"}),
        );
        same(
            serde_json::json!({"autovacuum_vacuum_scale_factor": 0.1}),
            serde_json::json!({"autovacuum_vacuum_scale_factor": "0.10"}),
        );
        let different = |a, b| {
            assert_ne!(
                with_parameters(a).canonical(),
                with_parameters(b).canonical()
            )
        };
        different(
            serde_json::json!({"fillfactor": 90}),
            serde_json::json!({"fillfactor": "70"}),
        );
        different(
            serde_json::json!({"autovacuum_enabled": false}),
            serde_json::json!({"autovacuum_enabled": "on"}),
        );
        // 1 and 0 are also numbers, so they are not read as booleans
        different(
            serde_json::json!({"autovacuum_enabled": true}),
            serde_json::json!({"autovacuum_enabled": "1"}),
        );
        different(
            serde_json::json!({"buffering": "auto"}),
            serde_json::json!({"buffering": "on"}),
        );
    }

    /// An empty map of storage parameters is none, as the build writes
    /// no list for it and pull writes none
    #[test]
    fn empty_storage_parameters_are_none() {
        let table = with_parameters(serde_json::json!({})).canonical();
        assert_eq!(table.storage_parameters, None);
        assert_eq!(table.indexes.unwrap()[0].storage_parameters, None);
        let view: crate::models::MaterializedView =
            serde_json::from_value(serde_json::json!({
                "name": "v", "schema": "s", "owner": "o",
                "storage_parameters": {}, "query": "SELECT 1",
            }))
            .unwrap();
        assert_eq!(view.canonical().storage_parameters, None);
    }

    /// A CHECK on a column compares equal to the CHECK of the table
    /// that PostgreSQL makes of it and pull writes:
    /// `<table>_<column>_check`, cut to 63 bytes, with a number when
    /// the name is in use. Each expected name is the one PostgreSQL 18
    /// gave the constraint.
    #[test]
    fn column_checks_compare_as_table_checks() {
        let written = with_fields(serde_json::json!({
            "columns": [
                {"name": "ee", "data_type": "integer",
                 "check_constraint": "ee > 0"},
                {"name": "label", "data_type": "text"},
            ],
            "check_constraints": [
                {"name": "t_label_check", "expression": "(label <> '')"},
            ],
        }));
        let pulled = with_fields(serde_json::json!({
            "check_constraints": [
                {"name": "t_label_check", "expression": "(label <> '')"},
                {"name": "t_ee_check", "expression": "(ee > 0)"},
            ],
            "columns": [
                {"name": "ee", "data_type": "integer"},
                {"name": "label", "data_type": "text"},
            ],
        }));
        assert_eq!(written.canonical(), pulled.canonical());
        let c40 = "c".repeat(40);
        let long: Table = serde_json::from_value(serde_json::json!({
            "name": "a".repeat(40), "schema": "s", "owner": "o",
            "columns": [
                {"name": format!("{c40}_1"), "data_type": "integer",
                 "check_constraint": format!("{c40}_1 > 0")},
                {"name": format!("{c40}_2"), "data_type": "integer",
                 "check_constraint": format!("{c40}_2 > 0")},
            ],
        }))
        .unwrap();
        let names: Vec<String> = long
            .with_table_checks()
            .check_constraints
            .unwrap()
            .into_iter()
            .map(|c| c.name)
            .collect();
        assert_eq!(
            names,
            [
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaa_cccccccccccccccccccccccccccc_check",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaa_ccccccccccccccccccccccccccc_check1",
            ]
        );
    }

    /// A collation compares as PostgreSQL finds it: pg_catalog is
    /// always searched, and a name that is not quoted is in lowercase
    #[test]
    fn collations_compare_as_postgresql_finds_them() {
        let table = |collation: &str| {
            let mut table = with_fields(serde_json::json!({
                "exclude_constraints": [{
                    "name": "x",
                    "elements": [{"name": "label", "operator": "="}],
                }],
                "partition": {
                    "type": "list",
                    "columns": [{"name": "label"}],
                },
            }));
            let collation = Some(String::from(collation));
            table.columns.as_mut().unwrap()[0].collation = collation.clone();
            table.indexes.as_mut().unwrap()[0].columns.as_mut().unwrap()[0]
                .collation = collation.clone();
            table.exclude_constraints.as_mut().unwrap()[0].elements[0]
                .collation = collation.clone();
            table.partition.as_mut().unwrap().columns[0] =
                TablePartitionColumn::Detailed {
                    name: Some("label".into()),
                    expression: None,
                    collation,
                    opclass: None,
                };
            table.canonical()
        };
        assert_eq!(table("\"C\""), table("pg_catalog.\"C\""));
        assert_eq!(table("\"POSIX\""), table("PG_CATALOG.\"POSIX\""));
        assert_eq!(table("s.plain_c"), table("S.\"plain_c\""));
        // `C` is the collation `c`, which is not `"C"`
        assert_ne!(table("C"), table("\"C\""));
        assert_ne!(table("\"C\""), table("\"POSIX\""));
        assert_ne!(table("s.\"C\""), table("\"C\""));
    }

    /// The type of a cast in an index expression compares in the form
    /// that PostgreSQL writes. Text in quotes keeps its form.
    #[test]
    fn index_cast_types_compare_in_standard_form() {
        let table = |expression: &str| {
            let mut table = with_fields(serde_json::json!({}));
            table.indexes.as_mut().unwrap()[0].columns =
                Some(vec![IndexColumn {
                    name: None,
                    expression: Some(expression.into()),
                    collation: None,
                    opclass: None,
                    direction: None,
                    null_placement: None,
                }]);
            table.canonical()
        };
        let same = |a, b| assert_eq!(table(a), table(b));
        same("(label)::varchar(20)", "(label)::character varying(20)");
        same("(label)::VARCHAR(20)", "(label)::character varying(20)");
        same("(n)::pg_catalog.int8", "(n)::bigint");
        same("(n)::int4[]", "(n)::integer[]");
        same(
            "lower((label)::varchar)",
            "lower((label)::character varying)",
        );
        let different = |a, b| assert_ne!(table(a), table(b));
        different("(label)::varchar(30)", "(label)::character varying(20)");
        different("(label)::text", "(label)::character varying(20)");
        different("(n)::public.int4", "(n)::integer");
        // a string literal and a quoted name keep their text
        different("('a::int4'::text)", "('a::integer'::text)");
        different("(\"a::int4\")::text", "(\"a::integer\")::text");
    }

    /// The type of a cast in an exclusion constraint expression
    /// compares in the form that PostgreSQL writes, as in an index
    #[test]
    fn exclusion_cast_types_compare_in_standard_form() {
        let table = |expression: &str| {
            with_fields(serde_json::json!({
                "exclude_constraints": [{
                    "name": "x",
                    "elements": [
                        {"expression": expression, "operator": "="},
                    ],
                }],
            }))
            .canonical()
        };
        assert_eq!(
            table("(label)::VARCHAR(20)"),
            table("(label)::character varying(20)")
        );
        assert_eq!(table("(n)::float(10)"), table("(n)::real"));
        assert_ne!(
            table("(label)::varchar(30)"),
            table("(label)::character varying(20)")
        );
    }

    /// The type of a cast in a WHERE clause, a CHECK constraint or a
    /// default compares in the form that PostgreSQL writes:
    /// pg_get_expr writes `CHECK (((price)::numeric(10,2) > 0))` and
    /// `DEFAULT 'x'::character varying` with the type in the
    /// format_type form
    #[test]
    fn expression_cast_types_compare_in_standard_form() {
        let canonical =
            |value: serde_json::Value| with_fields(value).canonical();
        let check = |expression: &str| {
            canonical(serde_json::json!({
                "check_constraints": [
                    {"name": "c", "expression": expression},
                ],
            }))
        };
        assert_eq!(
            check("((label)::DECIMAL(10, 2) > (0)::NUMERIC)"),
            check("((label)::numeric(10,2) > (0)::numeric)")
        );
        assert_ne!(
            check("((label)::INT8 > 0)"),
            check("((label)::integer > 0)")
        );
        let default = |default: serde_json::Value| {
            canonical(serde_json::json!({
                "columns": [
                    {"name": "label", "data_type": "text", "default": default},
                ],
            }))
        };
        assert_eq!(
            default("'x'::VARCHAR".into()),
            default("'x'::character varying".into())
        );
        assert_eq!(default("'x::int4'".into()), default("'x::int4'".into()));
        assert_ne!(
            default("'x::int4'".into()),
            default("'x::integer'".into())
        );
        assert_ne!(
            default("'x'::text".into()),
            default("'x'::character varying".into())
        );
        // a default that is not a string has no cast
        assert_eq!(default(3.into()), default(3.into()));
        let inherited = |default: &str| {
            canonical(serde_json::json!({
                "column_defaults": [{"column": "label", "default": default}],
            }))
        };
        assert_eq!(inherited("(4)::INT8"), inherited("(4)::bigint"));
        let index_where = |clause: &str| {
            canonical(serde_json::json!({
                "indexes": [{
                    "name": "i", "columns": [{"name": "label"}],
                    "where": clause,
                }],
            }))
        };
        assert_eq!(
            index_where("((label)::INT4 > 0)"),
            index_where("((label)::integer > 0)")
        );
        assert_ne!(
            index_where("((label)::INT8 > 0)"),
            index_where("((label)::integer > 0)")
        );
        let exclude_where = |clause: &str| {
            canonical(serde_json::json!({
                "exclude_constraints": [{
                    "name": "x",
                    "elements": [{"name": "label", "operator": "="}],
                    "where": clause,
                }],
            }))
        };
        assert_eq!(
            exclude_where("((label)::FLOAT8 > (0)::FLOAT8)"),
            exclude_where(
                "((label)::double precision > (0)::double precision)"
            )
        );
        assert_ne!(
            exclude_where("((label)::REAL > 0)"),
            exclude_where("((label)::double precision > 0)")
        );
    }

    /// The type of a cast in a generated column expression, a policy
    /// USING or WITH CHECK expression and a trigger WHEN condition
    /// compares in the form that PostgreSQL writes
    #[test]
    fn generated_policy_and_trigger_cast_types_compare_in_standard_form() {
        let canonical =
            |value: serde_json::Value| with_fields(value).canonical();
        let generated = |expression: &str| {
            canonical(serde_json::json!({
                "columns": [{
                    "name": "g", "data_type": "bigint",
                    "generated": {"expression": expression},
                }],
            }))
        };
        assert_eq!(
            generated("((label)::INT8 * 2)"),
            generated("((label)::bigint * 2)")
        );
        assert_ne!(
            generated("((label)::INT4 * 2)"),
            generated("((label)::bigint * 2)")
        );
        let policy = |using: &str, with_check: &str| {
            canonical(serde_json::json!({
                "policies": [
                    {"name": "p", "using": using, "with_check": with_check},
                ],
            }))
        };
        assert_eq!(
            policy("((label)::INT4 > 0)", "((label)::VARCHAR <> ''::TEXT)"),
            policy(
                "((label)::integer > 0)",
                "((label)::character varying <> ''::text)"
            )
        );
        assert_ne!(
            policy("((label)::INT8 > 0)", "true"),
            policy("((label)::integer > 0)", "true")
        );
        assert_ne!(
            policy("true", "((label)::INT8 > 0)"),
            policy("true", "((label)::integer > 0)")
        );
        let trigger = |condition: &str| {
            canonical(serde_json::json!({
                "triggers": [{
                    "name": "t", "when": "BEFORE", "events": ["UPDATE"],
                    "for_each": "ROW", "condition": condition,
                    "function": "f()",
                }],
            }))
        };
        assert_eq!(
            trigger("((new.label)::INT4 > 0)"),
            trigger("((new.label)::integer > 0)")
        );
        assert_ne!(
            trigger("((new.label)::INT8 > 0)"),
            trigger("((new.label)::integer > 0)")
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
