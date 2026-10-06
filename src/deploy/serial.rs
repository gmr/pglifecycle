//! Serial column types (`serial`, `bigserial`, `smallserial` and
//! `serial4`, `serial8`, `serial2`). PostgreSQL keeps no serial type:
//! it makes an `integer` (`bigint`, `smallint`) column, NOT NULL, with
//! the default `nextval()` of a new sequence that the column owns. pull
//! writes that form. Before deploy compares the project, it changes a
//! serial column of the project to that form, and it adds the sequence
//! to the project. deploy finds the sequence in the database by its
//! OWNED BY, not by its name: PostgreSQL cuts a long name and adds a
//! number to a name that is in use. When the database column owns no
//! sequence (an `integer` column that becomes `serial`), deploy adds a
//! new sequence with the name that PostgreSQL would give it, thus the
//! plan makes the sequence, sets the default and links the sequence to
//! the column. A column that the database does not have stays serial,
//! thus a new table or a new column is made with the serial type, as
//! build writes it.
//!
//! New in the Rust implementation: the Python implementation had no
//! `deploy` command, so no Python file ports to this module.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::alter::names;
use crate::build;
use crate::constants::ObjectType;
use crate::models::{Definition, Item, Sequence};
use crate::project::Project;
use crate::pull::Assembly;
use crate::utils::{make_object_name, quote_ident};

/// The integer type of a serial type, as PostgreSQL reads the name: in
/// any case, or quoted in lowercase
pub(crate) fn integer_type(data_type: &str) -> Option<&'static str> {
    match names::name(data_type).as_str() {
        "serial" | "serial4" => Some("integer"),
        "bigserial" | "serial8" => Some("bigint"),
        "smallserial" | "serial2" => Some("smallint"),
        _ => None,
    }
}

/// Change each serial column of the project that the database has to
/// the form that PostgreSQL stores. Add the sequence that a serial
/// column makes, with the name of the database sequence that the
/// column owns, unless the project has a sequence of that name. When
/// the column owns no sequence in the database, use the project
/// sequence that the column owns, else add a new sequence. Return the
/// ids of the added sequences.
pub(crate) fn expand(
    project: &mut Project,
    assembly: &Assembly,
) -> BTreeSet<usize> {
    let mut listed: BTreeSet<(String, String)> = project
        .inventory
        .iter()
        .filter_map(|item| match &item.definition {
            Definition::Sequence(s) => {
                Some((s.schema.clone(), s.name.clone()))
            }
            _ => None,
        })
        .collect();
    // the sequence of the project that each column owns, by the column
    let listed_owned: BTreeMap<String, (String, String)> = project
        .inventory
        .iter()
        .filter_map(|item| match &item.definition {
            Definition::Sequence(s) => s.owned_by.as_deref().map(|owner| {
                (names::name(owner), (s.schema.clone(), s.name.clone()))
            }),
            _ => None,
        })
        .collect();
    // the relations of the database, which a new sequence must not
    // have the name of
    let taken: BTreeSet<(String, String)> = assembly
        .tables
        .iter()
        .flat_map(|t| {
            std::iter::once(t.name.clone())
                .chain(t.indexes.iter().flatten().map(|i| i.name.clone()))
                .map(|name| (t.schema.clone(), name))
        })
        .chain(
            assembly
                .sequences
                .iter()
                .map(|s| (s.schema.clone(), s.name.clone())),
        )
        .chain(
            assembly
                .views
                .iter()
                .map(|v| (v.schema.clone(), v.name.clone())),
        )
        .chain(
            assembly
                .materialized_views
                .iter()
                .map(|v| (v.schema.clone(), v.name.clone())),
        )
        .collect();
    let mut next = project.inventory.len();
    let mut added = Vec::new();
    for item in &mut project.inventory {
        let Definition::Table(table) = &mut item.definition else {
            continue;
        };
        let Some(db) = assembly
            .tables
            .iter()
            .find(|t| t.schema == table.schema && t.name == table.name)
        else {
            continue;
        };
        for column in table.columns.iter_mut().flatten() {
            let Some(data_type) = integer_type(&column.data_type) else {
                continue;
            };
            let Some(existing) =
                db.columns.iter().flatten().find(|c| c.name == column.name)
            else {
                continue;
            };
            let owner = names::name(&format!(
                "{}.{}.{}",
                quote_ident(&table.schema),
                quote_ident(&table.name),
                quote_ident(&column.name)
            ));
            let owned: Vec<&Sequence> = assembly
                .sequences
                .iter()
                .filter(|s| {
                    s.owned_by.as_deref().map(names::name).as_ref()
                        == Some(&owner)
                })
                .collect();
            // the column can own more than one sequence: use the
            // sequence that its default names, else the first one
            let used = match &existing.default {
                Some(Value::String(text)) => build::nextval_target(text),
                _ => None,
            };
            // the sequence, as (schema, name, owner, OWNED BY)
            let (schema, name, sequence_owner, owned_by) = match owned
                .iter()
                .find(|s| {
                    used.as_ref() == Some(&(s.schema.clone(), s.name.clone()))
                })
                .or(owned.first())
            {
                Some(s) => (
                    s.schema.clone(),
                    s.name.clone(),
                    s.owner.clone(),
                    s.owned_by.clone(),
                ),
                // the column owns no sequence in the database: use the
                // sequence of the project that the column owns, else
                // make the sequence that PostgreSQL makes for a serial
                // column
                None => match listed_owned.get(&owner) {
                    Some((schema, name)) => {
                        column.data_type = data_type.to_string();
                        column.nullable = Some(false);
                        column.default =
                            Some(Value::String(nextval(schema, name)));
                        continue;
                    }
                    None => (
                        table.schema.clone(),
                        free_name(
                            &taken,
                            &listed,
                            &table.schema,
                            &table.name,
                            &column.name,
                        ),
                        table.owner.clone(),
                        Some(owner.clone()),
                    ),
                },
            };
            let target = (schema.clone(), name.clone());
            column.data_type = data_type.to_string();
            column.nullable = Some(false);
            column.default = Some(Value::String(match &existing.default {
                Some(Value::String(text))
                    if build::nextval_target(text).as_ref()
                        == Some(&target) =>
                {
                    text.clone()
                }
                _ => nextval(&schema, &name),
            }));
            if !listed.insert(target) {
                continue;
            }
            added.push(Item {
                id: next,
                desc: ObjectType::Sequence,
                definition: Definition::Sequence(Sequence {
                    name,
                    schema,
                    // PostgreSQL refuses an owner change of a sequence
                    // that a column owns: ALTER TABLE ... OWNER TO
                    // changes it with the table
                    owner: sequence_owner,
                    sql: None,
                    // pg_dump writes no AS for bigint
                    data_type: (data_type != "bigint")
                        .then(|| data_type.to_string()),
                    increment_by: Some(1),
                    min_value: None,
                    max_value: None,
                    start_with: Some(1),
                    cache: Some(1),
                    cycle: None,
                    owned_by,
                    comment: None,
                }),
                dependencies: BTreeSet::new(),
            });
            next += 1;
        }
    }
    let ids = added.iter().map(|item| item.id).collect();
    project.inventory.extend(added);
    ids
}

/// The default that calls nextval() of the sequence `schema.name`
fn nextval(schema: &str, name: &str) -> String {
    let name = format!("{}.{}", quote_ident(schema), quote_ident(name));
    format!("nextval('{}'::regclass)", name.replace('\'', "''"))
}

/// The name that PostgreSQL gives the sequence of a serial column (see
/// ChooseRelationName): `<table>_<column>_seq`, cut to fit, with a
/// number after `seq` when a relation of the database or a sequence of
/// the project has the name
fn free_name(
    taken: &BTreeSet<(String, String)>,
    listed: &BTreeSet<(String, String)>,
    schema: &str,
    table: &str,
    column: &str,
) -> String {
    let mut number = 0;
    loop {
        let label = match number {
            0 => String::from("seq"),
            _ => format!("seq{number}"),
        };
        let name = make_object_name(table, Some(column), &label);
        let key = (schema.to_string(), name.clone());
        if !taken.contains(&key) && !listed.contains(&key) {
            return name;
        }
        number += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Table;

    fn project(tables: Vec<Value>, sequences: Vec<Value>) -> Project {
        let tables = tables.into_iter().map(|value| {
            (
                ObjectType::Table,
                Definition::Table(serde_json::from_value(value).unwrap()),
            )
        });
        let sequences = sequences.into_iter().map(|value| {
            (
                ObjectType::Sequence,
                Definition::Sequence(serde_json::from_value(value).unwrap()),
            )
        });
        Project {
            name: String::from("test"),
            superuser: String::from("postgres"),
            default_schema: String::from("public"),
            path: std::path::PathBuf::new(),
            settings: Default::default(),
            inventory: tables
                .chain(sequences)
                .enumerate()
                .map(|(id, (desc, definition))| Item {
                    id,
                    desc,
                    definition,
                    dependencies: BTreeSet::new(),
                })
                .collect(),
        }
    }

    /// The database table `test.t` and the sequences that its columns
    /// own, as pull reads them
    fn database() -> Assembly {
        let table: Table = serde_json::from_value(serde_json::json!({
            "name": "t", "schema": "test", "owner": "postgres",
            "columns": [
                {"name": "id", "data_type": "integer", "nullable": false,
                 "default": "nextval('test.t_id_seq1'::regclass)"},
                {"name": "b", "data_type": "bigint", "nullable": false,
                 "default": "nextval('test.t_b_seq'::regclass)"},
                {"name": "Q", "data_type": "integer", "nullable": false},
                {"name": "x", "data_type": "integer"},
            ],
        }))
        .unwrap();
        let sequence = |name: &str, data_type: Option<&str>, owned: &str| {
            serde_json::from_value(serde_json::json!({
                "name": name, "schema": "test", "owner": "postgres",
                "data_type": data_type, "increment_by": 1,
                "start_with": 1, "cache": 1, "owned_by": owned,
            }))
            .unwrap()
        };
        let mut assembly = Assembly::default();
        assembly.tables = vec![table];
        assembly.sequences = vec![
            // a collision: PostgreSQL added a number
            sequence("t_id_seq1", Some("integer"), "test.t.id"),
            sequence("t_b_seq", None, "test.t.b"),
            sequence("it's", Some("integer"), "test.t.\"Q\""),
        ];
        assembly
    }

    fn columns(project: &Project) -> Value {
        let Definition::Table(table) = &project.inventory[0].definition else {
            unreachable!()
        };
        serde_json::to_value(&table.columns).unwrap()
    }

    #[test]
    fn integer_types() {
        assert_eq!(integer_type("serial"), Some("integer"));
        assert_eq!(integer_type("SERIAL4"), Some("integer"));
        assert_eq!(integer_type("\"serial\""), Some("integer"));
        assert_eq!(integer_type("BigSerial"), Some("bigint"));
        assert_eq!(integer_type("serial8"), Some("bigint"));
        assert_eq!(integer_type("smallserial"), Some("smallint"));
        assert_eq!(integer_type("serial2"), Some("smallint"));
        assert_eq!(integer_type("\"SERIAL\""), None);
        assert_eq!(integer_type("integer"), None);
        assert_eq!(integer_type("serial[]"), None);
    }

    /// A serial column that the database has is in the stored form,
    /// with the sequence that it owns in the database, found by OWNED
    /// BY. The sequence keeps the owner that it has in the database. A
    /// new serial column stays serial.
    #[test]
    fn expands_serial_columns() {
        let mut project = project(
            vec![serde_json::json!({
                "name": "t", "schema": "test", "owner": "o",
                "columns": [
                    {"name": "id", "data_type": "serial"},
                    {"name": "b", "data_type": "BIGSERIAL"},
                    {"name": "Q", "data_type": "\"serial\""},
                    {"name": "x", "data_type": "integer"},
                    {"name": "n", "data_type": "smallserial"},
                ],
            })],
            vec![],
        );
        let implied = expand(&mut project, &database());
        assert_eq!(
            columns(&project),
            serde_json::json!([
                {"name": "id", "data_type": "integer", "nullable": false,
                 "default": "nextval('test.t_id_seq1'::regclass)"},
                {"name": "b", "data_type": "bigint", "nullable": false,
                 "default": "nextval('test.t_b_seq'::regclass)"},
                // the database column has no default
                {"name": "Q", "data_type": "integer", "nullable": false,
                 "default": "nextval('test.\"it''s\"'::regclass)"},
                {"name": "x", "data_type": "integer"},
                {"name": "n", "data_type": "smallserial"},
            ])
        );
        assert_eq!(implied, BTreeSet::from([1, 2, 3]));
        let sequences: Vec<Value> = project.inventory[1..]
            .iter()
            .map(|item| {
                assert_eq!(item.desc, ObjectType::Sequence);
                serde_json::to_value(&item.definition).unwrap()
            })
            .collect();
        assert_eq!(
            sequences,
            vec![
                serde_json::json!({
                    "name": "t_id_seq1", "schema": "test", "owner": "postgres",
                    "data_type": "integer", "increment_by": 1,
                    "start_with": 1, "cache": 1, "owned_by": "test.t.id",
                }),
                serde_json::json!({
                    "name": "t_b_seq", "schema": "test", "owner": "postgres",
                    "increment_by": 1, "start_with": 1, "cache": 1,
                    "owned_by": "test.t.b",
                }),
                serde_json::json!({
                    "name": "it's", "schema": "test", "owner": "postgres",
                    "data_type": "integer", "increment_by": 1,
                    "start_with": 1, "cache": 1,
                    "owned_by": "test.t.\"Q\"",
                }),
            ]
        );
    }

    /// The kind of the project gives the type, not the database: a
    /// serial column that is bigserial in the project is a bigint
    #[test]
    fn project_kind_gives_the_type() {
        let mut project = project(
            vec![serde_json::json!({
                "name": "t", "schema": "test", "owner": "o",
                "columns": [{"name": "id", "data_type": "bigserial"}],
            })],
            vec![],
        );
        expand(&mut project, &database());
        assert_eq!(columns(&project)[0]["data_type"], "bigint");
        let Definition::Sequence(sequence) = &project.inventory[1].definition
        else {
            unreachable!()
        };
        assert_eq!(sequence.data_type, None);
    }

    /// Of two sequences that the column owns, deploy uses the sequence
    /// that the database default names, not the first one
    #[test]
    fn owned_sequence_of_the_default() {
        let mut assembly = database();
        assembly.tables[0].columns.as_mut().unwrap()[0].default =
            Some(Value::String("nextval('test.t_id_seq2'::regclass)".into()));
        let old: Sequence = serde_json::from_value(serde_json::json!({
            "name": "t_id_seq", "schema": "test", "owner": "postgres",
            "data_type": "integer", "owned_by": "test.t.id",
        }))
        .unwrap();
        let mut current = old.clone();
        current.name = String::from("t_id_seq2");
        assembly.sequences = vec![old, current];
        let mut project = project(
            vec![serde_json::json!({
                "name": "t", "schema": "test", "owner": "o",
                "columns": [{"name": "id", "data_type": "serial"}],
            })],
            vec![],
        );
        expand(&mut project, &assembly);
        assert_eq!(
            columns(&project)[0]["default"],
            "nextval('test.t_id_seq2'::regclass)"
        );
        let Definition::Sequence(sequence) = &project.inventory[1].definition
        else {
            unreachable!()
        };
        assert_eq!(sequence.name, "t_id_seq2");
    }

    /// A sequence that the project lists is not added again, and a
    /// table that the database does not have stays serial
    #[test]
    fn listed_sequences_and_new_tables() {
        let mut project = project(
            vec![
                serde_json::json!({
                    "name": "t", "schema": "test", "owner": "o",
                    "columns": [{"name": "id", "data_type": "serial"}],
                }),
                serde_json::json!({
                    "name": "new", "schema": "test", "owner": "o",
                    "columns": [{"name": "id", "data_type": "serial"}],
                }),
            ],
            vec![serde_json::json!({
                "name": "t_id_seq1", "schema": "test", "owner": "o",
                "owned_by": "test.t.id",
            })],
        );
        let implied = expand(&mut project, &database());
        assert!(implied.is_empty());
        assert_eq!(project.inventory.len(), 3);
        assert_eq!(columns(&project)[0]["data_type"], "integer");
        let Definition::Table(new) = &project.inventory[1].definition else {
            unreachable!()
        };
        assert_eq!(new.columns.as_ref().unwrap()[0].data_type, "serial");
    }

    /// A serial column that owns no sequence in the database gets a
    /// new sequence, as PostgreSQL makes it for a serial column: with
    /// the name that PostgreSQL chooses, the owner of the table, and
    /// OWNED BY the column. A name that is in use gets a number.
    #[test]
    fn new_sequence_for_a_column_with_none() {
        let mut assembly = database();
        let mut taken = assembly.sequences[1].clone();
        taken.name = String::from("t_x_seq");
        taken.owned_by = None;
        assembly.sequences.push(taken);
        let mut project = project(
            vec![serde_json::json!({
                "name": "t", "schema": "test", "owner": "o",
                "columns": [{"name": "x", "data_type": "serial"}],
            })],
            vec![],
        );
        let implied = expand(&mut project, &assembly);
        assert_eq!(implied, BTreeSet::from([1]));
        assert_eq!(
            columns(&project),
            serde_json::json!([
                {"name": "x", "data_type": "integer", "nullable": false,
                 "default": "nextval('test.t_x_seq1'::regclass)"},
            ])
        );
        assert_eq!(
            serde_json::to_value(&project.inventory[1].definition).unwrap(),
            serde_json::json!({
                "name": "t_x_seq1", "schema": "test", "owner": "o",
                "data_type": "integer", "increment_by": 1,
                "start_with": 1, "cache": 1, "owned_by": "test.t.x",
            })
        );
    }

    /// A sequence of the project that the column owns is used, and is
    /// not added again
    #[test]
    fn listed_sequence_for_a_column_with_none() {
        let mut project = project(
            vec![serde_json::json!({
                "name": "t", "schema": "test", "owner": "o",
                "columns": [{"name": "x", "data_type": "serial"}],
            })],
            vec![serde_json::json!({
                "name": "mine", "schema": "test", "owner": "o",
                "owned_by": "test.t.x",
            })],
        );
        let implied = expand(&mut project, &database());
        assert!(implied.is_empty());
        assert_eq!(project.inventory.len(), 2);
        assert_eq!(
            columns(&project)[0]["default"],
            "nextval('test.mine'::regclass)"
        );
    }
}
