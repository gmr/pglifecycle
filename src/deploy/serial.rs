//! Serial column types (`serial`, `bigserial`, `smallserial` and
//! `serial4`, `serial8`, `serial2`). PostgreSQL keeps no serial type:
//! it makes an `integer` (`bigint`, `smallint`) column, NOT NULL, with
//! the default `nextval()` of a new sequence that the column owns. pull
//! writes that form. Before deploy compares the project, it changes a
//! serial column of the project to that form, and it adds the sequence
//! to the project. deploy finds the sequence in the database by its
//! OWNED BY, not by its name: PostgreSQL cuts a long name and adds a
//! number to a name that is in use. A column that the database does
//! not have stays serial, thus a new table or a new column is made with
//! the serial type, as build writes it.
//!
//! New in the Rust implementation: the Python implementation had no
//! `deploy` command, so no Python file ports to this module.

use std::collections::BTreeSet;

use serde_json::Value;

use super::alter::names;
use crate::build;
use crate::constants::ObjectType;
use crate::models::{Definition, Item, Sequence};
use crate::project::Project;
use crate::pull::Assembly;
use crate::utils::quote_ident;

/// The integer type of a serial type, as PostgreSQL reads the name: in
/// any case, or quoted in lowercase
fn integer_type(data_type: &str) -> Option<&'static str> {
    match names::name(data_type).as_str() {
        "serial" | "serial4" => Some("integer"),
        "bigserial" | "serial8" => Some("bigint"),
        "smallserial" | "serial2" => Some("smallint"),
        _ => None,
    }
}

/// Change each serial column of the project that the database has, and
/// that owns a sequence in the database, to the form that PostgreSQL
/// stores. Add the sequence that a serial column makes, with the name
/// of the database sequence, unless the project has a sequence of that
/// name. Return the ids of the added sequences.
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
            let Some(sequence) = owned
                .iter()
                .find(|s| {
                    used.as_ref() == Some(&(s.schema.clone(), s.name.clone()))
                })
                .or(owned.first())
            else {
                continue;
            };
            let target = (sequence.schema.clone(), sequence.name.clone());
            column.data_type = data_type.to_string();
            column.nullable = Some(false);
            column.default = Some(Value::String(match &existing.default {
                Some(Value::String(text))
                    if build::nextval_target(text).as_ref()
                        == Some(&target) =>
                {
                    text.clone()
                }
                _ => {
                    let name = format!(
                        "{}.{}",
                        quote_ident(&sequence.schema),
                        quote_ident(&sequence.name)
                    );
                    format!(
                        "nextval('{}'::regclass)",
                        name.replace('\'', "''")
                    )
                }
            }));
            if !listed.insert(target) {
                continue;
            }
            added.push(Item {
                id: next,
                desc: ObjectType::Sequence,
                definition: Definition::Sequence(Sequence {
                    name: sequence.name.clone(),
                    schema: sequence.schema.clone(),
                    // PostgreSQL refuses an owner change of a sequence
                    // that a column owns: ALTER TABLE ... OWNER TO
                    // changes it with the table
                    owner: sequence.owner.clone(),
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
                    owned_by: sequence.owned_by.clone(),
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
}
