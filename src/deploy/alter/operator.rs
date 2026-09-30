//! Operators: `ALTER OPERATOR ... SET (...)` changes the estimators
//! (`RESTRICT`, `JOIN`) in place, to another function or to none. In
//! PostgreSQL 18 it can also set `COMMUTATOR`, `NEGATOR`, `HASHES` and
//! `MERGES` when they are not set, but it cannot clear or change them
//! when they are. Such a change, and a changed function, drops the
//! operator and makes it again.

use std::collections::BTreeMap;

use super::names::{argument_type, name, operator as operator_name};
use super::{Alter, Resolution, push_comment};
use crate::constants::ObjectType;
use crate::deploy::diff::ObjectKey;
use crate::models::{Definition, Operator};
use crate::project::Project;
use crate::utils::quote_ident;

pub(super) fn operator(repo: &Operator, db: &Operator) -> Resolution {
    let (r, d) = (canonical(repo), canonical(db));
    if r.function != d.function {
        return Resolution::Replace;
    }
    let mut options = Vec::new();
    for (option, wanted, existing, value) in [
        ("COMMUTATOR", &r.commutator, &d.commutator, &repo.commutator),
        ("NEGATOR", &r.negator, &d.negator, &repo.negator),
    ] {
        match (wanted, existing) {
            (a, b) if a == b => {}
            (Some(_), None) => options.push(format!(
                "{option} = {}",
                operator_reference(value.as_deref().unwrap_or_default())
            )),
            _ => return Resolution::Replace,
        }
    }
    for (option, wanted, existing) in [
        ("HASHES", r.hashes, d.hashes),
        ("MERGES", r.merges, d.merges),
    ] {
        match (wanted, existing) {
            (a, b) if a == b => {}
            (Some(true), None) => options.push(option.to_string()),
            _ => return Resolution::Replace,
        }
    }
    for (option, wanted, existing, value) in [
        ("RESTRICT", &r.restrict, &d.restrict, &repo.restrict),
        ("JOIN", &r.join, &d.join, &repo.join),
    ] {
        if wanted != existing {
            options.push(format!(
                "{option} = {}",
                value.as_deref().unwrap_or("NONE")
            ));
        }
    }
    let target = target(repo);
    let mut alters = Vec::new();
    if !options.is_empty() {
        alters.push(Alter::new(format!(
            "ALTER OPERATOR {target} SET ({});\n",
            options.join(", ")
        )));
    }
    push_comment(&mut alters, "OPERATOR", &target, &repo.comment, &db.comment);
    Resolution::Statements(alters)
}

/// An operator reference in the form that ALTER OPERATOR reads: a
/// qualified name must be in `OPERATOR(...)`
fn operator_reference(value: &str) -> String {
    let value = value.trim();
    if value.to_ascii_uppercase().starts_with("OPERATOR")
        || !value.contains('.')
    {
        value.to_string()
    } else {
        format!("OPERATOR({value})")
    }
}

/// The operator as PostgreSQL keeps it: canonical names and types, no
/// `NONE` argument, and no option at its default
pub(crate) fn canonical(operator: &Operator) -> Operator {
    let argument = |value: &Option<String>| {
        value
            .as_deref()
            .map(argument_type)
            .filter(|argument| argument != "none")
    };
    let names = |value: &Option<String>| value.as_deref().map(name);
    let operators =
        |value: &Option<String>| value.as_deref().map(operator_name);
    Operator {
        function: name(&operator.function),
        left_arg: argument(&operator.left_arg),
        right_arg: argument(&operator.right_arg),
        commutator: operators(&operator.commutator),
        negator: operators(&operator.negator),
        restrict: names(&operator.restrict),
        join: names(&operator.join),
        hashes: operator.hashes.filter(|v| *v),
        merges: operator.merges.filter(|v| *v),
        ..operator.clone()
    }
}

/// The qualified name of an operator, as [`operator_name`] gives a
/// reference to it
fn reference(operator: &Operator) -> String {
    operator_name(&format!(
        "{}.{}",
        quote_ident(&operator.schema),
        operator.name
    ))
}

/// The database operators without the commutator or the negator that
/// PostgreSQL gives them from the other operator of the pair. When
/// `CREATE OPERATOR` or `ALTER OPERATOR` names a commutator or a
/// negator that exists, PostgreSQL also links that operator back. A
/// project can give the link on one side only, so a link that the
/// project gives on the other side is not a difference.
pub(crate) fn align(
    project: &Project,
    database: &mut BTreeMap<ObjectKey, Definition>,
) {
    let operators: Vec<Operator> = project
        .inventory
        .iter()
        .filter_map(|item| match &item.definition {
            Definition::Operator(operator) if operator.sql.is_none() => {
                Some(canonical(operator))
            }
            _ => None,
        })
        .collect();
    for repo in &operators {
        let key = ObjectKey::new(
            ObjectType::Operator,
            &Definition::Operator(repo.clone()),
        );
        let Some(Definition::Operator(db)) = database.get_mut(&key) else {
            continue;
        };
        let existing = canonical(db);
        let me = reference(repo);
        // the commutator of `a op b` is an operator of `b op a`, and
        // the negator one of `a op b`
        let linked = |link: &Option<String>, swapped: bool| {
            let Some(link) = link else {
                return false;
            };
            operators.iter().any(|other| {
                let (left, right) = if swapped {
                    (&repo.right_arg, &repo.left_arg)
                } else {
                    (&repo.left_arg, &repo.right_arg)
                };
                reference(other) == *link
                    && other.left_arg == *left
                    && other.right_arg == *right
                    && (if swapped {
                        &other.commutator
                    } else {
                        &other.negator
                    })
                    .as_deref()
                        == Some(me.as_str())
            })
        };
        if repo.commutator.is_none() && linked(&existing.commutator, true) {
            db.commutator = None;
        }
        if repo.negator.is_none() && linked(&existing.negator, false) {
            db.negator = None;
        }
    }
}

/// `schema.op (left, right)`, which ALTER, COMMENT ON and DROP
/// OPERATOR read
fn target(operator: &Operator) -> String {
    format!(
        "{}.{} ({}, {})",
        quote_ident(&operator.schema),
        operator.name,
        operator.left_arg.as_deref().unwrap_or("NONE"),
        operator.right_arg.as_deref().unwrap_or("NONE")
    )
}

/// The DROP statement of an operator that only the database has
pub(crate) fn drop(operator: &Operator) -> String {
    format!("DROP OPERATOR IF EXISTS {};\n", target(operator))
}

/// The identity of an operator archive entry. Its tag is only the
/// operator name, so the argument types come from its DROP statement,
/// `DROP OPERATOR schema.op (left, right);`.
pub(crate) fn entry_name(entry: &libpgdump::Entry) -> Option<String> {
    let tag = entry.tag.as_deref()?;
    let drop = entry.drop_stmt.as_deref()?;
    let open = drop.rfind('(')?;
    let close = drop.rfind(')')?;
    let types: Vec<String> = drop
        .get(open + 1..close)?
        .split(',')
        .map(argument_type)
        .collect();
    let [left, right] = types.as_slice() else {
        return None;
    };
    Some(format!("{tag}({left}, {right})"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn parse(value: serde_json::Value) -> Operator {
        serde_json::from_value(value).expect("operator deserializes")
    }

    fn parity(extra: serde_json::Value) -> Operator {
        let mut value = json!({
            "name": "=~=", "schema": "test", "owner": "postgres",
            "function": "test.same_parity",
            "left_arg": "integer", "right_arg": "integer",
        });
        for (key, v) in extra.as_object().unwrap() {
            value[key] = v.clone();
        }
        parse(value)
    }

    #[test]
    fn short_forms_are_canonical() {
        let pulled = parse(json!({
            "name": "!!!", "schema": "test", "owner": "postgres",
            "function": "int4um", "right_arg": "integer",
            "commutator": "OPERATOR(test.!!!)", "restrict": "eqsel",
        }));
        let written = parse(json!({
            "name": "!!!", "schema": "test", "owner": "postgres",
            "function": "PG_CATALOG.INT4UM", "left_arg": "NONE",
            "right_arg": "INT4", "commutator": "OPERATOR(TEST.!!!)",
            "restrict": "pg_catalog.eqsel", "hashes": false,
            "merges": false,
        }));
        assert_eq!(canonical(&written), canonical(&pulled));
    }

    #[test]
    fn estimators_change_in_place() {
        let repo = parity(json!({"restrict": "eqsel", "join": "eqjoinsel",
                                 "comment": "Same parity"}));
        let db = parity(json!({"restrict": "scalarltsel"}));
        let Resolution::Statements(alters) = operator(&repo, &db) else {
            panic!("expected statements");
        };
        let sql: Vec<&str> = alters.iter().map(|a| a.sql.as_str()).collect();
        assert_eq!(
            sql,
            [
                "ALTER OPERATOR test.=~= (integer, integer) SET (RESTRICT = \
                 eqsel, JOIN = eqjoinsel);\n",
                "COMMENT ON OPERATOR test.=~= (integer, integer) IS \
                 $$Same parity$$;\n",
            ]
        );
        let Resolution::Statements(alters) = operator(&db, &repo) else {
            panic!("expected statements");
        };
        assert!(
            alters[0]
                .sql
                .contains("SET (RESTRICT = scalarltsel, JOIN = NONE)")
        );
        assert!(alters.iter().all(|a| !a.destructive));
    }

    #[test]
    fn links_and_flags_are_set_in_place_only_when_unset() {
        let repo = parity(json!({"negator": "OPERATOR(test.<%>)",
                                 "hashes": true, "merges": true}));
        let db = parity(json!({}));
        let Resolution::Statements(alters) = operator(&repo, &db) else {
            panic!("expected statements");
        };
        assert_eq!(
            alters[0].sql,
            "ALTER OPERATOR test.=~= (integer, integer) SET (NEGATOR = \
             OPERATOR(test.<%>), HASHES, MERGES);\n"
        );
        // PostgreSQL cannot clear or change them
        for (repo, db) in [
            (parity(json!({})), parity(json!({"hashes": true}))),
            (
                parity(json!({})),
                parity(json!({"negator": "OPERATOR(test.<%>)"})),
            ),
            (
                parity(json!({"commutator": "OPERATOR(test.=~=)"})),
                parity(json!({"commutator": "OPERATOR(test.~~~)"})),
            ),
            (parity(json!({"function": "int4eq"})), parity(json!({}))),
        ] {
            assert!(matches!(operator(&repo, &db), Resolution::Replace));
        }
    }

    #[test]
    fn a_link_that_the_other_operator_gives_is_not_a_change() {
        let less = parse(json!({
            "name": "<<<", "schema": "test", "owner": "postgres",
            "function": "int4lt", "left_arg": "integer",
            "right_arg": "integer", "commutator": "OPERATOR(test.>>>)",
        }));
        let greater = |commutator: Option<&str>| {
            let mut value = json!({
                "name": ">>>", "schema": "test", "owner": "postgres",
                "function": "int4gt", "left_arg": "integer",
                "right_arg": "integer",
            });
            if let Some(commutator) = commutator {
                value["commutator"] = commutator.into();
            }
            parse(value)
        };
        let project = Project {
            name: String::from("test"),
            encoding: String::from("UTF8"),
            stdstrings: true,
            superuser: String::from("postgres"),
            default_schema: String::from("public"),
            path: std::path::PathBuf::new(),
            inventory: [less.clone(), greater(None)]
                .into_iter()
                .enumerate()
                .map(|(id, operator)| crate::models::Item {
                    id,
                    desc: ObjectType::Operator,
                    definition: Definition::Operator(operator),
                    dependencies: Default::default(),
                })
                .collect(),
        };
        let key = |operator: &Operator| {
            ObjectKey::new(
                ObjectType::Operator,
                &Definition::Operator(operator.clone()),
            )
        };
        // PostgreSQL links >>> back to <<<
        let linked = greater(Some("OPERATOR(test.<<<)"));
        let mut database = BTreeMap::from([
            (key(&less), Definition::Operator(less.clone())),
            (key(&linked), Definition::Operator(linked.clone())),
        ]);
        align(&project, &mut database);
        assert_eq!(
            database.get(&key(&linked)),
            Some(&Definition::Operator(greater(None)))
        );
        // a link to another operator stays a difference
        let other = greater(Some("OPERATOR(test.~~~)"));
        let mut database = BTreeMap::from([(
            key(&other),
            Definition::Operator(other.clone()),
        )]);
        align(&project, &mut database);
        assert_eq!(
            database.get(&key(&other)),
            Some(&Definition::Operator(other))
        );
    }

    #[test]
    fn the_entry_name_has_the_argument_types() {
        let mut dump =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let id = dump
            .add_entry(
                libpgdump::ObjectType::Operator,
                Some("test"),
                Some("!!!"),
                Some("postgres"),
                Some("CREATE OPERATOR ...;\n"),
                Some("DROP OPERATOR test.!!! (NONE, bigint);\n"),
                None,
                &[],
            )
            .expect("add entry");
        let entry = dump
            .entries()
            .iter()
            .find(|entry| entry.dump_id == id)
            .expect("the entry");
        let operator = parse(json!({
            "name": "!!!", "schema": "test", "owner": "postgres",
            "function": "int8um", "right_arg": "bigint",
        }));
        assert_eq!(
            entry_name(entry),
            Some(
                ObjectKey::new(
                    ObjectType::Operator,
                    &Definition::Operator(operator.clone())
                )
                .name
            )
        );
        assert_eq!(
            drop(&operator),
            "DROP OPERATOR IF EXISTS test.!!! (NONE, bigint);\n"
        );
    }
}
