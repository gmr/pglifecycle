//! Operator classes and operator families. PostgreSQL has no ALTER for
//! the definition of a class, but `ALTER OPERATOR FAMILY ... ADD |
//! DROP` changes the members of a family, and a class uses the members
//! of its family. Thus:
//!
//! - a member that the project gives and the database does not have
//!   is added to the family in place, for a class and for a family;
//! - a member that only the database family has is dropped in place,
//!   gated by `--allow-drop`;
//! - a member that only the database class has, or any other change
//!   to a class, drops the class and makes it again.
//!
//! PostgreSQL keeps some members of a class as members of its family
//! only (for example a btree sort support function, and each operator
//! of a GiST class), and pg_dump writes them in the family. [`align`]
//! moves such a member back to the class when the project gives it
//! there, so the two forms compare equal.

use std::collections::BTreeMap;

use super::names::{argument_type, name, operator, signature, type_name};
use super::{Alter, Resolution, push_comment};
use crate::constants::ObjectType;
use crate::deploy::diff::ObjectKey;
use crate::models::{
    Definition, OperatorClass, OperatorClassFunction, OperatorClassOperator,
    OperatorFamily,
};
use crate::project::{Project, split_sql_name};
use crate::utils::quote_ident;

pub(super) fn operator_class(
    repo: &OperatorClass,
    db: &OperatorClass,
) -> Resolution {
    let (r, d) = (canonical_class(repo), canonical_class(db));
    // the owner is compared on its own
    let definition = |class: &OperatorClass| OperatorClass {
        owner: String::new(),
        operators: None,
        functions: None,
        comment: None,
        ..class.clone()
    };
    let operators = difference(&r.operators, &d.operators);
    let functions = difference(&r.functions, &d.functions);
    if definition(&r) != definition(&d)
        || !difference(&d.operators, &r.operators).is_empty()
        || !difference(&d.functions, &r.functions).is_empty()
    {
        return Resolution::Replace;
    }
    let mut alters = Vec::new();
    if let Some(items) = members(&operators, &functions) {
        alters.push(Alter::new(format!(
            "ALTER OPERATOR FAMILY {} USING {} ADD\n    {items};\n",
            r.family.as_deref().unwrap_or_default(),
            quote_ident(&r.method)
        )));
    }
    push_comment(
        &mut alters,
        "OPERATOR CLASS",
        &target(&repo.schema, &repo.name, &repo.method),
        &repo.comment,
        &db.comment,
    );
    Resolution::Statements(alters)
}

pub(super) fn operator_family(
    repo: &OperatorFamily,
    db: &OperatorFamily,
) -> Resolution {
    let (r, d) = (canonical_family(repo), canonical_family(db));
    let family = target(&repo.schema, &repo.name, &repo.method);
    let mut alters = Vec::new();
    // drop first, so that a member can be given again in another form
    let dropped_operators = difference(&d.operators, &r.operators);
    let dropped_functions = difference(&d.functions, &r.functions);
    let dropped: Vec<String> = dropped_operators
        .iter()
        .map(|o| format!("OPERATOR {} ({})", o.strategy, types(&o.arguments)))
        .chain(
            dropped_functions.iter().map(|f| {
                format!("FUNCTION {} ({})", f.support, types(&f.types))
            }),
        )
        .collect();
    if !dropped.is_empty() {
        alters.push(Alter::destructive(format!(
            "ALTER OPERATOR FAMILY {family} DROP\n    {};\n",
            dropped.join(",\n    ")
        )));
    }
    // PostgreSQL refuses a member in the slot of a member that is still
    // there, and an operator that is still there for the same purpose.
    // Such an ADD needs the DROP, so it is gated with it: a script
    // without --allow-drop does not keep it and fail.
    let (after_drop_operators, new_operators): (Vec<_>, Vec<_>) =
        difference(&r.operators, &d.operators)
            .into_iter()
            .partition(|o| {
                dropped_operators.iter().any(|d| {
                    d.arguments == o.arguments
                        && (d.strategy == o.strategy
                            || (d.name == o.name
                                && d.order_by.is_some()
                                    == o.order_by.is_some()))
                })
            });
    let (after_drop_functions, new_functions): (Vec<_>, Vec<_>) =
        difference(&r.functions, &d.functions)
            .into_iter()
            .partition(|f| {
                dropped_functions
                    .iter()
                    .any(|d| d.support == f.support && d.types == f.types)
            });
    if let Some(items) = members(&new_operators, &new_functions) {
        alters.push(Alter::new(format!(
            "ALTER OPERATOR FAMILY {family} ADD\n    {items};\n"
        )));
    }
    if let Some(items) = members(&after_drop_operators, &after_drop_functions)
    {
        alters.push(Alter::destructive(format!(
            "ALTER OPERATOR FAMILY {family} ADD\n    {items};\n"
        )));
    }
    push_comment(
        &mut alters,
        "OPERATOR FAMILY",
        &family,
        &repo.comment,
        &db.comment,
    );
    Resolution::Statements(alters)
}

fn types(types: &Option<Vec<String>>) -> String {
    types.as_deref().unwrap_or_default().join(", ")
}

/// The items of `a` that `b` does not have
fn difference<T: Clone + PartialEq>(
    a: &Option<Vec<T>>,
    b: &Option<Vec<T>>,
) -> Vec<T> {
    let b = b.as_deref().unwrap_or_default();
    a.iter()
        .flatten()
        .filter(|item| !b.contains(item))
        .cloned()
        .collect()
}

/// The OPERATOR and FUNCTION items of ALTER OPERATOR FAMILY ... ADD,
/// or None when there are none
fn members(
    operators: &[OperatorClassOperator],
    functions: &[OperatorClassFunction],
) -> Option<String> {
    let items: Vec<String> = operators
        .iter()
        .map(|o| {
            let mut item = format!(
                "OPERATOR {} {}({})",
                o.strategy,
                o.name,
                types(&o.arguments)
            );
            if let Some(order_by) = &o.order_by {
                item.push_str(&format!(" FOR ORDER BY {order_by}"));
            }
            item
        })
        .chain(functions.iter().map(|f| {
            format!(
                "FUNCTION {} ({}) {}",
                f.support,
                types(&f.types),
                f.function
            )
        }))
        .collect();
    (!items.is_empty()).then(|| items.join(",\n    "))
}

/// `schema.name USING method`, which ALTER, COMMENT ON and DROP read
fn target(schema: &str, name: &str, method: &str) -> String {
    format!(
        "{}.{} USING {}",
        quote_ident(schema),
        quote_ident(name),
        quote_ident(&method.to_lowercase())
    )
}

/// The class as PostgreSQL keeps it: canonical names and types, its
/// family (PostgreSQL makes one with the name of the class when there
/// is none), no storage type that is the column type, no flag at its
/// default, and its members in order with the types that PostgreSQL
/// supplies when the project gives none
pub(crate) fn canonical_class(class: &OperatorClass) -> OperatorClass {
    let method = class.method.to_lowercase();
    let data_type = type_name(&class.data_type);
    let family = match &class.family {
        Some(family) => name(family),
        None => format!(
            "{}.{}",
            quote_ident(&class.schema),
            quote_ident(&class.name)
        ),
    };
    OperatorClass {
        method: method.clone(),
        default: class.default.filter(|v| *v),
        family: Some(family),
        storage: class
            .storage
            .as_deref()
            .map(type_name)
            .filter(|storage| *storage != data_type),
        operators: canonical_operators(&class.operators, Some(&data_type)),
        functions: canonical_functions(
            &class.functions,
            &method,
            Some(&data_type),
        ),
        data_type,
        ..class.clone()
    }
}

/// The family as PostgreSQL keeps it: see [`canonical_class`]. A family
/// has no column type, so PostgreSQL supplies fewer types.
pub(crate) fn canonical_family(family: &OperatorFamily) -> OperatorFamily {
    let method = family.method.to_lowercase();
    OperatorFamily {
        operators: canonical_operators(&family.operators, None),
        functions: canonical_functions(&family.functions, &method, None),
        method,
        ..family.clone()
    }
}

fn canonical_operators(
    operators: &Option<Vec<OperatorClassOperator>>,
    data_type: Option<&str>,
) -> Option<Vec<OperatorClassOperator>> {
    let mut operators: Vec<OperatorClassOperator> = operators
        .iter()
        .flatten()
        .map(|o| OperatorClassOperator {
            strategy: o.strategy,
            name: operator(&o.name),
            // the operands of a class operator are the column type
            arguments: match &o.arguments {
                Some(arguments) => {
                    Some(arguments.iter().map(|a| argument_type(a)).collect())
                }
                None => data_type.map(|t| vec![t.to_string(), t.to_string()]),
            },
            order_by: o.order_by.as_deref().map(name),
        })
        .collect();
    operators.sort_by(|a, b| {
        (a.strategy, &a.arguments).cmp(&(b.strategy, &b.arguments))
    });
    (!operators.is_empty()).then_some(operators)
}

fn canonical_functions(
    functions: &Option<Vec<OperatorClassFunction>>,
    method: &str,
    data_type: Option<&str>,
) -> Option<Vec<OperatorClassFunction>> {
    let mut functions: Vec<OperatorClassFunction> = functions
        .iter()
        .flatten()
        .map(|f| {
            let function = signature(&f.function);
            let types = match &f.types {
                Some(types) => {
                    Some(types.iter().map(|t| argument_type(t)).collect())
                }
                None => {
                    supplied_types(method, f.support, &function, data_type)
                }
            };
            OperatorClassFunction {
                support: f.support,
                types,
                function,
            }
        })
        .collect();
    functions
        .sort_by(|a, b| (a.support, &a.types).cmp(&(b.support, &b.types)));
    (!functions.is_empty()).then_some(functions)
}

/// The operand types that PostgreSQL gives a support function with
/// none (`assignProcTypes` in opclasscmds.c): the input types of a
/// btree comparison (1) or in_range (3) function and of a hash function
/// (1, 2), and the column type of the class for the others. A family
/// has no column type, so then the project must give the types.
fn supplied_types(
    method: &str,
    support: u32,
    function: &str,
    data_type: Option<&str>,
) -> Option<Vec<String>> {
    let arguments = super::names::signature_types(function);
    let pair = |left: &String, right: &String| {
        Some(vec![left.clone(), right.clone()])
    };
    match (method, support, arguments.as_slice()) {
        ("btree", 1, [left, right, ..]) => pair(left, right),
        ("btree", 3, [left, _, right, ..]) => pair(left, right),
        ("hash", 1 | 2, [left, ..]) => pair(left, left),
        _ => data_type.map(|t| vec![t.to_string(), t.to_string()]),
    }
}

/// The key of the family of a class, from the canonical class
fn family_key(class: &OperatorClass) -> ObjectKey {
    let (schema, family) =
        split_sql_name(class.family.as_deref().unwrap_or_default());
    ObjectKey {
        desc: ObjectType::OperatorFamily,
        schema,
        name: format!("{family} USING {}", class.method),
    }
}

/// Align the database classes and families with the project before
/// they compare:
///
/// 1. A member that a project class gives, and that the database has
///    as a member of the family of the class only, moves from the
///    database family to the database class.
/// 2. The family that PostgreSQL made for a project class with no
///    family (or that the class names) is not an object that only the
///    database has when the project does not give it. Dropping it would
///    drop the class too.
pub(crate) fn align(
    project: &Project,
    database: &mut BTreeMap<ObjectKey, Definition>,
) {
    let classes: Vec<OperatorClass> = project
        .inventory
        .iter()
        .filter_map(|item| match &item.definition {
            Definition::OperatorClass(class) => Some(canonical_class(class)),
            _ => None,
        })
        .collect();
    for repo in &classes {
        let key = ObjectKey::new(
            ObjectType::OperatorClass,
            &Definition::OperatorClass(repo.clone()),
        );
        let Some(Definition::OperatorClass(db)) = database.get(&key) else {
            continue;
        };
        let mut db_class = canonical_class(db);
        let Some(Definition::OperatorFamily(db_family)) =
            database.get_mut(&family_key(&db_class))
        else {
            continue;
        };
        let mut family = canonical_family(db_family);
        let moved_operators = shared(
            &repo.operators,
            &mut db_class.operators,
            &mut family.operators,
        );
        let moved_functions = shared(
            &repo.functions,
            &mut db_class.functions,
            &mut family.functions,
        );
        if !moved_operators && !moved_functions {
            continue;
        }
        *db_family = family;
        database.insert(key, Definition::OperatorClass(db_class));
    }
    let families: Vec<ObjectKey> = project
        .inventory
        .iter()
        .filter(|item| item.desc == ObjectType::OperatorFamily)
        .map(|item| ObjectKey::new(item.desc, &item.definition))
        .collect();
    for repo in &classes {
        let key = family_key(repo);
        if !families.contains(&key) {
            database.remove(&key);
        }
    }
}

/// Move each member of `wanted` that `class` does not have and `family`
/// has from `family` to `class`; true when one moved
fn shared<T: Clone + PartialEq>(
    wanted: &Option<Vec<T>>,
    class: &mut Option<Vec<T>>,
    family: &mut Option<Vec<T>>,
) -> bool {
    let mut moved = false;
    for member in wanted.iter().flatten() {
        let in_class = class.iter().flatten().any(|m| m == member);
        let Some(members) = family.as_mut() else {
            break;
        };
        let Some(index) = members.iter().position(|m| m == member) else {
            continue;
        };
        if in_class {
            continue;
        }
        members.remove(index);
        class.get_or_insert_with(Vec::new).push(member.clone());
        moved = true;
    }
    if family.as_ref().is_some_and(Vec::is_empty) {
        *family = None;
    }
    moved
}

/// The DROP statement of an operator class that only the database has
pub(crate) fn drop_class(class: &OperatorClass) -> String {
    format!(
        "DROP OPERATOR CLASS IF EXISTS {};\n",
        target(&class.schema, &class.name, &class.method)
    )
}

/// The DROP statement of an operator family that only the database has.
/// PostgreSQL drops the classes of the family with it; a class that
/// only the database has drops before, in dependency order.
pub(crate) fn drop_family(family: &OperatorFamily) -> String {
    format!(
        "DROP OPERATOR FAMILY IF EXISTS {};\n",
        target(&family.schema, &family.name, &family.method)
    )
}

/// The identity of an operator class or family archive entry. Its tag
/// is only the name, so the index method comes from its DROP
/// statement, `DROP OPERATOR CLASS schema.name USING method;`.
pub(crate) fn entry_name(entry: &libpgdump::Entry) -> Option<String> {
    let tag = entry.tag.as_deref()?;
    let drop = entry.drop_stmt.as_deref()?;
    let (_, method) = drop.rsplit_once(" USING ")?;
    let (_, method) = split_sql_name(method.trim_end().trim_end_matches(';'));
    Some(format!("{tag} USING {method}"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn class(value: serde_json::Value) -> OperatorClass {
        serde_json::from_value(value).expect("class deserializes")
    }

    fn family(value: serde_json::Value) -> OperatorFamily {
        serde_json::from_value(value).expect("family deserializes")
    }

    #[test]
    fn class_short_forms_are_canonical() {
        let pulled = class(json!({
            "name": "int_class", "schema": "test", "owner": "postgres",
            "method": "btree", "data_type": "integer",
            "family": "test.int_class",
            "operators": [
                {"strategy": 1, "name": "<",
                 "arguments": ["integer", "integer"]},
                {"strategy": 3, "name": "=",
                 "arguments": ["integer", "integer"]},
            ],
            "functions": [
                {"support": 1, "types": ["integer", "integer"],
                 "function": "btint4cmp(integer,integer)"},
                {"support": 2, "types": ["integer", "integer"],
                 "function": "btint4sortsupport(internal)"},
            ],
        }));
        let written = class(json!({
            "name": "int_class", "schema": "test", "owner": "postgres",
            "method": "BTREE", "data_type": "INT4", "default": false,
            "storage": "int4",
            "operators": [
                {"strategy": 3, "name": "OPERATOR(pg_catalog.=)"},
                {"strategy": 1, "name": "pg_catalog.<"},
            ],
            "functions": [
                {"support": 2,
                 "function": "pg_catalog.btint4sortsupport(INTERNAL)"},
                {"support": 1, "function": "BTINT4CMP(INT4, INT)"},
            ],
        }));
        assert_eq!(canonical_class(&written), canonical_class(&pulled));
    }

    #[test]
    fn supplied_types_follow_the_method() {
        let function = |method: &str, support: u32, function: &str| {
            let functions = Some(vec![OperatorClassFunction {
                support,
                types: None,
                function: function.to_string(),
            }]);
            canonical_functions(&functions, method, Some("integer")).unwrap()
                [0]
            .types
            .clone()
        };
        let pair = |a: &str, b: &str| Some(vec![a.to_string(), b.to_string()]);
        assert_eq!(
            function("btree", 1, "btint48cmp(integer, bigint)"),
            pair("integer", "bigint")
        );
        assert_eq!(
            function(
                "btree",
                3,
                "in_range(integer, integer, bigint, bool, bool)"
            ),
            pair("integer", "bigint")
        );
        assert_eq!(
            function("hash", 2, "hashint8extended(int8, int8)"),
            pair("bigint", "bigint")
        );
        assert_eq!(
            function("btree", 2, "btint4sortsupport(internal)"),
            pair("integer", "integer")
        );
        assert_eq!(
            function("gist", 3, "gist_point_compress(internal)"),
            pair("integer", "integer")
        );
    }

    #[test]
    fn an_added_class_member_is_added_to_the_family() {
        let with = |functions: serde_json::Value| {
            class(json!({
                "name": "c", "schema": "test", "owner": "postgres",
                "method": "btree", "data_type": "integer",
                "operators": [{"strategy": 1, "name": "<"}],
                "functions": functions,
            }))
        };
        let db = with(
            json!([{"support": 1, "function": "btint4cmp(integer, integer)"}]),
        );
        let repo = with(json!([
            {"support": 1, "function": "btint4cmp(integer, integer)"},
            {"support": 2, "function": "btint4sortsupport(internal)"},
        ]));
        let Resolution::Statements(alters) = operator_class(&repo, &db) else {
            panic!("expected statements");
        };
        assert_eq!(
            alters[0].sql,
            "ALTER OPERATOR FAMILY test.c USING btree ADD\n    FUNCTION 2 \
             (integer, integer) btint4sortsupport(internal);\n"
        );
        assert!(!alters[0].destructive);
        // a member that only the database class has needs a rebuild
        assert!(matches!(operator_class(&db, &repo), Resolution::Replace));
        // as does another class definition
        let default = OperatorClass {
            default: Some(true),
            ..repo.clone()
        };
        assert!(matches!(
            operator_class(&default, &repo),
            Resolution::Replace
        ));
        // the owner is compared on its own
        let owned = OperatorClass {
            owner: String::from("Gate Owner"),
            ..repo.clone()
        };
        assert!(matches!(
            operator_class(&owned, &repo),
            Resolution::Statements(alters) if alters.is_empty()
        ));
    }

    #[test]
    fn family_members_change_in_place() {
        let with = |operators: serde_json::Value| {
            family(json!({
                "name": "int_family", "schema": "test", "owner": "postgres",
                "method": "btree", "operators": operators,
            }))
        };
        let repo = with(json!([{"strategy": 1, "name": "<",
                                "arguments": ["int4", "int8"]}]));
        let db = with(json!([{"strategy": 3, "name": "=",
                              "arguments": ["integer", "bigint"]}]));
        let Resolution::Statements(alters) = operator_family(&repo, &db)
        else {
            panic!("expected statements");
        };
        let sql: Vec<(&str, bool)> = alters
            .iter()
            .map(|a| (a.sql.as_str(), a.destructive))
            .collect();
        assert_eq!(
            sql,
            [
                (
                    "ALTER OPERATOR FAMILY test.int_family USING btree DROP\n    \
                     OPERATOR 3 (integer, bigint);\n",
                    true
                ),
                (
                    "ALTER OPERATOR FAMILY test.int_family USING btree ADD\n    \
                     OPERATOR 1 <(integer, bigint);\n",
                    false
                ),
            ]
        );
    }

    #[test]
    fn an_add_that_needs_a_drop_is_gated_with_it() {
        let with = |operators: serde_json::Value,
                    functions: serde_json::Value| {
            family(json!({
                "name": "int_family", "schema": "test", "owner": "postgres",
                "method": "btree", "operators": operators,
                "functions": functions,
            }))
        };
        let db = with(
            json!([
                {"strategy": 1, "name": "<", "arguments": ["int4", "int8"]},
                {"strategy": 2, "name": "<=", "arguments": ["int4", "int8"]},
            ]),
            json!([{"support": 1, "types": ["int4", "int8"],
                    "function": "btint48cmp(int4, int8)"}]),
        );
        // strategy 1 has another operator, the operator of strategy 2
        // moves to strategy 4, support 1 has another function, and
        // strategy 5 is new
        let repo = with(
            json!([
                {"strategy": 1, "name": "test.<",
                 "arguments": ["int4", "int8"]},
                {"strategy": 4, "name": "<=", "arguments": ["int4", "int8"]},
                {"strategy": 5, "name": ">", "arguments": ["int4", "int8"]},
            ]),
            json!([{"support": 1, "types": ["int4", "int8"],
                    "function": "test.cmp(int4, int8)"}]),
        );
        let Resolution::Statements(alters) = operator_family(&repo, &db)
        else {
            panic!("expected statements");
        };
        let sql: Vec<(&str, bool)> = alters
            .iter()
            .map(|a| (a.sql.as_str(), a.destructive))
            .collect();
        // PostgreSQL refuses each gated ADD while the member that the
        // DROP removes is there, so a script without --allow-drop must
        // not have it
        assert_eq!(
            sql,
            [
                (
                    "ALTER OPERATOR FAMILY test.int_family USING btree DROP\n    \
                     OPERATOR 1 (integer, bigint),\n    \
                     OPERATOR 2 (integer, bigint),\n    \
                     FUNCTION 1 (integer, bigint);\n",
                    true
                ),
                (
                    "ALTER OPERATOR FAMILY test.int_family USING btree ADD\n    \
                     OPERATOR 5 >(integer, bigint);\n",
                    false
                ),
                (
                    "ALTER OPERATOR FAMILY test.int_family USING btree ADD\n    \
                     OPERATOR 1 test.<(integer, bigint),\n    \
                     OPERATOR 4 <=(integer, bigint),\n    \
                     FUNCTION 1 (integer, bigint) test.cmp(integer, bigint);\n",
                    true
                ),
            ]
        );
    }

    fn project(items: Vec<(ObjectType, Definition)>) -> Project {
        Project {
            name: String::from("test"),
            encoding: String::from("UTF8"),
            stdstrings: true,
            superuser: String::from("postgres"),
            default_schema: String::from("public"),
            path: std::path::PathBuf::new(),
            inventory: items
                .into_iter()
                .enumerate()
                .map(|(id, (desc, definition))| crate::models::Item {
                    id,
                    desc,
                    definition,
                    dependencies: Default::default(),
                })
                .collect(),
        }
    }

    #[test]
    fn align_moves_family_members_to_the_class() {
        // a GiST class as a person writes it: all members in the class
        let repo = class(json!({
            "name": "pd", "schema": "test", "owner": "postgres",
            "method": "gist", "data_type": "point",
            "operators": [{"strategy": 15, "name": "<->",
                           "arguments": ["point", "point"],
                           "order_by": "float_ops"}],
            "functions": [{"support": 1,
                           "function": "gist_point_consistent(internal, point, int2, oid, internal)"}],
        }));
        // as pg_dump writes it: the operator is in the family
        let db = class(json!({
            "name": "pd", "schema": "test", "owner": "postgres",
            "method": "gist", "data_type": "point", "family": "test.pd",
            "functions": [{"support": 1, "types": ["point", "point"],
                           "function": "gist_point_consistent(internal,point,smallint,oid,internal)"}],
        }));
        let db_family = family(json!({
            "name": "pd", "schema": "test", "owner": "postgres",
            "method": "gist",
            "operators": [{"strategy": 15, "name": "<->",
                           "arguments": ["point", "point"],
                           "order_by": "pg_catalog.float_ops"}],
        }));
        let project = project(vec![(
            ObjectType::OperatorClass,
            Definition::OperatorClass(repo.clone()),
        )]);
        let class_key = ObjectKey::new(
            ObjectType::OperatorClass,
            &Definition::OperatorClass(db.clone()),
        );
        let family_key = ObjectKey::new(
            ObjectType::OperatorFamily,
            &Definition::OperatorFamily(db_family.clone()),
        );
        let mut database = BTreeMap::from([
            (class_key.clone(), Definition::OperatorClass(db)),
            (family_key.clone(), Definition::OperatorFamily(db_family)),
        ]);
        align(&project, &mut database);
        let Some(Definition::OperatorClass(aligned)) =
            database.get(&class_key)
        else {
            panic!("the class is in the database");
        };
        assert_eq!(canonical_class(aligned), canonical_class(&repo));
        // the family of the class is not in the project, so it is not
        // an object that only the database has
        assert!(!database.contains_key(&family_key));
    }

    #[test]
    fn the_entry_name_has_the_method() {
        let mut dump =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let id = dump
            .add_entry(
                libpgdump::ObjectType::OperatorClass,
                Some("test"),
                Some("int_class"),
                Some("postgres"),
                Some("CREATE OPERATOR CLASS ...;\n"),
                Some("DROP OPERATOR CLASS test.int_class USING btree;\n"),
                None,
                &[],
            )
            .expect("add entry");
        let entry = dump
            .entries()
            .iter()
            .find(|entry| entry.dump_id == id)
            .expect("the entry");
        assert_eq!(
            entry_name(entry).as_deref(),
            Some("int_class USING btree")
        );
        let class = class(json!({
            "name": "Int Class", "schema": "test", "owner": "postgres",
            "method": "btree", "data_type": "integer",
        }));
        assert_eq!(
            drop_class(&class),
            "DROP OPERATOR CLASS IF EXISTS test.\"Int Class\" USING btree;\n"
        );
    }
}
