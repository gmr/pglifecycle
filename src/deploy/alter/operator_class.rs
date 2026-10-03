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
//!
//! A class with no family in the project gets a family of its own name
//! (an implied family). Deploy does not drop that family, as that drops
//! the class too. [`align`] moves the members of the family that no
//! project class gives to the database class, and the class drops them
//! from the family in place, gated by `--allow-drop`.
//!
//! The drop of a class does not drop the members that PostgreSQL keeps
//! in the family, and the create of the class gives them again. Thus
//! the rebuild of a class drops them from the family first. The drop of
//! a family drops its classes too, so the rebuild drops the class only
//! if it exists: a class that moves to another family is gone when the
//! plan drops the old family first.

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
use crate::pull::Assembly;
use crate::utils::quote_ident;

/// The members that the database keeps for a project class in the
/// family of the database class, and not in the class (see [`align`])
#[derive(Debug, Default)]
pub(crate) struct Loose {
    /// The family, `schema.name USING method`
    family: String,
    /// The family stays when the plan makes the class again: the
    /// project gives it, or a project class is in it. The plan drops
    /// each other family first, and its members with it.
    kept: bool,
    /// The members that the project class gives
    operators: Vec<OperatorClassOperator>,
    functions: Vec<OperatorClassFunction>,
    /// The members of an implied family that no project class gives
    extra_operators: Vec<OperatorClassOperator>,
    extra_functions: Vec<OperatorClassFunction>,
}

/// The [`Loose`] members of the project classes, by the key of the
/// class
pub(crate) type Families = BTreeMap<ObjectKey, Loose>;

/// The operators and the functions of a class or a family
type Members<'a> = (
    &'a Option<Vec<OperatorClassOperator>>,
    &'a Option<Vec<OperatorClassFunction>>,
);

pub(super) fn operator_class(
    repo: &OperatorClass,
    db: &OperatorClass,
    families: &Families,
) -> Resolution {
    let (r, d) = (canonical_class(repo), canonical_class(db));
    let loose = families.get(&class_key(&r));
    // the owner is compared on its own
    let definition = |class: &OperatorClass| OperatorClass {
        owner: String::new(),
        operators: None,
        functions: None,
        comment: None,
        ..class.clone()
    };
    // the members of the implied family that no project class gives
    // are not members of the class: the class drops them from the
    // family in place
    let extra_operators = loose.map(|l| l.extra_operators.as_slice());
    let extra_functions = loose.map(|l| l.extra_functions.as_slice());
    let class_operators = without(&d.operators, extra_operators);
    let class_functions = without(&d.functions, extra_functions);
    if definition(&r) != definition(&d)
        || !difference(&class_operators, &r.operators).is_empty()
        || !difference(&class_functions, &r.functions).is_empty()
    {
        return rebuild(db, loose);
    }
    let family = format!(
        "{} USING {}",
        r.family.as_deref().unwrap_or_default(),
        quote_ident(&r.method)
    );
    let mut alters = family_members(
        &family,
        (&r.operators, &r.functions),
        (&d.operators, &d.functions),
    );
    push_comment(
        &mut alters,
        "OPERATOR CLASS",
        &target(&repo.schema, &repo.name, &repo.method),
        &repo.comment,
        &db.comment,
    );
    Resolution::Statements(alters)
}

/// The drop and the create of a class. The create gives again the
/// members that PostgreSQL keeps in the family, so the rebuild drops
/// them from the family first, with the members of an implied family
/// that no project class gives. A family that does not stay drops
/// before the rebuild, and its classes and members with it, so the
/// class is dropped only if it exists.
fn rebuild(db: &OperatorClass, loose: Option<&Loose>) -> Resolution {
    let before = loose
        .filter(|l| l.kept)
        .and_then(|l| {
            dropped_members(
                &l.family,
                &[l.operators.as_slice(), &l.extra_operators].concat(),
                &[l.functions.as_slice(), &l.extra_functions].concat(),
            )
        })
        .map(Alter::destructive)
        .into_iter()
        .collect();
    Resolution::Rebuild {
        before,
        drop: drop_class(db),
    }
}

pub(super) fn operator_family(
    repo: &OperatorFamily,
    db: &OperatorFamily,
) -> Resolution {
    let (r, d) = (canonical_family(repo), canonical_family(db));
    let family = target(&repo.schema, &repo.name, &repo.method);
    let mut alters = family_members(
        &family,
        (&r.operators, &r.functions),
        (&d.operators, &d.functions),
    );
    push_comment(
        &mut alters,
        "OPERATOR FAMILY",
        &family,
        &repo.comment,
        &db.comment,
    );
    Resolution::Statements(alters)
}

/// The ALTER OPERATOR FAMILY statements that change the members of
/// `family` from `db` to `repo`
fn family_members(family: &str, repo: Members, db: Members) -> Vec<Alter> {
    let mut alters = Vec::new();
    // drop first, so that a member can be given again in another form
    let dropped_operators = difference(db.0, repo.0);
    let dropped_functions = difference(db.1, repo.1);
    if let Some(sql) =
        dropped_members(family, &dropped_operators, &dropped_functions)
    {
        alters.push(Alter::destructive(sql));
    }
    // PostgreSQL refuses a member in the slot of a member that is still
    // there, and an operator that is still there for the same purpose.
    // Such an ADD needs the DROP, so it is gated with it: a script
    // without --allow-drop does not keep it and fail.
    let (after_drop_operators, new_operators): (Vec<_>, Vec<_>) =
        difference(repo.0, db.0).into_iter().partition(|o| {
            dropped_operators.iter().any(|d| {
                d.arguments == o.arguments
                    && (d.strategy == o.strategy
                        || (d.name == o.name
                            && d.order_by.is_some() == o.order_by.is_some()))
            })
        });
    let (after_drop_functions, new_functions): (Vec<_>, Vec<_>) =
        difference(repo.1, db.1).into_iter().partition(|f| {
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
    alters
}

/// The ALTER OPERATOR FAMILY ... DROP statement of these members, or
/// None when there are none
fn dropped_members(
    family: &str,
    operators: &[OperatorClassOperator],
    functions: &[OperatorClassFunction],
) -> Option<String> {
    let items: Vec<String> = operators
        .iter()
        .map(|o| format!("OPERATOR {} ({})", o.strategy, types(&o.arguments)))
        .chain(
            functions.iter().map(|f| {
                format!("FUNCTION {} ({})", f.support, types(&f.types))
            }),
        )
        .collect();
    (!items.is_empty()).then(|| {
        format!(
            "ALTER OPERATOR FAMILY {family} DROP\n    {};\n",
            items.join(",\n    ")
        )
    })
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

/// The items of `a` that are not in `remove`
fn without<T: Clone + PartialEq>(
    a: &Option<Vec<T>>,
    remove: Option<&[T]>,
) -> Option<Vec<T>> {
    let remove = remove.unwrap_or_default();
    a.as_ref().map(|items| {
        items
            .iter()
            .filter(|item| !remove.contains(item))
            .cloned()
            .collect()
    })
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

/// The key of a class, from the class
fn class_key(class: &OperatorClass) -> ObjectKey {
    ObjectKey::new(
        ObjectType::OperatorClass,
        &Definition::OperatorClass(class.clone()),
    )
}

/// Align the database classes and families with the project before
/// they compare, and return the [`Loose`] members of each project
/// class:
///
/// 1. A member that a project class gives, and that the database has
///    as a member of the family of the class only, moves from the
///    database family to the database class.
/// 2. The family that PostgreSQL made for a project class with no
///    family (or that the class names) is not an object that only the
///    database has when the project does not give it (an implied
///    family). Dropping it would drop the class too. The members that
///    stay in it after step 1, which no project class gives, move to
///    the first project class that the database has in the family, so
///    that the class drops them from the family.
pub(crate) fn align(
    project: &Project,
    database: &mut BTreeMap<ObjectKey, Definition>,
) -> Families {
    let classes: Vec<OperatorClass> = project
        .inventory
        .iter()
        .filter_map(|item| match &item.definition {
            Definition::OperatorClass(class) => Some(canonical_class(class)),
            _ => None,
        })
        .collect();
    let given: Vec<ObjectKey> = project
        .inventory
        .iter()
        .filter(|item| item.desc == ObjectType::OperatorFamily)
        .map(|item| ObjectKey::new(item.desc, &item.definition))
        .collect();
    let implied: Vec<ObjectKey> = classes
        .iter()
        .map(family_key)
        .filter(|key| !given.contains(key))
        .collect();
    let mut families = Families::new();
    for repo in &classes {
        let key = class_key(repo);
        let Some(Definition::OperatorClass(db)) = database.get(&key) else {
            continue;
        };
        let mut db_class = canonical_class(db);
        let db_family_key = family_key(&db_class);
        let Some(Definition::OperatorFamily(db_family)) =
            database.get_mut(&db_family_key)
        else {
            continue;
        };
        let mut family = canonical_family(db_family);
        let operators = shared(
            &repo.operators,
            &mut db_class.operators,
            &mut family.operators,
        );
        let functions = shared(
            &repo.functions,
            &mut db_class.functions,
            &mut family.functions,
        );
        if operators.is_empty() && functions.is_empty() {
            continue;
        }
        families.insert(
            key.clone(),
            Loose {
                family: target(&family.schema, &family.name, &family.method),
                kept: given.contains(&db_family_key)
                    || implied.contains(&db_family_key),
                operators,
                functions,
                ..Loose::default()
            },
        );
        *db_family = family;
        database.insert(key, Definition::OperatorClass(db_class));
    }
    for implied_key in &implied {
        let Some(Definition::OperatorFamily(family)) =
            database.remove(implied_key)
        else {
            continue;
        };
        let Some(key) = classes.iter().map(class_key).find(|key| {
            matches!(
                database.get(key),
                Some(Definition::OperatorClass(db))
                    if family_key(&canonical_class(db)) == *implied_key
            )
        }) else {
            continue;
        };
        let Some(Definition::OperatorClass(db)) = database.get_mut(&key)
        else {
            continue;
        };
        let mut class = canonical_class(db);
        let extra_operators =
            canonical_operators(&family.operators, Some(&class.data_type))
                .unwrap_or_default();
        let extra_functions = canonical_functions(
            &family.functions,
            &class.method,
            Some(&class.data_type),
        )
        .unwrap_or_default();
        if extra_operators.is_empty() && extra_functions.is_empty() {
            continue;
        }
        class
            .operators
            .get_or_insert_with(Vec::new)
            .extend(extra_operators.iter().cloned());
        class
            .functions
            .get_or_insert_with(Vec::new)
            .extend(extra_functions.iter().cloned());
        *db = canonical_class(&class);
        let loose = families.entry(key).or_insert_with(|| Loose {
            family: target(&family.schema, &family.name, &family.method),
            kept: true,
            ..Loose::default()
        });
        loose.extra_operators = extra_operators;
        loose.extra_functions = extra_functions;
    }
    families
}

/// The [`Loose`] members of the project classes: [`align`] on the
/// classes and the families of the database, as the diff aligns them
pub(crate) fn families(project: &Project, assembly: &Assembly) -> Families {
    let classes = assembly
        .operator_classes
        .iter()
        .map(|class| Definition::OperatorClass(class.clone()))
        .map(|class| {
            (ObjectKey::new(ObjectType::OperatorClass, &class), class)
        });
    let families = assembly
        .operator_families
        .iter()
        .map(|family| Definition::OperatorFamily(family.clone()))
        .map(|family| {
            (ObjectKey::new(ObjectType::OperatorFamily, &family), family)
        });
    align(project, &mut classes.chain(families).collect())
}

/// Move each member of `wanted` that `class` does not have and `family`
/// has from `family` to `class`, and return the members that moved
fn shared<T: Clone + PartialEq>(
    wanted: &Option<Vec<T>>,
    class: &mut Option<Vec<T>>,
    family: &mut Option<Vec<T>>,
) -> Vec<T> {
    let mut moved = Vec::new();
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
        moved.push(member.clone());
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
        let Resolution::Statements(alters) =
            operator_class(&repo, &db, &Families::new())
        else {
            panic!("expected statements");
        };
        assert_eq!(
            alters[0].sql,
            "ALTER OPERATOR FAMILY test.c USING btree ADD\n    FUNCTION 2 \
             (integer, integer) btint4sortsupport(internal);\n"
        );
        assert!(!alters[0].destructive);
        // a member that only the database class has needs a rebuild,
        // which drops the class only if it exists
        let Resolution::Rebuild { before, drop } =
            operator_class(&db, &repo, &Families::new())
        else {
            panic!("expected a rebuild");
        };
        assert!(before.is_empty());
        assert_eq!(
            drop,
            "DROP OPERATOR CLASS IF EXISTS test.c USING btree;\n"
        );
        // as does another class definition
        let default = OperatorClass {
            default: Some(true),
            ..repo.clone()
        };
        assert!(matches!(
            operator_class(&default, &repo, &Families::new()),
            Resolution::Rebuild { .. }
        ));
        // the owner is compared on its own
        let owned = OperatorClass {
            owner: String::from("Gate Owner"),
            ..repo.clone()
        };
        assert!(matches!(
            operator_class(&owned, &repo, &Families::new()),
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
            superuser: String::from("postgres"),
            default_schema: String::from("public"),
            path: std::path::PathBuf::new(),
            settings: Default::default(),
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

    /// A btree class for integer, with its sort support function in the
    /// class, as a person writes it
    fn int_class(name: &str, family: Option<&str>) -> OperatorClass {
        class(json!({
            "name": name, "schema": "test", "owner": "postgres",
            "method": "btree", "data_type": "integer", "family": family,
            "operators": [{"strategy": 1, "name": "<"}],
            "functions": [
                {"support": 1, "function": "btint4cmp(integer, integer)"},
                {"support": 2, "function": "btint4sortsupport(internal)"},
            ],
        }))
    }

    /// The database as pg_dump gives an [`int_class`] in the family
    /// `family_name`: the sort support function `sort_support` is in the
    /// family, with the family operators `operators`
    fn dumped(
        repo: &OperatorClass,
        family_name: &str,
        sort_support: &str,
        operators: serde_json::Value,
    ) -> BTreeMap<ObjectKey, Definition> {
        let db = Definition::OperatorClass(OperatorClass {
            family: Some(format!("test.{family_name}")),
            functions: Some(vec![OperatorClassFunction {
                support: 1,
                types: None,
                function: String::from("btint4cmp(integer, integer)"),
            }]),
            ..repo.clone()
        });
        let db_family = Definition::OperatorFamily(family(json!({
            "name": family_name, "schema": "test", "owner": "postgres",
            "method": "btree", "operators": operators,
            "functions": [{"support": 2, "types": ["integer", "integer"],
                           "function": sort_support}],
        })));
        BTreeMap::from([
            (ObjectKey::new(ObjectType::OperatorClass, &db), db),
            (
                ObjectKey::new(ObjectType::OperatorFamily, &db_family),
                db_family,
            ),
        ])
    }

    fn statements(resolution: Resolution) -> Vec<(String, bool)> {
        let Resolution::Statements(alters) = resolution else {
            panic!("expected statements");
        };
        alters.into_iter().map(|a| (a.sql, a.destructive)).collect()
    }

    fn database_class<'a>(
        database: &'a BTreeMap<ObjectKey, Definition>,
        repo: &OperatorClass,
    ) -> &'a OperatorClass {
        match database.get(&class_key(repo)) {
            Some(Definition::OperatorClass(db)) => db,
            _ => panic!("the class is in the database"),
        }
    }

    #[test]
    fn an_implied_family_drops_the_members_that_no_class_gives() {
        let repo = int_class("c", None);
        let project = project(vec![(
            ObjectType::OperatorClass,
            Definition::OperatorClass(repo.clone()),
        )]);
        let mut database = dumped(
            &repo,
            "c",
            "btint4sortsupport(internal)",
            json!([{"strategy": 3, "name": "=",
                    "arguments": ["integer", "bigint"]}]),
        );
        let families = align(&project, &mut database);
        // the implied family is not an object that only the database has
        assert_eq!(database.len(), 1);
        let db = database_class(&database, &repo);
        // the member that no class gives is not in the project class, so
        // the class changes, and it drops the member from the family
        assert_ne!(canonical_class(db), canonical_class(&repo));
        assert_eq!(
            statements(operator_class(&repo, db, &families)),
            [(
                String::from(
                    "ALTER OPERATOR FAMILY test.c USING btree DROP\n    \
                     OPERATOR 3 (integer, bigint);\n"
                ),
                true
            )]
        );
        // a class change drops the members that the family keeps before
        // the class is made again, as the create gives them again
        let changed = OperatorClass {
            default: Some(true),
            ..repo.clone()
        };
        let Resolution::Rebuild { before, drop } =
            operator_class(&changed, db, &families)
        else {
            panic!("expected a rebuild");
        };
        let before: Vec<(&str, bool)> = before
            .iter()
            .map(|a| (a.sql.as_str(), a.destructive))
            .collect();
        assert_eq!(
            before,
            [(
                "ALTER OPERATOR FAMILY test.c USING btree DROP\n    \
                 OPERATOR 3 (integer, bigint),\n    \
                 FUNCTION 2 (integer, integer);\n",
                true
            )]
        );
        assert_eq!(
            drop,
            "DROP OPERATOR CLASS IF EXISTS test.c USING btree;\n"
        );
    }

    #[test]
    fn an_add_in_the_slot_of_an_implied_family_member_is_gated() {
        let repo = int_class("c", None);
        let project = project(vec![(
            ObjectType::OperatorClass,
            Definition::OperatorClass(repo.clone()),
        )]);
        // the database has another sort support function in the family
        let mut database = dumped(
            &repo,
            "c",
            "btint8sortsupport(internal)",
            serde_json::Value::Null,
        );
        let families = align(&project, &mut database);
        let db = database_class(&database, &repo);
        // PostgreSQL refuses the ADD while the other function is there
        assert_eq!(
            statements(operator_class(&repo, db, &families)),
            [
                (
                    String::from(
                        "ALTER OPERATOR FAMILY test.c USING btree DROP\n    \
                         FUNCTION 2 (integer, integer);\n"
                    ),
                    true
                ),
                (
                    String::from(
                        "ALTER OPERATOR FAMILY test.c USING btree ADD\n    \
                         FUNCTION 2 (integer, integer) \
                         btint4sortsupport(internal);\n"
                    ),
                    true
                ),
            ]
        );
    }

    #[test]
    fn a_shared_implied_family_drops_each_member_once() {
        let first = int_class("c", None);
        let second = class(json!({
            "name": "c8", "schema": "test", "owner": "postgres",
            "method": "btree", "data_type": "bigint", "family": "test.c",
            "operators": [{"strategy": 1, "name": "<"}],
            "functions": [{"support": 1,
                           "function": "btint8cmp(bigint, bigint)"}],
        }));
        let project = project(vec![
            (
                ObjectType::OperatorClass,
                Definition::OperatorClass(first.clone()),
            ),
            (
                ObjectType::OperatorClass,
                Definition::OperatorClass(second.clone()),
            ),
        ]);
        let mut database = dumped(
            &first,
            "c",
            "btint4sortsupport(internal)",
            json!([{"strategy": 3, "name": "=",
                    "arguments": ["integer", "bigint"]}]),
        );
        database.insert(
            class_key(&second),
            Definition::OperatorClass(second.clone()),
        );
        let families = align(&project, &mut database);
        // the first class in the family drops the member
        let extras: Vec<&ObjectKey> = families
            .iter()
            .filter(|(_, loose)| !loose.extra_operators.is_empty())
            .map(|(key, _)| key)
            .collect();
        assert_eq!(extras, [&class_key(&first)]);
        assert!(matches!(
            operator_class(
                &second,
                database_class(&database, &second),
                &families
            ),
            Resolution::Statements(alters) if alters.is_empty()
        ));
    }

    #[test]
    fn a_class_that_leaves_a_dropped_family_drops_no_members() {
        // the class moves to a family of the project, and its old family
        // is only in the database: the plan drops that family first, and
        // the class and its members with it
        let repo = int_class("c", Some("test.new_family"));
        let new_family = family(json!({
            "name": "new_family", "schema": "test", "owner": "postgres",
            "method": "btree",
        }));
        let project = project(vec![
            (
                ObjectType::OperatorFamily,
                Definition::OperatorFamily(new_family),
            ),
            (
                ObjectType::OperatorClass,
                Definition::OperatorClass(repo.clone()),
            ),
        ]);
        let mut database = dumped(
            &repo,
            "old_family",
            "btint4sortsupport(internal)",
            serde_json::Value::Null,
        );
        let families = align(&project, &mut database);
        assert!(!families[&class_key(&repo)].kept);
        let old_family = ObjectKey {
            desc: ObjectType::OperatorFamily,
            schema: String::from("test"),
            name: String::from("old_family USING btree"),
        };
        assert!(database.contains_key(&old_family));
        let Resolution::Rebuild { before, drop } =
            operator_class(&repo, database_class(&database, &repo), &families)
        else {
            panic!("expected a rebuild");
        };
        assert!(before.is_empty());
        assert_eq!(
            drop,
            "DROP OPERATOR CLASS IF EXISTS test.c USING btree;\n"
        );
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
