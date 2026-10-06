//! The functions that the column defaults and the CHECK constraints of
//! a table call, and which of these expressions the build has to emit
//! as their own entries (deviation 40).
//!
//! PostgreSQL finds the functions of a default or a CHECK constraint
//! when it makes the table, so each function has to exist before the
//! table. A function with a SQL-standard body (`BEGIN ATOMIC` or
//! `RETURN`) can also read the table, and then it has to come after the
//! table. No order satisfies both. pg_dump repairs this dependency loop
//! in pg_dump_sort.c (`repairTableAttrDefMultiLoop`,
//! `repairTableConstraintMultiLoop`): it makes the table without the
//! default or the check, then the function, then the default as a
//! `DEFAULT` entry and the check as a `CHECK CONSTRAINT` entry, each
//! after the function. The build does the same.
//!
//! A CHECK of a domain that calls a function that takes or returns the
//! domain makes the same loop. pg_dump makes the domain without the
//! check, then the function, then the check as a `CHECK CONSTRAINT`
//! entry (`repairDomainConstraintMultiLoop`). The build does the same
//! (deviation 65).

use std::collections::{HashMap, HashSet};

use tree_sitter::Parser;

use crate::build::render_default;
use crate::ddl::{NodeExt, any_name};
use crate::models::{Definition, Domain, Table};
use crate::project::{Project, routine_base_name, split_sql_name};

/// The functions that the expressions of one table, or the CHECKs of
/// one domain, call, as inventory ids
#[derive(Default)]
pub(super) struct TableCalls {
    /// The functions of each column default, by column name
    pub defaults: HashMap<String, Vec<usize>>,
    /// The functions of each CHECK constraint, by constraint name. The
    /// name of a domain CHECK with no name is the name that PostgreSQL
    /// gives it (`Domain::with_check_names`)
    pub checks: HashMap<String, Vec<usize>>,
    /// The column defaults that CREATE TABLE cannot contain, because a
    /// function that they call needs the table
    pub separate_defaults: HashSet<String>,
    /// The CHECK constraints that CREATE TABLE or CREATE DOMAIN cannot
    /// contain, for the same cause, and the NOT VALID ones of a table
    /// (deviation 19) or a domain (deviation 92)
    pub separate_checks: HashSet<String>,
}

impl TableCalls {
    /// The functions of the expressions that CREATE TABLE contains,
    /// which the table entry has to come after
    pub fn inline_functions(&self) -> impl Iterator<Item = usize> + '_ {
        let defaults = self
            .defaults
            .iter()
            .filter(|(column, _)| !self.separate_defaults.contains(*column));
        let checks = self
            .checks
            .iter()
            .filter(|(name, _)| !self.separate_checks.contains(*name));
        defaults
            .chain(checks)
            .flat_map(|(_, functions)| functions.iter().copied())
    }
}

/// The [`TableCalls`] of each table that has an expression, and each
/// domain that has a CHECK, that calls a function of the project, by
/// inventory id.
///
/// An expression is separate when a function that it calls depends on
/// its table or domain, directly or through other objects. The
/// dependency graph for this check has the edges of the inventory, an
/// edge from each table and domain to each function that its
/// expressions call, and an edge from each function to each domain
/// that its parameters or its result name. A default that
/// CREATE TABLE cannot contain in any case (see
/// `Builder::split_column_defaults`) and a NOT VALID check are separate
/// already, and add no edge.
pub(super) fn table_calls(project: &Project) -> HashMap<usize, TableCalls> {
    let functions = function_index(project);
    let mut parser = Parser::new();
    if parser
        .set_language(&tree_sitter_postgres::LANGUAGE.into())
        .is_err()
    {
        return HashMap::new();
    }
    let mut result: HashMap<usize, TableCalls> = HashMap::new();
    let mut forced: HashMap<usize, (HashSet<String>, HashSet<String>)> =
        HashMap::new();
    for item in &project.inventory {
        if let Definition::Domain(domain) = &item.definition {
            if domain.sql.is_none() {
                let calls = domain_calls(&mut parser, domain, &functions);
                if !calls.checks.is_empty()
                    || !calls.separate_checks.is_empty()
                {
                    forced.entry(item.id).or_default().1 =
                        calls.separate_checks.clone();
                    result.insert(item.id, calls);
                }
            }
            continue;
        }
        let Definition::Table(table) = &item.definition else {
            continue;
        };
        // an sql table is written as it is, and the columns of a LIKE
        // table come from the other table. A foreign table keeps its
        // expressions in CREATE FOREIGN TABLE, because the separate
        // entries use ALTER TABLE
        if table.sql.is_some()
            || table.like_table.is_some()
            || table.server.is_some()
        {
            continue;
        }
        // the CHECKs as the build writes them (deviation 60)
        let table = &table.with_table_checks();
        let mut calls = TableCalls::default();
        let (forced_defaults, forced_checks) =
            forced.entry(item.id).or_default();
        for column in table.columns.iter().flatten() {
            if let Some(default) = &column.default {
                let called = called_functions(
                    &mut parser,
                    &render_default(default),
                    &functions,
                );
                if !called.is_empty() {
                    if separate_by_form(table, column) {
                        forced_defaults.insert(column.name.clone());
                    }
                    calls.defaults.insert(column.name.clone(), called);
                }
            }
        }
        for default in table.column_defaults.iter().flatten() {
            let called = called_functions(
                &mut parser,
                &render_default(&default.default),
                &functions,
            );
            if !called.is_empty() {
                forced_defaults.insert(default.column.clone());
                calls.defaults.insert(default.column.clone(), called);
            }
        }
        for check in table.check_constraints.iter().flatten() {
            if check.not_valid == Some(true) {
                forced_checks.insert(check.name.clone());
                calls.separate_checks.insert(check.name.clone());
            }
            let called =
                called_functions(&mut parser, &check.expression, &functions);
            if !called.is_empty() {
                calls.checks.insert(check.name.clone(), called);
            }
        }
        if !calls.defaults.is_empty() || !calls.checks.is_empty() {
            result.insert(item.id, calls);
        }
    }
    // the edges of the dependency graph: each object needs the objects
    // that it lists
    let mut graph: HashMap<usize, Vec<usize>> = project
        .inventory
        .iter()
        .map(|item| (item.id, item.dependencies.iter().copied().collect()))
        .collect();
    for (function, domains) in signature_domains(project) {
        graph.entry(function).or_default().extend(domains);
    }
    for (table, calls) in &result {
        let (forced_defaults, forced_checks) = &forced[table];
        let defaults = calls
            .defaults
            .iter()
            .filter(|(column, _)| !forced_defaults.contains(*column));
        let checks = calls
            .checks
            .iter()
            .filter(|(name, _)| !forced_checks.contains(*name));
        graph.entry(*table).or_default().extend(
            defaults
                .chain(checks)
                .flat_map(|(_, functions)| functions.iter().copied()),
        );
    }
    for (table, calls) in &mut result {
        let (forced_defaults, forced_checks) = &forced[table];
        let loops = |functions: &Vec<usize>| {
            functions.iter().any(|f| reaches(&graph, *f, *table))
        };
        calls.separate_defaults = calls
            .defaults
            .iter()
            .filter(|(column, functions)| {
                forced_defaults.contains(*column) || loops(functions)
            })
            .map(|(column, _)| column.clone())
            .collect();
        let checks: Vec<String> = calls
            .checks
            .iter()
            .filter(|(name, functions)| {
                forced_checks.contains(*name) || loops(functions)
            })
            .map(|(name, _)| name.clone())
            .collect();
        calls.separate_checks.extend(checks);
    }
    result
}

/// The [`TableCalls`] of a domain: the functions of each CHECK
fn domain_calls(
    parser: &mut Parser,
    domain: &Domain,
    functions: &HashMap<(String, String), Vec<usize>>,
) -> TableCalls {
    let mut calls = TableCalls::default();
    let domain = domain.with_check_names();
    for check in domain.check_constraints.iter().flatten() {
        let (Some(name), Some(expression)) = (&check.name, &check.expression)
        else {
            continue;
        };
        // only ALTER DOMAIN can add a NOT VALID check (deviation 92)
        if check.not_valid == Some(true) {
            calls.separate_checks.insert(name.clone());
        }
        let called = called_functions(parser, expression, functions);
        if !called.is_empty() {
            calls.checks.insert(name.clone(), called);
        }
    }
    calls
}

/// The domains of the project that the parameters or the result of
/// each function name, by the inventory id of the function. A name
/// without a schema is a built-in type, as `called_functions` reads a
/// function name.
fn signature_domains(project: &Project) -> HashMap<usize, Vec<usize>> {
    let domains: HashMap<(String, String), usize> = project
        .inventory
        .iter()
        .filter_map(|item| match &item.definition {
            Definition::Domain(d) => {
                Some(((d.schema.clone(), d.name.clone()), item.id))
            }
            _ => None,
        })
        .collect();
    let mut result = HashMap::new();
    for item in &project.inventory {
        let Definition::Function(f) = &item.definition else {
            continue;
        };
        let names = f
            .parameters
            .iter()
            .flatten()
            .map(|p| p.data_type.as_str())
            .chain(f.returns.as_deref());
        let found: Vec<usize> = names
            .filter_map(|name| {
                let name = name.trim();
                let name = name
                    .get(..6)
                    .filter(|word| word.eq_ignore_ascii_case("setof "))
                    .map_or(name, |_| &name[6..]);
                let name = without_modifiers(name).trim();
                let (schema, name) = split_sql_name(name);
                domains.get(&(schema, name)).copied()
            })
            .collect();
        if !found.is_empty() {
            result.insert(item.id, found);
        }
    }
    result
}

/// A type name without its modifier or its array suffix: the text
/// before the first `(` or `[` that is not in a quoted identifier
fn without_modifiers(name: &str) -> &str {
    let mut quoted = false;
    for (index, c) in name.char_indices() {
        match c {
            // a doubled quote in a quoted identifier toggles twice
            '"' => quoted = !quoted,
            '(' | '[' if !quoted => return &name[..index],
            _ => {}
        }
    }
    name
}

/// Whether the build emits a column default as its own entry whatever
/// it calls: the `nextval` default of a plain table, as
/// `Builder::split_column_defaults` does
fn separate_by_form(table: &Table, column: &crate::models::Column) -> bool {
    table.from_type.is_none()
        && super::sequence_backed_default(column).is_some()
}

/// The functions of the project, by schema and by name. A function
/// whose name includes its argument types, as test-project/functions
/// writes it, is by its name without them.
fn function_index(project: &Project) -> HashMap<(String, String), Vec<usize>> {
    let mut index: HashMap<(String, String), Vec<usize>> = HashMap::new();
    for item in &project.inventory {
        if let Definition::Function(f) = &item.definition {
            let name = routine_base_name(&f.name, &f.parameters);
            index
                .entry((f.schema.clone(), name.to_string()))
                .or_default()
                .push(item.id);
        }
    }
    index
}

/// The functions of the project that `expression` calls. Only a name
/// with a schema is looked up: pg_restore and deploy run with an empty
/// `search_path`, so a name without a schema is a `pg_catalog`
/// function. The arguments of a call do not have known types, so a
/// call names each overload of its name.
pub(crate) fn called_functions(
    parser: &mut Parser,
    expression: &str,
    functions: &HashMap<(String, String), Vec<usize>>,
) -> Vec<usize> {
    let sql = format!("SELECT {expression};");
    let Some(tree) = parser.parse(&sql, None) else {
        return Vec::new();
    };
    let root = tree.root_node();
    if root.has_error() {
        return Vec::new();
    }
    let mut called: Vec<usize> = root
        .find_all("func_name")
        .iter()
        .map(|node| any_name(node, &sql))
        .filter_map(|name| functions.get(&(name.schema?, name.name)).cloned())
        .flatten()
        .collect();
    called.sort_unstable();
    called.dedup();
    called
}

/// Whether `to` is reachable from `from` in `graph`
fn reaches(
    graph: &HashMap<usize, Vec<usize>>,
    from: usize,
    to: usize,
) -> bool {
    let mut stack = vec![from];
    let mut seen = HashSet::new();
    while let Some(node) = stack.pop() {
        if node == to {
            return true;
        }
        if seen.insert(node) {
            stack.extend(graph.get(&node).into_iter().flatten().copied());
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;

    use super::*;
    use crate::constants::ObjectType;
    use crate::models::Item;

    fn project(inventory: Vec<Item>) -> Project {
        Project {
            name: "calls".into(),
            superuser: "postgres".into(),
            default_schema: "public".into(),
            path: std::path::PathBuf::new(),
            settings: Default::default(),
            inventory,
        }
    }

    fn item(
        id: usize,
        desc: ObjectType,
        definition: Definition,
        dependencies: &[usize],
    ) -> Item {
        Item {
            id,
            desc,
            definition,
            dependencies: dependencies.iter().copied().collect(),
        }
    }

    fn function(id: usize, name: &str, dependencies: &[usize]) -> Item {
        item(
            id,
            ObjectType::Function,
            Definition::Function(
                serde_json::from_value(json!({
                    "name": name, "schema": "test", "owner": "postgres",
                    "returns": "integer", "language": "sql",
                    "sql_body": "RETURN 1",
                }))
                .unwrap(),
            ),
            dependencies,
        )
    }

    fn table(id: usize, name: &str, fields: serde_json::Value) -> Item {
        let mut value = json!({"name": name, "schema": "test",
                               "owner": "postgres"});
        value
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        item(
            id,
            ObjectType::Table,
            Definition::Table(serde_json::from_value(value).unwrap()),
            &[],
        )
    }

    /// A default or a check that calls a function that reads its table
    /// is separate. One that calls a function that does not, or reads
    /// the table through another object, is separate only in the
    /// second case. A name without a schema is a `pg_catalog`
    /// function, and a nested call is a call too
    #[test]
    fn separates_the_expressions_of_a_dependency_loop() {
        let project = project(vec![
            table(
                0,
                "t",
                json!({
                    "columns": [
                        {"name": "a", "data_type": "integer",
                         "default": "test.reads_t()"},
                        {"name": "b", "data_type": "integer",
                         "default": "abs(test.plain())"},
                        {"name": "c", "data_type": "integer",
                         "default": "reads_t()"},
                        {"name": "d", "data_type": "integer",
                         "default": "test.reads_view()"},
                    ],
                    "check_constraints": [
                        {"name": "t_a_check",
                         "expression": "(test.reads_t() > a)"},
                        {"name": "t_b_check",
                         "expression": "(test.plain() > b)"},
                    ],
                }),
            ),
            function(1, "reads_t", &[0]),
            function(2, "plain", &[]),
            item(
                3,
                ObjectType::View,
                Definition::View(
                    serde_json::from_value(json!({
                        "name": "v", "schema": "test", "owner": "postgres",
                        "query": "SELECT 1",
                    }))
                    .unwrap(),
                ),
                &[0],
            ),
            function(4, "reads_view", &[3]),
        ]);
        let calls = table_calls(&project);
        let calls = &calls[&0];
        assert_eq!(calls.defaults["a"], [1]);
        assert_eq!(calls.defaults["b"], [2]);
        assert!(!calls.defaults.contains_key("c"));
        let separate: BTreeSet<&str> =
            calls.separate_defaults.iter().map(String::as_str).collect();
        assert_eq!(separate, ["a", "d"].into());
        let separate: BTreeSet<&str> =
            calls.separate_checks.iter().map(String::as_str).collect();
        assert_eq!(separate, ["t_a_check"].into());
        let mut inline: Vec<usize> = calls.inline_functions().collect();
        inline.sort_unstable();
        assert_eq!(inline, [2, 2]);
    }

    /// A function of another table's default can close the loop: `t`
    /// calls `f`, `f` reads `u`, and `u` calls `g`, which reads `t`
    #[test]
    fn separates_a_loop_through_another_table() {
        let default = |f: &str| {
            json!({"columns": [{"name": "n", "data_type": "integer",
                                "default": format!("test.{f}()")}]})
        };
        let project = project(vec![
            table(0, "t", default("f")),
            table(1, "u", default("g")),
            function(2, "f", &[1]),
            function(3, "g", &[0]),
        ]);
        let calls = table_calls(&project);
        assert!(calls[&0].separate_defaults.contains("n"));
        assert!(calls[&1].separate_defaults.contains("n"));
    }

    /// A CHECK on a column is a CHECK of the table with its name, as
    /// the build writes it (deviation 60), thus one that calls a
    /// function that reads its table is separate too
    #[test]
    fn separates_a_column_check() {
        let project = project(vec![
            table(
                0,
                "t",
                json!({"columns": [{"name": "a", "data_type": "integer",
                                    "check_constraint":
                                        "test.reads_t() > a"}]}),
            ),
            function(1, "reads_t", &[0]),
        ]);
        let calls = table_calls(&project);
        assert_eq!(calls[&0].checks["t_a_check"], [1]);
        assert!(calls[&0].separate_checks.contains("t_a_check"));
    }

    /// A domain CHECK that calls a function that takes the domain is
    /// separate, and one that calls a function that does not is in
    /// CREATE DOMAIN, which then comes after the function. A CHECK with
    /// no name has the name that PostgreSQL gives it
    #[test]
    fn separates_a_domain_check_of_a_dependency_loop() {
        let domain = |id, name: &str, checks: serde_json::Value| {
            item(
                id,
                ObjectType::Domain,
                Definition::Domain(
                    serde_json::from_value(json!({
                        "name": name, "schema": "test", "owner": "postgres",
                        "data_type": "integer", "check_constraints": checks,
                    }))
                    .unwrap(),
                ),
                &[],
            )
        };
        let takes = |id, data_type: &str| {
            item(
                id,
                ObjectType::Function,
                Definition::Function(
                    serde_json::from_value(json!({
                        "name": format!("takes_{id}"), "schema": "test",
                        "owner": "postgres", "returns": "boolean",
                        "language": "sql", "sql_body": "RETURN true",
                        "parameters": [{"mode": "IN", "data_type": data_type}],
                    }))
                    .unwrap(),
                ),
                &[],
            )
        };
        let project = project(vec![
            domain(
                0,
                "d",
                json!([
                    {"name": "d_loop", "expression": "test.takes_1(VALUE)"},
                    {"expression": "test.takes_2(VALUE)"},
                ]),
            ),
            takes(1, "test.d[]"),
            takes(2, "integer"),
        ]);
        let calls = table_calls(&project);
        let calls = &calls[&0];
        assert_eq!(calls.checks["d_loop"], [1]);
        assert_eq!(calls.checks["d_check"], [2]);
        let separate: BTreeSet<&str> =
            calls.separate_checks.iter().map(String::as_str).collect();
        assert_eq!(separate, ["d_loop"].into());
        assert_eq!(calls.inline_functions().collect::<Vec<_>>(), [2]);
    }

    /// A parenthesis or a bracket in a quoted domain name is part of
    /// the name, not a modifier or an array suffix
    #[test]
    fn signature_domains_keep_a_quoted_name() {
        let domain = |id, name: &str| {
            item(
                id,
                ObjectType::Domain,
                Definition::Domain(
                    serde_json::from_value(json!({
                        "name": name, "schema": "test", "owner": "postgres",
                        "data_type": "integer",
                    }))
                    .unwrap(),
                ),
                &[],
            )
        };
        let project = project(vec![
            domain(0, "d(x)"),
            domain(1, "e[\"y\"]"),
            item(
                2,
                ObjectType::Function,
                Definition::Function(
                    serde_json::from_value(json!({
                        "name": "f", "schema": "test", "owner": "postgres",
                        "returns": "test.\"e[\"\"y\"\"]\"[]",
                        "language": "sql", "sql_body": "RETURN 1",
                        "parameters": [
                            {"mode": "IN", "data_type": "test.\"d(x)\""},
                        ],
                    }))
                    .unwrap(),
                ),
                &[],
            ),
        ]);
        assert_eq!(signature_domains(&project)[&2], [0, 1]);
    }
}
