//! The query of a view or a materialized view in the form that the
//! server stores it. PostgreSQL adds casts and parentheses when it
//! stores a query (`name = 'x'` becomes `(name = 'x'::text)`), and
//! formatting does not add them. Thus a hand-written project query is
//! different from the query of the database on each deploy.
//!
//! Against a live database, deploy gives each project query that is
//! different after formatting to the server: in one psql session, in a
//! transaction that it rolls back, it makes a temporary view of the
//! query and reads it with `pg_get_viewdef`, as pg_dump does. When the
//! result is the query of the database, the project query is that
//! result. When the server cannot deparse a query (for example, the
//! query uses a table that is new in the project), deploy compares the
//! query as text and gives a warning.
//!
//! New in the Rust implementation: the Python implementation had no
//! `deploy` command, so no Python file ports to this module.

use std::collections::HashMap;
use std::io::Write;

use super::diff::canonical_query;
use crate::models::{Definition, ViewColumn};
use crate::project::Project;
use crate::pull::Assembly;
use crate::utils::quote_ident;
use crate::{cli, pgdump};

/// The start of each stderr line that marks the output of one query
const MARKER: &str = "pglifecycle_deparse ";

/// A project query that the server deparses
struct Pending {
    /// The index of the item in the project inventory
    index: usize,
    /// The kind and the name of the view, for the log
    label: String,
    /// The CREATE TEMP VIEW statement, with no `;`; psql does not read
    /// it as is (see `execute`)
    create: String,
    /// The query of the database
    stored: String,
}

/// Replace each view and materialized view query of the project that is
/// different from the query of the database with its deparsed form,
/// when that form is the query of the database
pub(super) fn deparse(
    project: &mut Project,
    assembly: &Assembly,
    conn: &cli::Connection,
) {
    let stored: HashMap<(bool, &str, &str), &str> = assembly
        .views
        .iter()
        .filter_map(|v| {
            Some((
                (false, v.schema.as_str(), v.name.as_str()),
                v.query.as_deref()?,
            ))
        })
        .chain(assembly.materialized_views.iter().filter_map(|v| {
            Some((
                (true, v.schema.as_str(), v.name.as_str()),
                v.query.as_deref()?,
            ))
        }))
        .collect();
    let mut pending = Vec::new();
    for (index, item) in project.inventory.iter().enumerate() {
        let (matview, schema, name, query, recursive, columns) =
            match &item.definition {
                Definition::View(v) if v.sql.is_none() => (
                    false,
                    &v.schema,
                    &v.name,
                    v.query.as_deref(),
                    v.recursive == Some(true),
                    v.columns.as_deref(),
                ),
                Definition::MaterializedView(v) if v.sql.is_none() => (
                    true,
                    &v.schema,
                    &v.name,
                    v.query.as_deref(),
                    false,
                    v.columns.as_deref(),
                ),
                _ => continue,
            };
        let Some(query) = query else { continue };
        let Some(db) = stored.get(&(matview, schema.as_str(), name.as_str()))
        else {
            continue;
        };
        if canonical_query(query) == canonical_query(db) {
            continue;
        }
        let label = format!(
            "{} {schema}.{name}",
            if matview { "MATERIALIZED VIEW" } else { "VIEW" }
        );
        let create =
            create_temp_view(pending.len(), recursive, columns, query);
        // psql reads the script, thus a `;` or a psql command in the
        // query must not run as one more statement
        if !one_statement(&create) {
            log::warn!(
                "Cannot deparse the query of {label} on the server: the \
                 query is not one SQL statement; deploy compares the query \
                 as text"
            );
            continue;
        }
        pending.push(Pending {
            index,
            label,
            create,
            stored: db.to_string(),
        });
    }
    if pending.is_empty() {
        return;
    }
    let results = match run(&pending, conn) {
        Ok(results) => results,
        Err(error) => {
            log::debug!("psql failed: {error}");
            vec![Err((first_line(&error), error)); pending.len()]
        }
    };
    for (pending, result) in pending.iter().zip(results) {
        match result {
            Ok(deparsed) => {
                if canonical_query(&deparsed)
                    != canonical_query(&pending.stored)
                {
                    continue;
                }
                match &mut project.inventory[pending.index].definition {
                    Definition::View(v) => v.query = Some(deparsed),
                    Definition::MaterializedView(v) => {
                        v.query = Some(deparsed)
                    }
                    _ => unreachable!(),
                }
            }
            Err((reason, detail)) => {
                log::warn!(
                    "Cannot deparse the query of {} on the server ({reason}); \
                     deploy compares the query as text",
                    pending.label
                );
                log::debug!("psql output for {}: {detail}", pending.label);
            }
        }
    }
}

/// The CREATE TEMP VIEW statement for query `n`, with the column names
/// of the project view, as build writes them
fn create_temp_view(
    n: usize,
    recursive: bool,
    columns: Option<&[ViewColumn]>,
    query: &str,
) -> String {
    let mut create = String::from("CREATE TEMP ");
    if recursive {
        create.push_str("RECURSIVE ");
    }
    create.push_str(&format!("VIEW pglifecycle_deparse_{n} "));
    if let Some(columns) = columns {
        let names: Vec<String> = columns
            .iter()
            .map(|c| quote_ident(super::alter::view_column_name(c)))
            .collect();
        create.push_str(&format!("({}) ", names.join(", ")));
    }
    // a `--` comment at the end of the query must not hide the `;`
    create.push_str(&format!("AS {}\n", crate::pull::strip_trailing(query)));
    create
}

/// Whether `create` is one SQL statement and no more
fn one_statement(create: &str) -> bool {
    crate::ddl::Parser::new()
        .and_then(|mut parser| parser.parse(&format!("{create};")))
        .is_ok_and(|statements| statements.len() == 1)
}

/// The psql script that deparses each pending query. All statements
/// are in one transaction that the script rolls back. Each query has
/// its savepoint, thus an error stops only that query. The session
/// settings are those of the deploy script (see `render_script`)
fn script(pending: &[Pending], role: Option<&str>) -> String {
    let mut script = String::from("SET client_encoding = 'UTF8';\nBEGIN;\n");
    if let Some(role) = role {
        script.push_str(&format!("SET LOCAL ROLE {};\n", quote_ident(role)));
    }
    script.push_str(
        "SET LOCAL search_path = '';\n\
         SET LOCAL standard_conforming_strings = on;\n",
    );
    for (n, pending) in pending.iter().enumerate() {
        script.push_str(&format!(
            "\\warn {MARKER}{n}\n\
             SAVEPOINT pglifecycle_deparse;\n\
             {}\n\
             SELECT pg_catalog.json_build_object('view', {n}, 'query', \
             pg_catalog.pg_get_viewdef(\
             'pg_temp.pglifecycle_deparse_{n}'::pg_catalog.regclass));\n\
             ROLLBACK TO SAVEPOINT pglifecycle_deparse;\n",
            execute(&pending.create)
        ));
    }
    script.push_str("ROLLBACK;\n");
    script
}

/// A DO statement that runs `create`. psql reads the script and acts
/// on a `\` command or a `:name` variable that is not in quotes. Thus
/// `create`, with the query and the column names, is only in a dollar
/// quote, and psql does not read it. The tag of each dollar quote does
/// not occur in `create`; the two tags are different
fn execute(create: &str) -> String {
    let inner = dollar_tag(create, "pgl_q");
    let outer = dollar_tag(create, "pgl_o");
    // `create` ends with a line break, thus the end of the query and
    // the closing tag do not make one more tag together
    format!("DO {outer} BEGIN EXECUTE {inner}{create}{inner}; END {outer};")
}

/// The first of `$name$`, `$name1$`, `$name2$`, ... that does not occur
/// in `text`
fn dollar_tag(text: &str, name: &str) -> String {
    (0..)
        .map(|n| match n {
            0 => format!("${name}$"),
            n => format!("${name}{n}$"),
        })
        .find(|tag| !text.contains(tag.as_str()))
        .expect("a tag that does not occur in the text")
}

/// The deparsed query, or the reason and the psql output of the
/// failure
type Deparsed = Result<String, (String, String)>;

/// Deparse the pending queries with one psql session
fn run(
    pending: &[Pending],
    conn: &cli::Connection,
) -> Result<Vec<Deparsed>, String> {
    let mut file = tempfile::Builder::new()
        .prefix("pglifecycle-deparse-")
        .suffix(".sql")
        .tempfile()
        .map_err(|e| format!("failed to make a temporary file: {e}"))?;
    file.write_all(script(pending, conn.role.as_deref()).as_bytes())
        .map_err(|e| format!("failed to write a temporary file: {e}"))?;
    let (stdout, stderr) = pgdump::run_script(conn, file.path())?;
    Ok(parse(pending.len(), &stdout, &stderr))
}

/// The result of each of `count` queries. Each stdout line with a
/// result is a JSON object, thus a query with line breaks stays on one
/// line. The stderr lines after the marker of a query are its errors;
/// the lines before the first marker are the errors of the session
fn parse(count: usize, stdout: &str, stderr: &str) -> Vec<Deparsed> {
    let mut deparsed: HashMap<usize, String> = stdout
        .lines()
        .filter_map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).ok()?;
            Some((
                usize::try_from(value["view"].as_u64()?).ok()?,
                value["query"].as_str()?.to_string(),
            ))
        })
        .collect();
    let mut session = String::new();
    let mut errors: HashMap<usize, String> = HashMap::new();
    let mut current = None;
    for line in stderr.lines() {
        if let Some(n) = line
            .strip_prefix(MARKER)
            .and_then(|n| n.trim().parse::<usize>().ok())
        {
            current = Some(n);
            continue;
        }
        let text = match current {
            Some(n) => errors.entry(n).or_default(),
            None => &mut session,
        };
        text.push_str(line);
        text.push('\n');
    }
    (0..count)
        .map(|n| match deparsed.remove(&n) {
            Some(query) => Ok(query),
            None => {
                // a session error, such as SET LOCAL ROLE to a missing
                // role, aborts the transaction and is the real cause
                let detail = Some(&session)
                    .filter(|text| text.contains("ERROR:"))
                    .or_else(|| {
                        errors.get(&n).filter(|text| !text.trim().is_empty())
                    })
                    .unwrap_or(&session)
                    .trim()
                    .to_string();
                Err((reason(&detail), detail))
            }
        })
        .collect()
}

/// The first error message of the psql output, else its first line
fn reason(detail: &str) -> String {
    detail
        .lines()
        .find_map(|line| line.split_once("ERROR:"))
        .map(|(_, message)| message.trim().to_string())
        .unwrap_or_else(|| match first_line(detail) {
            line if line.is_empty() => String::from("psql gave no result"),
            line => line,
        })
}

fn first_line(text: &str) -> String {
    text.lines().next().unwrap_or_default().trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn makes_a_temp_view_with_the_columns() {
        let columns = [
            ViewColumn::Name("Out Col".into()),
            ViewColumn::Name("id".into()),
        ];
        assert_eq!(
            create_temp_view(3, true, Some(&columns), "SELECT 1, 2;\n"),
            "CREATE TEMP RECURSIVE VIEW pglifecycle_deparse_3 \
             (\"Out Col\", id) AS SELECT 1, 2\n"
        );
        assert_eq!(
            create_temp_view(0, false, None, "SELECT 1 -- one"),
            "CREATE TEMP VIEW pglifecycle_deparse_0 AS SELECT 1 -- one\n"
        );
    }

    /// psql runs each statement of the script, thus a query that is
    /// more than one statement is not given to it
    #[test]
    fn accepts_only_one_statement() {
        let create = |query| create_temp_view(0, false, None, query);
        assert!(one_statement(&create("SELECT 'a;b' AS x;")));
        assert!(!one_statement(&create("SELECT 1; COMMIT; DROP TABLE t")));
        assert!(!one_statement(&create("SELECT 1\n\\! rm -rf /tmp/x\n")));
    }

    #[test]
    fn rolls_back_each_query_and_the_session() {
        let pending = [Pending {
            index: 0,
            label: "VIEW s.v".into(),
            create: create_temp_view(0, false, None, "SELECT 1"),
            stored: String::new(),
        }];
        let script = script(&pending, Some("App Role"));
        assert!(script.starts_with(
            "SET client_encoding = 'UTF8';\nBEGIN;\n\
             SET LOCAL ROLE \"App Role\";\n\
             SET LOCAL search_path = '';\n"
        ));
        assert!(script.contains(
            "\\warn pglifecycle_deparse 0\n\
             SAVEPOINT pglifecycle_deparse;\n\
             DO $pgl_o$ BEGIN EXECUTE $pgl_q$\
             CREATE TEMP VIEW pglifecycle_deparse_0 AS SELECT 1\n\
             $pgl_q$; END $pgl_o$;\n"
        ));
        assert!(script.ends_with(
            "ROLLBACK TO SAVEPOINT pglifecycle_deparse;\nROLLBACK;\n"
        ));
        assert!(!script.contains("COMMIT"));
    }

    /// psql must not read a `\` command or a `:name` variable in the
    /// query: the query is only in the dollar quote, and no tag occurs
    /// in the query
    #[test]
    fn keeps_the_query_in_a_dollar_quote() {
        let query = "SELECT 1 AS \"$pgl_o$\"\n\\! echo pwned\n\
                     , :foo, :'foo', '$pgl_q$', '$pgl_q1$' -- $pgl_q";
        let create = create_temp_view(0, false, None, query);
        let script = execute(&create);
        assert_eq!(
            script,
            format!(
                "DO $pgl_o1$ BEGIN EXECUTE $pgl_q2${create}$pgl_q2$; END $pgl_o1$;"
            )
        );
        for tag in ["$pgl_o1$", "$pgl_q2$"] {
            assert!(!query.contains(tag), "{tag}");
            assert_eq!(script.matches(tag).count(), 2, "{tag}");
        }
        let body = &script["DO $pgl_o1$ BEGIN EXECUTE $pgl_q2$".len()
            ..script.len() - "$pgl_q2$; END $pgl_o1$;".len()];
        assert_eq!(body, create);
        for text in ["\\! echo pwned", ":foo", ":'foo'", "$pgl_q$"] {
            assert_eq!(script.matches(text).count(), 1, "{text}");
            assert!(body.contains(text), "{text}");
        }
    }

    /// The output of psql for three queries: the second uses a table
    /// that the database does not have
    #[test]
    fn parses_each_result_and_each_error() {
        let stdout = "\n{\"view\" : 0, \"query\" : \" SELECT id\\n   FROM \
                      public.t;\"}\n\
                      {\"view\" : 2, \"query\" : \" SELECT 1;\"}\n";
        let stderr = "pglifecycle_deparse 0\n\
                      pglifecycle_deparse 1\n\
                      psql:d.sql:14: ERROR:  relation \"public.missing\" \
                      does not exist\n\
                      LINE 1: ...AS SELECT id FROM public.mis...\n\
                      psql:d.sql:15: ERROR:  current transaction is \
                      aborted\n\
                      pglifecycle_deparse 2\n";
        let results = parse(3, stdout, stderr);
        assert_eq!(results[0], Ok(" SELECT id\n   FROM public.t;".into()));
        let Err((reason, detail)) = &results[1] else {
            panic!("{:?}", results[1]);
        };
        assert_eq!(reason, "relation \"public.missing\" does not exist");
        assert!(detail.contains("LINE 1:"), "{detail}");
        assert_eq!(results[2], Ok(" SELECT 1;".into()));
    }

    /// A connection error comes before the first marker; it is the
    /// reason for each query
    #[test]
    fn gives_the_session_error_to_each_query() {
        let stderr = "psql: error: connection to server on socket \
                      failed: FATAL:  role \"x\" does not exist\n";
        let results = parse(2, "", stderr);
        for result in results {
            let Err((reason, _)) = result else {
                panic!("{result:?}");
            };
            assert_eq!(
                reason,
                "psql: error: connection to server on socket failed: \
                 FATAL:  role \"x\" does not exist"
            );
        }
        assert_eq!(
            parse(1, "", ""),
            vec![Err((String::from("psql gave no result"), String::new()))]
        );
    }

    /// An error in the session setup aborts the transaction; it is the
    /// reason for each query, not the errors that come after it
    #[test]
    fn prefers_a_session_setup_error() {
        let stderr = "psql:d.sql:3: ERROR:  role \"x\" does not exist\n\
                      pglifecycle_deparse 0\n\
                      psql:d.sql:6: ERROR:  current transaction is \
                      aborted\n";
        let Err((reason, _)) = &parse(1, "", stderr)[0] else {
            panic!("expected an error");
        };
        assert_eq!(reason, "role \"x\" does not exist");
    }
}
