//! Extended statistics. `ALTER STATISTICS ... SET STATISTICS` changes
//! the target in place, and the comment and the owner (see
//! `deploy::owner_sql`) also change in place. PostgreSQL has no ALTER
//! for the table, the kinds, the columns or the expressions, so a change
//! to one of them drops and makes the statistics again.

use super::{Alter, Resolution, push_comment, qualified};
use crate::models::{Statistics, canonical_relation};
use crate::utils::strip_outer_parens;

/// The kinds in the order that PostgreSQL writes them. Statistics with
/// all of them write none.
const KINDS: [&str; 3] = ["ndistinct", "dependencies", "mcv"];

pub(super) fn statistics(repo: &Statistics, db: &Statistics) -> Resolution {
    let wanted = canonical(repo);
    let existing = canonical(db);
    if wanted.table != existing.table
        || wanted.kinds != existing.kinds
        || wanted.elements != existing.elements
    {
        return Resolution::Replace;
    }
    let name = qualified(&repo.schema, &repo.name);
    let mut alters = Vec::new();
    if wanted.target != existing.target {
        // -1 sets the default target again
        alters.push(Alter::new(format!(
            "ALTER STATISTICS {name} SET STATISTICS {};\n",
            wanted.target.unwrap_or(-1)
        )));
    }
    push_comment(&mut alters, "STATISTICS", &name, &repo.comment, &db.comment);
    Resolution::Statements(alters)
}

/// The statistics in the form that deploy compares. The table name is
/// as PostgreSQL resolves it. The kinds are a set in the order of
/// [`KINDS`], and all of them are the same as none. PostgreSQL keeps
/// the columns in the order of the table, and writes them before the
/// expressions, so the columns are in name order before the
/// expressions (see [`canonical_elements`]). A target of -1 is the
/// default, the same as none.
pub(crate) fn canonical(statistics: &Statistics) -> Statistics {
    let mut kinds: Vec<String> = statistics
        .kinds
        .iter()
        .flatten()
        .map(|kind| kind.trim().to_lowercase())
        .collect();
    let position = |kind: &String| {
        KINDS.iter().position(|k| k == kind).unwrap_or(KINDS.len())
    };
    kinds.sort_by(|a, b| position(a).cmp(&position(b)).then(a.cmp(b)));
    kinds.dedup();
    let all = kinds.iter().map(String::as_str).eq(KINDS);
    Statistics {
        table: canonical_relation(&statistics.table),
        kinds: (!kinds.is_empty() && !all).then_some(kinds),
        elements: canonical_elements(&statistics.elements),
        target: statistics.target.filter(|target| *target != -1),
        ..statistics.clone()
    }
}

/// The columns, in name order, and then the expressions, in their own
/// order. An element that is a name in parentheses is a column, as
/// PostgreSQL makes it one. A column name is as PostgreSQL resolves it.
/// An expression loses the parentheses that enclose all of it, and is
/// compared with no spaces and in lowercase, other than in quotes. Other
/// than that, it is compared as text: PostgreSQL writes an expression
/// back in its own form (for example with casts), so write it as `pull`
/// does.
fn canonical_elements(elements: &[String]) -> Vec<String> {
    let mut columns = Vec::new();
    let mut expressions = Vec::new();
    for element in elements {
        let element = strip_outer_parens(element);
        if is_name(element) {
            columns.push(canonical_relation(element));
        } else {
            expressions.push(canonical_expression(element));
        }
    }
    columns.sort();
    columns.extend(expressions);
    columns
}

/// Whether the text is one name, quoted or not
fn is_name(text: &str) -> bool {
    if let Some(inner) = text
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
    {
        return !inner.is_empty() && !inner.replace("\"\"", "").contains('"');
    }
    let mut chars = text.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

/// The expression with no spaces and in lowercase, other than in quoted
/// strings and names
fn canonical_expression(expression: &str) -> String {
    let mut quote: Option<char> = None;
    let mut result = String::new();
    for c in expression.chars() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
                result.push(c);
            }
            None if c == '\'' || c == '"' => {
                quote = Some(c);
                result.push(c);
            }
            None if c.is_whitespace() => {}
            None => result.push(c.to_ascii_lowercase()),
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn parse(value: serde_json::Value) -> Statistics {
        serde_json::from_value(value).expect("statistics deserialize")
    }

    fn pulled(target: Option<i64>, comment: Option<&str>) -> Statistics {
        let mut value = json!({
            "name": "Stats",
            "schema": "My Schema",
            "owner": "postgres",
            "table": "test.measurements",
            "kinds": ["ndistinct", "mcv"],
            "elements": ["a", "\"Id\"", "(a + b)", "lower(label)"],
        });
        if let Some(target) = target {
            value["target"] = target.into();
        }
        if let Some(comment) = comment {
            value["comment"] = comment.into();
        }
        parse(value)
    }

    fn sql(resolution: Resolution) -> Vec<String> {
        match resolution {
            Resolution::Statements(alters) => {
                alters.into_iter().map(|a| a.sql).collect()
            }
            _ => panic!("expected in-place statements"),
        }
    }

    #[test]
    fn short_forms_are_the_same_statistics() {
        let written = parse(json!({
            "name": "Stats",
            "schema": "My Schema",
            "owner": "postgres",
            "table": "TEST.\"measurements\"",
            "kinds": ["MCV", "ndistinct"],
            "elements": ["(\"Id\")", "(a+b)", "A", "(LOWER( label ))"],
            "target": -1,
        }));
        assert_eq!(canonical(&written), canonical(&pulled(None, None)));
        assert!(sql(statistics(&written, &pulled(None, None))).is_empty());
    }

    #[test]
    fn all_kinds_are_the_same_as_none() {
        let with = |kinds: Option<Vec<&str>>| {
            canonical(&Statistics {
                kinds: kinds.map(|kinds| {
                    kinds.into_iter().map(String::from).collect()
                }),
                ..pulled(None, None)
            })
        };
        assert_eq!(with(Some(vec!["mcv", "dependencies", "ndistinct"])), {
            with(None)
        });
        assert_ne!(with(Some(vec!["mcv", "dependencies"])), with(None));
    }

    #[test]
    fn expressions_keep_their_order_and_quoted_text() {
        assert_eq!(
            canonical_elements(&[
                String::from("(b || ' X')"),
                String::from("b"),
                String::from("(\"A B\" + 1)"),
                String::from("a"),
            ]),
            ["a", "b", "b||' X'", "\"A B\"+1"]
        );
        assert_ne!(
            canonical_elements(&[
                String::from("(a + b)"),
                String::from("(a - b)")
            ]),
            canonical_elements(&[
                String::from("(a - b)"),
                String::from("(a + b)")
            ])
        );
    }

    #[test]
    fn targets_and_comments_change_in_place() {
        assert_eq!(
            sql(statistics(
                &pulled(Some(500), Some("new")),
                &pulled(Some(50), None)
            )),
            [
                "ALTER STATISTICS \"My Schema\".\"Stats\" SET STATISTICS \
                 500;\n",
                "COMMENT ON STATISTICS \"My Schema\".\"Stats\" IS $$new$$;\n",
            ]
        );
        assert_eq!(
            sql(statistics(&pulled(None, None), &pulled(Some(10), None))),
            ["ALTER STATISTICS \"My Schema\".\"Stats\" SET STATISTICS -1;\n"]
        );
    }

    #[test]
    fn other_changes_replace() {
        let db = pulled(None, None);
        for repo in [
            Statistics {
                table: String::from("test.other"),
                ..db.clone()
            },
            Statistics {
                kinds: None,
                ..db.clone()
            },
            Statistics {
                elements: vec![String::from("a"), String::from("b")],
                ..db.clone()
            },
        ] {
            assert!(matches!(statistics(&repo, &db), Resolution::Replace));
        }
    }
}
