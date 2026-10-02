//! Procedures: a changed procedure is made again with `CREATE OR
//! REPLACE PROCEDURE`, as a function is. A changed list of input types
//! is a different procedure (see `diff::object_identity`), so it is
//! made and the old one is dropped.

use super::{Resolution, comment_delta};
use crate::deploy::diff::identity_type;
use crate::models::Procedure;
use crate::project::routine_base_name;
use crate::utils::{quote_ident, quote_routine_name};

pub(super) fn procedure(repo: &Procedure, db: &Procedure) -> Resolution {
    if !replaceable(repo, db) {
        return Resolution::Replace;
    }
    Resolution::OrReplace {
        comment: comment_delta(
            "PROCEDURE",
            &signature(repo),
            &repo.comment,
            &db.comment,
        ),
        then: Vec::new(),
    }
}

/// Whether `CREATE OR REPLACE PROCEDURE` can change `db` into `repo`.
/// PostgreSQL does not let it rename a parameter, change the output
/// parameters (`OUT` and `INOUT`), or remove a default. Thus each
/// parameter must keep its mode, its name and its type, and each
/// default that the database has must stay. PostgreSQL accepts some
/// other parameter changes, for example a name for a parameter that
/// had none. They are rare, so they also use the drop.
fn replaceable(repo: &Procedure, db: &Procedure) -> bool {
    let shape = |procedure: &Procedure| {
        procedure
            .parameters
            .iter()
            .flatten()
            .map(|p| {
                (p.mode.clone(), p.name.clone(), identity_type(&p.data_type))
            })
            .collect::<Vec<_>>()
    };
    let defaults = |procedure: &Procedure| {
        procedure
            .parameters
            .iter()
            .flatten()
            .map(|p| p.default.is_some())
            .collect::<Vec<_>>()
    };
    shape(repo) == shape(db)
        && defaults(repo)
            .into_iter()
            .zip(defaults(db))
            .all(|(repo, db)| repo || !db)
}

/// The qualified name and the input types, which is all that `COMMENT
/// ON PROCEDURE` reads. A name that has its argument list and no
/// `parameters` keeps its list, and a name that has the types of its
/// parameters at its end is the name without them.
fn signature(procedure: &Procedure) -> String {
    let parameters = procedure.parameters.as_deref().unwrap_or_default();
    let name = if parameters.is_empty() && procedure.name.contains('(') {
        quote_routine_name(&procedure.name)
    } else {
        let types: Vec<&str> = parameters
            .iter()
            .filter(|p| p.mode != "OUT")
            .map(|p| p.data_type.as_str())
            .collect();
        let name = routine_base_name(&procedure.name, &procedure.parameters);
        format!("{}({})", quote_ident(name), types.join(", "))
    };
    format!("{}.{name}", quote_ident(&procedure.schema))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn parse(value: serde_json::Value) -> Procedure {
        serde_json::from_value(value).expect("procedure deserializes")
    }

    fn archive(
        parameters: serde_json::Value,
        definition: &str,
        comment: Option<&str>,
    ) -> Procedure {
        let mut value = json!({
            "name": "archive_before",
            "schema": "test",
            "owner": "postgres",
            "parameters": parameters,
            "language": "plpgsql",
            "definition": definition,
        });
        if let Some(comment) = comment {
            value["comment"] = comment.into();
        }
        parse(value)
    }

    fn parameters() -> serde_json::Value {
        json!([
            {"mode": "IN", "name": "days", "data_type": "integer"},
            {"mode": "INOUT", "name": "archived", "data_type": "integer",
             "default": "0"},
        ])
    }

    #[test]
    fn body_change_uses_or_replace() {
        let repo = archive(parameters(), "BEGIN END;", Some("Archives"));
        let db = archive(parameters(), "BEGIN NULL; END;", Some("Archives"));
        assert!(matches!(
            procedure(&repo, &db),
            Resolution::OrReplace { comment: None, .. }
        ));
    }

    #[test]
    fn comment_change_names_the_input_types() {
        let repo = archive(parameters(), "BEGIN END;", None);
        let db = archive(parameters(), "BEGIN END;", Some("old"));
        let Resolution::OrReplace { comment, .. } = procedure(&repo, &db)
        else {
            panic!("expected OR REPLACE");
        };
        assert_eq!(
            comment.as_deref(),
            Some(
                "COMMENT ON PROCEDURE test.archive_before(integer, integer) \
                 IS NULL;\n"
            )
        );
    }

    #[test]
    fn quoted_names_are_quoted() {
        let quoted = |comment: &str| {
            parse(json!({
                "name": "Quoted Proc",
                "schema": "Quoted Schema",
                "owner": "postgres",
                "language": "sql",
                "definition": "SELECT 1;",
                "comment": comment,
            }))
        };
        let Resolution::OrReplace { comment, .. } =
            procedure(&quoted("new"), &quoted("old"))
        else {
            panic!("expected OR REPLACE");
        };
        assert_eq!(
            comment.as_deref(),
            Some(
                "COMMENT ON PROCEDURE \"Quoted Schema\".\"Quoted Proc\"() \
                 IS $$new$$;\n"
            )
        );
    }

    #[test]
    fn an_out_parameter_is_not_an_input_type() {
        let p = parse(json!({
            "name": "p", "schema": "test", "owner": "postgres",
            "parameters": [
                {"mode": "IN", "name": "a", "data_type": "integer"},
                {"mode": "OUT", "name": "b", "data_type": "text"},
            ],
            "language": "sql", "definition": "SELECT 'x'",
        }));
        assert_eq!(signature(&p), "test.p(integer)");
    }

    /// The argument types at the end of a name are not part of the
    /// name when they are the types of the parameters, as in the build
    #[test]
    fn a_name_with_the_parameter_types_is_the_name_without_them() {
        let p = |name: &str| {
            parse(json!({
                "name": name, "schema": "test", "owner": "postgres",
                "parameters": [
                    {"mode": "IN", "name": "a", "data_type": "integer"},
                ],
                "language": "sql", "definition": "SELECT 1",
            }))
        };
        assert_eq!(signature(&p("p(integer)")), "test.p(integer)");
        assert_eq!(signature(&p("p(x)")), "test.\"p(x)\"(integer)");
    }

    #[test]
    fn parameter_changes_that_postgres_refuses_replace() {
        let with = |parameters: serde_json::Value| {
            archive(parameters, "BEGIN END;", None)
        };
        let db = with(parameters());
        // a renamed parameter
        let renamed = with(json!([
            {"mode": "IN", "name": "age", "data_type": "integer"},
            {"mode": "INOUT", "name": "archived", "data_type": "integer",
             "default": "0"},
        ]));
        // an output parameter made an input one
        let input = with(json!([
            {"mode": "IN", "name": "days", "data_type": "integer"},
            {"mode": "IN", "name": "archived", "data_type": "integer",
             "default": "0"},
        ]));
        // a removed default
        let no_default = with(json!([
            {"mode": "IN", "name": "days", "data_type": "integer"},
            {"mode": "INOUT", "name": "archived", "data_type": "integer"},
        ]));
        // an added output parameter
        let added = with(json!([
            {"mode": "IN", "name": "days", "data_type": "integer"},
            {"mode": "INOUT", "name": "archived", "data_type": "integer",
             "default": "0"},
            {"mode": "OUT", "name": "total", "data_type": "bigint"},
        ]));
        for repo in [renamed, input, no_default, added] {
            assert!(matches!(procedure(&repo, &db), Resolution::Replace));
        }
    }

    #[test]
    fn parameter_changes_that_postgres_accepts_use_or_replace() {
        let db = archive(parameters(), "BEGIN END;", None);
        // a changed default, an added one, and a type written as an
        // alias with a typmod, which PostgreSQL does not keep
        let repo = archive(
            json!([
                {"mode": "IN", "name": "days", "data_type": "INT4",
                 "default": 30},
                {"mode": "INOUT", "name": "archived",
                 "data_type": "numeric(10)", "default": "1"},
            ]),
            "BEGIN END;",
            None,
        );
        let db = Procedure {
            parameters: db.parameters.map(|mut parameters| {
                parameters[1].data_type = String::from("numeric");
                parameters
            }),
            ..db
        };
        assert!(matches!(
            procedure(&repo, &db),
            Resolution::OrReplace { .. }
        ));
    }
}
