//! Schemas, domains, types, sequences, and comments

use tree_sitter::Node;

use crate::ddl::{
    NodeExt, QualifiedName, Statement, any_name, truncate, unquote,
};
use crate::models::{
    Domain, DomainConstraint, Schema, Sequence, Type, TypeColumn,
};

/// CREATE SCHEMA → Schema
pub(crate) fn create_schema(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .child_of_kind("ColId")
        .map(|n| unquote(n.text(src)))
        .or_else(|| node.find("OptSchemaName").map(|n| unquote(n.text(src))))
        .ok_or_else(|| String::from("CREATE SCHEMA without a name"))?;
    let authorization = node.find("RoleSpec").map(|n| unquote(n.text(src)));
    Ok(Statement::CreateSchema(Schema {
        name,
        owner: String::new(),
        authorization,
        comment: None,
    }))
}

/// CREATE DOMAIN → Domain
pub(crate) fn create_domain(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .child_of_kind("any_name")
        .map(|n| any_name(&n, src))
        .ok_or_else(|| String::from("CREATE DOMAIN without a name"))?;
    let mut domain = Domain {
        name: name.name,
        schema: name.schema.unwrap_or_default(),
        owner: String::new(),
        sql: None,
        data_type: node
            .child_of_kind("Typename")
            .map(|n| n.text(src).to_string()),
        collation: None,
        default: None,
        check_constraints: None,
        comment: None,
    };
    for constraint in node.find_all("ColConstraint") {
        let name = constraint
            .child_of_kind("name")
            .map(|n| unquote(n.text(src)));
        let Some(elem) = constraint.child_of_kind("ColConstraintElem") else {
            if constraint.has("kw_collate")
                && let Some(collation) = constraint.child_of_kind("any_name")
            {
                domain.collation = Some(collation.text(src).to_string());
            }
            continue;
        };
        if elem.has("kw_check") {
            if let Some(expr) = elem.find("a_expr") {
                domain.check_constraints.get_or_insert_default().push(
                    DomainConstraint {
                        name,
                        nullable: None,
                        expression: Some(expr.text(src).to_string()),
                    },
                );
            }
        } else if elem.has("kw_not") && elem.has("kw_null") {
            domain.check_constraints.get_or_insert_default().push(
                DomainConstraint {
                    name,
                    nullable: Some(false),
                    expression: None,
                },
            );
        } else if elem.has("kw_default")
            && let Some(expr) = elem.child_of_kind("b_expr")
        {
            domain.default = Some(expr.text(src).to_string());
        }
    }
    Ok(Statement::CreateDomain(domain))
}

/// CREATE TYPE (DefineStmt): enum, composite, range, and base forms
pub(crate) fn create_type(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .child_of_kind("any_name")
        .map(|n| any_name(&n, src))
        .ok_or_else(|| String::from("CREATE TYPE without a name"))?;
    let mut value = Type {
        name: name.name,
        schema: name.schema.unwrap_or_default(),
        owner: String::new(),
        sql: None,
        type_kind: None,
        input: None,
        output: None,
        receive: None,
        send: None,
        typmod_in: None,
        typmod_out: None,
        analyze: None,
        internal_length: None,
        passed_by_value: None,
        alignment: None,
        storage: None,
        like_type: None,
        category: None,
        preferred: None,
        default: None,
        element: None,
        delimiter: None,
        collatable: None,
        columns: None,
        enum_values: None,
        subtype: None,
        subtype_opclass: None,
        collation: None,
        canonical: None,
        subtype_diff: None,
        comment: None,
    };
    if node.has("kw_enum") {
        value.type_kind = Some(String::from("enum"));
        value.enum_values = Some(
            node.find_all("Sconst")
                .iter()
                .map(|n| string_value(n, src))
                .collect(),
        );
    } else if node.has("kw_range") {
        value.type_kind = Some(String::from("range"));
        for elem in node.find_all("def_elem") {
            let key = elem
                .child_of_kind("ColLabel")
                .map(|n| n.text(src).to_lowercase())
                .unwrap_or_default();
            let arg = elem
                .child_of_kind("def_arg")
                .map(|n| n.text(src).to_string())
                .unwrap_or_default();
            match key.as_str() {
                "subtype" => value.subtype = Some(arg),
                "subtype_opclass" => value.subtype_opclass = Some(arg),
                "collation" => value.collation = Some(arg),
                "canonical" => value.canonical = Some(arg),
                "subtype_diff" => value.subtype_diff = Some(arg),
                _ => {
                    log::warn!(
                        "Unsupported range type option {key:?} for {}",
                        value.name
                    );
                }
            }
        }
    } else if node.has("OptTableFuncElementList") {
        value.type_kind = Some(String::from("composite"));
        value.columns = Some(
            node.find_all("TableFuncElement")
                .iter()
                .map(|element| TypeColumn {
                    name: element
                        .child_of_kind("ColId")
                        .map(|n| unquote(n.text(src)))
                        .unwrap_or_default(),
                    data_type: element
                        .child_of_kind("Typename")
                        .map(|n| n.text(src).to_string())
                        .unwrap_or_default(),
                    collation: element
                        .find("opt_collate_clause")
                        .and_then(|n| n.child_of_kind("any_name"))
                        .map(|n| n.text(src).to_string()),
                })
                .collect(),
        );
    } else if node.has("definition") {
        value.type_kind = Some(String::from("base"));
        for elem in node.find_all("def_elem") {
            let key = elem
                .child_of_kind("ColLabel")
                .map(|n| n.text(src).to_lowercase())
                .unwrap_or_default();
            let arg = elem
                .child_of_kind("def_arg")
                .map(|n| n.text(src).to_string())
                .unwrap_or_default();
            match key.as_str() {
                "input" => value.input = Some(arg),
                "output" => value.output = Some(arg),
                "receive" => value.receive = Some(arg),
                "send" => value.send = Some(arg),
                "typmod_in" => value.typmod_in = Some(arg),
                "typmod_out" => value.typmod_out = Some(arg),
                "analyze" => value.analyze = Some(arg),
                "internallength" => {
                    value.internal_length =
                        Some(serde_json::Value::String(arg));
                }
                "passedbyvalue" => value.passed_by_value = Some(true),
                "alignment" => value.alignment = Some(arg),
                "storage" => value.storage = Some(arg),
                "like" => value.like_type = Some(arg),
                "category" => value.category = Some(unstring(&arg)),
                "preferred" => {
                    value.preferred = Some(serde_json::Value::String(arg));
                }
                "default" => {
                    value.default = Some(serde_json::Value::String(arg));
                }
                "element" => value.element = Some(arg),
                "delimiter" => value.delimiter = Some(unstring(&arg)),
                "collatable" => {
                    value.collatable = Some(
                        arg.is_empty() || arg.eq_ignore_ascii_case("true"),
                    );
                }
                _ => {
                    log::warn!(
                        "Unsupported base type option {key:?} for {}",
                        value.name
                    );
                }
            }
        }
    } else {
        // shell type: CREATE TYPE name;
        value.type_kind = None;
    }
    Ok(Statement::CreateType(Box::new(value)))
}

/// CREATE SEQUENCE → Sequence; ALTER SEQUENCE ... OWNED BY also maps
/// here so the assembly can merge it into the owning sequence
pub(crate) fn create_sequence(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .find("qualified_name")
        .ok_or_else(|| String::from("CREATE SEQUENCE without a name"))?;
    let name = crate::ddl::qualified_name(&name, src)?;
    let mut sequence = Sequence {
        name: name.name,
        schema: name.schema.unwrap_or_default(),
        owner: String::new(),
        sql: None,
        data_type: None,
        increment_by: None,
        min_value: None,
        max_value: None,
        start_with: None,
        cache: None,
        cycle: None,
        owned_by: None,
        comment: None,
    };
    apply_seq_options(&mut sequence, node, src);
    if node.kind() == "AlterSeqStmt" {
        return Ok(Statement::AlterSequence(sequence));
    }
    Ok(Statement::CreateSequence(sequence))
}

fn apply_seq_options(sequence: &mut Sequence, node: &Node, src: &str) {
    for elem in node.find_all("SeqOptElem") {
        let number = elem
            .find("NumericOnly")
            .and_then(|n| n.text(src).parse::<i64>().ok());
        if elem.has("kw_start") {
            sequence.start_with = number;
        } else if elem.has("kw_increment") {
            sequence.increment_by = number;
        } else if elem.has("kw_minvalue") {
            sequence.min_value = number;
        } else if elem.has("kw_maxvalue") {
            sequence.max_value = number;
        } else if elem.has("kw_cache") {
            sequence.cache = number;
        } else if elem.has("kw_cycle") {
            sequence.cycle = Some(!elem.has("kw_no"));
        } else if elem.has("kw_owned") {
            sequence.owned_by = elem
                .child_of_kind("any_name")
                .map(|n| n.text(src).to_string());
        } else if elem.has("kw_as") {
            sequence.data_type = elem
                .child_of_kind("SimpleTypename")
                .or_else(|| elem.find("SimpleTypename"))
                .map(|n| n.text(src).to_string());
        }
    }
}

/// COMMENT ON <type> <name> IS '...'
pub(crate) fn comment(node: &Node, src: &str) -> Result<Statement, String> {
    let text = node
        .child_of_kind("comment_text")
        .and_then(|n| n.find("Sconst"))
        .map(|n| string_value(&n, src))
        .ok_or_else(|| {
            format!("COMMENT without text: {}", truncate(node.text(src), 80))
        })?;
    // the object type is the keyword sequence between ON and the name
    let mut object_type = Vec::new();
    let mut target: Option<QualifiedName> = None;
    let mut cursor = node.walk();
    let mut past_on = false;
    for child in node.children(&mut cursor) {
        match child.kind() {
            "kw_on" => past_on = true,
            "kw_is" => break,
            kind if kind.starts_with("kw_") && past_on => {
                object_type
                    .push(kind.trim_start_matches("kw_").to_uppercase());
            }
            "any_name" => target = Some(any_name(&child, src)),
            "qualified_name" => {
                target = Some(crate::ddl::qualified_name(&child, src)?);
            }
            "Typename" => {
                let text = child.text(src);
                target = Some(split_dotted(text));
            }
            "name" | "ColId" => {
                target = Some(QualifiedName {
                    schema: None,
                    name: unquote(child.text(src)),
                });
            }
            _ => {}
        }
    }
    Ok(Statement::Comment {
        on: object_type.join(" "),
        target: target.unwrap_or_default(),
        comment: text,
    })
}

/// `a.b.c` → schema `a.b`, name `c` (COLUMN comments use three parts)
fn split_dotted(value: &str) -> QualifiedName {
    match value.rsplit_once('.') {
        Some((head, tail)) => QualifiedName {
            schema: Some(unquote(head)),
            name: unquote(tail),
        },
        None => QualifiedName {
            schema: None,
            name: unquote(value),
        },
    }
}

/// The value of a string constant node (single quotes or dollar
/// quoting stripped, escapes collapsed)
pub(crate) fn string_value(node: &Node, src: &str) -> String {
    let text = node.text(src);
    unstring(text)
}

pub(crate) fn unstring(text: &str) -> String {
    if text.len() >= 2 && text.starts_with('\'') && text.ends_with('\'') {
        return text[1..text.len() - 1].replace("''", "'");
    }
    if text.starts_with('$')
        && let Some(end) = text[1..].find('$')
    {
        let tag = &text[..end + 2];
        if text.len() >= tag.len() * 2 && text.ends_with(tag) {
            return text[tag.len()..text.len() - tag.len()].to_string();
        }
    }
    text.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::Parser;
    use crate::ddl::Statement;

    fn parse_one(sql: &str) -> Statement {
        let mut parser = Parser::new().unwrap();
        let mut statements = parser.parse(sql).unwrap();
        assert_eq!(statements.len(), 1, "expected one statement");
        statements.remove(0)
    }

    #[test]
    fn parses_create_schema() {
        let Statement::CreateSchema(schema) = parse_one("CREATE SCHEMA test;")
        else {
            panic!("expected CreateSchema")
        };
        assert_eq!(schema.name, "test");
    }

    #[test]
    fn parses_create_domain() {
        let Statement::CreateDomain(domain) = parse_one(
            "CREATE DOMAIN test.bcp47_locale AS text\n\
             \tCONSTRAINT bcp47_locale_check CHECK \
             ((VALUE ~ '^[a-z]{2}-[A-Z]{2,3}$'::text));",
        ) else {
            panic!("expected CreateDomain")
        };
        assert_eq!(domain.schema, "test");
        assert_eq!(domain.name, "bcp47_locale");
        assert_eq!(domain.data_type, Some("text".into()));
        let constraints = domain.check_constraints.unwrap();
        assert_eq!(constraints.len(), 1);
        assert_eq!(constraints[0].name, Some("bcp47_locale_check".into()));
        assert_eq!(
            constraints[0].expression,
            Some("(VALUE ~ '^[a-z]{2}-[A-Z]{2,3}$'::text)".into())
        );
    }

    #[test]
    fn parses_enum_type() {
        let Statement::CreateType(value) = parse_one(
            "CREATE TYPE test.user_state AS ENUM ('unverified', \
             'verified', 'suspended');",
        ) else {
            panic!("expected CreateType")
        };
        assert_eq!(value.type_kind, Some("enum".into()));
        assert_eq!(
            value.enum_values,
            Some(vec![
                "unverified".into(),
                "verified".into(),
                "suspended".into()
            ])
        );
    }

    #[test]
    fn parses_composite_type() {
        let Statement::CreateType(value) =
            parse_one("CREATE TYPE test.compfoo AS (f1 integer, f2 text);")
        else {
            panic!("expected CreateType")
        };
        assert_eq!(value.type_kind, Some("composite".into()));
        let columns = value.columns.unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "f1");
        assert_eq!(columns[0].data_type, "integer");
        assert_eq!(columns[1].data_type, "text");
    }

    #[test]
    fn parses_range_type() {
        let Statement::CreateType(value) = parse_one(
            "CREATE TYPE test.float8_range AS RANGE (subtype = float8, \
             subtype_diff = float8mi);",
        ) else {
            panic!("expected CreateType")
        };
        assert_eq!(value.type_kind, Some("range".into()));
        assert_eq!(value.subtype, Some("float8".into()));
        assert_eq!(value.subtype_diff, Some("float8mi".into()));
    }

    #[test]
    fn parses_create_sequence() {
        let Statement::CreateSequence(sequence) = parse_one(
            "CREATE SEQUENCE test.seq AS bigint START WITH 100 \
             INCREMENT BY 10 MAXVALUE 1000000 CACHE 2 NO CYCLE;",
        ) else {
            panic!("expected CreateSequence")
        };
        assert_eq!(sequence.schema, "test");
        assert_eq!(sequence.name, "seq");
        assert_eq!(sequence.data_type, Some("bigint".into()));
        assert_eq!(sequence.start_with, Some(100));
        assert_eq!(sequence.increment_by, Some(10));
        assert_eq!(sequence.max_value, Some(1000000));
        assert_eq!(sequence.cache, Some(2));
        assert_eq!(sequence.cycle, Some(false));
    }

    #[test]
    fn parses_alter_sequence_owned_by() {
        let Statement::AlterSequence(sequence) =
            parse_one("ALTER SEQUENCE test.seq OWNED BY test.empty_table.id;")
        else {
            panic!("expected AlterSequence")
        };
        assert_eq!(sequence.name, "seq");
        assert_eq!(sequence.owned_by, Some("test.empty_table.id".into()));
    }

    #[test]
    fn parses_comments() {
        let Statement::Comment {
            on,
            target,
            comment,
        } = parse_one(
            "COMMENT ON DOMAIN test.bcp47_locale IS 'Simplified locale \
             check, doesn''t conform';",
        )
        else {
            panic!("expected Comment")
        };
        assert_eq!(on, "DOMAIN");
        assert_eq!(target.to_string(), "test.bcp47_locale");
        assert_eq!(comment, "Simplified locale check, doesn't conform");
    }

    #[test]
    fn parses_column_comments() {
        let Statement::Comment { on, target, .. } =
            parse_one("COMMENT ON COLUMN test.users.id IS 'The user ID';")
        else {
            panic!("expected Comment")
        };
        assert_eq!(on, "COLUMN");
        assert_eq!(target.schema, Some("test.users".into()));
        assert_eq!(target.name, "id");
    }

    #[test]
    fn unstrings_dollar_quotes() {
        assert_eq!(unstring("$$body$$"), "body");
        assert_eq!(unstring("$_$ BEGIN END $_$"), " BEGIN END ");
        assert_eq!(unstring("'it''s'"), "it's");
    }
}
