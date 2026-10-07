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
        security_labels: None,
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
        security_labels: None,
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
                        not_valid: None,
                        comment: None,
                    },
                );
            }
        } else if elem.has("kw_not") && elem.has("kw_null") {
            domain.check_constraints.get_or_insert_default().push(
                DomainConstraint {
                    name,
                    nullable: Some(false),
                    expression: None,
                    not_valid: None,
                    comment: None,
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

/// ALTER DOMAIN ... ADD CONSTRAINT ... CHECK → the CHECK of the domain,
/// with its NOT VALID. The other forms of ALTER DOMAIN are not
/// supported.
pub(crate) fn alter_domain(node: &Node, src: &str) -> Statement {
    let check = node
        .child_of_kind("DomainConstraint")
        .filter(|_| node.has("kw_add"))
        .and_then(|constraint| {
            let elem = constraint.child_of_kind("DomainConstraintElem")?;
            if !elem.has("kw_check") {
                return None;
            }
            // NOT VALID is the only attribute that a domain CHECK can
            // have
            let not_valid = match elem.child_of_kind("ConstraintAttributeSpec")
            {
                None => None,
                Some(spec) => {
                    let elems = spec.find_all("ConstraintAttributeElem");
                    let only_not_valid = !elems.is_empty()
                        && elems
                            .iter()
                            .all(|e| e.has("kw_valid") && e.has("kw_not"));
                    if !only_not_valid {
                        return None;
                    }
                    Some(true)
                }
            };
            Some(DomainConstraint {
                name: constraint
                    .child_of_kind("name")
                    .map(|n| unquote(n.text(src))),
                nullable: None,
                expression: Some(
                    elem.child_of_kind("a_expr")?.text(src).to_string(),
                ),
                not_valid,
                comment: None,
            })
        });
    match (node.child_of_kind("any_name"), check) {
        (Some(name), Some(check)) => Statement::AddDomainCheck {
            domain: any_name(&name, src),
            check,
        },
        _ => Statement::Unsupported(node.kind().to_string()),
    }
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
        security_labels: None,
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
                    comment: None,
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
                // a number of bytes, or VARIABLE
                "internallength" => {
                    value.internal_length = Some(arg.parse::<i64>().map_or(
                        serde_json::Value::String(arg.to_uppercase()),
                        serde_json::Value::from,
                    ));
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
        security_labels: None,
    };
    apply_seq_options(&mut sequence, node, src);
    if node.kind() == "AlterSeqStmt" {
        return Ok(Statement::AlterSequence(sequence));
    }
    Ok(Statement::CreateSequence(sequence))
}

pub(crate) fn apply_seq_options(
    sequence: &mut Sequence,
    node: &Node,
    src: &str,
) {
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
    // a transform is named by its type and its language
    if node.child_of_kind("kw_transform").is_some() {
        let data_type = node
            .child_of_kind("Typename")
            .map(|n| n.text(src).to_string())
            .unwrap_or_default();
        let language = node
            .child_of_kind("name")
            .map(|n| unquote(n.text(src)))
            .unwrap_or_default();
        return Ok(Statement::Comment {
            on: String::from("TRANSFORM"),
            target: QualifiedName {
                schema: None,
                name: format!("FOR {data_type} LANGUAGE {language}"),
            },
            comment: text,
        });
    }
    let (on, target) = comment_target(node, src, "COMMENT")?;
    Ok(Statement::Comment {
        on,
        target,
        comment: text,
    })
}

/// SECURITY LABEL [FOR provider] ON <type> <name> IS '...'. pg_dump
/// always names the provider. The label is `None` for IS NULL.
pub(crate) fn security_label(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let provider = node
        .child_of_kind("opt_provider")
        .and_then(|n| n.child_of_kind("NonReservedWord_or_Sconst"))
        .map(|n| match n.find("Sconst") {
            Some(sconst) => string_value(&sconst, src),
            None => unquote(n.text(src)),
        })
        .ok_or_else(|| {
            format!(
                "SECURITY LABEL without provider: {}",
                truncate(node.text(src), 80)
            )
        })?;
    let label = node
        .child_of_kind("security_label")
        .and_then(|n| n.find("Sconst"))
        .map(|n| string_value(&n, src));
    let (on, target) = comment_target(node, src, "SECURITY LABEL")?;
    Ok(Statement::SecurityLabel {
        on,
        target,
        provider,
        label,
    })
}

/// The object type and the name of the object of a COMMENT or a
/// SECURITY LABEL statement (`verb`, for the warning when the name is
/// not found)
fn comment_target(
    node: &Node,
    src: &str,
    verb: &str,
) -> Result<(String, QualifiedName), String> {
    // the object type is the keyword sequence between ON and the name
    let mut object_type = Vec::new();
    let mut target: Option<QualifiedName> = None;
    // two-name forms (`COMMENT ON TRIGGER trg ON tbl`, `... RULE r ON
    // tbl`, `... POLICY p ON tbl`, `... CONSTRAINT c ON [DOMAIN] tbl`)
    // give the first name (trg/r/p/c) before the second, so `target` is
    // set from the leading `name`/`ColId` and this flag marks that it
    // still needs qualifying by the node that follows rather than being
    // overwritten by it
    let mut pending_first_name = false;
    let mut cursor = node.walk();
    let mut past_on = false;
    // once a target-bearing node has been seen, later `kw_*` children
    // (e.g. `DOMAIN` in `CONSTRAINT c ON DOMAIN d`) are part of the
    // target's own syntax, not the object-type keyword sequence
    let mut in_target = false;
    let mut using = false;
    for child in node.children(&mut cursor) {
        let kind = child.kind();
        match kind {
            "kw_on" => past_on = true,
            "kw_is" => break,
            _ if kind.starts_with("kw_") && past_on && !in_target => {
                object_type
                    .push(kind.trim_start_matches("kw_").to_uppercase());
            }
            // most object types nest their keywords (e.g.
            // `object_type_any_name (kw_table)`), including the
            // two-name forms (`object_type_name_on_any_name (kw_trigger
            // | kw_rule | kw_policy)`)
            _ if kind.starts_with("object_type") && past_on => {
                collect_keywords(&child, &mut object_type);
            }
            // a cast is named by its two types: `(source AS target)`
            "Typename" if past_on && object_type == ["CAST"] => {
                in_target = true;
                let text = child.text(src);
                target = Some(QualifiedName {
                    schema: None,
                    name: match target.take() {
                        Some(source) => format!("({} AS {text})", source.name),
                        None => text.to_string(),
                    },
                });
            }
            "any_name" | "qualified_name" | "Typename" if past_on => {
                in_target = true;
                let qualifier = match kind {
                    "qualified_name" => {
                        crate::ddl::qualified_name(&child, src)?
                    }
                    "Typename" => split_dotted(child.text(src)),
                    _ => any_name(&child, src),
                };
                target = Some(if pending_first_name {
                    QualifiedName {
                        schema: Some(qualifier.to_string()),
                        name: target.take().unwrap_or_default().name,
                    }
                } else {
                    qualifier
                });
                pending_first_name = false;
            }
            // the signature is the text between the parentheses, as
            // pg_dump writes it in the CREATE: `(integer ORDER BY
            // integer)`, or `(*)`
            "aggregate_with_argtypes" if past_on => {
                in_target = true;
                let mut name = child
                    .find("func_name")
                    .map(|n| any_name(&n, src))
                    .unwrap_or_default();
                let args = child
                    .child_of_kind("aggr_args")
                    .map(|n| n.text(src))
                    .unwrap_or("()");
                name.name = format!("{}{args}", name.name);
                target = Some(name);
                pending_first_name = false;
            }
            // `schema.op (left, right)`, named as the operator and its
            // argument types
            "operator_with_argtypes" if past_on => {
                in_target = true;
                let operator = child.child_of_kind("any_operator");
                let schema = operator
                    .and_then(|n| n.child_of_kind("ColId"))
                    .map(|n| unquote(n.text(src)));
                let symbol = operator
                    .and_then(|n| n.find("all_Op"))
                    .map(|n| n.text(src).to_string())
                    .unwrap_or_default();
                let args = child
                    .child_of_kind("oper_argtypes")
                    .map(|n| {
                        let mut cursor = n.walk();
                        n.children(&mut cursor)
                            .filter(|c| {
                                c.kind() == "Typename" || c.kind() == "kw_none"
                            })
                            .map(|c| c.text(src).to_string())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                target = Some(QualifiedName {
                    schema,
                    name: format!("{symbol}({})", args.join(", ")),
                });
                pending_first_name = false;
            }
            "function_with_argtypes" if past_on => {
                in_target = true;
                let mut name = child
                    .find("func_name")
                    .map(|n| any_name(&n, src))
                    .unwrap_or_default();
                let args: Vec<&str> = child
                    .find_all("func_arg")
                    .iter()
                    .map(|a| a.text(src))
                    .collect();
                name.name = format!("{}({})", name.name, args.join(", "));
                target = Some(name);
                pending_first_name = false;
            }
            // an operator class or family is named with its index
            // method: `name USING method`
            "kw_using" if in_target => using = true,
            "name" if using => {
                if let Some(target) = target.as_mut() {
                    target.name =
                        format!("{} USING {}", target.name, child.text(src));
                }
            }
            "name" | "ColId" if past_on => {
                in_target = true;
                target = Some(QualifiedName {
                    schema: None,
                    name: unquote(child.text(src)),
                });
                pending_first_name = true;
            }
            _ => {}
        }
    }
    if target.is_none() {
        log::warn!(
            "Unhandled {verb} ON {} target: {}",
            object_type.join(" "),
            truncate(node.text(src), 80)
        );
    }
    Ok((object_type.join(" "), target.unwrap_or_default()))
}

/// Collect all `kw_*` descendants, uppercased without the prefix
fn collect_keywords(node: &Node, into: &mut Vec<String>) {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind().starts_with("kw_") {
            into.push(child.kind().trim_start_matches("kw_").to_uppercase());
        } else {
            collect_keywords(&child, into);
        }
    }
}

/// `a.b.c` → schema `a.b`, name `c` (COLUMN comments use three parts).
/// A period in a quoted identifier does not separate the parts.
fn split_dotted(value: &str) -> QualifiedName {
    let mut quoted = false;
    let mut last = None;
    for (index, c) in value.char_indices() {
        match c {
            '"' => quoted = !quoted,
            '.' if !quoted => last = Some(index),
            _ => {}
        }
    }
    match last.map(|index| (&value[..index], &value[index + 1..])) {
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
    // pg_dumpall writes a string with a backslash as an escape string
    if text.len() >= 3
        && (text.starts_with("E'") || text.starts_with("e'"))
        && text.ends_with('\'')
    {
        return unescape(&text[2..text.len() - 1])
            .unwrap_or_else(|| text.to_string());
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

/// The value of the body of an escape string constant (`E'...'`), as
/// PostgreSQL reads it: `''` and `\'` are a quote; `\b`, `\f`, `\n`,
/// `\r` and `\t` are control characters; `\` and one to three octal
/// digits, or `\x` and one or two hexadecimal digits, are one byte;
/// `\u` and four, or `\U` and eight, hexadecimal digits are a Unicode
/// character, where a UTF-16 surrogate pair is one character; `\` and
/// any other character is that character. `None` for a body that
/// PostgreSQL refuses in a UTF8 database.
fn unescape(body: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(body.len());
    let mut chars = body.chars().peekable();
    // the high surrogate of a pair, until its low surrogate
    let mut high: Option<u32> = None;
    while let Some(c) = chars.next() {
        let mut buf = [0u8; 4];
        match c {
            '\'' => {
                // a quote in the body is written two times
                if chars.next() != Some('\'') {
                    return None;
                }
                bytes.push(b'\'');
            }
            '\\' => {
                let escape = chars.next()?;
                if high.is_some() && !matches!(escape, 'u' | 'U') {
                    return None;
                }
                match escape {
                    'b' => bytes.push(0x08),
                    'f' => bytes.push(0x0c),
                    'n' => bytes.push(b'\n'),
                    'r' => bytes.push(b'\r'),
                    't' => bytes.push(b'\t'),
                    '0'..='7' => {
                        let mut value = escape.to_digit(8)?;
                        for _ in 0..2 {
                            match chars.peek().and_then(|d| d.to_digit(8)) {
                                Some(digit) => {
                                    value = value * 8 + digit;
                                    chars.next();
                                }
                                None => break,
                            }
                        }
                        // PostgreSQL keeps the low byte of the value
                        bytes.push(value as u8);
                    }
                    'x' if chars
                        .peek()
                        .is_some_and(char::is_ascii_hexdigit) =>
                    {
                        let mut value = 0;
                        for _ in 0..2 {
                            match chars.peek().and_then(|d| d.to_digit(16)) {
                                Some(digit) => {
                                    value = value * 16 + digit;
                                    chars.next();
                                }
                                None => break,
                            }
                        }
                        bytes.push(value as u8);
                    }
                    'u' | 'U' => {
                        let digits = if escape == 'u' { 4 } else { 8 };
                        let mut value = 0u32;
                        for _ in 0..digits {
                            value = value * 16 + chars.next()?.to_digit(16)?;
                        }
                        let character = match (high.take(), value) {
                            (None, 0xd800..=0xdbff) => {
                                high = Some(value);
                                continue;
                            }
                            (Some(first), 0xdc00..=0xdfff) => char::from_u32(
                                0x10000
                                    + ((first - 0xd800) << 10)
                                    + (value - 0xdc00),
                            )?,
                            (None, _) => char::from_u32(value)?,
                            (Some(_), _) => return None,
                        };
                        bytes.extend_from_slice(
                            character.encode_utf8(&mut buf).as_bytes(),
                        );
                    }
                    other => bytes.extend_from_slice(
                        other.encode_utf8(&mut buf).as_bytes(),
                    ),
                }
            }
            _ if high.is_some() => return None,
            other => {
                bytes.extend_from_slice(other.encode_utf8(&mut buf).as_bytes())
            }
        }
    }
    if high.is_some() || bytes.contains(&0) {
        return None;
    }
    String::from_utf8(bytes).ok()
}

/// Parse a `reloptions` node (`key = value, ...` inside `WITH (...)`)
/// into a string map; values keep their raw text (dequoted if a
/// string literal) so they round-trip unchanged through
/// `utils::raw_value`
pub(crate) fn reloptions(
    node: &Node,
    src: &str,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let elems = node.find_all("reloption_elem");
    if elems.is_empty() {
        return None;
    }
    let mut map = serde_json::Map::new();
    for elem in elems {
        let labels = elem.find_all("ColLabel");
        let Some(first) = labels.first() else {
            continue;
        };
        let key = match labels.get(1) {
            Some(second) => {
                format!(
                    "{}.{}",
                    unquote(first.text(src)),
                    unquote(second.text(src))
                )
            }
            None => unquote(first.text(src)),
        };
        // A bare option (`WITH (security_barrier)`) has no def_arg;
        // PostgreSQL treats it as shorthand for `= true`.
        let value = match elem.child_of_kind("def_arg") {
            Some(value) => value
                .child_of_kind("Sconst")
                .map(|s| string_value(&s, src))
                .unwrap_or_else(|| value.text(src).to_string()),
            None => "true".to_string(),
        };
        map.insert(key, serde_json::Value::String(value));
    }
    (!map.is_empty()).then_some(map)
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
    fn comment_on_type_keeps_a_quoted_period() {
        let Statement::Comment { target, .. } =
            parse_one("COMMENT ON TYPE \"a.b\".\"c.d\" IS 'x';")
        else {
            panic!("expected Comment")
        };
        assert_eq!(target.schema.as_deref(), Some("a.b"));
        assert_eq!(target.name, "c.d");
    }

    #[test]
    fn names_operator_class_comments_with_their_method() {
        let Statement::Comment { on, target, .. } =
            parse_one("COMMENT ON OPERATOR FAMILY s.fam USING btree IS 'c';")
        else {
            panic!("expected Comment")
        };
        assert_eq!(on, "OPERATOR FAMILY");
        assert_eq!(target.schema.as_deref(), Some("s"));
        assert_eq!(target.name, "fam USING btree");
        let Statement::Comment { on, target, .. } =
            parse_one("COMMENT ON ACCESS METHOD heap_copy IS 'c';")
        else {
            panic!("expected Comment")
        };
        assert_eq!(on, "ACCESS METHOD");
        assert_eq!(target.name, "heap_copy");
    }

    #[test]
    fn parses_function_comment_with_signature() {
        let Statement::Comment {
            on,
            target,
            comment,
        } = parse_one("COMMENT ON FUNCTION test.fn(integer, text) IS 'x';")
        else {
            panic!("expected Comment")
        };
        assert_eq!(on, "FUNCTION");
        assert_eq!(target.schema.as_deref(), Some("test"));
        assert_eq!(target.name, "fn(integer, text)");
        assert_eq!(comment, "x");
    }

    #[test]
    fn unquoted_table_name_is_case_folded() {
        let Statement::CreateTable(table) =
            parse_one("CREATE TABLE MyTable (id int);")
        else {
            panic!("expected CreateTable")
        };
        assert_eq!(table.name, "mytable");
    }

    #[test]
    fn quoted_table_name_keeps_case() {
        let Statement::CreateTable(table) =
            parse_one("CREATE TABLE \"MyTable\" (id int);")
        else {
            panic!("expected CreateTable")
        };
        assert_eq!(table.name, "MyTable");
    }

    #[test]
    fn parses_trigger_comment_preserves_both_names() {
        let Statement::Comment {
            on,
            target,
            comment,
        } = parse_one("COMMENT ON TRIGGER trg ON tbl IS 'x';")
        else {
            panic!("expected Comment")
        };
        assert_eq!(on, "TRIGGER");
        assert_eq!(target.schema.as_deref(), Some("tbl"));
        assert_eq!(target.name, "trg");
        assert_eq!(comment, "x");
    }

    #[test]
    fn parses_policy_comment_preserves_both_names() {
        let Statement::Comment { on, target, .. } =
            parse_one("COMMENT ON POLICY pol ON tbl IS 'x';")
        else {
            panic!("expected Comment")
        };
        assert_eq!(on, "POLICY");
        assert_eq!(target.schema.as_deref(), Some("tbl"));
        assert_eq!(target.name, "pol");
    }

    #[test]
    fn parses_unhandled_comment_target_without_dropping_statement() {
        // LARGE OBJECT comments have no name/qualified-name/Typename
        // node for their numeric id, so the target falls through
        // unhandled; this should log a warning (see `object.rs`) but
        // still return a Comment statement with an empty target rather
        // than erroring or panicking
        let Statement::Comment { on, target, .. } =
            parse_one("COMMENT ON LARGE OBJECT 12345 IS 'x';")
        else {
            panic!("expected Comment")
        };
        assert_eq!(on, "LARGE OBJECT");
        assert_eq!(target, QualifiedName::default());
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

    /// pg_dump writes a domain NOT NULL with no name, or with
    /// `CONSTRAINT name` when the name is not `<domain>_not_null`
    #[test]
    fn parses_domain_not_null() {
        for (sql, name) in [
            ("CREATE DOMAIN test.d AS integer NOT NULL;", None),
            (
                "CREATE DOMAIN test.d AS integer CONSTRAINT nn NOT NULL;",
                Some("nn".to_string()),
            ),
        ] {
            let Statement::CreateDomain(domain) = parse_one(sql) else {
                panic!("expected CreateDomain")
            };
            assert_eq!(
                domain.check_constraints,
                Some(vec![DomainConstraint {
                    name,
                    nullable: Some(false),
                    expression: None,
                    not_valid: None,
                    comment: None,
                }])
            );
        }
    }

    /// pg_dump writes a domain CHECK that calls a function that needs
    /// the domain, and a NOT VALID one, as ALTER DOMAIN ... ADD
    #[test]
    fn parses_alter_domain_add_check() {
        let Statement::AddDomainCheck { domain, check } = parse_one(
            "ALTER DOMAIN s.a_dom\n    ADD CONSTRAINT \"A check\" \
             CHECK (s.z_ok((VALUE)::s.a_dom));",
        ) else {
            panic!("expected AddDomainCheck")
        };
        assert_eq!(domain.to_string(), "s.a_dom");
        assert_eq!(
            check,
            DomainConstraint {
                name: Some("A check".into()),
                nullable: None,
                expression: Some("s.z_ok((VALUE)::s.a_dom)".into()),
                not_valid: None,
                comment: None,
            }
        );
        let Statement::AddDomainCheck { check, .. } = parse_one(
            "ALTER DOMAIN s.d\n    ADD CONSTRAINT c CHECK (VALUE > 0) \
             NOT VALID;",
        ) else {
            panic!("expected AddDomainCheck")
        };
        assert_eq!(
            check,
            DomainConstraint {
                name: Some("c".into()),
                nullable: None,
                expression: Some("VALUE > 0".into()),
                not_valid: Some(true),
                comment: None,
            }
        );
        for sql in [
            "ALTER DOMAIN s.d ADD CONSTRAINT c NOT NULL;",
            "ALTER DOMAIN s.d DROP CONSTRAINT c;",
        ] {
            assert!(
                matches!(parse_one(sql), Statement::Unsupported(_)),
                "{sql}"
            );
        }
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
    fn parses_base_type_internal_length_as_number_or_variable() {
        for (length, expected) in [
            ("4", serde_json::json!(4)),
            ("variable", serde_json::json!("VARIABLE")),
        ] {
            let Statement::CreateType(value) = parse_one(&format!(
                "CREATE TYPE test.t (INTERNALLENGTH = {length}, \
                 INPUT = test.t_in, OUTPUT = test.t_out);"
            )) else {
                panic!("expected CreateType")
            };
            assert_eq!(value.type_kind, Some("base".into()));
            assert_eq!(value.internal_length, Some(expected));
        }
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
    fn parses_security_labels() {
        let Statement::SecurityLabel {
            on,
            target,
            provider,
            label,
        } = parse_one(
            "SECURITY LABEL FOR \"My Provider\" ON COLUMN test.users.id \
             IS 'it''s';",
        )
        else {
            panic!("expected SecurityLabel")
        };
        assert_eq!(on, "COLUMN");
        assert_eq!(target.schema, Some("test.users".into()));
        assert_eq!(target.name, "id");
        assert_eq!(provider, "My Provider");
        assert_eq!(label.as_deref(), Some("it's"));
        let Statement::SecurityLabel {
            on, target, label, ..
        } = parse_one(
            "SECURITY LABEL FOR selinux ON FUNCTION test.f(integer) IS NULL;",
        )
        else {
            panic!("expected SecurityLabel")
        };
        assert_eq!(on, "FUNCTION");
        assert_eq!(target.schema, Some("test".into()));
        assert_eq!(target.name, "f(integer)");
        assert_eq!(label, None);
        let Statement::SecurityLabel { on, target, .. } =
            parse_one("SECURITY LABEL FOR p ON ROLE app IS 'x';")
        else {
            panic!("expected SecurityLabel")
        };
        assert_eq!(on, "ROLE");
        assert_eq!(target.name, "app");
        // pg_dump always names the provider
        assert!(
            crate::ddl::Parser::new()
                .unwrap()
                .parse("SECURITY LABEL ON TABLE t IS 'x';")
                .is_err()
        );
    }

    #[test]
    fn unstrings_dollar_quotes() {
        assert_eq!(unstring("$$body$$"), "body");
        assert_eq!(unstring("$_$ BEGIN END $_$"), " BEGIN END ");
        assert_eq!(unstring("'it''s'"), "it's");
    }

    /// pg_dumpall writes a string with a backslash as an escape string
    /// constant, with each backslash written two times
    #[test]
    fn unstrings_escape_strings() {
        assert_eq!(unstring(r"E'rôle C:\\x ''q'' ü'"), r"rôle C:\x 'q' ü");
        assert_eq!(unstring(r"e'it\'s'"), "it's");
        assert_eq!(unstring(r"E'\b\f\n\r\t'"), "\u{8}\u{c}\n\r\t");
        assert_eq!(unstring(r"E'\q\é'"), "qé");
        assert_eq!(unstring("E''"), "");
    }

    /// An octal escape has one to three digits and a hexadecimal escape
    /// one or two; each gives one byte, and the bytes are UTF-8
    #[test]
    fn unstrings_octal_and_hex_escapes() {
        assert_eq!(unstring(r"E'\101\7\0101'"), "A\u{7}\u{8}1");
        assert_eq!(unstring(r"E'\x41\x7e\x4'"), "A~\u{4}");
        assert_eq!(unstring(r"E'\xC3\xA9t\303\251'"), "été");
        // with no hexadecimal digit, \x is an x
        assert_eq!(unstring(r"E'\xg'"), "xg");
    }

    #[test]
    fn unstrings_unicode_escapes() {
        assert_eq!(unstring(r"E'\u00e9\U0001F600'"), "é😀");
        // a UTF-16 surrogate pair is one character
        assert_eq!(unstring(r"E'\ud83d\ude00!'"), "😀!");
    }

    /// An escape string that PostgreSQL refuses stays as written, so
    /// that no text is lost
    #[test]
    fn keeps_an_invalid_escape_string() {
        for text in [
            r"E'\xff'",
            r"E'\u12'",
            r"E'\ud83d'",
            r"E'\ude00'",
            r"E'\U00110000'",
            r"E'\0'",
            r"E'a\'",
        ] {
            assert_eq!(unstring(text), text);
        }
    }

    #[test]
    fn parses_operator_comment_target() {
        let Statement::Comment { target, .. } =
            parse_one("COMMENT ON OPERATOR s.=== (integer, integer) IS 'eq';")
        else {
            panic!("expected Comment")
        };
        assert_eq!(target.schema.as_deref(), Some("s"));
        assert_eq!(target.name, "===(integer, integer)");
    }
}
