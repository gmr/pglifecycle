//! Aggregates, casts, collations, conversions, event triggers,
//! publications, and text search objects

use std::collections::BTreeMap;

use serde_json::{Map, Value};
use tree_sitter::Node;

use crate::ddl::object::string_value;
use crate::ddl::{NodeExt, Statement, any_name, unquote};
use crate::models::{
    Aggregate, Argument, Cast, Collation, Conversion, EventTrigger,
    EventTriggerFilter, FilteredPublicationTable, Language, Operator,
    Publication, PublicationTable, Rule, Statistics, TextSearchConfig,
    TextSearchDict, TextSearchParser, TextSearchTemplate,
};

/// A text search object and the schema it belongs to
#[derive(Clone, Debug, PartialEq)]
pub enum TextSearchObject {
    Configuration(TextSearchConfig),
    Dictionary(TextSearchDict),
    Parser(TextSearchParser),
    Template(TextSearchTemplate),
}

/// The `name = value` pairs of a `definition` node, keys in lower
/// case. A string constant yields its value, anything else its text,
/// and a bare key (`HYPOTHETICAL`) no value.
fn definition(node: &Node, src: &str) -> Vec<(String, Option<String>)> {
    node.find_all("def_elem")
        .iter()
        .map(|elem| {
            let key = elem
                .child_of_kind("ColLabel")
                .map(|n| n.text(src).to_lowercase())
                .unwrap_or_default();
            let value = elem.child_of_kind("def_arg").map(|arg| {
                match arg.child_of_kind("Sconst") {
                    Some(string) => string_value(&string, src),
                    None => arg.text(src).to_string(),
                }
            });
            (key, value)
        })
        .collect()
}

/// The schema and name of a possibly qualified `any_name`, with the
/// schema required
fn schema_and_name(
    node: &Node,
    src: &str,
    what: &str,
) -> Result<(String, String), String> {
    let name = node
        .child_of_kind("any_name")
        .map(|n| any_name(&n, src))
        .ok_or_else(|| format!("{what} without a name"))?;
    Ok((name.schema.unwrap_or_default(), name.name))
}

/// DefineStmt for an aggregate, collation or text search object, or
/// `None` for another kind (a type)
pub(crate) fn define(
    node: &Node,
    src: &str,
) -> Option<Result<Statement, String>> {
    if node.child_of_kind("kw_aggregate").is_some() {
        Some(create_aggregate(node, src))
    } else if node.child_of_kind("kw_collation").is_some() {
        Some(create_collation(node, src))
    } else if node.child_of_kind("kw_search").is_some() {
        Some(create_text_search(node, src))
    } else if node.child_of_kind("kw_operator").is_some() {
        Some(create_operator(node, src))
    } else {
        None
    }
}

fn arguments(list: &Node, src: &str) -> Vec<Argument> {
    list.find_all("func_arg")
        .iter()
        .map(|arg| Argument {
            data_type: arg
                .find("func_type")
                .map(|n| n.text(src).to_string())
                .unwrap_or_default(),
            // IN is the only mode an aggregate argument can have besides
            // VARIADIC, and build writes IN when none is given
            mode: arg
                .find("arg_class")
                .map(|n| n.text(src).to_uppercase())
                .filter(|mode| mode != "IN"),
            name: arg.find("param_name").map(|n| unquote(n.text(src))),
        })
        .collect()
}

fn create_aggregate(node: &Node, src: &str) -> Result<Statement, String> {
    let name = node
        .child_of_kind("func_name")
        .map(|n| any_name(&n, src))
        .ok_or_else(|| String::from("CREATE AGGREGATE without a name"))?;
    let args = node.child_of_kind("aggr_args").ok_or_else(|| {
        String::from("CREATE AGGREGATE without an argument list")
    })?;
    // `(direct ORDER BY aggregated)`: the lists either side of ORDER BY
    let mut direct = Vec::new();
    let mut order_by = None;
    let mut cursor = args.walk();
    let mut past_order = false;
    for child in args.children(&mut cursor) {
        match child.kind() {
            "kw_order" => {
                past_order = true;
                order_by.get_or_insert_with(Vec::new);
            }
            "aggr_args_list" if past_order => {
                order_by = Some(arguments(&child, src));
            }
            "aggr_args_list" => direct = arguments(&child, src),
            _ => {}
        }
    }
    let mut aggregate = Aggregate {
        name: name.name,
        schema: name.schema.unwrap_or_default(),
        owner: String::new(),
        arguments: direct,
        order_by,
        sfunc: String::new(),
        state_data_type: String::new(),
        state_data_size: None,
        ffunc: None,
        finalfunc_extra: None,
        finalfunc_modify: None,
        combinefunc: None,
        serialfunc: None,
        deserialfunc: None,
        initial_condition: None,
        msfunc: None,
        minvfunc: None,
        mstate_data_type: None,
        mstate_data_size: None,
        mffunc: None,
        mfinalfunc_extra: None,
        mfinalfunc_modify: None,
        minitial_condition: None,
        sort_operator: None,
        parallel: None,
        hypothetical: None,
        sql: None,
        comment: None,
    };
    let definition = node
        .child_of_kind("definition")
        .map(|n| definition(&n, src))
        .unwrap_or_default();
    for (key, value) in definition {
        let size = |value: &Option<String>| {
            value.as_deref().and_then(|v| v.parse::<i64>().ok())
        };
        match key.as_str() {
            "sfunc" => aggregate.sfunc = value.unwrap_or_default(),
            "stype" => aggregate.state_data_type = value.unwrap_or_default(),
            "sspace" => aggregate.state_data_size = size(&value),
            "finalfunc" => aggregate.ffunc = value,
            "finalfunc_extra" => aggregate.finalfunc_extra = Some(true),
            "finalfunc_modify" => {
                aggregate.finalfunc_modify = value.map(|v| v.to_uppercase());
            }
            "combinefunc" => aggregate.combinefunc = value,
            "serialfunc" => aggregate.serialfunc = value,
            "deserialfunc" => aggregate.deserialfunc = value,
            "initcond" => aggregate.initial_condition = value,
            "msfunc" => aggregate.msfunc = value,
            "minvfunc" => aggregate.minvfunc = value,
            "mstype" => aggregate.mstate_data_type = value,
            "msspace" => aggregate.mstate_data_size = size(&value),
            "mfinalfunc" => aggregate.mffunc = value,
            "mfinalfunc_extra" => aggregate.mfinalfunc_extra = Some(true),
            "mfinalfunc_modify" => {
                aggregate.mfinalfunc_modify = value.map(|v| v.to_uppercase());
            }
            "minitcond" => aggregate.minitial_condition = value,
            "sortop" => aggregate.sort_operator = value,
            "parallel" => {
                aggregate.parallel = value.map(|v| v.to_uppercase());
            }
            "hypothetical" => aggregate.hypothetical = Some(true),
            other => {
                return Ok(Statement::Unsupported(format!(
                    "CREATE AGGREGATE option {other}"
                )));
            }
        }
    }
    Ok(Statement::CreateAggregate(Box::new(aggregate)))
}

/// CREATE OPERATOR → Operator
fn create_operator(node: &Node, src: &str) -> Result<Statement, String> {
    // the name is `schema.op`: the grammar gives the schema as a ColId
    // and the operator as an all_Op beneath any_operator
    let name = node
        .child_of_kind("any_operator")
        .ok_or_else(|| String::from("CREATE OPERATOR without a name"))?;
    let schema = name
        .child_of_kind("ColId")
        .map(|n| unquote(n.text(src)))
        .unwrap_or_default();
    let operator = name
        .find("all_Op")
        .map(|n| n.text(src).to_string())
        .ok_or_else(|| String::from("CREATE OPERATOR without an operator"))?;
    let mut value = Operator {
        name: operator,
        schema,
        owner: String::new(),
        function: String::new(),
        left_arg: None,
        right_arg: None,
        commutator: None,
        negator: None,
        restrict: None,
        join: None,
        hashes: None,
        merges: None,
        sql: None,
        comment: None,
    };
    let definition = node
        .child_of_kind("definition")
        .map(|n| definition(&n, src))
        .unwrap_or_default();
    for (key, arg) in definition {
        match key.as_str() {
            "function" | "procedure" => {
                value.function = arg.unwrap_or_default()
            }
            "leftarg" => value.left_arg = arg,
            "rightarg" => value.right_arg = arg,
            "commutator" => value.commutator = arg,
            "negator" => value.negator = arg,
            "restrict" => value.restrict = arg,
            "join" => value.join = arg,
            "hashes" => value.hashes = Some(true),
            "merges" => value.merges = Some(true),
            other => {
                return Ok(Statement::Unsupported(format!(
                    "CREATE OPERATOR option {other}"
                )));
            }
        }
    }
    Ok(Statement::CreateOperator(Box::new(value)))
}

fn create_collation(node: &Node, src: &str) -> Result<Statement, String> {
    let (schema, name) = schema_and_name(node, src, "CREATE COLLATION")?;
    let mut collation = Collation {
        name,
        schema,
        owner: String::new(),
        sql: None,
        locale: None,
        lc_collate: None,
        lc_ctype: None,
        provider: None,
        deterministic: None,
        version: None,
        rules: None,
        copy_from: None,
        comment: None,
    };
    if node.child_of_kind("kw_from").is_some() {
        // the name to copy is the second any_name
        collation.copy_from = node
            .find_all("any_name")
            .get(1)
            .map(|n| n.text(src).to_string());
    }
    let definition = node
        .child_of_kind("definition")
        .map(|n| definition(&n, src))
        .unwrap_or_default();
    for (key, value) in definition {
        match key.as_str() {
            "locale" => collation.locale = value,
            "lc_collate" => collation.lc_collate = value,
            "lc_ctype" => collation.lc_ctype = value,
            "provider" => collation.provider = value.map(|v| v.to_lowercase()),
            // true is the default, and pg_dump writes only false
            "deterministic" => {
                collation.deterministic = value
                    .filter(|v| v.eq_ignore_ascii_case("false"))
                    .map(|_| false);
            }
            "version" => collation.version = value,
            "rules" => collation.rules = value,
            other => {
                return Ok(Statement::Unsupported(format!(
                    "CREATE COLLATION option {other}"
                )));
            }
        }
    }
    Ok(Statement::CreateCollation(collation))
}

fn create_text_search(node: &Node, src: &str) -> Result<Statement, String> {
    let (schema, name) = schema_and_name(node, src, "CREATE TEXT SEARCH")?;
    let definition = node
        .child_of_kind("definition")
        .map(|n| definition(&n, src))
        .unwrap_or_default();
    let mut values: BTreeMap<String, String> = BTreeMap::new();
    let mut options = Map::new();
    for (key, value) in definition {
        values.insert(key.clone(), value.clone().unwrap_or_default());
        options.insert(key, Value::String(value.unwrap_or_default()));
    }
    let take = |key: &str| values.get(key).cloned();
    let object = if node.child_of_kind("kw_configuration").is_some() {
        TextSearchObject::Configuration(TextSearchConfig {
            name,
            sql: None,
            parser: take("parser"),
            source: take("copy"),
            mappings: None,
            comment: None,
        })
    } else if node.child_of_kind("kw_dictionary").is_some() {
        options.remove("template");
        TextSearchObject::Dictionary(TextSearchDict {
            name,
            sql: None,
            template: take("template"),
            options: (!options.is_empty()).then_some(options),
            comment: None,
        })
    } else if node.child_of_kind("kw_parser").is_some() {
        TextSearchObject::Parser(TextSearchParser {
            name,
            sql: None,
            start_function: take("start"),
            gettoken_function: take("gettoken"),
            end_function: take("end"),
            lextypes_function: take("lextypes"),
            headline_function: take("headline"),
            comment: None,
        })
    } else if node.child_of_kind("kw_template").is_some() {
        TextSearchObject::Template(TextSearchTemplate {
            name,
            sql: None,
            lexize_function: take("lexize"),
            init_function: take("init"),
            comment: None,
        })
    } else {
        return Ok(Statement::Unsupported(String::from(
            "CREATE TEXT SEARCH object of an unknown kind",
        )));
    };
    Ok(Statement::CreateTextSearch { schema, object })
}

/// ALTER TEXT SEARCH CONFIGURATION ... ADD MAPPING FOR ... WITH ...,
/// the only form pg_dump writes
pub(crate) fn alter_text_search_configuration(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    if node.child_of_kind("kw_add").is_none()
        || node.child_of_kind("kw_mapping").is_none()
    {
        return Ok(Statement::Unsupported(String::from(
            "ALTER TEXT SEARCH CONFIGURATION other than ADD MAPPING",
        )));
    }
    let configuration = node
        .child_of_kind("any_name")
        .map(|n| any_name(&n, src))
        .ok_or_else(|| {
            String::from("ALTER TEXT SEARCH CONFIGURATION without a name")
        })?;
    let tokens = node
        .child_of_kind("name_list")
        .map(|list| {
            list.find_all("name")
                .iter()
                .map(|n| unquote(n.text(src)))
                .collect()
        })
        .unwrap_or_default();
    let dictionaries = node
        .child_of_kind("any_name_list")
        .map(|list| {
            list.find_all("any_name")
                .iter()
                .map(|n| n.text(src).to_string())
                .collect()
        })
        .unwrap_or_default();
    Ok(Statement::AddTextSearchMapping {
        configuration,
        tokens,
        dictionaries,
    })
}

/// CREATE CAST → Cast
pub(crate) fn create_cast(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let types: Vec<String> = {
        let mut cursor = node.walk();
        node.children(&mut cursor)
            .filter(|n| n.kind() == "Typename")
            .map(|n| n.text(src).to_string())
            .collect()
    };
    let [source, target] = types.as_slice() else {
        return Err(String::from("CREATE CAST without two types"));
    };
    let function = node
        .child_of_kind("function_with_argtypes")
        .map(|n| n.text(src).to_string());
    let context = node.child_of_kind("cast_context");
    // a cast has no schema of its own; the project files it with the
    // first schema its function or types name
    let schema = [function.as_deref(), Some(source), Some(target)]
        .into_iter()
        .flatten()
        .find_map(|name| {
            let name = name.split('(').next().unwrap_or(name);
            name.rsplit_once('.').map(|(schema, _)| unquote(schema))
        })
        .unwrap_or_else(|| String::from("public"));
    Ok(Statement::CreateCast(Cast {
        schema,
        owner: String::new(),
        sql: None,
        source_type: Some(source.clone()),
        target_type: Some(target.clone()),
        inout: node.child_of_kind("kw_inout").is_some().then_some(true),
        function,
        assignment: context
            .is_some_and(|c| c.child_of_kind("kw_assignment").is_some())
            .then_some(true),
        implicit: context
            .is_some_and(|c| c.child_of_kind("kw_implicit").is_some())
            .then_some(true),
        comment: None,
    }))
}

/// CREATE [DEFAULT] CONVERSION → Conversion
pub(crate) fn create_conversion(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let names = node.find_all("any_name");
    let [name, function] = names.as_slice() else {
        return Err(String::from("CREATE CONVERSION without a function"));
    };
    let name = any_name(name, src);
    let encodings: Vec<String> = {
        let mut cursor = node.walk();
        node.children(&mut cursor)
            .filter(|n| n.kind() == "Sconst")
            .map(|n| string_value(&n, src))
            .collect()
    };
    Ok(Statement::CreateConversion(Conversion {
        name: name.name,
        schema: name.schema.unwrap_or_default(),
        owner: String::new(),
        sql: None,
        default: node.child_of_kind("opt_default").is_some().then_some(true),
        encoding_from: encodings.first().cloned(),
        encoding_to: encodings.get(1).cloned(),
        function: Some(function.text(src).to_string()),
        comment: None,
    }))
}

/// CREATE LANGUAGE → Language. pg_dump writes OR REPLACE and no
/// handler when the handler is not part of the dump, which is the case
/// for a handler in `pg_catalog`.
pub(crate) fn create_language(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .child_of_kind("name")
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| String::from("CREATE LANGUAGE without a name"))?;
    let handler = |n: Option<Node>| {
        n.and_then(|n| n.find_all("handler_name").into_iter().next())
            .map(|n| n.text(src).to_string())
    };
    Ok(Statement::CreateLanguage(Language {
        name,
        replace: node
            .child_of_kind("opt_or_replace")
            .is_some()
            .then_some(true),
        trusted: node.child_of_kind("opt_trusted").is_some().then_some(true),
        handler: node
            .child_of_kind("handler_name")
            .map(|n| n.text(src).to_string()),
        inline_handler: handler(node.child_of_kind("opt_inline_handler")),
        validator: handler(node.child_of_kind("opt_validator")),
        comment: None,
    }))
}

/// CREATE EVENT TRIGGER → EventTrigger
pub(crate) fn create_event_trigger(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .child_of_kind("name")
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| String::from("CREATE EVENT TRIGGER without a name"))?;
    let event = node
        .child_of_kind("ColLabel")
        .map(|n| n.text(src).to_lowercase());
    let mut tags = Vec::new();
    for item in node.find_all("event_trigger_when_item") {
        let variable = item.child_of_kind("ColId").map(|n| n.text(src));
        if !variable.is_some_and(|v| v.eq_ignore_ascii_case("tag")) {
            return Ok(Statement::Unsupported(String::from(
                "CREATE EVENT TRIGGER filter other than TAG",
            )));
        }
        tags.extend(
            item.find_all("string_literal")
                .iter()
                .map(|n| crate::ddl::object::unstring(n.text(src))),
        );
    }
    let function = node
        .child_of_kind("func_name")
        .map(|n| format!("{}()", n.text(src)));
    Ok(Statement::CreateEventTrigger(EventTrigger {
        name,
        sql: None,
        event,
        filter: (!tags.is_empty()).then_some(EventTriggerFilter { tags }),
        function,
        enabled: None,
        comment: None,
    }))
}

/// ALTER EVENT TRIGGER ... ENABLE [REPLICA | ALWAYS] | DISABLE
pub(crate) fn alter_event_trigger(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .child_of_kind("name")
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| String::from("ALTER EVENT TRIGGER without a name"))?;
    let Some(state) = node.child_of_kind("enable_trigger") else {
        return Ok(Statement::Unsupported(String::from(
            "ALTER EVENT TRIGGER other than ENABLE or DISABLE",
        )));
    };
    let enabled = if state.child_of_kind("kw_disable").is_some() {
        Some("DISABLED")
    } else if state.child_of_kind("kw_replica").is_some() {
        Some("REPLICA")
    } else if state.child_of_kind("kw_always").is_some() {
        Some("ALWAYS")
    } else {
        None
    };
    Ok(Statement::AlterEventTrigger {
        name,
        enabled: enabled.map(String::from),
    })
}

/// The tables and schemas of a `pub_obj_list`
fn publication_objects(
    node: &Node,
    src: &str,
) -> Result<(Vec<PublicationTable>, Vec<String>), String> {
    let mut tables = Vec::new();
    let mut schemas = Vec::new();
    for spec in node.find_all("PublicationObjSpec") {
        if spec.child_of_kind("kw_schema").is_some() {
            let schema = spec.child_of_kind("ColId").ok_or_else(|| {
                String::from("TABLES IN SCHEMA without a schema name")
            })?;
            schemas.push(unquote(schema.text(src)));
            continue;
        }
        let name = spec
            .find("qualified_name")
            .ok_or_else(|| String::from("publication table without a name"))?
            .text(src)
            .to_string();
        let columns = spec.child_of_kind("opt_column_list").map(|list| {
            list.find_all("columnElem")
                .iter()
                .map(|n| unquote(n.text(src)))
                .collect::<Vec<_>>()
        });
        let row_filter = spec
            .child_of_kind("OptWhereClause")
            .and_then(|n| n.child_of_kind("a_expr"))
            .map(|n| n.text(src).to_string());
        tables.push(if columns.is_none() && row_filter.is_none() {
            PublicationTable::Name(name)
        } else {
            PublicationTable::Filtered(FilteredPublicationTable {
                name,
                columns,
                row_filter,
            })
        });
    }
    Ok((tables, schemas))
}

/// CREATE PUBLICATION → Publication. Options at their default are left
/// out, since pg_dump writes `publish` whatever its value.
pub(crate) fn create_publication(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .child_of_kind("name")
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| String::from("CREATE PUBLICATION without a name"))?;
    let mut all_tables = None;
    if let Some(all) = node.child_of_kind("pub_all_obj_type_list") {
        if all.find("kw_tables").is_none()
            || all.find("kw_sequences").is_some()
        {
            return Ok(Statement::Unsupported(String::from(
                "CREATE PUBLICATION FOR ALL other than TABLES",
            )));
        }
        all_tables = Some(true);
    }
    let (tables, schemas) = match node.child_of_kind("pub_obj_list") {
        Some(list) => publication_objects(&list, src)?,
        None => (Vec::new(), Vec::new()),
    };
    let mut parameters = Map::new();
    let options = node
        .child_of_kind("opt_definition")
        .and_then(|n| n.child_of_kind("definition"))
        .map(|n| definition(&n, src))
        .unwrap_or_default();
    for (key, value) in options {
        let value = value.unwrap_or_default();
        match key.as_str() {
            "publish" => {
                let operations: Vec<Value> = value
                    .split(',')
                    .map(|op| Value::String(op.trim().to_lowercase()))
                    .filter(|op| op.as_str() != Some(""))
                    .collect();
                let all = ["insert", "update", "delete", "truncate"];
                if operations.len() != all.len()
                    || !all.iter().all(|op| {
                        operations.contains(&Value::String((*op).into()))
                    })
                {
                    parameters.insert(key, Value::Array(operations));
                }
            }
            "publish_via_partition_root" => {
                if value.eq_ignore_ascii_case("true") {
                    parameters.insert(key, Value::Bool(true));
                }
            }
            "publish_generated_columns" => {
                if !value.eq_ignore_ascii_case("none") {
                    parameters
                        .insert(key, Value::String(value.to_lowercase()));
                }
            }
            other => {
                return Ok(Statement::Unsupported(format!(
                    "CREATE PUBLICATION option {other}"
                )));
            }
        }
    }
    Ok(Statement::CreatePublication(Publication {
        name,
        tables: (!tables.is_empty()).then_some(tables),
        schemas: (!schemas.is_empty()).then_some(schemas),
        all_tables,
        parameters: (!parameters.is_empty()).then_some(parameters),
        comment: None,
    }))
}

/// ALTER PUBLICATION ... ADD, the form pg_dump writes for each table
/// and schema
pub(crate) fn alter_publication(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .child_of_kind("name")
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| String::from("ALTER PUBLICATION without a name"))?;
    let (Some(_), Some(list)) = (
        node.child_of_kind("kw_add"),
        node.child_of_kind("pub_obj_list"),
    ) else {
        return Ok(Statement::Unsupported(String::from(
            "ALTER PUBLICATION other than ADD",
        )));
    };
    let (tables, schemas) = publication_objects(&list, src)?;
    Ok(Statement::AddToPublication {
        name,
        tables,
        schemas,
    })
}

/// CREATE STATISTICS → Statistics
pub(crate) fn create_statistics(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .child_of_kind("opt_qualified_name")
        .and_then(|n| n.child_of_kind("any_name"))
        .map(|n| any_name(&n, src))
        .ok_or_else(|| String::from("CREATE STATISTICS without a name"))?;
    let tables = node
        .child_of_kind("from_list")
        .map(|n| n.find_all("table_ref"))
        .unwrap_or_default();
    let [table] = tables.as_slice() else {
        return Err(String::from("CREATE STATISTICS without one table"));
    };
    let kinds: Vec<String> = node
        .child_of_kind("opt_name_list")
        .map(|n| {
            n.find_all("name")
                .iter()
                .map(|k| unquote(k.text(src)))
                .collect()
        })
        .unwrap_or_default();
    let elements = node
        .child_of_kind("stats_params")
        .map(|n| {
            n.find_all("stats_param")
                .iter()
                .map(|p| p.text(src).to_string())
                .collect()
        })
        .unwrap_or_default();
    Ok(Statement::CreateStatistics(Statistics {
        name: name.name,
        schema: name.schema.unwrap_or_default(),
        owner: String::new(),
        table: table.text(src).to_string(),
        kinds: (!kinds.is_empty()).then_some(kinds),
        elements,
        target: None,
        comment: None,
    }))
}

/// ALTER STATISTICS ... SET STATISTICS n, the form pg_dump writes
pub(crate) fn alter_statistics(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .child_of_kind("any_name")
        .map(|n| any_name(&n, src))
        .ok_or_else(|| String::from("ALTER STATISTICS without a name"))?;
    let Some(target) = node
        .find("SignedIconst")
        .and_then(|n| n.text(src).trim().parse::<i64>().ok())
    else {
        return Ok(Statement::Unsupported(String::from(
            "ALTER STATISTICS other than SET STATISTICS",
        )));
    };
    Ok(Statement::AlterStatistics { name, target })
}

/// CREATE RULE → (relation, Rule)
pub(crate) fn create_rule(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .child_of_kind("name")
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| String::from("CREATE RULE without a name"))?;
    let relation = node
        .child_of_kind("qualified_name")
        .ok_or_else(|| String::from("CREATE RULE without a relation"))?;
    let relation = crate::ddl::qualified_name(&relation, src)?;
    let event = node
        .child_of_kind("event")
        .map(|n| n.text(src).to_uppercase())
        .ok_or_else(|| String::from("CREATE RULE without an event"))?;
    let actions = node
        .child_of_kind("RuleActionList")
        .ok_or_else(|| String::from("CREATE RULE without actions"))?;
    let commands: Vec<String> = actions
        .find_all("RuleActionStmt")
        .iter()
        .map(|n| n.text(src).trim().to_string())
        .collect();
    Ok(Statement::CreateRule {
        relation,
        rule: Rule {
            name,
            event,
            condition: node
                .child_of_kind("where_clause")
                .and_then(|n| n.child_of_kind("a_expr"))
                .map(|n| n.text(src).to_string()),
            instead: node
                .child_of_kind("opt_instead")
                .is_some_and(|n| n.child_of_kind("kw_instead").is_some())
                .then_some(true),
            commands: (!commands.is_empty()).then_some(commands),
            enabled: None,
            comment: None,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::Parser;

    fn parse_one(sql: &str) -> Statement {
        let mut parser = Parser::new().unwrap();
        let mut statements = parser.parse(sql).unwrap();
        assert_eq!(statements.len(), 1, "expected one statement");
        statements.remove(0)
    }

    #[test]
    fn parses_an_ordered_set_aggregate() {
        let Statement::CreateAggregate(aggregate) = parse_one(
            "CREATE AGGREGATE s.sorted(integer ORDER BY text) (\n    \
             SFUNC = s.add, STYPE = integer, INITCOND = '0',\n    \
             PARALLEL = safe, HYPOTHETICAL );",
        ) else {
            panic!("expected CreateAggregate")
        };
        assert_eq!(aggregate.schema, "s");
        assert_eq!(aggregate.arguments[0].data_type, "integer");
        assert_eq!(aggregate.order_by.unwrap()[0].data_type, "text");
        assert_eq!(aggregate.sfunc, "s.add");
        assert_eq!(aggregate.initial_condition.as_deref(), Some("0"));
        assert_eq!(aggregate.parallel.as_deref(), Some("SAFE"));
        assert_eq!(aggregate.hypothetical, Some(true));
    }

    #[test]
    fn parses_languages() {
        let Statement::CreateLanguage(language) = parse_one(
            "CREATE TRUSTED PROCEDURAL LANGUAGE plcopy HANDLER s.h \
             INLINE s.i VALIDATOR s.v;",
        ) else {
            panic!("expected CreateLanguage")
        };
        assert_eq!(language.name, "plcopy");
        assert_eq!((language.replace, language.trusted), (None, Some(true)));
        assert_eq!(language.handler.as_deref(), Some("s.h"));
        assert_eq!(language.inline_handler.as_deref(), Some("s.i"));
        assert_eq!(language.validator.as_deref(), Some("s.v"));
        // pg_dump's form for a language whose handler it does not dump
        let Statement::CreateLanguage(language) =
            parse_one("CREATE OR REPLACE PROCEDURAL LANGUAGE plcopy;")
        else {
            panic!("expected CreateLanguage")
        };
        assert_eq!((language.replace, language.trusted), (Some(true), None));
        assert_eq!(language.handler, None);
    }

    #[test]
    fn parses_casts() {
        let Statement::CreateCast(cast) = parse_one(
            "CREATE CAST (s.pair AS integer) WITH FUNCTION \
             s.pair_x(s.pair) AS IMPLICIT;",
        ) else {
            panic!("expected CreateCast")
        };
        assert_eq!(cast.schema, "s");
        assert_eq!(cast.source_type.as_deref(), Some("s.pair"));
        assert_eq!(cast.function.as_deref(), Some("s.pair_x(s.pair)"));
        assert_eq!(cast.implicit, Some(true));
        let Statement::CreateCast(cast) =
            parse_one("CREATE CAST (integer AS text) WITH INOUT;")
        else {
            panic!("expected CreateCast")
        };
        assert_eq!((cast.inout, cast.assignment), (Some(true), None));
        assert_eq!(cast.schema, "public");
    }

    #[test]
    fn parses_collations() {
        let Statement::CreateCollation(collation) = parse_one(
            "CREATE COLLATION s.ci (provider = icu, deterministic = false, \
             locale = 'und-u-ks-level2', rules = '&a < b');",
        ) else {
            panic!("expected CreateCollation")
        };
        assert_eq!(collation.provider.as_deref(), Some("icu"));
        assert_eq!(collation.deterministic, Some(false));
        assert_eq!(collation.locale.as_deref(), Some("und-u-ks-level2"));
        assert_eq!(collation.rules.as_deref(), Some("&a < b"));
    }

    #[test]
    fn parses_conversions() {
        let Statement::CreateConversion(conversion) = parse_one(
            "CREATE DEFAULT CONVERSION s.l1 FOR 'LATIN1' TO 'UTF8' \
             FROM iso8859_1_to_utf8;",
        ) else {
            panic!("expected CreateConversion")
        };
        assert_eq!(conversion.default, Some(true));
        assert_eq!(conversion.encoding_from.as_deref(), Some("LATIN1"));
        assert_eq!(conversion.encoding_to.as_deref(), Some("UTF8"));
        assert_eq!(conversion.function.as_deref(), Some("iso8859_1_to_utf8"));
    }

    #[test]
    fn parses_event_triggers() {
        let Statement::CreateEventTrigger(trigger) = parse_one(
            "CREATE EVENT TRIGGER et ON ddl_command_start\n         \
             WHEN TAG IN ('CREATE TABLE', 'DROP TABLE')\n   \
             EXECUTE FUNCTION s.on_ddl();",
        ) else {
            panic!("expected CreateEventTrigger")
        };
        assert_eq!(trigger.event.as_deref(), Some("ddl_command_start"));
        assert_eq!(
            trigger.filter.unwrap().tags,
            vec![String::from("CREATE TABLE"), String::from("DROP TABLE")]
        );
        assert_eq!(trigger.function.as_deref(), Some("s.on_ddl()"));
        for (sql, state) in [
            ("ALTER EVENT TRIGGER et DISABLE;", Some("DISABLED")),
            ("ALTER EVENT TRIGGER et ENABLE REPLICA;", Some("REPLICA")),
            ("ALTER EVENT TRIGGER et ENABLE ALWAYS;", Some("ALWAYS")),
            ("ALTER EVENT TRIGGER et ENABLE;", None),
        ] {
            let Statement::AlterEventTrigger { enabled, .. } = parse_one(sql)
            else {
                panic!("expected AlterEventTrigger")
            };
            assert_eq!(enabled.as_deref(), state, "{sql}");
        }
    }

    #[test]
    fn parses_publications() {
        let Statement::CreatePublication(publication) = parse_one(
            "CREATE PUBLICATION p WITH (publish = 'insert, update, delete, \
             truncate', publish_via_partition_root = true);",
        ) else {
            panic!("expected CreatePublication")
        };
        // the default operations are left out
        let parameters = publication.parameters.unwrap();
        assert!(parameters.get("publish").is_none());
        assert_eq!(
            parameters["publish_via_partition_root"],
            Value::Bool(true)
        );
        let Statement::AddToPublication { tables, .. } = parse_one(
            "ALTER PUBLICATION p ADD TABLE ONLY s.t (id, a) WHERE ((a > 1));",
        ) else {
            panic!("expected AddToPublication")
        };
        assert_eq!(
            tables,
            vec![PublicationTable::Filtered(FilteredPublicationTable {
                name: String::from("s.t"),
                columns: Some(vec![String::from("id"), String::from("a")]),
                row_filter: Some(String::from("(a > 1)")),
            })]
        );
        let Statement::AddToPublication { schemas, .. } =
            parse_one("ALTER PUBLICATION p ADD TABLES IN SCHEMA s;")
        else {
            panic!("expected AddToPublication")
        };
        assert_eq!(schemas, vec![String::from("s")]);
    }

    #[test]
    fn parses_text_search_objects() {
        let Statement::CreateTextSearch { schema, object } = parse_one(
            "CREATE TEXT SEARCH DICTIONARY s.d (\n    \
             TEMPLATE = pg_catalog.simple,\n    stopwords = 'english' );",
        ) else {
            panic!("expected CreateTextSearch")
        };
        assert_eq!(schema, "s");
        let TextSearchObject::Dictionary(dictionary) = object else {
            panic!("expected a dictionary")
        };
        assert_eq!(dictionary.template.as_deref(), Some("pg_catalog.simple"));
        assert_eq!(
            dictionary.options.unwrap()["stopwords"],
            Value::String(String::from("english"))
        );
        let Statement::AddTextSearchMapping {
            tokens,
            dictionaries,
            ..
        } = parse_one(
            "ALTER TEXT SEARCH CONFIGURATION s.c\n    \
             ADD MAPPING FOR \"float\", uint WITH s.d, simple;",
        )
        else {
            panic!("expected AddTextSearchMapping")
        };
        assert_eq!(tokens, vec![String::from("float"), String::from("uint")]);
        assert_eq!(
            dictionaries,
            vec![String::from("s.d"), String::from("simple")]
        );
    }

    #[test]
    fn parses_statistics() {
        let Statement::CreateStatistics(statistics) = parse_one(
            "CREATE STATISTICS s.m_expr (mcv) ON (a + b), lower(c), a \
             FROM s.m;",
        ) else {
            panic!("expected CreateStatistics")
        };
        assert_eq!(
            (statistics.schema.as_str(), statistics.name.as_str()),
            ("s", "m_expr")
        );
        assert_eq!(statistics.table, "s.m");
        assert_eq!(statistics.kinds, Some(vec![String::from("mcv")]));
        assert_eq!(statistics.elements, vec!["(a + b)", "lower(c)", "a"]);
        let Statement::AlterStatistics { target, .. } =
            parse_one("ALTER STATISTICS s.m_all SET STATISTICS 500;")
        else {
            panic!("expected AlterStatistics")
        };
        assert_eq!(target, 500);
    }

    #[test]
    fn parses_rules() {
        let Statement::CreateRule { relation, rule } = parse_one(
            "CREATE RULE t_log AS\n    ON INSERT TO public.t\n   WHERE \
             (new.id > 0) DO ( INSERT INTO public.log (id)\n  VALUES \
             (new.id);\n INSERT INTO public.log (id)\n  VALUES ((- \
             new.id));\n);",
        ) else {
            panic!("expected CreateRule")
        };
        assert_eq!(relation.to_string(), "public.t");
        assert_eq!(rule.event, "INSERT");
        assert_eq!(rule.condition.as_deref(), Some("(new.id > 0)"));
        assert_eq!(rule.instead, None);
        assert_eq!(rule.commands.map(|c| c.len()), Some(2));
        let Statement::CreateRule { rule, .. } = parse_one(
            "CREATE RULE r AS ON DELETE TO public.t DO INSTEAD NOTHING;",
        ) else {
            panic!("expected CreateRule")
        };
        assert_eq!((rule.instead, rule.commands), (Some(true), None));
    }

    #[test]
    fn parses_operators() {
        let Statement::CreateOperator(operator) = parse_one(
            "CREATE OPERATOR s.=== (\n    FUNCTION = s.eq,\n    LEFTARG = \
             integer,\n    RIGHTARG = integer,\n    COMMUTATOR = \
             OPERATOR(s.===),\n    MERGES,\n    HASHES,\n    RESTRICT = \
             eqsel,\n    JOIN = eqjoinsel\n);",
        ) else {
            panic!("expected CreateOperator")
        };
        assert_eq!(
            (operator.schema.as_str(), operator.name.as_str()),
            ("s", "===")
        );
        assert_eq!(operator.function, "s.eq");
        assert_eq!(operator.left_arg.as_deref(), Some("integer"));
        assert_eq!(operator.commutator.as_deref(), Some("OPERATOR(s.===)"));
        assert_eq!(
            (operator.hashes, operator.merges),
            (Some(true), Some(true))
        );
    }
}
