//! Functions and procedures (CreateFunctionStmt)

use tree_sitter::Node;

use crate::ddl::object::{string_value, unstring};
use crate::ddl::{NodeExt, Statement, any_name, unquote};
use crate::models::{Function, FunctionParameter};

/// CREATE [OR REPLACE] FUNCTION/PROCEDURE → Function
pub(crate) fn create_function(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .child_of_kind("func_name")
        .map(|n| any_name(&n, src))
        .ok_or_else(|| String::from("CREATE FUNCTION without a name"))?;
    let mut function = Function {
        name: name.name,
        schema: name.schema.unwrap_or_default(),
        owner: String::new(),
        sql: None,
        parameters: None,
        returns: node
            .child_of_kind("func_return")
            .map(|n| n.text(src).to_string()),
        language: None,
        transform_types: None,
        window: None,
        immutable: None,
        stable: None,
        volatile: None,
        leak_proof: None,
        called_on_null_input: None,
        strict: None,
        security: None,
        parallel: None,
        cost: None,
        rows: None,
        support: None,
        configuration: None,
        definition: None,
        object_file: None,
        link_symbol: None,
        comment: None,
    };
    let parameters: Vec<FunctionParameter> = node
        .find_all("func_arg_with_default")
        .iter()
        .map(|arg| parameter(arg, src))
        .collect();
    if !parameters.is_empty() {
        function.parameters = Some(parameters);
    }
    for option in node.find_all("createfunc_opt_item") {
        if option.has("kw_language") {
            function.language = option
                .child_of_kind("NonReservedWord_or_Sconst")
                .map(|n| unquote(n.text(src)));
        } else if option.has("kw_as") {
            if let Some(body) = option.find("Sconst") {
                function.definition = Some(string_value(&body, src));
            }
        } else if option.has("kw_window") {
            function.window = Some(true);
        } else if let Some(common) =
            option.child_of_kind("common_func_opt_item")
        {
            apply_common_option(&mut function, &common, src);
        }
    }
    Ok(Statement::CreateFunction(Box::new(function)))
}

fn apply_common_option(function: &mut Function, node: &Node, src: &str) {
    if node.has("kw_immutable") {
        function.immutable = Some(true);
    } else if node.has("kw_stable") {
        function.stable = Some(true);
    } else if node.has("kw_volatile") {
        function.volatile = Some(true);
    } else if node.has("kw_leakproof") {
        function.leak_proof = Some(!node.has("kw_not"));
    } else if node.has("kw_strict") {
        function.strict = Some(true);
    } else if node.has("kw_called") {
        function.called_on_null_input = Some(true);
    } else if node.has("kw_null") && node.has("kw_returns") {
        function.called_on_null_input = Some(false);
    } else if node.has("kw_security") {
        function.security = Some(
            if node.has("kw_definer") {
                "DEFINER"
            } else {
                "INVOKER"
            }
            .to_string(),
        );
    } else if node.has("kw_parallel") {
        function.parallel = node
            .child_of_kind("ColId")
            .map(|n| n.text(src).to_uppercase());
    } else if node.has("kw_cost") {
        function.cost = node
            .find("NumericOnly")
            .and_then(|n| n.text(src).parse().ok());
    } else if node.has("kw_rows") {
        function.rows = node
            .find("NumericOnly")
            .and_then(|n| n.text(src).parse().ok());
    } else if node.has("kw_support") {
        function.support = node
            .child_of_kind("any_name")
            .map(|n| n.text(src).to_string());
    } else if node.has("kw_set")
        && let Some(config) = node.find("set_rest_more")
    {
        let text = config.text(src);
        if let Some((name, value)) = split_set(text) {
            function
                .configuration
                .get_or_insert_default()
                .insert(name, serde_json::Value::String(value));
        }
    }
}

/// `name TO value` / `name = value` from a SET clause body
fn split_set(text: &str) -> Option<(String, String)> {
    let (name, value) = text
        .split_once(" TO ")
        .or_else(|| text.split_once(" to "))
        .or_else(|| text.split_once('='))?;
    Some((name.trim().to_string(), unstring(value.trim())))
}

fn parameter(node: &Node, src: &str) -> FunctionParameter {
    let mode = node
        .find("arg_class")
        .map(|n| n.text(src).to_uppercase())
        .unwrap_or_else(|| String::from("IN"));
    FunctionParameter {
        mode,
        data_type: node
            .find("func_type")
            .map(|n| n.text(src).to_string())
            .unwrap_or_default(),
        name: node.find("param_name").map(|n| unquote(n.text(src))),
        default: node
            .child_of_kind("a_expr")
            .map(|n| serde_json::Value::String(n.text(src).to_string())),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::ddl::Parser;

    fn parse_one(sql: &str) -> Statement {
        let mut parser = Parser::new().unwrap();
        let mut statements = parser.parse(sql).unwrap();
        assert_eq!(statements.len(), 1, "expected one statement");
        statements.remove(0)
    }

    #[test]
    fn parses_create_function() {
        let Statement::CreateFunction(function) = parse_one(
            "CREATE FUNCTION test.fn(a integer, b text DEFAULT 'x') \
             RETURNS text\n    LANGUAGE sql STABLE\n    AS $$ SELECT b \
             $$;",
        ) else {
            panic!("expected CreateFunction")
        };
        assert_eq!(function.schema, "test");
        assert_eq!(function.name, "fn");
        assert_eq!(function.returns, Some("text".into()));
        assert_eq!(function.language, Some("sql".into()));
        assert_eq!(function.stable, Some(true));
        assert_eq!(function.definition, Some(" SELECT b ".into()));
        let parameters = function.parameters.unwrap();
        assert_eq!(parameters.len(), 2);
        assert_eq!(parameters[0].mode, "IN");
        assert_eq!(parameters[0].name, Some("a".into()));
        assert_eq!(parameters[0].data_type, "integer");
        assert_eq!(parameters[1].default, Some(json!("'x'")));
    }

    #[test]
    fn parses_function_options() {
        let Statement::CreateFunction(function) = parse_one(
            "CREATE FUNCTION f(OUT result integer) RETURNS integer \
             LANGUAGE plpgsql SECURITY DEFINER STRICT COST 100 \
             AS $_$BEGIN END$_$;",
        ) else {
            panic!("expected CreateFunction")
        };
        assert_eq!(function.security, Some("DEFINER".into()));
        assert_eq!(function.strict, Some(true));
        assert_eq!(function.cost, Some(100));
        assert_eq!(function.definition, Some("BEGIN END".into()));
        let parameters = function.parameters.unwrap();
        assert_eq!(parameters[0].mode, "OUT");
    }

    #[test]
    fn identity_keeps_out_parameters() {
        // pg_get_function_identity_arguments (PG17) includes OUT
        // params, so COMMENT ON / GRANT ON FUNCTION signatures carry
        // them; identity() must match or the comment goes unattached
        let Statement::CreateFunction(function) = parse_one(
            "CREATE FUNCTION public.get_count(in_a_id integer, \
             in_from timestamp with time zone, OUT upload_count bigint) \
             RETURNS bigint LANGUAGE sql AS $$ SELECT 1::bigint $$;",
        ) else {
            panic!("expected CreateFunction")
        };
        assert_eq!(
            function.identity(),
            "get_count(in_a_id integer, in_from timestamp with time \
             zone, OUT upload_count bigint)"
        );
    }

    #[test]
    fn parses_unqualified_function() {
        let Statement::CreateFunction(function) = parse_one(
            "CREATE FUNCTION uppercase(value text) RETURNS text \
             LANGUAGE sql AS $$ SELECT upper(value) $$;",
        ) else {
            panic!("expected CreateFunction")
        };
        assert_eq!(function.schema, "");
        assert_eq!(function.name, "uppercase");
    }
}
