//! Foreign data wrappers, servers, user mappings, and foreign tables
//! (CreateFdwStmt / CreateForeignServerStmt / CreateUserMappingStmt /
//! CreateForeignTableStmt)

use serde_json::{Map, Value};
use tree_sitter::Node;

use crate::ddl::object::string_value;
use crate::ddl::table::column;
use crate::ddl::{NodeExt, Statement, qualified_name, unquote};
use crate::models::{
    ForeignDataWrapper, Server, Table, UserMapping, UserMappingServer,
};

/// CREATE FOREIGN DATA WRAPPER → ForeignDataWrapper
pub(crate) fn create_fdw(node: &Node, src: &str) -> Result<Statement, String> {
    let name = node
        .child_of_kind("name")
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| {
            String::from("CREATE FOREIGN DATA WRAPPER without a name")
        })?;
    let mut fdw = ForeignDataWrapper {
        name,
        owner: String::new(),
        handler: None,
        validator: None,
        options: generic_options(node, src),
        comment: None,
    };
    // each fdw_option is HANDLER/VALIDATOR <name> or NO HANDLER/VALIDATOR
    // (the latter carries kw_no and no handler_name)
    for option in node.find_all("fdw_option") {
        let Some(handler) = option.child_of_kind("handler_name") else {
            continue;
        };
        let value = Some(handler.text(src).to_string());
        if option.has("kw_handler") {
            fdw.handler = value;
        } else if option.has("kw_validator") {
            fdw.validator = value;
        }
    }
    Ok(Statement::CreateForeignDataWrapper(fdw))
}

/// CREATE SERVER → Server
pub(crate) fn create_server(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    // two `name` children: the server, then its foreign data wrapper
    let names = direct_children(node, "name");
    let name = names
        .first()
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| String::from("CREATE SERVER without a name"))?;
    let foreign_data_wrapper = names
        .get(1)
        .map(|n| unquote(n.text(src)))
        .unwrap_or_default();
    Ok(Statement::CreateServer(Server {
        name,
        foreign_data_wrapper,
        server_type: node
            .child_of_kind("opt_type")
            .and_then(|t| t.find("Sconst"))
            .map(|s| string_value(&s, src)),
        version: node
            .find("foreign_server_version")
            .and_then(|v| v.find("Sconst"))
            .map(|s| string_value(&s, src)),
        options: generic_options(node, src),
        comment: None,
    }))
}

/// CREATE USER MAPPING → UserMapping (one server entry; the assembly
/// merges mappings that share a user)
pub(crate) fn create_user_mapping(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .child_of_kind("auth_ident")
        .map(|a| unquote(a.text(src)))
        .ok_or_else(|| String::from("CREATE USER MAPPING without a user"))?;
    let server = node
        .child_of_kind("name")
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| String::from("CREATE USER MAPPING without a server"))?;
    Ok(Statement::CreateUserMapping(UserMapping {
        name,
        servers: vec![UserMappingServer {
            name: server,
            options: generic_options(node, src),
        }],
    }))
}

/// CREATE FOREIGN TABLE → Table with `server`/`options` set (foreign
/// tables share the tables/ directory and the Table model)
pub(crate) fn create_foreign_table(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .find("qualified_name")
        .ok_or_else(|| String::from("CREATE FOREIGN TABLE without a name"))?;
    let name = qualified_name(&name, src)?;
    let columns: Vec<_> = node
        .find_all("columnDef")
        .iter()
        .map(|c| column(c, src))
        .collect();
    Ok(Statement::CreateTable(Box::new(Table {
        name: name.name,
        schema: name.schema.unwrap_or_default(),
        owner: String::new(),
        sql: None,
        unlogged: None,
        from_type: None,
        parents: None,
        like_table: None,
        columns: (!columns.is_empty()).then_some(columns),
        indexes: None,
        primary_key: None,
        check_constraints: None,
        unique_constraints: None,
        foreign_keys: None,
        triggers: None,
        partition: None,
        partitions: None,
        access_method: None,
        storage_parameters: None,
        tablespace: None,
        index_tablespace: None,
        // the `name` after SERVER (the only bare `name` child)
        server: node.child_of_kind("name").map(|n| unquote(n.text(src))),
        options: generic_options(node, src),
        comment: None,
    })))
}

/// Parse a `create_generic_options` (`OPTIONS (key 'value', ...)`) into
/// a string→value map; the values are always quoted string literals
fn generic_options(node: &Node, src: &str) -> Option<Map<String, Value>> {
    let options = node.child_of_kind("create_generic_options")?;
    let mut map = Map::new();
    for elem in options.find_all("generic_option_elem") {
        let Some(name) = elem.child_of_kind("generic_option_name") else {
            continue;
        };
        let value = elem
            .child_of_kind("generic_option_arg")
            .and_then(|arg| arg.find("Sconst"))
            .map(|s| string_value(&s, src))
            .unwrap_or_default();
        map.insert(unquote(name.text(src)), Value::String(value));
    }
    (!map.is_empty()).then_some(map)
}

/// Direct children of `node` of the given kind (not recursive)
fn direct_children<'a>(node: &Node<'a>, kind: &str) -> Vec<Node<'a>> {
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .filter(|n| n.kind() == kind)
        .collect()
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
    fn parses_fdw_with_handler_and_options() {
        let Statement::CreateForeignDataWrapper(fdw) = parse_one(
            "CREATE FOREIGN DATA WRAPPER warehouse HANDLER \
             postgres_fdw_handler VALIDATOR postgres_fdw_validator \
             OPTIONS (debug 'true');",
        ) else {
            panic!("expected CreateForeignDataWrapper")
        };
        assert_eq!(fdw.name, "warehouse");
        assert_eq!(fdw.handler.as_deref(), Some("postgres_fdw_handler"));
        assert_eq!(fdw.validator.as_deref(), Some("postgres_fdw_validator"));
        assert_eq!(
            fdw.options.unwrap().get("debug"),
            Some(&Value::String("true".into()))
        );
    }

    #[test]
    fn parses_fdw_no_handler() {
        let Statement::CreateForeignDataWrapper(fdw) = parse_one(
            "CREATE FOREIGN DATA WRAPPER w NO HANDLER NO VALIDATOR;",
        ) else {
            panic!("expected CreateForeignDataWrapper")
        };
        assert_eq!(fdw.handler, None);
        assert_eq!(fdw.validator, None);
    }

    #[test]
    fn parses_server() {
        let Statement::CreateServer(server) = parse_one(
            "CREATE SERVER wh TYPE 'oracle' VERSION '19' FOREIGN DATA \
             WRAPPER warehouse OPTIONS (host 'db', dbname 'w');",
        ) else {
            panic!("expected CreateServer")
        };
        assert_eq!(server.name, "wh");
        assert_eq!(server.foreign_data_wrapper, "warehouse");
        assert_eq!(server.server_type.as_deref(), Some("oracle"));
        assert_eq!(server.version.as_deref(), Some("19"));
        let options = server.options.unwrap();
        assert_eq!(options.get("host"), Some(&Value::String("db".into())));
        assert_eq!(options.get("dbname"), Some(&Value::String("w".into())));
    }

    #[test]
    fn parses_user_mapping() {
        let Statement::CreateUserMapping(mapping) = parse_one(
            "CREATE USER MAPPING FOR app SERVER wh OPTIONS \
             (user 'remote', password 'secret');",
        ) else {
            panic!("expected CreateUserMapping")
        };
        assert_eq!(mapping.name, "app");
        assert_eq!(mapping.servers.len(), 1);
        assert_eq!(mapping.servers[0].name, "wh");
        let options = mapping.servers[0].options.as_ref().unwrap();
        assert_eq!(options.get("user"), Some(&Value::String("remote".into())));
        assert_eq!(
            options.get("password"),
            Some(&Value::String("secret".into()))
        );
    }

    #[test]
    fn parses_foreign_table() {
        let Statement::CreateTable(table) = parse_one(
            "CREATE FOREIGN TABLE fdw_warehouse.orders (id integer NOT \
             NULL, total numeric) SERVER wh OPTIONS (schema_name \
             'public', table_name 'orders');",
        ) else {
            panic!("expected CreateTable")
        };
        assert_eq!(table.schema, "fdw_warehouse");
        assert_eq!(table.name, "orders");
        assert_eq!(table.server.as_deref(), Some("wh"));
        let columns = table.columns.unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].nullable, Some(false));
        let options = table.options.unwrap();
        assert_eq!(
            options.get("schema_name"),
            Some(&Value::String("public".into()))
        );
    }
}
