//! GRANT/REVOKE and role statements (for ACL entries and
//! --extract-roles)

use tree_sitter::Node;

use crate::ddl::object::{string_value, unstring};
use crate::ddl::{
    Acl, AclTarget, NodeExt, Privilege, RoleDef, Statement, any_name,
    qualified_name, truncate, unquote,
};
use crate::models::RoleOptions;

/// GRANT or REVOKE privileges ON objects TO/FROM roles
pub(crate) fn grant(
    node: &Node,
    src: &str,
    revoke: bool,
) -> Result<Statement, String> {
    let target = node.find("privilege_target").ok_or_else(|| {
        format!(
            "{} without a target",
            if revoke { "REVOKE" } else { "GRANT" }
        )
    })?;
    if target.has("kw_all") {
        // GRANT ... ON ALL TABLES IN SCHEMA — never emitted by pg_dump
        return Ok(Statement::Unsupported(format!(
            "{}: ALL ... IN SCHEMA",
            node.kind()
        )));
    }
    let Some(kind) = target_kind(&target) else {
        return Ok(Statement::Unsupported(format!(
            "{}: {}",
            node.kind(),
            truncate(target.text(src), 80)
        )));
    };
    Ok(Statement::Acl(Acl {
        revoke,
        privileges: privileges(node, src),
        target: kind,
        objects: object_names(&target, kind, src),
        roles: role_specs(node, src),
        with_grant_option: node.has("opt_grant_grant_option"),
    }))
}

/// GRANT role TO role / REVOKE role FROM role
pub(crate) fn grant_role(
    node: &Node,
    src: &str,
    revoke: bool,
) -> Result<Statement, String> {
    let roles = node
        .find_all("privilege")
        .iter()
        .map(|n| unquote(n.text(src)))
        .collect();
    let members = node
        .find("role_list")
        .map(|n| {
            n.find_all("RoleSpec")
                .iter()
                .map(|r| unquote(r.text(src)))
                .collect()
        })
        .unwrap_or_default();
    Ok(Statement::RoleMembership {
        revoke,
        roles,
        members,
    })
}

/// CREATE ROLE / ALTER ROLE ... [WITH] options
pub(crate) fn role(node: &Node, src: &str) -> Result<Statement, String> {
    let name = node
        .find("RoleSpec")
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| format!("{} without a role name", node.kind()))?;
    let mut def = RoleDef {
        name,
        ..RoleDef::default()
    };
    for elem in node.find_all("AlterOptRoleElem") {
        role_option(&elem, src, &mut def);
    }
    Ok(if node.kind() == "CreateRoleStmt" {
        Statement::CreateRole(def)
    } else {
        Statement::AlterRole(def)
    })
}

/// ALTER ROLE name [IN DATABASE db] SET setting
pub(crate) fn role_setting(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let role = node
        .find("RoleSpec")
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| String::from("ALTER ROLE SET without a role name"))?;
    if node.has("opt_in_database") {
        // database-scoped settings have no place in the project model
        return Ok(Statement::Unsupported(
            "AlterRoleSetStmt: IN DATABASE".into(),
        ));
    }
    let set = node.find("generic_set").ok_or_else(|| {
        format!(
            "unsupported ALTER ROLE SET: {}",
            truncate(node.text(src), 80)
        )
    })?;
    // mixed-case / dotted GUC names are quoted by pg_dump
    // (e.g. "TimeZone"); store the bare identifier
    let name = set
        .find("var_name")
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| String::from("ALTER ROLE SET without a setting"))?;
    let values: Vec<String> = set
        .find_all("var_value")
        .iter()
        .map(|v| unstring(v.text(src)))
        .collect();
    Ok(Statement::AlterRoleSetting {
        role,
        name,
        value: values,
    })
}

/// Map a privilege_target's keywords onto the ACL target kind;
/// a bare object list means TABLE
fn target_kind(target: &Node) -> Option<AclTarget> {
    if target.has("kw_schema") {
        Some(AclTarget::Schema)
    } else if target.has("kw_sequence") {
        Some(AclTarget::Sequence)
    } else if target.has("kw_function")
        || target.has("kw_procedure")
        || target.has("kw_routine")
    {
        Some(AclTarget::Function)
    } else if target.has("kw_database") {
        Some(AclTarget::Database)
    } else if target.has("kw_domain") {
        Some(AclTarget::Domain)
    } else if target.has("kw_language") {
        Some(AclTarget::Language)
    } else if target.has("kw_large") {
        Some(AclTarget::LargeObject)
    } else if target.has("kw_data") {
        Some(AclTarget::ForeignDataWrapper)
    } else if target.has("kw_server") {
        Some(AclTarget::ForeignServer)
    } else if target.has("kw_tablespace") {
        Some(AclTarget::Tablespace)
    } else if target.has("kw_type") {
        Some(AclTarget::Type)
    } else if target.has("kw_parameter") {
        None
    } else if target.has("qualified_name_list") {
        Some(AclTarget::Table)
    } else {
        None
    }
}

/// The privilege list: ALL, or per-privilege names with optional
/// column lists
fn privileges(node: &Node, src: &str) -> Vec<Privilege> {
    let Some(privs) = node.child_of_kind("privileges") else {
        return Vec::new();
    };
    if privs.child_of_kind("kw_all").is_some() {
        return vec![Privilege {
            name: "ALL".into(),
            columns: None,
        }];
    }
    privs
        .find_all("privilege")
        .iter()
        .map(|p| {
            let columns: Vec<String> = p
                .find_all("columnElem")
                .iter()
                .map(|c| unquote(c.text(src)))
                .collect();
            let name = match p.child_of_kind("opt_column_list") {
                Some(list) => p.text(src)
                    [..list.start_byte() - p.start_byte()]
                    .trim()
                    .to_string(),
                None => p.text(src).to_string(),
            };
            Privilege {
                name: name.to_uppercase(),
                columns: (!columns.is_empty()).then_some(columns),
            }
        })
        .collect()
}

/// Format the target object names per target kind
fn object_names(target: &Node, kind: AclTarget, src: &str) -> Vec<String> {
    match kind {
        AclTarget::Table | AclTarget::Sequence => target
            .find_all("qualified_name")
            .iter()
            .filter_map(|n| qualified_name(n, src).ok())
            .map(|n| n.to_string())
            .collect(),
        AclTarget::Domain | AclTarget::Type => target
            .find_all("any_name")
            .iter()
            .map(|n| any_name(n, src).to_string())
            .collect(),
        AclTarget::Function => target
            .find_all("function_with_argtypes")
            .iter()
            .map(|f| function_signature(f, src))
            .collect(),
        AclTarget::LargeObject => target
            .find_all("NumericOnly")
            .iter()
            .map(|n| n.text(src).to_string())
            .collect(),
        _ => target
            .find_all("name")
            .iter()
            .map(|n| unquote(n.text(src)))
            .collect(),
    }
}

/// `schema.fn(integer, text)` from a function_with_argtypes node
fn function_signature(node: &Node, src: &str) -> String {
    let name = node
        .find("func_name")
        .map(|n| any_name(&n, src).to_string())
        .unwrap_or_default();
    let args: Vec<&str> = node
        .find_all("func_arg")
        .iter()
        .map(|a| a.text(src))
        .collect();
    format!("{name}({})", args.join(", "))
}

/// The grantees of a GRANT/REVOKE statement
fn role_specs(node: &Node, src: &str) -> Vec<String> {
    node.find("grantee_list")
        .map(|n| {
            n.find_all("RoleSpec")
                .iter()
                .map(|r| unquote(r.text(src)))
                .collect()
        })
        .unwrap_or_default()
}

/// Apply one CREATE/ALTER ROLE option element to the definition;
/// boolean options surface as bare identifiers in the CST
fn role_option(elem: &Node, src: &str, def: &mut RoleDef) {
    let options = &mut def.options;
    if elem.has("kw_password") {
        def.password = elem.find("Sconst").map(|n| string_value(&n, src));
        return;
    }
    if elem.has("kw_valid") {
        def.valid_until = elem.find("Sconst").map(|n| string_value(&n, src));
        return;
    }
    if elem.has("kw_connection") {
        options.connection_limit = elem
            .find("SignedIconst")
            .and_then(|n| n.text(src).parse().ok());
        return;
    }
    if elem.has("kw_inherit") {
        options.inherit = Some(true);
        return;
    }
    let Some(word) = elem.child_of_kind("identifier") else {
        log::warn!(
            "Unsupported role option: {:?}",
            truncate(elem.text(src), 64)
        );
        return;
    };
    apply_boolean_option(&word.text(src).to_uppercase(), options, elem, src);
}

fn apply_boolean_option(
    word: &str,
    options: &mut RoleOptions,
    elem: &Node,
    src: &str,
) {
    match word {
        "SUPERUSER" => options.superuser = Some(true),
        "NOSUPERUSER" => options.superuser = Some(false),
        "CREATEDB" => options.create_db = Some(true),
        "NOCREATEDB" => options.create_db = Some(false),
        "CREATEROLE" => options.create_role = Some(true),
        "NOCREATEROLE" => options.create_role = Some(false),
        "NOINHERIT" => options.inherit = Some(false),
        "LOGIN" => options.login = Some(true),
        "NOLOGIN" => options.login = Some(false),
        "REPLICATION" => options.replication = Some(true),
        "NOREPLICATION" => options.replication = Some(false),
        "BYPASSRLS" => options.bypass_rls = Some(true),
        "NOBYPASSRLS" => options.bypass_rls = Some(false),
        _ => log::warn!(
            "Unsupported role option: {:?}",
            truncate(elem.text(src), 64)
        ),
    }
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
    fn parses_grant_on_schema() {
        let Statement::Acl(acl) =
            parse_one("GRANT USAGE ON SCHEMA test TO PUBLIC;")
        else {
            panic!("expected Acl")
        };
        assert!(!acl.revoke);
        assert_eq!(
            acl.privileges,
            vec![Privilege {
                name: "USAGE".into(),
                columns: None
            }]
        );
        assert_eq!(acl.target, AclTarget::Schema);
        assert_eq!(acl.objects, vec!["test"]);
        assert_eq!(acl.roles, vec!["PUBLIC"]);
        assert!(!acl.with_grant_option);
    }

    #[test]
    fn parses_grant_on_table() {
        let Statement::Acl(acl) =
            parse_one("GRANT SELECT, INSERT ON TABLE test.users TO app_user;")
        else {
            panic!("expected Acl")
        };
        assert_eq!(acl.target, AclTarget::Table);
        assert_eq!(acl.objects, vec!["test.users"]);
        assert_eq!(
            acl.privileges
                .iter()
                .map(|p| p.name.as_str())
                .collect::<Vec<_>>(),
            vec!["SELECT", "INSERT"]
        );
        assert_eq!(acl.roles, vec!["app_user"]);
    }

    #[test]
    fn parses_grant_without_object_keyword() {
        let Statement::Acl(acl) =
            parse_one("REVOKE ALL ON test.users FROM PUBLIC;")
        else {
            panic!("expected Acl")
        };
        assert!(acl.revoke);
        assert_eq!(acl.target, AclTarget::Table);
        assert_eq!(
            acl.privileges,
            vec![Privilege {
                name: "ALL".into(),
                columns: None
            }]
        );
    }

    #[test]
    fn parses_grant_on_function() {
        let Statement::Acl(acl) = parse_one(
            "GRANT EXECUTE ON FUNCTION test.fn(integer, text) TO app_user;",
        ) else {
            panic!("expected Acl")
        };
        assert_eq!(acl.target, AclTarget::Function);
        assert_eq!(acl.objects, vec!["test.fn(integer, text)"]);
    }

    #[test]
    fn parses_column_grant() {
        let Statement::Acl(acl) = parse_one(
            "GRANT SELECT (id, email) ON TABLE test.users TO app_user;",
        ) else {
            panic!("expected Acl")
        };
        assert_eq!(
            acl.privileges,
            vec![Privilege {
                name: "SELECT".into(),
                columns: Some(vec!["id".into(), "email".into()])
            }]
        );
    }

    #[test]
    fn parses_grant_option() {
        let Statement::Acl(acl) = parse_one(
            "GRANT USAGE ON SCHEMA test TO app_user WITH GRANT OPTION;",
        ) else {
            panic!("expected Acl")
        };
        assert!(acl.with_grant_option);
    }

    #[test]
    fn all_in_schema_is_unsupported() {
        let statement = parse_one(
            "GRANT SELECT ON ALL TABLES IN SCHEMA test TO app_user;",
        );
        assert!(matches!(statement, Statement::Unsupported(_)));
    }

    #[test]
    fn parses_role_membership() {
        let Statement::RoleMembership {
            revoke,
            roles,
            members,
        } = parse_one("GRANT developers TO alice GRANTED BY postgres;")
        else {
            panic!("expected RoleMembership")
        };
        assert!(!revoke);
        assert_eq!(roles, vec!["developers"]);
        assert_eq!(members, vec!["alice"]);
    }

    #[test]
    fn parses_revoke_role_membership() {
        let Statement::RoleMembership {
            revoke, members, ..
        } = parse_one("REVOKE developers FROM bob;")
        else {
            panic!("expected RoleMembership")
        };
        assert!(revoke);
        assert_eq!(members, vec!["bob"]);
    }

    #[test]
    fn parses_create_role() {
        let Statement::CreateRole(def) =
            parse_one("CREATE ROLE developers WITH NOLOGIN;")
        else {
            panic!("expected CreateRole")
        };
        assert_eq!(def.name, "developers");
        assert_eq!(def.options.login, Some(false));
    }

    #[test]
    fn parses_alter_role_options() {
        let Statement::AlterRole(def) = parse_one(
            "ALTER ROLE app_user WITH NOSUPERUSER INHERIT NOCREATEROLE \
             NOCREATEDB LOGIN NOREPLICATION NOBYPASSRLS \
             PASSWORD 'md5abc123' VALID UNTIL '2026-01-01 00:00:00+00' \
             CONNECTION LIMIT 5;",
        ) else {
            panic!("expected AlterRole")
        };
        assert_eq!(def.name, "app_user");
        assert_eq!(def.options.superuser, Some(false));
        assert_eq!(def.options.inherit, Some(true));
        assert_eq!(def.options.create_role, Some(false));
        assert_eq!(def.options.create_db, Some(false));
        assert_eq!(def.options.login, Some(true));
        assert_eq!(def.options.replication, Some(false));
        assert_eq!(def.options.bypass_rls, Some(false));
        assert_eq!(def.options.connection_limit, Some(5));
        assert_eq!(def.password, Some("md5abc123".into()));
        assert_eq!(def.valid_until, Some("2026-01-01 00:00:00+00".into()));
    }

    #[test]
    fn parses_role_setting() {
        let Statement::AlterRoleSetting { role, name, value } =
            parse_one("ALTER ROLE app_user SET search_path TO test, public;")
        else {
            panic!("expected AlterRoleSetting")
        };
        assert_eq!(role, "app_user");
        assert_eq!(name, "search_path");
        assert_eq!(value, vec!["test", "public"]);
    }

    #[test]
    fn role_setting_unquotes_mixed_case_name() {
        // pg_dump quotes mixed-case GUC names; the project stores the
        // bare identifier so it matches the settings name pattern
        let Statement::AlterRoleSetting { name, value, .. } =
            parse_one("ALTER ROLE app SET \"TimeZone\" TO 'UTC';")
        else {
            panic!("expected AlterRoleSetting")
        };
        assert_eq!(name, "TimeZone");
        assert_eq!(value, vec!["UTC"]);
    }

    #[test]
    fn database_scoped_setting_is_unsupported() {
        let statement = parse_one(
            "ALTER ROLE app_user IN DATABASE fixtures SET work_mem \
             TO '64MB';",
        );
        assert!(matches!(statement, Statement::Unsupported(_)));
    }
}
