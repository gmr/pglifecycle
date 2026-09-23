//! Row-level security policies (CreatePolicyStmt)

use tree_sitter::Node;

use crate::ddl::{NodeExt, Statement, qualified_name, unquote, unquote_role};
use crate::models::Policy;

/// CREATE POLICY → (table, Policy)
pub(crate) fn create_policy(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .child_of_kind("name")
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| String::from("CREATE POLICY without a name"))?;
    let table = node
        .child_of_kind("qualified_name")
        .ok_or_else(|| String::from("CREATE POLICY without a relation"))?;
    let table = qualified_name(&table, src)?;
    // AS PERMISSIVE / AS RESTRICTIVE: the kind is a plain identifier,
    // not a keyword, in the grammar
    let restrictive = node
        .child_of_kind("RowSecurityDefaultPermissive")
        .and_then(|n| n.child_of_kind("identifier"))
        .is_some_and(|n| n.text(src).eq_ignore_ascii_case("restrictive"))
        .then_some(true);
    let command = node
        .child_of_kind("RowSecurityDefaultForCmd")
        .and_then(|n| n.child_of_kind("row_security_cmd"))
        .map(|n| n.text(src).to_uppercase())
        .filter(|command| command != "ALL");
    let roles: Vec<String> = node
        .child_of_kind("RowSecurityDefaultToRole")
        .map(|n| {
            n.find_all("RoleSpec")
                .iter()
                .map(|role| unquote_role(role.text(src)))
                .collect()
        })
        .unwrap_or_default();
    let roles = (!roles.is_empty() && roles != ["PUBLIC"]).then_some(roles);
    let expression = |kind: &str| {
        node.child_of_kind(kind)
            .and_then(|n| n.child_of_kind("a_expr"))
            .map(|n| n.text(src).to_string())
    };
    Ok(Statement::CreatePolicy {
        table,
        policy: Policy {
            name,
            restrictive,
            command,
            roles,
            using: expression("RowSecurityOptionalExpr"),
            with_check: expression("RowSecurityOptionalWithCheck"),
            comment: None,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::Parser;

    fn parse_one(sql: &str) -> (String, Policy) {
        let mut parser = Parser::new().unwrap();
        let mut statements = parser.parse(sql).unwrap();
        assert_eq!(statements.len(), 1, "expected one statement");
        let Statement::CreatePolicy { table, policy } = statements.remove(0)
        else {
            panic!("expected CreatePolicy")
        };
        (table.to_string(), policy)
    }

    #[test]
    fn parses_every_clause() {
        let (table, policy) = parse_one(
            "CREATE POLICY \"Read Own\" ON public.accounts AS RESTRICTIVE \
             FOR UPDATE TO alice, \"Bob\" USING ((owner = CURRENT_USER)) \
             WITH CHECK ((tenant = 1));",
        );
        assert_eq!(table, "public.accounts");
        assert_eq!(
            policy,
            Policy {
                name: String::from("Read Own"),
                restrictive: Some(true),
                command: Some(String::from("UPDATE")),
                roles: Some(vec![String::from("alice"), String::from("Bob")]),
                using: Some(String::from("(owner = CURRENT_USER)")),
                with_check: Some(String::from("(tenant = 1)")),
                comment: None,
            }
        );
    }

    #[test]
    fn omits_the_defaults() {
        let (_, policy) = parse_one(
            "CREATE POLICY p ON t AS PERMISSIVE FOR ALL TO PUBLIC \
             USING (true);",
        );
        assert_eq!(policy.restrictive, None);
        assert_eq!(policy.command, None);
        assert_eq!(policy.roles, None);
        assert_eq!(policy.with_check, None);
    }
}
