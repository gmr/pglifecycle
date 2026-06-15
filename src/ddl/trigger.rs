//! Triggers (CreateTrigStmt)

use tree_sitter::Node;

use crate::ddl::object::string_value;
use crate::ddl::{NodeExt, Statement, qualified_name, unquote};
use crate::models::Trigger;

/// CREATE TRIGGER → (table, Trigger)
pub(crate) fn create_trigger(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let table = node
        .find("qualified_name")
        .ok_or_else(|| String::from("CREATE TRIGGER without a relation"))?;
    let table = qualified_name(&table, src)?;
    let name = node
        .child_of_kind("name")
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| String::from("CREATE TRIGGER without a name"))?;
    // a plain trigger nests the timing in TriggerActionTime, but a
    // CONSTRAINT TRIGGER puts the keyword (always AFTER) directly under
    // the statement, so match on the keyword anywhere in the node
    let when = if node.has("kw_instead") {
        Some("INSTEAD OF".to_string())
    } else if node.has("kw_before") {
        Some("BEFORE".to_string())
    } else if node.has("kw_after") {
        Some("AFTER".to_string())
    } else {
        None
    };
    let events: Vec<String> = node
        .find_all("TriggerOneEvent")
        .iter()
        .map(|event| {
            if event.has("kw_insert") {
                "INSERT".to_string()
            } else if event.has("kw_delete") {
                "DELETE".to_string()
            } else if event.has("kw_truncate") {
                "TRUNCATE".to_string()
            } else if let Some(columns) = event.child_of_kind("columnList") {
                format!("UPDATE OF {}", columns.text(src))
            } else {
                "UPDATE".to_string()
            }
        })
        .collect();
    // likewise FOR EACH ROW/STATEMENT is nested for a plain trigger but
    // bare for a CONSTRAINT TRIGGER (which is always FOR EACH ROW)
    let for_each = if node.has("kw_row") {
        Some("ROW".to_string())
    } else if node.has("kw_statement") {
        Some("STATEMENT".to_string())
    } else {
        None
    };
    let condition = node
        .child_of_kind("TriggerWhen")
        .and_then(|n| n.child_of_kind("a_expr"))
        .map(|n| n.text(src).to_string());
    let function = node
        .child_of_kind("func_name")
        .map(|n| n.text(src).to_string());
    let arguments: Vec<serde_json::Value> = node
        .find_all("TriggerFuncArg")
        .iter()
        .map(|arg| {
            if let Some(string) = arg.find("Sconst") {
                serde_json::Value::String(string_value(&string, src))
            } else if let Ok(number) = arg.text(src).parse::<i64>() {
                serde_json::Value::Number(number.into())
            } else {
                serde_json::Value::String(arg.text(src).to_string())
            }
        })
        .collect();
    Ok(Statement::CreateTrigger {
        table,
        trigger: Trigger {
            sql: None,
            name: Some(name),
            when,
            events: (!events.is_empty()).then_some(events),
            for_each,
            condition,
            function: function.map(|f| format!("{f}()")),
            arguments: (!arguments.is_empty()).then_some(arguments),
            comment: None,
        },
    })
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
    fn parses_create_trigger() {
        let Statement::CreateTrigger { table, trigger } = parse_one(
            "CREATE TRIGGER audit BEFORE INSERT OR UPDATE ON test.users \
             FOR EACH ROW EXECUTE FUNCTION test.audit_row();",
        ) else {
            panic!("expected CreateTrigger")
        };
        assert_eq!(table.to_string(), "test.users");
        assert_eq!(trigger.name, Some("audit".into()));
        assert_eq!(trigger.when, Some("BEFORE".into()));
        assert_eq!(
            trigger.events,
            Some(vec!["INSERT".into(), "UPDATE".into()])
        );
        assert_eq!(trigger.for_each, Some("ROW".into()));
        assert_eq!(trigger.function, Some("test.audit_row()".into()));
    }

    #[test]
    fn constraint_trigger_captures_when_and_for_each() {
        // a CONSTRAINT TRIGGER carries the timing/for-each keywords bare
        // (no TriggerActionTime/TriggerForSpec); they must still be
        // captured or the trigger fails schema validation
        let Statement::CreateTrigger { trigger, .. } = parse_one(
            "CREATE CONSTRAINT TRIGGER emit AFTER INSERT OR UPDATE \
             ON test.accounts FOR EACH ROW EXECUTE FUNCTION test.emit();",
        ) else {
            panic!("expected CreateTrigger")
        };
        assert_eq!(trigger.when, Some("AFTER".into()));
        assert_eq!(trigger.for_each, Some("ROW".into()));
        assert_eq!(
            trigger.events,
            Some(vec!["INSERT".into(), "UPDATE".into()])
        );
    }

    #[test]
    fn parses_trigger_condition_and_arguments() {
        let Statement::CreateTrigger { trigger, .. } = parse_one(
            "CREATE TRIGGER t AFTER DELETE ON x FOR EACH STATEMENT \
             WHEN (OLD.id IS NOT NULL) \
             EXECUTE FUNCTION f(1, 'two');",
        ) else {
            panic!("expected CreateTrigger")
        };
        assert_eq!(trigger.when, Some("AFTER".into()));
        assert_eq!(trigger.for_each, Some("STATEMENT".into()));
        assert_eq!(trigger.condition, Some("OLD.id IS NOT NULL".into()));
        assert_eq!(trigger.arguments, Some(vec![json!(1), json!("two")]));
    }
}
