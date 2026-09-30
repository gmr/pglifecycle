//! Aggregates: a changed aggregate is made again with `CREATE OR
//! REPLACE AGGREGATE`. That statement cannot change the result type or
//! the kind of the aggregate, or rename an argument, so a change to
//! the state type, the final function, its extra arguments, the
//! arguments or `HYPOTHETICAL` drops the aggregate and makes it again.
//! A changed list of input types is a different aggregate (see
//! `diff::object_identity`).

use super::names::{argument_type, name, operator, type_name};
use super::{Resolution, comment_delta};
use crate::build::render_aggregate_signature;
use crate::models::{Aggregate, Argument};
use crate::utils::quote_ident;

pub(super) fn aggregate(repo: &Aggregate, db: &Aggregate) -> Resolution {
    let (r, d) = (canonical(repo), canonical(db));
    if r.arguments != d.arguments
        || r.order_by != d.order_by
        || r.state_data_type != d.state_data_type
        || r.ffunc != d.ffunc
        || r.finalfunc_extra != d.finalfunc_extra
        || r.hypothetical != d.hypothetical
    {
        return Resolution::Replace;
    }
    Resolution::OrReplace {
        comment: comment_delta(
            "AGGREGATE",
            &target(repo),
            &repo.comment,
            &db.comment,
        ),
        then: Vec::new(),
    }
}

/// The aggregate as PostgreSQL keeps it: canonical names and types,
/// and no option at its default. pg_dump does not write an option at
/// its default, and does not write the options of a final function
/// that the aggregate does not have. The default of `finalfunc_modify`
/// is `READ_ONLY` for a normal aggregate and `READ_WRITE` for an
/// ordered-set one; that of `mfinalfunc_modify` is `READ_ONLY`, as
/// only a normal aggregate has a moving mode.
pub(crate) fn canonical(aggregate: &Aggregate) -> Aggregate {
    let names = |value: &Option<String>| value.as_deref().map(name);
    let ordered = aggregate.order_by.is_some();
    let modify =
        |value: &Option<String>, function: &Option<String>, ordered| {
            let default = if ordered { "READ_WRITE" } else { "READ_ONLY" };
            value
                .as_deref()
                .map(str::to_uppercase)
                .filter(|value| value != default && function.is_some())
        };
    let set = |value: Option<bool>| value.filter(|v| *v);
    let size = |value: Option<i64>| value.filter(|v| *v != 0);
    Aggregate {
        arguments: arguments(&aggregate.arguments),
        order_by: aggregate.order_by.as_deref().map(arguments),
        sfunc: name(&aggregate.sfunc),
        state_data_type: type_name(&aggregate.state_data_type),
        state_data_size: size(aggregate.state_data_size),
        ffunc: names(&aggregate.ffunc),
        finalfunc_extra: set(aggregate.finalfunc_extra)
            .filter(|_| aggregate.ffunc.is_some()),
        finalfunc_modify: modify(
            &aggregate.finalfunc_modify,
            &aggregate.ffunc,
            ordered,
        ),
        combinefunc: names(&aggregate.combinefunc),
        serialfunc: names(&aggregate.serialfunc),
        deserialfunc: names(&aggregate.deserialfunc),
        msfunc: names(&aggregate.msfunc),
        minvfunc: names(&aggregate.minvfunc),
        mstate_data_type: aggregate.mstate_data_type.as_deref().map(type_name),
        mstate_data_size: size(aggregate.mstate_data_size),
        mffunc: names(&aggregate.mffunc),
        mfinalfunc_extra: set(aggregate.mfinalfunc_extra)
            .filter(|_| aggregate.mffunc.is_some()),
        mfinalfunc_modify: modify(
            &aggregate.mfinalfunc_modify,
            &aggregate.mffunc,
            false,
        ),
        sort_operator: aggregate.sort_operator.as_deref().map(operator),
        parallel: aggregate
            .parallel
            .as_deref()
            .map(str::to_uppercase)
            .filter(|parallel| parallel != "UNSAFE"),
        hypothetical: set(aggregate.hypothetical),
        ..aggregate.clone()
    }
}

/// Arguments with canonical types, and no mode for `IN`, the default
fn arguments(arguments: &[Argument]) -> Vec<Argument> {
    arguments
        .iter()
        .map(|argument| Argument {
            data_type: argument_type(&argument.data_type),
            mode: argument
                .mode
                .as_deref()
                .map(str::to_uppercase)
                .filter(|mode| mode != "IN"),
            name: argument.name.clone(),
        })
        .collect()
}

/// The qualified name and the signature, which COMMENT ON AGGREGATE
/// and DROP AGGREGATE read
fn target(aggregate: &Aggregate) -> String {
    format!(
        "{}.{} {}",
        quote_ident(&aggregate.schema),
        quote_ident(&aggregate.name),
        render_aggregate_signature(aggregate)
    )
}

/// The DROP statement of an aggregate that only the database has
pub(crate) fn drop(aggregate: &Aggregate) -> String {
    format!("DROP AGGREGATE IF EXISTS {};\n", target(aggregate))
}

/// The name that pg_dump gives an aggregate in its archive tag: all
/// its input types, with no ORDER BY, or `*` for none
pub(crate) fn tag_name(aggregate: &Aggregate) -> String {
    let types: Vec<String> = aggregate
        .arguments
        .iter()
        .chain(aggregate.order_by.iter().flatten())
        .map(|argument| argument_type(&argument.data_type))
        .collect();
    if types.is_empty() {
        format!("{}(*)", aggregate.name)
    } else {
        format!("{}({})", aggregate.name, types.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn parse(value: serde_json::Value) -> Aggregate {
        serde_json::from_value(value).expect("aggregate deserializes")
    }

    #[test]
    fn short_forms_are_canonical() {
        // as pull writes it
        let pulled = parse(json!({
            "name": "gate_max", "schema": "test", "owner": "postgres",
            "arguments": [{"data_type": "integer"}],
            "sfunc": "int4larger", "state_data_type": "integer",
            "ffunc": "int4abs", "sort_operator": "OPERATOR(pg_catalog.>)",
        }));
        // as a person writes it
        let written = parse(json!({
            "name": "gate_max", "schema": "test", "owner": "app",
            "arguments": [{"data_type": "INT4", "mode": "IN"}],
            "sfunc": "pg_catalog.int4larger", "state_data_type": "INT",
            "state_data_size": 0, "ffunc": "PG_CATALOG.INT4ABS",
            "finalfunc_extra": false, "finalfunc_modify": "READ_ONLY",
            "sort_operator": ">", "parallel": "UNSAFE",
            "hypothetical": false,
        }));
        assert_eq!(
            canonical(&written),
            Aggregate {
                owner: String::from("app"),
                ..canonical(&pulled)
            }
        );
    }

    #[test]
    fn an_ordered_set_aggregate_reads_and_writes_by_default() {
        let aggregate = |modify: &str| {
            parse(json!({
                "name": "pick", "schema": "test", "owner": "postgres",
                "arguments": [{"data_type": "integer"}],
                "order_by": [{"data_type": "integer"}],
                "sfunc": "test.add_ints", "state_data_type": "integer",
                "ffunc": "test.add_ints", "finalfunc_modify": modify,
            }))
        };
        assert_eq!(canonical(&aggregate("READ_WRITE")).finalfunc_modify, None);
        assert_eq!(
            canonical(&aggregate("READ_ONLY"))
                .finalfunc_modify
                .as_deref(),
            Some("READ_ONLY")
        );
    }

    #[test]
    fn the_modify_option_needs_a_final_function() {
        let aggregate = parse(json!({
            "name": "a", "schema": "test", "owner": "postgres",
            "arguments": [{"data_type": "integer"}],
            "sfunc": "int4pl", "state_data_type": "integer",
            "finalfunc_modify": "SHAREABLE", "finalfunc_extra": true,
        }));
        let canonical = canonical(&aggregate);
        assert_eq!(canonical.finalfunc_modify, None);
        assert_eq!(canonical.finalfunc_extra, None);
    }

    fn sum(initial: &str, state: &str) -> Aggregate {
        parse(json!({
            "name": "sum_ints", "schema": "test", "owner": "postgres",
            "arguments": [{"data_type": "integer"}],
            "sfunc": "test.add_ints", "state_data_type": state,
            "initial_condition": initial, "comment": "Adds integers",
        }))
    }

    #[test]
    fn a_changed_option_uses_or_replace() {
        let Resolution::OrReplace { comment, .. } =
            aggregate(&sum("0", "integer"), &sum("1", "integer"))
        else {
            panic!("expected OR REPLACE");
        };
        assert_eq!(comment, None);
        let db = Aggregate {
            comment: None,
            ..sum("0", "integer")
        };
        let Resolution::OrReplace { comment, .. } =
            aggregate(&sum("0", "integer"), &db)
        else {
            panic!("expected OR REPLACE");
        };
        assert_eq!(
            comment.as_deref(),
            Some(
                "COMMENT ON AGGREGATE test.sum_ints (IN integer) IS \
                 $$Adds integers$$;\n"
            )
        );
    }

    #[test]
    fn a_changed_result_type_replaces() {
        assert!(matches!(
            aggregate(&sum("0", "integer"), &sum("0", "bigint")),
            Resolution::Replace
        ));
    }

    #[test]
    fn the_tag_has_all_input_types() {
        let sorted = parse(json!({
            "name": "sum_sorted", "schema": "test", "owner": "postgres",
            "arguments": [{"data_type": "integer"}],
            "order_by": [{"data_type": "INT4"}],
            "sfunc": "test.add_ints", "state_data_type": "integer",
        }));
        assert_eq!(tag_name(&sorted), "sum_sorted(integer, integer)");
        assert_eq!(
            drop(&sorted),
            "DROP AGGREGATE IF EXISTS test.sum_sorted (IN integer ORDER BY \
             IN INT4);\n"
        );
        let count = parse(json!({
            "name": "Count", "schema": "Quoted Schema", "owner": "postgres",
            "arguments": [], "sfunc": "int8inc", "state_data_type": "int8",
        }));
        assert_eq!(tag_name(&count), "Count(*)");
        assert_eq!(
            drop(&count),
            "DROP AGGREGATE IF EXISTS \"Quoted Schema\".\"Count\" (*);\n"
        );
    }
}
