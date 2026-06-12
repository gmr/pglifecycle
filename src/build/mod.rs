//! Build a pg_restore-compatible archive from a project (ports dump.py)
//!
//! Rendering is bug-for-bug faithful to the Python implementation so
//! the Phase 2 parity gate can compare archives entry-by-entry, except
//! where the Python output was unambiguously broken SQL. Those fixes
//! are listed in tests/build_parity.rs as explicit deviations:
//!
//! 1. BYPASSRLS for roles/users renders from `bypass_rls` (Python read
//!    `create_db`)
//! 2. Primary keys render from the YAML value (Python's loader turned
//!    them into an empty list and silently dropped them)
//! 3. Unique constraint INCLUDE columns render as `INCLUDE (...)`
//! 4. Foreign keys render `ON DELETE` / `ON UPDATE` (Python emitted
//!    `ON_DELETE` / `ON_UPDATE`)
//! 5. Text search entries are tagged with the object name and proper
//!    option clauses (Python tagged them with the comment text and
//!    interpolated Python list reprs into the SQL)
//! 6. Event trigger filters render `WHEN TAG IN ('x', 'y')` (Python
//!    interpolated a Python list repr)
//! 7. Table column collations render `COLLATE x` (Python crashed)
//! 8. Base/range type options render from their own fields (Python
//!    read `receive` for SEND and `subtype` for SUBTYPE_OPCLASS)
//! 9. String DEFAULT values that look like SQL expressions render raw
//!    (Python quoted every string, e.g. `DEFAULT 'uuid_generate_v4()'`)
//! 10. CREATE INDEX names render unqualified (Python emitted
//!     `CREATE INDEX schema.name`, which PostgreSQL rejects)
//! 11. Roles with `create: false` (e.g. PUBLIC) emit no entry (Python
//!     emitted CREATE ROLE anyway)

mod acls;

use std::collections::HashMap;
use std::path::Path;

use serde_json::{Map, Value};

use crate::models::{
    Column, ConstraintColumns, Definition, Index, Item, RoleOptions,
    TablePartitionColumn, Trigger, ViewColumn,
};
use crate::project::Project;
use crate::utils::{postgres_value, quote_ident, raw_value};

pub fn build(project: &Project, destination: &Path) -> Result<(), String> {
    log::info!(
        "Saving build artifact to {} for {}",
        destination.display(),
        project.name
    );
    let dump = libpgdump::new(&project.name, &project.encoding, "18.0")
        .map_err(|e| e.to_string())?;
    let mut builder = Builder {
        dump,
        dump_id_map: HashMap::new(),
        superuser: project.superuser.clone(),
    };
    for item in &project.inventory {
        builder.dump_item(item)?;
    }
    acls::dump_acls(&mut builder, project)?;
    // record inventory dependency edges on the entries so the weighted
    // pg_dump topological sort in libpgdump (run by save) can order
    // them; the Python implementation instead pre-ordered its adds with
    // a flat toposort and recorded no edges
    for item in &project.inventory {
        if item.dependencies.is_empty() {
            continue;
        }
        let deps: Vec<i32> = item
            .dependencies
            .iter()
            .filter_map(|dep| builder.dump_id_map.get(dep).copied())
            .collect();
        if let Some(dump_id) = builder.dump_id_map.get(&item.id)
            && let Some(entry) = builder.dump.get_entry_mut(*dump_id)
        {
            entry.dependencies.extend(deps);
        }
    }
    builder
        .dump
        .save(destination)
        .map_err(|e| format!("failed to save archive: {e}"))?;
    log::debug!(
        "Saved pg_dump -Fc compatible dump to {} with {} entries",
        destination.display(),
        builder.dump.entries().len(),
    );
    Ok(())
}

struct Builder {
    dump: libpgdump::Dump,
    dump_id_map: HashMap<usize, i32>,
    superuser: String,
}

impl Builder {
    fn dump_item(&mut self, item: &Item) -> Result<(), String> {
        match &item.definition {
            Definition::Aggregate(_) => self.dump_aggregate(item),
            Definition::Cast(_) => self.dump_cast(item),
            Definition::Collation(_) => self.dump_collation(item),
            Definition::Conversion(_) => self.dump_conversion(item),
            Definition::Domain(_) => self.dump_domain(item),
            Definition::EventTrigger(_) => self.dump_event_trigger(item),
            Definition::Extension(_) => self.dump_extension(item),
            Definition::ForeignDataWrapper(_) => self.dump_fdw(item),
            Definition::Function(_) => self.dump_function(item),
            Definition::Group(_) => self.dump_group(item),
            Definition::Language(_) => self.dump_language(item),
            Definition::MaterializedView(_) => {
                self.dump_materialized_view(item)
            }
            Definition::Operator(_) => self.dump_operator(item),
            Definition::Publication(_) => self.dump_publication(item),
            Definition::Role(_) => self.dump_role(item),
            Definition::Schema(_) => self.dump_schema(item),
            Definition::Sequence(_) => self.dump_sequence(item),
            Definition::Server(_) => self.dump_server(item),
            Definition::Subscription(_) => self.dump_subscription(item),
            Definition::Table(_) => self.dump_table(item),
            Definition::Tablespace(_) => self.dump_tablespace(item),
            Definition::TextSearch(_) => self.dump_text_search(item),
            Definition::Type(_) => self.dump_type(item),
            Definition::User(_) => self.dump_user(item),
            Definition::UserMapping(_) => self.dump_user_mapping(item),
            Definition::View(_) => self.dump_view(item),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn add_entry(
        &mut self,
        desc: &str,
        namespace: &str,
        tag: &str,
        owner: &str,
        defn: &[String],
        drop_stmt: &[String],
        dependencies: &[i32],
        tablespace: Option<&str>,
    ) -> Result<i32, String> {
        log::debug!("Adding {desc} {namespace}.{tag}");
        let dump_id = self
            .dump
            .add_entry(
                libpgdump::ObjectType::from(desc),
                Some(namespace),
                Some(tag),
                Some(owner),
                Some(&format!("{};\n", defn.join(" "))),
                Some(&format!("{};\n", drop_stmt.join(" "))),
                None,
                dependencies,
            )
            .map_err(|e| format!("failed to add {desc} {tag}: {e}"))?;
        if let Some(tablespace) = tablespace
            && let Some(entry) = self.dump.get_entry_mut(dump_id)
        {
            entry.tablespace = Some(tablespace.to_string());
        }
        Ok(dump_id)
    }

    /// Add the entry for an inventory item plus its comment entry
    /// (ports _add_item)
    fn add_item(
        &mut self,
        item: &Item,
        defn: Vec<String>,
        drop_stmt: Vec<String>,
        no_owner: bool,
    ) -> Result<(), String> {
        let namespace =
            item.definition.schema().unwrap_or_default().to_string();
        let tag = item.definition.name();
        let owner = match item.definition.owner() {
            Some(owner) => owner.to_string(),
            None if no_owner => String::new(),
            None => self.superuser.clone(),
        };
        let tablespace = item.definition.tablespace().map(str::to_string);
        let dump_id = self.add_entry(
            item.desc.as_str(),
            &namespace,
            &tag,
            &owner,
            &defn,
            &drop_stmt,
            &[],
            tablespace.as_deref(),
        )?;
        self.dump_id_map.insert(item.id, dump_id);
        if let Some(comment) = item.definition.comment() {
            self.add_comment(
                item.desc.as_str(),
                &namespace,
                &tag,
                &owner,
                dump_id,
                comment,
            )?;
        }
        Ok(())
    }

    /// Add a COMMENT ON entry tied to its parent (ports _add_comment)
    fn add_comment(
        &mut self,
        desc: &str,
        namespace: &str,
        tag: &str,
        owner: &str,
        parent_dump_id: i32,
        comment: &str,
    ) -> Result<(), String> {
        // extensions record the schema they install into as their
        // namespace, but COMMENT ON EXTENSION takes an unqualified name
        let name = if desc == "EXTENSION" {
            quote_ident(tag)
        } else if namespace.is_empty() {
            tag.to_string()
        } else {
            format!("{namespace}.{tag}")
        };
        let defn = vec![
            String::from("COMMENT"),
            String::from("ON"),
            desc.to_string(),
            name,
            String::from("IS"),
            format!("$${comment}$$;\n"),
        ];
        self.add_entry(
            "COMMENT",
            namespace,
            tag,
            owner,
            &defn,
            &[],
            &[parent_dump_id],
            None,
        )?;
        Ok(())
    }

    /// `schema.name` with identifier quoting (ports _item_name)
    fn item_name(&self, item: &Item) -> String {
        let name = item.definition.name();
        match item.definition.schema() {
            Some(schema) if !schema.is_empty() => {
                format!("{}.{}", quote_ident(schema), quote_ident(&name))
            }
            _ => quote_ident(&name),
        }
    }

    fn dump_aggregate(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Aggregate(d) = &item.definition else {
            unreachable!()
        };
        if let Some(sql) = &d.sql {
            return self.add_item(item, vec![sql.clone()], vec![], false);
        }
        let mut create =
            vec!["CREATE".into(), "AGGREGATE".into(), self.item_name(item)];
        let args: Vec<String> = d
            .arguments
            .iter()
            .map(|a| {
                let mut arg = vec![a.mode.clone().unwrap_or("IN".into())];
                if let Some(name) = &a.name {
                    arg.push(name.clone());
                }
                arg.push(a.data_type.clone());
                arg.join(" ")
            })
            .collect();
        create.push(format!("({})", args.join(", ")));
        let mut options = vec![
            format!("SFUNC = {}", d.sfunc),
            format!("STYPE = {}", d.state_data_type),
        ];
        if let Some(v) = d.state_data_size {
            options.push(format!("SSPACE = {v}"));
        }
        if let Some(v) = &d.ffunc {
            options.push(format!("FINALFUNC = {v}"));
        }
        if d.finalfunc_extra == Some(true) {
            options.push("FINALFUNC_EXTRA = True".into());
        }
        if let Some(v) = &d.finalfunc_modify {
            options.push(format!("FINALFUNC_MODIFY = {v}"));
        }
        if let Some(v) = &d.combinefunc {
            options.push(format!("COMBINEFUNC = {v}"));
        }
        if let Some(v) = &d.serialfunc {
            options.push(format!("SERIALFUNC = {v}"));
        }
        if let Some(v) = &d.deserialfunc {
            options.push(format!("DESERIALFUNC = {v}"));
        }
        if let Some(v) = &d.initial_condition {
            options.push(format!("INITCOND = {v}"));
        }
        if let Some(v) = &d.msfunc {
            options.push(format!("MSFUNC = {v}"));
        }
        if let Some(v) = &d.minvfunc {
            options.push(format!("MINVFUNC = {v}"));
        }
        if let Some(v) = &d.mstate_data_type {
            options.push(format!("MSTYPE = {v}"));
        }
        if let Some(v) = d.mstate_data_size {
            options.push(format!("MSSPACE = {v}"));
        }
        if let Some(v) = &d.mffunc {
            options.push(format!("MFINALFUNC = {v}"));
        }
        if d.mfinalfunc_extra == Some(true) {
            options.push("MFINALFUNC_EXTRA".into());
        }
        if let Some(v) = &d.mfinalfunc_modify {
            options.push(format!("MFINALFUNC_MODIFY = {v}"));
        }
        if let Some(v) = &d.minitial_condition {
            options.push(format!("MINITCOND = {v}"));
        }
        if let Some(v) = &d.sort_operator {
            options.push(format!("SORTOP = {v}"));
        }
        if let Some(v) = &d.parallel {
            options.push(format!("PARALLEL = {v}"));
        }
        if d.hypothetical == Some(true) {
            options.push("HYPOTHETICAL".into());
        }
        create.push(format!("({})", options.join(", ")));
        let drop = vec![
            "DROP AGGREGATE IF EXISTS".into(),
            self.item_name(item),
            format!("({})", args.join(", ")),
        ];
        self.add_item(item, create, drop, false)
    }

    fn dump_cast(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Cast(d) = &item.definition else {
            unreachable!()
        };
        if let Some(sql) = &d.sql {
            return self.add_item(item, vec![sql.clone()], vec![], false);
        }
        let name = format!(
            "({} AS {})",
            quote_ident(d.source_type.as_deref().unwrap_or_default()),
            quote_ident(d.target_type.as_deref().unwrap_or_default())
        );
        let mut create = vec!["CREATE".into(), "CAST".into(), name.clone()];
        if let Some(function) = &d.function {
            create.push("WITH FUNCTION".into());
            create.push(function.clone());
        } else if d.inout == Some(true) {
            create.push("WITH INOUT".into());
        } else {
            create.push("WITHOUT FUNCTION".into());
        }
        if d.assignment == Some(true) {
            create.push("AS ASSIGNMENT".into());
        }
        if d.implicit == Some(true) {
            create.push("AS IMPLICIT".into());
        }
        let drop = vec!["DROP CAST IF EXISTS".into(), name];
        self.add_item(item, create, drop, false)
    }

    fn dump_collation(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Collation(d) = &item.definition else {
            unreachable!()
        };
        if let Some(sql) = &d.sql {
            return self.add_item(item, vec![sql.clone()], vec![], false);
        }
        let mut create =
            vec!["CREATE".into(), "COLLATION".into(), self.item_name(item)];
        if let Some(copy_from) = &d.copy_from {
            create.push("FROM".into());
            create.push(copy_from.clone());
        } else {
            let mut options = Vec::new();
            if let Some(v) = &d.locale {
                options.push(format!("LOCALE = {v}"));
            }
            if let Some(v) = &d.lc_collate {
                options.push(format!("LC_COLLATE = {v}"));
            }
            if let Some(v) = &d.lc_ctype {
                options.push(format!("LC_CTYPE = {v}"));
            }
            if let Some(v) = &d.provider {
                options.push(format!("PROVIDER = {v}"));
            }
            if d.deterministic == Some(true) {
                options.push("DETERMINISTIC = True".into());
            }
            if let Some(v) = &d.version {
                options.push(format!("VERSION = {v}"));
            }
            create.push(format!("({})", options.join(", ")));
        }
        let drop =
            vec!["DROP COLLATION IF EXISTS".into(), self.item_name(item)];
        self.add_item(item, create, drop, false)
    }

    fn dump_conversion(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Conversion(d) = &item.definition else {
            unreachable!()
        };
        let create = if let Some(sql) = &d.sql {
            vec![sql.clone()]
        } else {
            let mut create = vec!["CREATE".into()];
            if d.default == Some(true) {
                create.push("DEFAULT".into());
            }
            create.push("CONVERSION".into());
            create.push(self.item_name(item));
            create.push("FOR".into());
            create.push(d.encoding_from.clone().unwrap_or_default());
            create.push("TO".into());
            create.push(d.encoding_to.clone().unwrap_or_default());
            create.push("FROM".into());
            create.push(d.function.clone().unwrap_or_default());
            create
        };
        let drop =
            vec!["DROP CONVERSION IF EXISTS".into(), self.item_name(item)];
        self.add_item(item, create, drop, false)
    }

    fn dump_domain(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Domain(d) = &item.definition else {
            unreachable!()
        };
        if let Some(sql) = &d.sql {
            return self.add_item(item, vec![sql.clone()], vec![], false);
        }
        let mut create = vec![
            "CREATE".into(),
            "DOMAIN".into(),
            self.item_name(item),
            "AS".into(),
            d.data_type.clone().unwrap_or_default(),
        ];
        if let Some(collation) = &d.collation {
            create.push("COLLATE".into());
            create.push(collation.clone());
        }
        if let Some(default) = &d.default {
            create.push("DEFAULT".into());
            create.push(render_default(&Value::String(default.clone())));
        }
        if let Some(constraints) = &d.check_constraints {
            let mut rendered = Vec::new();
            for c in constraints {
                let mut value = vec![String::from("CONSTRAINT")];
                if let Some(name) = &c.name {
                    value.push(name.clone());
                }
                if let Some(nullable) = c.nullable {
                    value.push(
                        if nullable { "NULL" } else { "NOT NULL" }.into(),
                    );
                }
                if let Some(expression) = &c.expression {
                    value.push(format!("CHECK ({expression})"));
                }
                rendered.push(value.join(" "));
            }
            create.push(rendered.join(" "));
        }
        let drop = vec!["DROP DOMAIN IF EXISTS".into(), self.item_name(item)];
        self.add_item(item, create, drop, false)
    }

    fn dump_event_trigger(&mut self, item: &Item) -> Result<(), String> {
        let Definition::EventTrigger(d) = &item.definition else {
            unreachable!()
        };
        if let Some(sql) = &d.sql {
            return self.add_item(item, vec![sql.clone()], vec![], false);
        }
        let mut create = vec![
            "CREATE".into(),
            "EVENT TRIGGER".into(),
            self.item_name(item),
            "ON".into(),
            d.event.clone().unwrap_or_default(),
        ];
        if let Some(filter) = &d.filter {
            let tags: Vec<String> = filter
                .tags
                .iter()
                .map(|t| postgres_value(&Value::String(t.clone())))
                .collect();
            create.push(format!("WHEN TAG IN ({})", tags.join(", ")));
        }
        create.push("EXECUTE".into());
        create.push("FUNCTION".into());
        create.push(d.function.clone().unwrap_or_default());
        let drop =
            vec!["DROP EVENT TRIGGER IF EXISTS".into(), self.item_name(item)];
        self.add_item(item, create, drop, false)
    }

    fn dump_extension(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Extension(d) = &item.definition else {
            unreachable!()
        };
        let mut create = vec![
            "CREATE".into(),
            "EXTENSION".into(),
            "IF NOT EXISTS".into(),
            quote_ident(&d.name),
        ];
        if d.schema.is_some() || d.version.is_some() {
            create.push("WITH".into());
        }
        if let Some(schema) = &d.schema {
            create.push("SCHEMA".into());
            create.push(quote_ident(schema));
        }
        if let Some(version) = &d.version {
            create.push("VERSION".into());
            create.push(quote_ident(version));
        }
        if d.cascade == Some(true) {
            create.push("CASCADE".into());
        }
        let drop =
            vec!["DROP EXTENSION IF EXISTS".into(), quote_ident(&d.name)];
        self.add_item(item, create, drop, true)
    }

    fn dump_fdw(&mut self, item: &Item) -> Result<(), String> {
        let Definition::ForeignDataWrapper(d) = &item.definition else {
            unreachable!()
        };
        let mut create = vec![
            "CREATE".into(),
            "FOREIGN DATA WRAPPER".into(),
            self.item_name(item),
        ];
        if let Some(handler) = &d.handler {
            create.push("HANDLER".into());
            create.push(handler.clone());
        } else {
            create.push("NO HANDLER".into());
        }
        if let Some(validator) = &d.validator {
            create.push("VALIDATOR".into());
            create.push(validator.clone());
        } else {
            create.push("NO VALIDATOR".into());
        }
        if let Some(options) = &d.options {
            create.push(format!("OPTIONS ({})", render_options(options)));
        }
        let drop = vec![
            "DROP FOREIGN DATA WRAPPER IF EXISTS".into(),
            self.item_name(item),
        ];
        self.add_item(item, create, drop, false)
    }

    fn dump_function(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Function(d) = &item.definition else {
            unreachable!()
        };
        if let Some(sql) = &d.sql {
            return self.add_item(item, vec![sql.clone()], vec![], false);
        }
        let func_name = match &d.parameters {
            Some(parameters) if !parameters.is_empty() => {
                let params: Vec<String> = parameters
                    .iter()
                    .map(|p| {
                        let mut value = vec![p.mode.clone()];
                        if let Some(name) = &p.name {
                            value.push(name.clone());
                        }
                        value.push(p.data_type.clone());
                        if let Some(default) = &p.default {
                            value.push("=".into());
                            value.push(raw_value(default));
                        }
                        value.join(" ")
                    })
                    .collect();
                format!(
                    "{}({})",
                    d.name.split('(').next().unwrap_or_default(),
                    params.join(", ")
                )
            }
            _ => d.name.clone(),
        };
        let mut create = vec![
            "CREATE".into(),
            "FUNCTION".into(),
            func_name.clone(),
            "RETURNS".into(),
            d.returns.clone().unwrap_or_default(),
            "LANGUAGE".into(),
            d.language.clone().unwrap_or_default(),
        ];
        let drop = vec!["DROP FUNCTION IF EXISTS".into(), func_name];
        if let Some(transform_types) = &d.transform_types {
            let tts: Vec<String> = transform_types
                .iter()
                .map(|t| format!("FOR TYPE {t}"))
                .collect();
            create.push(format!("TRANSFORM {}", tts.join(", ")));
        }
        if d.window == Some(true) {
            create.push("WINDOW".into());
        }
        if d.immutable == Some(true) {
            create.push("IMMUTABLE".into());
        }
        if d.stable == Some(true) {
            create.push("STABLE".into());
        }
        if d.volatile == Some(true) {
            create.push("VOLATILE".into());
        }
        if let Some(leak_proof) = d.leak_proof {
            if !leak_proof {
                create.push("NOT".into());
            }
            create.push("LEAKPROOF".into());
        }
        match d.called_on_null_input {
            Some(true) => create.push("CALLED ON NULL INPUT".into()),
            Some(false) => create.push("RETURNS NULL ON NULL INPUT".into()),
            None => {}
        }
        if d.strict == Some(true) {
            create.push("STRICT".into());
        }
        if let Some(security) = &d.security {
            create.push("SECURITY".into());
            create.push(security.clone());
        }
        if let Some(parallel) = &d.parallel {
            create.push("PARALLEL".into());
            create.push(parallel.clone());
        }
        if let Some(cost) = d.cost {
            create.push("COST".into());
            create.push(cost.to_string());
        }
        if let Some(rows) = d.rows {
            create.push("ROWS".into());
            create.push(rows.to_string());
        }
        if let Some(support) = &d.support {
            create.push("SUPPORT".into());
            create.push(support.clone());
        }
        if let Some(configuration) = &d.configuration {
            for (k, v) in configuration {
                create.push(format!("SET {k} = {}", postgres_value(v)));
            }
        }
        create.push("AS".into());
        if let Some(definition) = &d.definition {
            let create_sql =
                vec![format!("{} $$\n{}\n$$", create.join(" "), definition)];
            return self.add_item(item, create_sql, drop, false);
        }
        if let (Some(object_file), Some(link_symbol)) =
            (&d.object_file, &d.link_symbol)
        {
            create.push(format!(
                "{}, {}",
                postgres_value(&Value::String(object_file.clone())),
                postgres_value(&Value::String(link_symbol.clone()))
            ));
        }
        self.add_item(item, create, drop, false)
    }

    fn dump_group(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Group(d) = &item.definition else {
            unreachable!()
        };
        let mut create =
            vec!["CREATE".into(), "GROUP".into(), quote_ident(&d.name)];
        if let Some(options) = &d.options {
            push_bool_option(&mut create, "CREATEDB", options.create_db);
            push_bool_option(&mut create, "CREATEROLE", options.create_role);
            push_bool_option(&mut create, "INHERIT", options.inherit);
            push_bool_option(&mut create, "SUPERUSER", options.superuser);
        }
        let drop = vec!["DROP GROUP IF EXISTS".into(), quote_ident(&d.name)];
        self.add_item(item, create, drop, false)
    }

    fn dump_language(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Language(d) = &item.definition else {
            unreachable!()
        };
        let mut create = vec!["CREATE".into()];
        if d.replace == Some(true) {
            create.push("OR REPLACE".into());
        }
        if d.trusted == Some(true) {
            create.push("TRUSTED".into());
        }
        create.push("LANGUAGE".into());
        create.push(self.item_name(item));
        if let Some(handler) = &d.handler {
            create.push("HANDLER".into());
            create.push(handler.clone());
        }
        if let Some(inline_handler) = &d.inline_handler {
            create.push("INLINE".into());
            create.push(inline_handler.clone());
        }
        if let Some(validator) = &d.validator {
            create.push("VALIDATOR".into());
            create.push(validator.clone());
        }
        let drop =
            vec!["DROP LANGUAGE IF EXISTS".into(), self.item_name(item)];
        self.add_item(item, create, drop, false)
    }

    fn dump_materialized_view(&mut self, item: &Item) -> Result<(), String> {
        let Definition::MaterializedView(d) = &item.definition else {
            unreachable!()
        };
        if let Some(sql) = &d.sql {
            return self.add_item(item, vec![sql.clone()], vec![], false);
        }
        let mut create = vec![
            "CREATE".into(),
            "MATERIALIZED VIEW".into(),
            self.item_name(item),
        ];
        if let Some(columns) = &d.columns {
            let names: Vec<&str> =
                columns.iter().map(view_column_name).collect();
            create.push(format!("({})", names.join(", ")));
        }
        if let Some(method) = &d.table_access_method {
            create.push("USING".into());
            create.push(method.clone());
        }
        if let Some(storage_parameters) = &d.storage_parameters {
            create.push("WITH".into());
            let params: Vec<String> = storage_parameters
                .iter()
                .map(|(k, v)| format!("{k} = {}", raw_value(v)))
                .collect();
            create.push(params.join(", "));
        }
        if let Some(tablespace) = &d.tablespace {
            create.push("TABLESPACE".into());
            create.push(tablespace.clone());
        }
        create.push("AS".into());
        create.push(d.query.clone().unwrap_or_default());
        let drop = vec![
            "DROP MATERIALIZED VIEW IF EXISTS".into(),
            self.item_name(item),
        ];
        self.add_item(item, create, drop, false)
    }

    fn dump_operator(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Operator(d) = &item.definition else {
            unreachable!()
        };
        if let Some(sql) = &d.sql {
            return self.add_item(item, vec![sql.clone()], vec![], false);
        }
        let name = format!("{}.{}", d.schema, d.name);
        let mut create =
            vec!["CREATE".into(), "OPERATOR".into(), name.clone()];
        let mut options = vec![format!("PROCEDURE = {}", d.function)];
        if let Some(v) = &d.left_arg {
            options.push(format!("LEFTARG = {v}"));
        }
        if let Some(v) = &d.right_arg {
            options.push(format!("RIGHTARG = {v}"));
        }
        if let Some(v) = &d.commutator {
            options.push(format!("COMMUTATOR = {v}"));
        }
        if let Some(v) = &d.negator {
            options.push(format!("NEGATOR = {v}"));
        }
        if let Some(v) = &d.restrict {
            options.push(format!("RESTRICT = {v}"));
        }
        if let Some(v) = &d.join {
            options.push(format!("JOIN = {v}"));
        }
        if d.hashes == Some(true) {
            options.push("HASHES".into());
        }
        if d.merges == Some(true) {
            options.push("MERGES".into());
        }
        create.push(format!("({})", options.join(", ")));
        let drop = vec![
            "DROP OPERATOR IF EXISTS".into(),
            name,
            format!(
                "({}, {})",
                d.left_arg.as_deref().unwrap_or("NONE"),
                d.right_arg.as_deref().unwrap_or("NONE")
            ),
        ];
        self.add_item(item, create, drop, false)
    }

    fn dump_publication(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Publication(d) = &item.definition else {
            unreachable!()
        };
        let mut create =
            vec!["CREATE".into(), "PUBLICATION".into(), self.item_name(item)];
        if d.all_tables == Some(true) {
            create.push("FOR ALL TABLES".into());
        } else {
            create.push("FOR".into());
            create.push("TABLE".into());
            create.push(d.tables.clone().unwrap_or_default().join(", "));
        }
        if let Some(parameters) = &d.parameters {
            create.push("WITH".into());
            create.push(render_parameters(parameters));
        }
        let drop =
            vec!["DROP PUBLICATION IF EXISTS".into(), self.item_name(item)];
        self.add_item(item, create, drop, false)
    }

    fn dump_role(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Role(d) = &item.definition else {
            unreachable!()
        };
        // create: false defines a role without creating it (e.g.
        // PUBLIC); deviation 11 — Python emitted CREATE ROLE anyway
        if d.create == Some(false) {
            return Ok(());
        }
        let mut create =
            vec!["CREATE".into(), "ROLE".into(), quote_ident(&d.name)];
        if let Some(options) = &d.options {
            push_role_options(&mut create, options, false);
        }
        let drop = if d.name == self.superuser {
            vec![]
        } else {
            vec!["DROP ROLE IF EXISTS".into(), quote_ident(&d.name)]
        };
        self.add_item(item, create, drop, false)
    }

    fn dump_schema(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Schema(d) = &item.definition else {
            unreachable!()
        };
        let mut create = vec![
            "CREATE".into(),
            "SCHEMA".into(),
            "IF NOT EXISTS".into(),
            self.item_name(item),
        ];
        if let Some(authorization) = &d.authorization {
            create.push("AUTHORIZATION".into());
            create.push(quote_ident(authorization));
        }
        let drop = vec!["DROP SCHEMA IF EXISTS".into(), self.item_name(item)];
        self.add_item(item, create, drop, false)
    }

    fn dump_sequence(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Sequence(d) = &item.definition else {
            unreachable!()
        };
        if let Some(sql) = &d.sql {
            return self.add_item(item, vec![sql.clone()], vec![], false);
        }
        let mut create =
            vec!["CREATE".into(), "SEQUENCE".into(), self.item_name(item)];
        if let Some(data_type) = &d.data_type {
            create.push("AS".into());
            create.push(data_type.clone());
        }
        if let Some(v) = d.increment_by {
            create.push("INCREMENT BY".into());
            create.push(v.to_string());
        }
        if let Some(v) = d.min_value {
            create.push("MINVALUE".into());
            create.push(v.to_string());
        }
        if let Some(v) = d.max_value {
            create.push("MAXVALUE".into());
            create.push(v.to_string());
        }
        if let Some(v) = d.start_with {
            create.push("START WITH".into());
            create.push(v.to_string());
        }
        if let Some(v) = d.cache {
            create.push("CACHE".into());
            create.push(v.to_string());
        }
        if let Some(cycle) = d.cycle {
            if !cycle {
                create.push("NO".into());
            }
            create.push("CYCLE".into());
        }
        if let Some(owned_by) = &d.owned_by {
            create.push("OWNED BY".into());
            create.push(owned_by.clone());
        }
        let drop =
            vec!["DROP SEQUENCE IF EXISTS".into(), self.item_name(item)];
        self.add_item(item, create, drop, false)
    }

    fn dump_server(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Server(d) = &item.definition else {
            unreachable!()
        };
        let mut create =
            vec!["CREATE".into(), "SERVER".into(), self.item_name(item)];
        if let Some(server_type) = &d.server_type {
            create.push("TYPE".into());
            create.push(postgres_value(&Value::String(server_type.clone())));
        }
        if let Some(version) = &d.version {
            create.push("VERSION".into());
            create.push(postgres_value(&Value::String(version.clone())));
        }
        create.push("FOREIGN DATA WRAPPER".into());
        create.push(d.foreign_data_wrapper.clone());
        if let Some(options) = &d.options {
            create.push(format!("OPTIONS {}", render_options(options)));
        }
        let drop = vec!["DROP SERVER IF EXISTS".into(), self.item_name(item)];
        self.add_item(item, create, drop, false)
    }

    fn dump_subscription(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Subscription(d) = &item.definition else {
            unreachable!()
        };
        let mut create = vec![
            "CREATE".into(),
            "SUBSCRIPTION".into(),
            self.item_name(item),
            "CONNECTION".into(),
            d.connection.clone(),
            "PUBLICATION".into(),
            d.publications.join(", "),
        ];
        if let Some(parameters) = &d.parameters {
            create.push("WITH".into());
            create.push(render_parameters(parameters));
        }
        let drop =
            vec!["DROP SUBSCRIPTION IF EXISTS".into(), self.item_name(item)];
        self.add_item(item, create, drop, false)
    }

    fn dump_table(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Table(d) = &item.definition else {
            unreachable!()
        };
        if let Some(sql) = &d.sql {
            self.add_item(item, vec![sql.clone()], vec![], false)?;
        } else {
            let mut create = vec!["CREATE".into()];
            if d.unlogged == Some(true) {
                create.push("UNLOGGED".into());
            }
            create.push("TABLE".into());
            create.push(self.item_name(item));
            create.push("(".into());
            if let Some(like_table) = &d.like_table {
                create.push("LIKE".into());
                create.push(like_table.name.clone());
                for (field, value) in [
                    ("comments", like_table.include_comments),
                    ("constraints", like_table.include_constraints),
                    ("defaults", like_table.include_defaults),
                    ("generated", like_table.include_generated),
                    ("identity", like_table.include_identity),
                    ("indexes", like_table.include_indexes),
                    ("statistics", like_table.include_statistics),
                    ("storage", like_table.include_storage),
                    ("all", like_table.include_all),
                ] {
                    if let Some(value) = value {
                        create.push(
                            if value { "INCLUDING" } else { "EXCLUDING" }
                                .into(),
                        );
                        create.push(format!("include_{field}"));
                    }
                }
            } else {
                let mut inner = Vec::new();
                for column in d.columns.as_deref().unwrap_or_default() {
                    inner.push(render_table_column(column));
                }
                for constraint in
                    d.unique_constraints.as_deref().unwrap_or_default()
                {
                    inner.push(render_constraint("UNIQUE", constraint));
                }
                if let Some(primary_key) = &d.primary_key {
                    inner.push(render_constraint("PRIMARY KEY", primary_key));
                }
                for fk in d.foreign_keys.as_deref().unwrap_or_default() {
                    let mut fk_sql = vec![
                        format!("FOREIGN KEY ({})", fk.columns.join(", ")),
                        "REFERENCES".into(),
                        fk.references.name.clone(),
                        format!("({})", fk.references.columns.join(", ")),
                    ];
                    if let Some(match_type) = &fk.match_type {
                        fk_sql.push("MATCH".into());
                        fk_sql.push(match_type.clone());
                    }
                    if let Some(on_delete) = &fk.on_delete
                        && on_delete != "NO ACTION"
                    {
                        fk_sql.push("ON DELETE".into());
                        fk_sql.push(on_delete.clone());
                    }
                    if let Some(on_update) = &fk.on_update
                        && on_update != "NO ACTION"
                    {
                        fk_sql.push("ON UPDATE".into());
                        fk_sql.push(on_update.clone());
                    }
                    if fk.deferrable == Some(true) {
                        fk_sql.push("DEFERRABLE".into());
                    }
                    if fk.initially_deferred == Some(true) {
                        fk_sql.push("INITIALLY DEFERRED".into());
                    }
                    inner.push(fk_sql.join(" "));
                }
                create.push(inner.join(", "));
                create.push(")".into());
            }
            if let Some(parents) = &d.parents {
                create.push(parents.join(", "));
            }
            if let Some(partition) = &d.partition {
                create.push("PARTITION BY".into());
                create.push(partition.partition_type.clone());
                create.push("(".into());
                let columns: Vec<String> = partition
                    .columns
                    .iter()
                    .map(render_partition_column)
                    .collect();
                create.push(columns.join(", "));
                create.push(")".into());
            }
            if let Some(access_method) = &d.access_method {
                create.push("USING".into());
                create.push(access_method.clone());
            }
            if let Some(storage_parameters) = &d.storage_parameters {
                create.push("WITH".into());
                let params: Vec<String> = storage_parameters
                    .iter()
                    .map(|(k, v)| format!("{k}={}", raw_value(v)))
                    .collect();
                create.push(params.join(", "));
            }
            if let Some(tablespace) = &d.tablespace {
                create.push("TABLESPACE".into());
                create.push(tablespace.clone());
            }
            let drop =
                vec!["DROP TABLE IF EXISTS".into(), self.item_name(item)];
            self.add_item(item, create, drop, false)?;
        }
        for index in d.indexes.as_deref().unwrap_or_default() {
            self.dump_index(index, item, d)?;
        }
        for trigger in d.triggers.as_deref().unwrap_or_default() {
            self.dump_trigger(trigger, item, d)?;
        }
        Ok(())
    }

    fn dump_index(
        &mut self,
        index: &Index,
        parent: &Item,
        table: &crate::models::Table,
    ) -> Result<(), String> {
        // index names cannot be schema-qualified in CREATE INDEX; the
        // index lives in its table's schema (deviation 10 — Python
        // emitted `CREATE INDEX schema.name`, which does not parse)
        let qualified = format!(
            "{}.{}",
            quote_ident(&table.schema),
            quote_ident(&index.name)
        );
        let mut create = vec!["CREATE".into()];
        if index.unique == Some(true) {
            create.push("UNIQUE".into());
        }
        create.push("INDEX".into());
        create.push(quote_ident(&index.name));
        create.push("ON".into());
        if index.recurse == Some(false) {
            create.push("ONLY".into());
        }
        create.push(self.item_name(parent));
        if let Some(method) = &index.method {
            create.push("USING".into());
            create.push(method.clone());
        }
        create.push("(".into());
        let columns: Vec<String> = index
            .columns
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|c| {
                let mut sql = vec![
                    c.name
                        .clone()
                        .or_else(|| c.expression.clone())
                        .unwrap_or_default(),
                ];
                if let Some(collation) = &c.collation {
                    sql.push("COLLATION".into());
                    sql.push(collation.clone());
                }
                if let Some(opclass) = &c.opclass {
                    sql.push(opclass.clone());
                }
                if let Some(direction) = &c.direction {
                    sql.push(direction.clone());
                }
                if let Some(null_placement) = &c.null_placement {
                    sql.push("NULLS".into());
                    sql.push(null_placement.clone());
                }
                sql.join(" ")
            })
            .collect();
        create.push(columns.join(", "));
        create.push(")".into());
        if let Some(include) = &index.include {
            create.push(format!("INCLUDE ({})", include.join(", ")));
        }
        if let Some(storage_parameters) = &index.storage_parameters {
            create.push("WITH".into());
            let params: Vec<String> = storage_parameters
                .iter()
                .map(|(k, v)| format!("{k}={}", raw_value(v)))
                .collect();
            create.push(params.join(", "));
        }
        if let Some(tablespace) = &index.tablespace {
            create.push("TABLESPACE".into());
            create.push(tablespace.clone());
        }
        if let Some(where_clause) = &index.where_clause {
            create.push("WHERE".into());
            create.push(where_clause.clone());
        }
        let drop = vec!["DROP INDEX IF EXISTS".into(), qualified];
        let parent_dump_id = self.dump_id_map[&parent.id];
        let dump_id = self.add_entry(
            "INDEX",
            &table.schema,
            &index.name,
            &table.owner,
            &create,
            &drop,
            &[parent_dump_id],
            index.tablespace.as_deref(),
        )?;
        if let Some(comment) = &index.comment {
            self.add_comment(
                "INDEX",
                &table.schema,
                &index.name,
                &table.owner,
                dump_id,
                comment,
            )?;
        }
        Ok(())
    }

    fn dump_trigger(
        &mut self,
        trigger: &Trigger,
        parent: &Item,
        table: &crate::models::Table,
    ) -> Result<(), String> {
        let name = trigger.name.clone().unwrap_or_default();
        let (create, drop) = if let Some(sql) = &trigger.sql {
            (vec![sql.clone()], vec![])
        } else {
            let mut create = vec![
                "CREATE".into(),
                "TRIGGER".into(),
                name.clone(),
                trigger.when.clone().unwrap_or_default(),
                trigger.events.clone().unwrap_or_default().join(" OR "),
                "ON".into(),
                self.item_name(parent),
            ];
            if let Some(for_each) = &trigger.for_each {
                create.push("FOR EACH".into());
                create.push(for_each.clone());
            }
            if let Some(condition) = &trigger.condition {
                create.push("WHEN".into());
                create.push(condition.clone());
            }
            create.push("EXECUTE".into());
            create.push("FUNCTION".into());
            create.push(trigger.function.clone().unwrap_or_default());
            if let Some(arguments) = &trigger.arguments {
                let args: Vec<String> =
                    arguments.iter().map(raw_value).collect();
                create.push(format!("({})", args.join(", ")));
            }
            let drop = vec![
                "DROP TRIGGER IF EXISTS".into(),
                name.clone(),
                "ON".into(),
                self.item_name(parent),
            ];
            (create, drop)
        };
        let parent_dump_id = self.dump_id_map[&parent.id];
        let dump_id = self.add_entry(
            "TRIGGER",
            &table.schema,
            &name,
            &table.owner,
            &create,
            &drop,
            &[parent_dump_id],
            None,
        )?;
        if let Some(comment) = &trigger.comment {
            self.add_comment(
                "TRIGGER",
                &table.schema,
                &name,
                &table.owner,
                dump_id,
                comment,
            )?;
        }
        Ok(())
    }

    fn dump_tablespace(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Tablespace(d) = &item.definition else {
            unreachable!()
        };
        let mut create = vec![
            "CREATE".into(),
            "TABLESPACE".into(),
            self.item_name(item),
            "OWNER".into(),
            d.owner.clone(),
            "LOCATION".into(),
            d.location.clone(),
        ];
        if let Some(options) = &d.options {
            let opts: Vec<String> = options
                .iter()
                .map(|(k, v)| format!("{k}={}", postgres_value(v)))
                .collect();
            create.push(format!("WITH ({})", opts.join(",")));
        }
        let drop =
            vec!["DROP TABLESPACE IF EXISTS".into(), self.item_name(item)];
        self.add_item(item, create, drop, false)
    }

    fn dump_text_search(&mut self, item: &Item) -> Result<(), String> {
        let Definition::TextSearch(d) = &item.definition else {
            unreachable!()
        };
        for config in d.configurations.as_deref().unwrap_or_default() {
            let (create, drop) = if let Some(sql) = &config.sql {
                (vec![sql.clone()], vec![])
            } else {
                let value = if let Some(parser) = &config.parser {
                    format!("PARSER = {parser}")
                } else if let Some(source) = &config.source {
                    format!("SOURCE = {source}")
                } else {
                    return Err(format!(
                        "text search configuration {} has no parser or \
                         source",
                        config.name
                    ));
                };
                (
                    vec![
                        "CREATE".into(),
                        "TEXT SEARCH CONFIGURATION".into(),
                        quote_ident(&config.name),
                        format!("({value})"),
                    ],
                    vec![
                        "DROP TEXT SEARCH CONFIGURATION IF EXISTS".into(),
                        quote_ident(&config.name),
                    ],
                )
            };
            self.add_text_search_item(
                d,
                "TEXT SEARCH CONFIGURATION",
                &config.name,
                create,
                drop,
                config.comment.as_deref(),
            )?;
        }
        for dictionary in d.dictionaries.as_deref().unwrap_or_default() {
            let (create, drop) = if let Some(sql) = &dictionary.sql {
                (vec![sql.clone()], vec![])
            } else {
                let mut value = vec![format!(
                    "TEMPLATE = {}",
                    dictionary.template.clone().unwrap_or_default()
                )];
                if let Some(options) = &dictionary.options {
                    for (k, v) in options {
                        value.push(format!("{k} = {}", postgres_value(v)));
                    }
                }
                (
                    vec![
                        "CREATE".into(),
                        "TEXT SEARCH DICTIONARY".into(),
                        quote_ident(&dictionary.name),
                        format!("({})", value.join(", ")),
                    ],
                    vec![
                        "DROP TEXT SEARCH DICTIONARY IF EXISTS".into(),
                        quote_ident(&dictionary.name),
                    ],
                )
            };
            self.add_text_search_item(
                d,
                "TEXT SEARCH DICTIONARY",
                &dictionary.name,
                create,
                drop,
                dictionary.comment.as_deref(),
            )?;
        }
        for parser in d.parsers.as_deref().unwrap_or_default() {
            let (create, drop) = if let Some(sql) = &parser.sql {
                (vec![sql.clone()], vec![])
            } else {
                let mut value = vec![
                    format!(
                        "START = {}",
                        parser.start_function.clone().unwrap_or_default()
                    ),
                    format!(
                        "GETTOKEN = {}",
                        parser.gettoken_function.clone().unwrap_or_default()
                    ),
                    format!(
                        "END = {}",
                        parser.end_function.clone().unwrap_or_default()
                    ),
                    format!(
                        "LEXTYPES = {}",
                        parser.lextypes_function.clone().unwrap_or_default()
                    ),
                ];
                if let Some(headline) = &parser.headline_function {
                    value.push(format!("HEADLINE = {headline}"));
                }
                (
                    vec![
                        "CREATE".into(),
                        "TEXT SEARCH PARSER".into(),
                        quote_ident(&parser.name),
                        format!("({})", value.join(", ")),
                    ],
                    vec![
                        "DROP TEXT SEARCH PARSER IF EXISTS".into(),
                        quote_ident(&parser.name),
                    ],
                )
            };
            self.add_text_search_item(
                d,
                "TEXT SEARCH PARSER",
                &parser.name,
                create,
                drop,
                parser.comment.as_deref(),
            )?;
        }
        for template in d.templates.as_deref().unwrap_or_default() {
            let (create, drop) = if let Some(sql) = &template.sql {
                (vec![sql.clone()], vec![])
            } else {
                let mut value = Vec::new();
                if let Some(init) = &template.init_function {
                    value.push(format!("INIT = {init}"));
                }
                value.push(format!(
                    "LEXIZE = {}",
                    template.lexize_function.clone().unwrap_or_default()
                ));
                (
                    vec![
                        "CREATE".into(),
                        "TEXT SEARCH TEMPLATE".into(),
                        quote_ident(&template.name),
                        format!("({})", value.join(", ")),
                    ],
                    vec![
                        "DROP TEXT SEARCH TEMPLATE IF EXISTS".into(),
                        quote_ident(&template.name),
                    ],
                )
            };
            self.add_text_search_item(
                d,
                "TEXT SEARCH TEMPLATE",
                &template.name,
                create,
                drop,
                template.comment.as_deref(),
            )?;
        }
        Ok(())
    }

    fn add_text_search_item(
        &mut self,
        parent: &crate::models::TextSearch,
        desc: &str,
        name: &str,
        defn: Vec<String>,
        drop_stmt: Vec<String>,
        comment: Option<&str>,
    ) -> Result<(), String> {
        let dump_id = self.add_entry(
            desc,
            &parent.schema,
            name,
            &self.superuser.clone(),
            &defn,
            &drop_stmt,
            &[],
            None,
        )?;
        if let Some(comment) = comment {
            self.add_comment(
                desc,
                &parent.schema,
                name,
                &self.superuser.clone(),
                dump_id,
                comment,
            )?;
        }
        Ok(())
    }

    fn dump_type(&mut self, item: &Item) -> Result<(), String> {
        let Definition::Type(d) = &item.definition else {
            unreachable!()
        };
        if let Some(sql) = &d.sql {
            return self.add_item(item, vec![sql.clone()], vec![], false);
        }
        let mut create = vec![
            "CREATE".into(),
            "TYPE".into(),
            self.item_name(item),
            "AS".into(),
        ];
        match d.type_kind.as_deref() {
            Some("base") => {
                let mut options = vec![
                    format!("INPUT = {}", d.input.clone().unwrap_or_default()),
                    format!(
                        "OUTPUT = {}",
                        d.output.clone().unwrap_or_default()
                    ),
                ];
                if let Some(v) = &d.receive {
                    options.push(format!("RECEIVE = {v}"));
                }
                if let Some(v) = &d.send {
                    options.push(format!("SEND = {v}"));
                }
                if let Some(v) = &d.typmod_in {
                    options.push(format!("TYPMOD_IN = {v}"));
                }
                if let Some(v) = &d.typmod_out {
                    options.push(format!("TYPMOD_OUT = {v}"));
                }
                if let Some(v) = &d.analyze {
                    options.push(format!("ANALYZE = {v}"));
                }
                if let Some(v) = &d.internal_length {
                    options.push(format!("INTERNALLENGTH = {}", raw_value(v)));
                }
                if d.passed_by_value == Some(true) {
                    options.push("PASSEDBYVALUE".into());
                }
                if let Some(v) = &d.alignment {
                    options.push(format!("ALIGNMENT = {v}"));
                }
                if let Some(v) = &d.storage {
                    options.push(format!("STORAGE = {v}"));
                }
                if let Some(v) = &d.like_type {
                    options.push(format!("LIKE = {v}"));
                }
                if let Some(v) = &d.category {
                    options.push(format!(
                        "CATEGORY = {}",
                        postgres_value(&Value::String(v.clone()))
                    ));
                }
                if let Some(v) = &d.preferred {
                    options.push(format!("PREFERRED = {}", raw_value(v)));
                }
                if let Some(v) = &d.default {
                    options.push(format!("DEFAULT = {}", postgres_value(v)));
                }
                if let Some(v) = &d.element {
                    options.push(format!("ELEMENT = {v}"));
                }
                if let Some(v) = &d.delimiter {
                    options.push(format!(
                        "DELIMITER = {}",
                        postgres_value(&Value::String(v.clone()))
                    ));
                }
                if d.collatable == Some(true) {
                    options.push("COLLATABLE = True".into());
                }
                create.push(format!("({})", options.join(", ")));
            }
            Some("composite") => {
                let columns: Vec<String> = d
                    .columns
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|column| {
                        let mut col = vec![
                            column.name.clone(),
                            column.data_type.clone(),
                        ];
                        if let Some(collation) = &column.collation {
                            col.push("COLLATE".into());
                            col.push(collation.clone());
                        }
                        col.join(" ")
                    })
                    .collect();
                create.push(format!("({})", columns.join(", ")));
            }
            Some("enum") => {
                create.push("ENUM".into());
                let values: Vec<String> = d
                    .enum_values
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|e| postgres_value(&Value::String(e.clone())))
                    .collect();
                create.push(format!("({})", values.join(", ")));
            }
            Some("range") => {
                create.push("RANGE".into());
                let mut options = vec![format!(
                    "SUBTYPE = {}",
                    d.subtype.clone().unwrap_or_default()
                )];
                if let Some(v) = &d.subtype_opclass {
                    options.push(format!("SUBTYPE_OPCLASS = {v}"));
                }
                if let Some(v) = &d.collation {
                    options.push(format!("COLLATION = {v}"));
                }
                if let Some(v) = &d.canonical {
                    options.push(format!("CANONICAL = {v}"));
                }
                if let Some(v) = &d.subtype_diff {
                    options.push(format!("SUBTYPE_DIFF = {v}"));
                }
                create.push(format!("({})", options.join(", ")));
            }
            _ => {}
        }
        let drop = vec!["DROP TYPE IF EXISTS".into(), self.item_name(item)];
        self.add_item(item, create, drop, false)
    }

    fn dump_user(&mut self, item: &Item) -> Result<(), String> {
        let Definition::User(d) = &item.definition else {
            unreachable!()
        };
        let mut create =
            vec!["CREATE".into(), "USER".into(), quote_ident(&d.name)];
        if let Some(options) = &d.options {
            push_role_options(&mut create, options, true);
        } else {
            create.push("LOGIN".into());
        }
        if let Some(valid_until) = &d.valid_until {
            create.push("VALID UNTIL".into());
            create.push(postgres_value(&Value::String(valid_until.clone())));
        }
        match &d.password {
            None => create.push("PASSWORD NULL".into()),
            Some(password) if !password.is_empty() => {
                if password.starts_with("md5") {
                    create.push("ENCRYPTED".into());
                }
                create.push("PASSWORD".into());
                create.push(postgres_value(&Value::String(password.clone())));
            }
            Some(_) => {}
        }
        let drop = if d.name == self.superuser {
            vec![]
        } else {
            vec!["DROP USER IF EXISTS".into(), quote_ident(&d.name)]
        };
        self.add_item(item, create, drop, false)
    }

    fn dump_user_mapping(&mut self, item: &Item) -> Result<(), String> {
        let Definition::UserMapping(d) = &item.definition else {
            unreachable!()
        };
        for server in &d.servers {
            let mut create = vec![
                "CREATE".into(),
                "USER MAPPING".into(),
                "FOR".into(),
                quote_ident(&d.name),
                "SERVER".into(),
                quote_ident(&server.name),
            ];
            if let Some(options) = &server.options {
                let opts: Vec<String> = options
                    .iter()
                    .map(|(k, v)| format!("{k} {}", postgres_value(v)))
                    .collect();
                create.push(format!("OPTIONS ({})", opts.join(",")));
            }
            let drop = vec![
                "DROP USER MAPPING IF EXISTS".into(),
                "FOR".into(),
                quote_ident(&d.name),
                "SERVER".into(),
                quote_ident(&server.name),
            ];
            self.add_item(item, create, drop, false)?;
        }
        Ok(())
    }

    fn dump_view(&mut self, item: &Item) -> Result<(), String> {
        let Definition::View(d) = &item.definition else {
            unreachable!()
        };
        if let Some(sql) = &d.sql {
            return self.add_item(item, vec![sql.clone()], vec![], false);
        }
        let mut create = vec!["CREATE".into()];
        if d.recursive == Some(true) {
            create.push("RECURSIVE".into());
        }
        create.push("VIEW".into());
        create.push(self.item_name(item));
        if let Some(columns) = &d.columns {
            let names: Vec<&str> =
                columns.iter().map(view_column_name).collect();
            create.push(format!("({})", names.join(", ")));
        }
        if let Some(check_option) = &d.check_option {
            create.push(format!("WITH (check_option = {check_option})"));
        }
        if d.security_barrier == Some(true) {
            create.push("WITH (security_barrier = true)".into());
        }
        create.push("AS".into());
        create.push(d.query.clone().unwrap_or_default());
        let drop = vec!["DROP VIEW IF EXISTS".into(), self.item_name(item)];
        self.add_item(item, create, drop, false)
    }
}

/// CREATEDB / NOCREATEDB style option rendering
/// (ports _format_bool_option)
fn push_bool_option(sql: &mut Vec<String>, name: &str, value: Option<bool>) {
    match value {
        Some(true) => sql.push(name.to_string()),
        Some(false) => sql.push(format!("NO{name}")),
        None => {}
    }
}

/// Shared role/user option rendering (ports _dump_role / _dump_user,
/// with the BYPASSRLS-reads-create_db bug fixed)
fn push_role_options(
    sql: &mut Vec<String>,
    options: &RoleOptions,
    is_user: bool,
) {
    push_bool_option(sql, "BYPASSRLS", options.bypass_rls);
    if let Some(limit) = options.connection_limit {
        sql.push("CONNECTION LIMIT".into());
        sql.push(limit.to_string());
    }
    push_bool_option(sql, "CREATEDB", options.create_db);
    push_bool_option(sql, "CREATEROLE", options.create_role);
    push_bool_option(sql, "INHERIT", options.inherit);
    if is_user {
        sql.push("LOGIN".into());
    } else {
        push_bool_option(sql, "LOGIN", options.login);
    }
    push_bool_option(sql, "SUPERUSER", options.superuser);
}

/// DEFAULT clause rendering: strings that look like SQL expressions
/// (quoted literals, casts, function calls, keyword-like all-caps)
/// pass through raw; everything else renders as a literal. Deviation
/// 9: Python quoted every string, producing unrestorable SQL for
/// expression defaults like `uuid_generate_v4()`.
fn render_default(value: &Value) -> String {
    if let Value::String(s) = value {
        let expression = s.starts_with('\'')
            || s.ends_with(')')
            || s.contains("::")
            || (!s.is_empty()
                && s.chars().all(|c| c.is_ascii_uppercase() || c == '_'));
        if expression {
            return s.clone();
        }
    }
    postgres_value(value)
}

fn render_table_column(column: &Column) -> String {
    let mut sql = vec![column.name.clone(), column.data_type.clone()];
    if let Some(collation) = &column.collation {
        sql.push("COLLATE".into());
        sql.push(collation.clone());
    }
    if column.nullable == Some(false) {
        sql.push("NOT NULL".into());
    }
    if let Some(check_constraint) = &column.check_constraint {
        sql.push("CHECK".into());
        sql.push(check_constraint.clone());
    }
    if let Some(default) = &column.default {
        sql.push("DEFAULT".into());
        sql.push(render_default(default));
    }
    if let Some(generated) = &column.generated {
        if let Some(expression) = &generated.expression {
            sql.push("GENERATED ALWAYS AS".into());
            sql.push(expression.clone());
            sql.push("STORED".into());
        } else if generated.sequence.is_some() {
            sql.push("GENERATED".into());
            sql.push(generated.sequence_behavior.clone().unwrap_or_default());
            sql.push("AS IDENTITY".into());
        }
    }
    sql.join(" ")
}

/// UNIQUE / PRIMARY KEY constraint rendering
/// (ports _format_sql_constraint; INCLUDE columns get a proper clause)
fn render_constraint(
    constraint_type: &str,
    constraint: &ConstraintColumns,
) -> String {
    let (columns, include): (Vec<String>, Option<&Vec<String>>) =
        match constraint {
            ConstraintColumns::Name(name) => (vec![name.clone()], None),
            ConstraintColumns::Columns(columns) => (columns.clone(), None),
            ConstraintColumns::Detailed { columns, include } => {
                (columns.clone(), include.as_ref())
            }
        };
    let mut sql = vec![format!("{constraint_type} ({})", columns.join(", "))];
    if let Some(include) = include {
        sql.push(format!("INCLUDE ({})", include.join(", ")));
    }
    sql.join(" ")
}

fn render_partition_column(column: &TablePartitionColumn) -> String {
    match column {
        TablePartitionColumn::Name(name) => name.clone(),
        TablePartitionColumn::Detailed {
            name,
            expression,
            collation,
            opclass,
        } => {
            let mut sql = vec![
                name.clone()
                    .or_else(|| expression.clone())
                    .unwrap_or_default(),
            ];
            if let Some(collation) = collation {
                sql.push("COLLATION".into());
                sql.push(collation.clone());
            }
            if let Some(opclass) = opclass {
                sql.push(opclass.clone());
            }
            sql.join(" ")
        }
    }
}

fn view_column_name(column: &ViewColumn) -> &str {
    match column {
        ViewColumn::Name(name) => name,
        ViewColumn::Detailed { name, .. } => name,
    }
}

/// `key 'value'` option rendering for FDW objects
fn render_options(options: &Map<String, Value>) -> String {
    options
        .iter()
        .map(|(k, v)| format!("{k} {}", postgres_value(v)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `key = value` parameter rendering (ports _format_parameters)
fn render_parameters(parameters: &Map<String, Value>) -> String {
    parameters
        .iter()
        .map(|(k, v)| format!("{k} = {}", postgres_value(v)))
        .collect::<Vec<_>>()
        .join(", ")
}
