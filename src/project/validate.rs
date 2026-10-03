//! Data validation using bundled JSON-Schema files (ports validation.py)

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use include_dir::{Dir, include_dir};
use serde_json::Value;

use crate::models::is_list_setting;
use crate::yamlio;

static SCHEMATA: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/schemata");

static CACHE: OnceLock<Mutex<HashMap<String, Value>>> = OnceLock::new();

/// Validate a data object against the bundled schema for its type,
/// logging each validation error. `obj_type` is the schema file stem
/// form (lowercase, underscores).
pub fn validate_object(obj_type: &str, name: &str, data: &Value) -> bool {
    let schema = match load_schema(obj_type) {
        Ok(schema) => schema,
        Err(error) => {
            log::error!("{error}");
            return false;
        }
    };
    let validator = match jsonschema::validator_for(&schema) {
        Ok(validator) => validator,
        Err(error) => {
            log::error!("Invalid schema for {obj_type}: {error}");
            return false;
        }
    };
    let mut valid = true;
    for error in validator.iter_errors(data) {
        log::error!(
            "Validation error for {obj_type} {name}: {error} at {}",
            error.instance_path()
        );
        valid = false;
    }
    // (path, name, value) of each setting of a routine, a role, a
    // user or the database
    let settings: Vec<(String, &String, &Value)> = match obj_type {
        "function" | "procedure" => data["configuration"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, v)| (format!("/configuration/{k}"), k, v))
            .collect(),
        "role" | "user" => settings_list("/settings", &data["settings"]),
        "project" => {
            let mut settings = settings_list("/settings", &data["settings"]);
            for (role, list) in
                data["role_settings"].as_object().into_iter().flatten()
            {
                settings.extend(settings_list(
                    &format!("/role_settings/{role}"),
                    list,
                ));
            }
            settings
        }
        _ => vec![],
    };
    for (path, setting, value) in settings {
        if let Some(error) = setting_error(setting, value) {
            log::error!(
                "Validation error for {obj_type} {name}: {error} at {path}"
            );
            valid = false;
        }
    }
    valid
}

/// (path, name, value) of each setting of a `settings` list, which has
/// one `{ name: value }` object for each setting
fn settings_list<'a>(
    path: &str,
    list: &'a Value,
) -> Vec<(String, &'a String, &'a Value)> {
    list.as_array()
        .into_iter()
        .flatten()
        .enumerate()
        .flat_map(|(i, object)| {
            object
                .as_object()
                .into_iter()
                .flatten()
                .map(move |(k, v)| (format!("{path}/{i}/{k}"), k, v))
        })
        .collect()
}

/// The error for a setting that has a form PostgreSQL does not keep.
/// For a setting that PostgreSQL keeps as a list, one string with a
/// comma is one name, not a list. pg_dump and pg_dumpall write each
/// other setting as one string, so a list for it does not pull back as
/// a list. SET takes no empty list.
fn setting_error(setting: &str, value: &Value) -> Option<String> {
    match (is_list_setting(setting), value) {
        (true, Value::Array(items)) if items.is_empty() => Some(format!(
            "{setting} is an empty list: write at least one name, or \
             write '' for an empty value"
        )),
        (true, Value::String(text)) if text.contains(',') => Some(format!(
            "{setting} is a list: write each name as a list item, not one \
             string with commas"
        )),
        (false, Value::Array(_)) => Some(format!(
            "{setting} is not a list: write its value as one string"
        )),
        _ => None,
    }
}

/// Load a schema by object type stem, merging `$package_schema`
/// references to other bundled schema files
fn load_schema(obj_type: &str) -> Result<Value, String> {
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(schema) = cache.lock().unwrap().get(obj_type) {
        return Ok(schema.clone());
    }
    let file_name = format!("{}.yml", obj_type.replace(' ', "_"));
    let file = SCHEMATA.get_file(&file_name).ok_or_else(|| {
        format!("Schema file not found for object type {obj_type:?}")
    })?;
    let raw = yamlio::load_str(file.contents_utf8().ok_or_else(|| {
        format!("Schema file {file_name} is not valid UTF-8")
    })?)
    .map_err(|e| format!("Failed to parse schema {file_name}: {e}"))?;
    let schema = preprocess(&raw)?;
    cache
        .lock()
        .unwrap()
        .insert(obj_type.to_string(), schema.clone());
    Ok(schema)
}

/// Merge in other bundled schemas wherever `$package_schema` appears
fn preprocess(schema: &Value) -> Result<Value, String> {
    Ok(match schema {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (key, value) in map {
                // a merged fragment brings its own `$schema` and
                // `$id`, which mean nothing (and in `$id`'s case change
                // reference resolution) inside the schema they are
                // merged into; the bundled files declare draft 2020-12,
                // which is what this crate applies by default
                if key == "$schema" || key == "$id" {
                    continue;
                }
                if key == "$package_schema" {
                    let name = value.as_str().ok_or_else(|| {
                        format!("$package_schema is not a string: {value}")
                    })?;
                    if let Value::Object(merged) = load_schema(name)? {
                        out.extend(merged);
                    }
                } else {
                    out.insert(key.clone(), preprocess(value)?);
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(preprocess)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        other => other.clone(),
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn validates_a_schema_object() {
        let data = json!({"name": "test", "owner": "postgres"});
        assert!(validate_object("schema", "test", &data));
    }

    #[test]
    fn rejects_invalid_objects() {
        let data = json!({"name": 42});
        assert!(!validate_object("schema", "bad", &data));
    }

    /// The options of a tablespace are numbers by name, as the model
    /// keeps them and as pull writes them
    #[test]
    fn validates_tablespace_options_by_name() {
        let tablespace = |options| {
            let data = json!({
                "name": "fast",
                "owner": "postgres",
                "location": "/srv/fast",
                "options": options,
            });
            validate_object("tablespace", "fast", &data)
        };
        assert!(tablespace(json!({
            "seq_page_cost": 1.5,
            "random_page_cost": 2,
            "effective_io_concurrency": 20,
            "maintenance_io_concurrency": 10,
        })));
        assert!(!tablespace(json!([{"seq_page_cost": 1.5}])));
        assert!(!tablespace(json!({"seq_page_cost": "1.5"})));
        assert!(!tablespace(json!({"fillfactor": 70})));
    }

    /// PostgreSQL rejects PERIOD on one side of a foreign key only
    #[test]
    fn foreign_key_period_needs_both_sides() {
        let fk = |period: Option<&str>, ref_period: Option<&str>| {
            let mut data = json!({
                "columns": ["id", "valid"],
                "references": {"name": "public.p", "columns": ["id"]},
            });
            if let Some(p) = period {
                data["period"] = json!(p);
            }
            if let Some(p) = ref_period {
                data["references"]["period"] = json!(p);
            }
            validate_object("foreign_key", "fk", &data)
        };
        assert!(fk(None, None));
        assert!(fk(Some("valid"), Some("valid")));
        assert!(!fk(Some("valid"), None));
        assert!(!fk(None, Some("valid")));
    }

    /// REVOKE GRANT OPTION FOR is not supported, and a plain REVOKE
    /// would take away the privilege itself
    #[test]
    fn revocation_cannot_set_with_grant_option() {
        let defaults = |grant_option: Option<bool>| {
            let mut declaration = json!({
                "object_type": "TABLES", "grantee": "reader",
                "privileges": ["SELECT"],
            });
            if let Some(value) = grant_option {
                declaration["with_grant_option"] = json!(value);
            }
            let data = json!({
                "name": "app",
                "grants": [declaration.clone()],
                "revocations": [declaration],
            });
            validate_object("default_privileges", "app", &data)
        };
        assert!(defaults(None));
        assert!(defaults(Some(false)));
        assert!(!defaults(Some(true)));
        // a grant to a role may still carry the option
        let grant = json!({
            "name": "app",
            "grants": [{"object_type": "TABLES", "grantee": "reader",
                        "privileges": ["SELECT"],
                        "with_grant_option": true}],
        });
        assert!(validate_object("default_privileges", "app", &grant));
    }

    /// PostgreSQL does not give grant options to PUBLIC
    #[test]
    fn public_cannot_get_grant_option() {
        let grant = |grantee: &str, grant_option: bool| {
            let data = json!({
                "name": "app",
                "grants": [{"object_type": "TABLES", "grantee": grantee,
                            "privileges": ["SELECT"],
                            "with_grant_option": grant_option}],
            });
            validate_object("default_privileges", "app", &data)
        };
        assert!(grant("PUBLIC", false));
        assert!(!grant("PUBLIC", true));
        assert!(!grant("public", true));
        assert!(grant("reader", true));
    }

    /// UPDATE OF needs the UPDATE event, and the raw SQL form cannot
    /// have it
    #[test]
    fn trigger_update_columns_need_update() {
        let trigger = |events: &[&str], sql: bool| {
            let mut data = json!({"update_columns": ["Name"]});
            if sql {
                data["sql"] = json!("CREATE TRIGGER t ...");
            } else {
                data["name"] = json!("t");
                data["when"] = json!("BEFORE");
                data["events"] = json!(events);
                data["function"] = json!("test.f()");
            }
            validate_object("trigger", "t", &data)
        };
        assert!(trigger(&["UPDATE"], false));
        assert!(trigger(&["INSERT", "UPDATE"], false));
        assert!(!trigger(&["INSERT"], false));
        assert!(!trigger(&[], true));
    }

    /// A routine has one body form only
    #[test]
    fn routine_body_forms_are_exclusive() {
        let routine = |kind: &str, keys: &[&str]| {
            let mut data = json!({
                "schema": "test", "name": "r", "owner": "app",
                "language": "sql",
            });
            if kind == "function" {
                data["returns"] = json!("integer");
            }
            for key in keys {
                data[*key] = json!("SELECT 1");
            }
            validate_object(kind, "r", &data)
        };
        assert!(routine("function", &["definition"]));
        assert!(routine("function", &["sql_body"]));
        assert!(!routine("function", &["definition", "sql_body"]));
        assert!(routine("function", &["object_file"]));
        assert!(!routine("function", &["object_file", "sql_body"]));
        assert!(!routine("function", &["object_file", "definition"]));
        assert!(routine("procedure", &["definition"]));
        assert!(routine("procedure", &["sql_body"]));
        assert!(routine("procedure", &["object_file"]));
        assert!(!routine("procedure", &["definition", "sql_body"]));
        assert!(!routine("procedure", &["definition", "sql"]));
        assert!(!routine("procedure", &["object_file", "sql_body"]));
        assert!(!routine("procedure", &["object_file", "definition"]));
    }

    /// A setting value is a string, a number, a boolean or a list of
    /// strings. A setting that PostgreSQL keeps as a list, written as
    /// one string with a comma, is one name to PostgreSQL, so it is
    /// refused. A list for a setting that PostgreSQL keeps as one
    /// string does not pull back as a list, so it is refused too.
    #[test]
    fn routine_settings_are_lists_only_where_postgres_keeps_lists() {
        let routine = |kind: &str, name: &str, value: Value| {
            let mut data = json!({
                "schema": "test", "name": "r", "owner": "app",
                "language": "sql", "definition": "SELECT 1",
                "configuration": {name: value},
            });
            if kind == "function" {
                data["returns"] = json!("integer");
            }
            validate_object(kind, "r", &data)
        };
        for kind in ["function", "procedure"] {
            assert!(routine(kind, "search_path", json!(["pg_catalog", "a"])));
            assert!(routine(kind, "search_path", json!("pg_catalog")));
            assert!(routine(kind, "search_path", json!(["a,b"])));
            assert!(routine(kind, "search_path", json!("")));
            assert!(!routine(kind, "search_path", json!("pg_catalog, a")));
            assert!(!routine(kind, "Search_Path", json!("pg_catalog,a")));
            assert!(!routine(kind, "temp_tablespaces", json!("a, b")));
            assert!(!routine(kind, "search_path", json!([1])));
            assert!(!routine(kind, "search_path", json!({"a": "b"})));
            assert!(routine(kind, "DateStyle", json!("iso, mdy")));
            assert!(routine(kind, "statement_timeout", json!(1000)));
            assert!(routine(kind, "enable_seqscan", json!(false)));
            assert!(!routine(kind, "DateStyle", json!(["iso", "mdy"])));
            assert!(!routine(kind, "search_path", json!([])));
        }
    }

    /// The settings of a role or a user have the same forms as the
    /// settings of a routine
    #[test]
    fn role_settings_are_lists_only_where_postgres_keeps_lists() {
        let role = |kind: &str, name: &str, value: Value| {
            let data = json!({
                "name": "r",
                "settings": [{"work_mem": "64MB"}, {name: value}],
            });
            validate_object(kind, "r", &data)
        };
        for kind in ["role", "user"] {
            assert!(role(kind, "search_path", json!(["$user", "my schema"])));
            assert!(role(kind, "search_path", json!("pg_catalog")));
            assert!(role(kind, "search_path", json!(["a,b"])));
            assert!(role(kind, "search_path", json!("")));
            assert!(!role(kind, "search_path", json!("$user, public")));
            assert!(!role(kind, "Search_Path", json!("pg_catalog,a")));
            assert!(!role(kind, "temp_tablespaces", json!("a, b")));
            assert!(!role(kind, "search_path", json!([1])));
            assert!(!role(kind, "search_path", json!({"a": "b"})));
            assert!(!role(kind, "search_path", json!([])));
            assert!(role(kind, "DateStyle", json!("iso, mdy")));
            assert!(role(kind, "statement_timeout", json!(1000)));
            assert!(role(kind, "enable_seqscan", json!(false)));
            assert!(!role(kind, "DateStyle", json!(["iso", "mdy"])));
        }
    }

    /// The settings of the database, and of a role in the database,
    /// have the same forms as the settings of a role
    #[test]
    fn database_settings_are_lists_only_where_postgres_keeps_lists() {
        let project = |name: &str, value: Value| {
            let database = json!({
                "name": "db",
                "settings": [{"work_mem": "64MB"}, {name: value.clone()}],
            });
            let role = json!({
                "name": "db",
                "role_settings": {"app": [{name: value}]},
            });
            (
                validate_object("project", "db", &database),
                validate_object("project", "db", &role),
            )
        };
        assert_eq!(
            project("search_path", json!(["$user", "a"])),
            (true, true)
        );
        assert_eq!(project("statement_timeout", json!(1000)), (true, true));
        assert_eq!(project("enable_seqscan", json!(false)), (true, true));
        assert_eq!(project("DateStyle", json!("iso, mdy")), (true, true));
        assert_eq!(project("search_path", json!("a, b")), (false, false));
        assert_eq!(project("search_path", json!([])), (false, false));
        assert_eq!(project("DateStyle", json!(["iso"])), (false, false));
        assert_eq!(project("bad-name", json!("x")), (false, false));
    }

    #[test]
    fn merges_package_schemas() {
        // casts.yml composes cast.yml via $package_schema
        let schema = load_schema("casts").unwrap();
        let items = &schema["properties"]["casts"]["items"];
        assert!(items.get("properties").is_some());
        assert!(items.get("$package_schema").is_none());
    }

    /// Every bundled schema compiles. A schema that does not is a
    /// silent pass: `validate_object` logs and rejects the object
    #[test]
    fn every_bundled_schema_compiles() {
        for file in SCHEMATA.files() {
            let stem = file
                .path()
                .file_stem()
                .and_then(|s| s.to_str())
                .expect("schema file stem");
            let schema =
                load_schema(stem).unwrap_or_else(|e| panic!("{stem}: {e}"));
            jsonschema::validator_for(&schema)
                .unwrap_or_else(|e| panic!("{stem}: {e}"));
        }
    }

    /// A `oneOf`/`anyOf` branch that gates on a property name the
    /// object does not define can never be satisfied, and JSON Schema
    /// reports nothing: the `partitions` branches gated on
    /// `for_values_when` where the property is `for_values_with`, so
    /// every hash partition failed validation under "is not valid
    /// under any of the schemas listed in the 'oneOf' keyword"
    #[test]
    fn schema_branches_only_gate_on_real_properties() {
        fn walk(stem: &str, path: &str, node: &Value) {
            let Value::Object(map) = node else {
                if let Value::Array(items) = node {
                    for (index, item) in items.iter().enumerate() {
                        walk(stem, &format!("{path}/{index}"), item);
                    }
                }
                return;
            };
            if let Some(Value::Object(properties)) = map.get("properties") {
                for keyword in ["oneOf", "anyOf", "allOf"] {
                    for branch in map
                        .get(keyword)
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        for name in required_names(branch) {
                            assert!(
                                properties.contains_key(&name),
                                "{stem}: {path}/{keyword} gates on \
                                 {name:?}, which is not a property of \
                                 the object it constrains"
                            );
                        }
                    }
                }
            }
            for (key, value) in map {
                walk(stem, &format!("{path}/{key}"), value);
            }
        }

        /// Every name a branch requires, including under `not`
        fn required_names(branch: &Value) -> Vec<String> {
            let mut names = Vec::new();
            for node in [branch, &branch["not"]] {
                match &node["required"] {
                    Value::Array(items) => names.extend(
                        items
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string),
                    ),
                    Value::String(name) => names.push(name.clone()),
                    _ => {}
                }
            }
            names
        }

        for file in SCHEMATA.files() {
            let stem = file
                .path()
                .file_stem()
                .and_then(|s| s.to_str())
                .expect("schema file stem");
            walk(stem, "", &load_schema(stem).unwrap());
        }
    }

    #[test]
    fn dependencies_schema_accepts_every_object_type() {
        // the dependencies schema previously listed only nine plural
        // keys with additionalProperties: false, so a dependency on an
        // aggregate/collation/event_trigger/materialized_view/etc. failed
        // validation even though the loader recognized the key
        let schema = load_schema("dependencies").unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let widened = json!({
            "aggregates": ["test.agg"],
            "collations": ["test.c"],
            "event_triggers": ["et"],
            "materialized_views": ["test.mv"],
            "publications": ["p"],
            "servers": ["s"],
            "subscriptions": ["sub"],
            "user_mappings": ["um"],
            "users": ["u"],
        });
        assert!(
            validator.iter_errors(&widened).next().is_none(),
            "widened dependency keys should validate"
        );
        // additionalProperties: false must still reject unknown keys
        let unknown = json!({"bogus_type": ["x"]});
        assert!(
            validator.iter_errors(&unknown).next().is_some(),
            "unknown dependency keys must still be rejected"
        );
    }
}
