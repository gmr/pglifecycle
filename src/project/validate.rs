//! Data validation using bundled JSON-Schema files (ports validation.py)

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use include_dir::{Dir, include_dir};
use serde_json::Value;

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
    valid
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
