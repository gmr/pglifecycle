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
                // the bundled files use the draft-agnostic
                // http://json-schema.org/schema# URI, which this crate
                // rejects; dropping it applies the default draft, as
                // Python's jsonschema did
                if key == "$schema" {
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
}
