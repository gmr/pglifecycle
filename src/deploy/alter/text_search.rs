//! Text search parsers, templates, dictionaries and configurations.
//!
//! The project keeps the text search objects of a schema in one
//! container, and so does `pull`. Deploy splits each container into one
//! container for each object ([`split`], [`split_inventory`]), so that
//! each object is an item of its own: it is keyed by its kind, schema
//! and name ([`key_name`]), it has its own archive entry, and it is
//! added, changed or removed apart from the other objects of its
//! schema.
//!
//! In place: the mappings of a configuration, the options of a
//! dictionary, and the comments. PostgreSQL has no ALTER for the parser
//! of a configuration, the template of a dictionary, or the functions
//! of a parser or a template, so such a change drops and makes the
//! object again.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use super::{Alter, Resolution, push_comment, qualified};
use crate::models::{
    Definition, Item, TextSearch, TextSearchConfig, TextSearchDict,
    TextSearchParser, TextSearchTemplate,
};
use crate::utils::{postgres_value, quote_ident};

/// The kinds of text search object, as the key and the SQL name them
const KINDS: [&str; 4] = ["PARSER", "TEMPLATE", "DICTIONARY", "CONFIGURATION"];

/// One text search object of a container
enum Object<'a> {
    Parser(&'a TextSearchParser),
    Template(&'a TextSearchTemplate),
    Dictionary(&'a TextSearchDict),
    Configuration(&'a TextSearchConfig),
}

impl Object<'_> {
    fn kind(&self) -> &'static str {
        match self {
            Object::Parser(_) => KINDS[0],
            Object::Template(_) => KINDS[1],
            Object::Dictionary(_) => KINDS[2],
            Object::Configuration(_) => KINDS[3],
        }
    }

    fn name(&self) -> &str {
        match self {
            Object::Parser(o) => &o.name,
            Object::Template(o) => &o.name,
            Object::Dictionary(o) => &o.name,
            Object::Configuration(o) => &o.name,
        }
    }
}

/// The objects of a container, in the order that they depend on each
/// other: a configuration names a parser and dictionaries, and a
/// dictionary names a template
fn objects(container: &TextSearch) -> Vec<Object<'_>> {
    let parsers = container.parsers.iter().flatten().map(Object::Parser);
    let templates = container.templates.iter().flatten().map(Object::Template);
    let dictionaries = container
        .dictionaries
        .iter()
        .flatten()
        .map(Object::Dictionary);
    let configurations = container
        .configurations
        .iter()
        .flatten()
        .map(Object::Configuration);
    parsers
        .chain(templates)
        .chain(dictionaries)
        .chain(configurations)
        .collect()
}

/// The object of a container that holds exactly one
fn only(container: &TextSearch) -> Option<Object<'_>> {
    let mut objects = objects(container);
    (objects.len() == 1).then(|| objects.remove(0))
}

/// One container for each object of `container`. A one-object container
/// takes the raw `sql` of its object, so that the diff treats the
/// object as a raw statement (`Definition::raw_sql`). The build reads
/// the `sql` of the object, not that of the container.
pub(crate) fn split(container: &TextSearch) -> Vec<TextSearch> {
    if container.sql.is_some() {
        return vec![container.clone()];
    }
    let empty = TextSearch {
        schema: container.schema.clone(),
        sql: None,
        configurations: None,
        dictionaries: None,
        parsers: None,
        templates: None,
    };
    objects(container)
        .into_iter()
        .map(|object| match object {
            Object::Parser(o) => TextSearch {
                sql: o.sql.clone(),
                parsers: Some(vec![o.clone()]),
                ..empty.clone()
            },
            Object::Template(o) => TextSearch {
                sql: o.sql.clone(),
                templates: Some(vec![o.clone()]),
                ..empty.clone()
            },
            Object::Dictionary(o) => TextSearch {
                sql: o.sql.clone(),
                dictionaries: Some(vec![o.clone()]),
                ..empty.clone()
            },
            Object::Configuration(o) => TextSearch {
                sql: o.sql.clone(),
                configurations: Some(vec![o.clone()]),
                ..empty.clone()
            },
        })
        .collect()
}

/// Replace each text search container of the inventory with one item
/// for each of its objects. The first object keeps the id of the
/// container, and the others get new ids at the end, so that an id is
/// still the position of its item. Each new item has the dependencies
/// of its container, and an item that depends on a container depends
/// on each object of it.
pub(crate) fn split_inventory(inventory: &mut Vec<Item>) {
    let mut next = inventory.len();
    let mut added = Vec::new();
    let mut ids: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for item in inventory.iter_mut() {
        let Definition::TextSearch(container) = &item.definition else {
            continue;
        };
        let mut containers = split(container).into_iter();
        let Some(first) = containers.next() else {
            continue;
        };
        for container in containers {
            added.push(Item {
                id: next,
                desc: item.desc,
                definition: Definition::TextSearch(container),
                dependencies: item.dependencies.clone(),
            });
            ids.entry(item.id).or_default().push(next);
            next += 1;
        }
        item.definition = Definition::TextSearch(first);
    }
    inventory.extend(added);
    for item in inventory.iter_mut() {
        let more: BTreeSet<usize> = item
            .dependencies
            .iter()
            .filter_map(|id| ids.get(id))
            .flatten()
            .copied()
            .filter(|id| *id != item.id)
            .collect();
        item.dependencies.extend(more);
    }
}

/// The name part of the key of a container: `KIND name` for a container
/// of one object, else the schema, as for the container as a whole
pub(crate) fn key_name(container: &TextSearch) -> String {
    match only(container) {
        Some(object) => format!("{} {}", object.kind(), object.name()),
        None => container.schema.clone(),
    }
}

/// The key name of an archive entry of type `desc` (for example `TEXT
/// SEARCH DICTIONARY`) with the tag `tag`, which is the object name
pub(crate) fn entry_key_name(desc: &str, tag: &str) -> Option<String> {
    let kind = desc.strip_prefix("TEXT SEARCH ")?;
    KINDS.contains(&kind).then(|| format!("{kind} {tag}"))
}

/// The kind and the object name of a key name from [`key_name`]
pub(crate) fn split_key_name(name: &str) -> Option<(&str, &str)> {
    let (kind, name) = name.split_once(' ')?;
    KINDS.contains(&kind).then_some((kind, name))
}

/// `DROP TEXT SEARCH <kind> IF EXISTS <schema>.<name>` for the key of a
/// database-only object
pub(crate) fn drop_sql(schema: &str, name: &str) -> Option<String> {
    let (kind, name) = split_key_name(name)?;
    Some(format!(
        "DROP TEXT SEARCH {kind} IF EXISTS {};\n",
        qualified(schema, name)
    ))
}

/// A name as PostgreSQL reads it in the build's SQL: each part not in
/// quotes folded to lowercase, and qualified. The build runs with an
/// empty `search_path`, so a name with no schema is in `pg_catalog`
/// (pg_dump writes such a name with no schema too). The object's own
/// schema is not searched.
fn canonical_name(name: &str) -> String {
    let mut parts = vec![String::new()];
    let mut quoted = false;
    let mut chars = name.chars().peekable();
    while let Some(c) = chars.next() {
        let part = parts.last_mut().expect("one part at least");
        match c {
            '"' if quoted && chars.peek() == Some(&'"') => {
                chars.next();
                part.push('"');
            }
            '"' => quoted = !quoted,
            '.' if !quoted => parts.push(String::new()),
            c if quoted => part.push(c),
            c if c.is_whitespace() => {}
            c => part.push(c.to_ascii_lowercase()),
        }
    }
    let name = parts.pop().unwrap_or_default();
    let schema = parts
        .pop()
        .filter(|schema| !schema.is_empty())
        .unwrap_or_else(|| String::from("pg_catalog"));
    qualified(&schema, &name)
}

/// A dictionary option name as PostgreSQL stores it: the build writes
/// the name with no quotes, so PostgreSQL folds it to lowercase
fn option_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// A dictionary option value as PostgreSQL stores it and pg_dump
/// writes it. The build writes a string in quotes, which keeps its
/// case, a boolean as the keyword `True` or `False`, which PostgreSQL
/// folds to lowercase, and a number as it is.
fn option_value(value: &Value) -> Value {
    match value {
        Value::Bool(b) => Value::String(b.to_string()),
        Value::Number(n) => Value::String(n.to_string()),
        other => other.clone(),
    }
}

fn canonical_options(
    options: &Option<Map<String, Value>>,
) -> Option<Map<String, Value>> {
    let options: Map<String, Value> = options
        .iter()
        .flatten()
        .map(|(k, v)| (option_name(k), option_value(v)))
        .collect();
    (!options.is_empty()).then_some(options)
}

/// A token type as PostgreSQL reads it: a name with no quotes, folded
/// to lowercase. The order of the token types is not significant, the
/// order of the dictionaries of each is.
fn canonical_mappings(
    mappings: &Option<BTreeMap<String, Vec<String>>>,
) -> Option<BTreeMap<String, Vec<String>>> {
    let mappings: BTreeMap<String, Vec<String>> = mappings
        .iter()
        .flatten()
        .map(|(token, dictionaries)| {
            (
                token.to_ascii_lowercase(),
                dictionaries.iter().map(|d| canonical_name(d)).collect(),
            )
        })
        .collect();
    (!mappings.is_empty()).then_some(mappings)
}

/// The container with each name, option and token type in the form
/// that PostgreSQL stores (see [`canonical_name`], [`option_value`] and
/// [`canonical_mappings`]), so that the same object written two ways
/// compares equal. pg_dump writes a copied configuration as its parser
/// and all of its mappings, so the source of a copy is not compared
/// (see [`copied_view`]).
pub(crate) fn canonical(container: &TextSearch) -> TextSearch {
    let name = |name: &Option<String>| name.as_deref().map(canonical_name);
    TextSearch {
        schema: container.schema.clone(),
        sql: container.sql.clone(),
        parsers: container.parsers.as_ref().map(|parsers| {
            parsers
                .iter()
                .map(|p| TextSearchParser {
                    start_function: name(&p.start_function),
                    gettoken_function: name(&p.gettoken_function),
                    end_function: name(&p.end_function),
                    lextypes_function: name(&p.lextypes_function),
                    headline_function: name(&p.headline_function),
                    ..p.clone()
                })
                .collect()
        }),
        templates: container.templates.as_ref().map(|templates| {
            templates
                .iter()
                .map(|t| TextSearchTemplate {
                    lexize_function: name(&t.lexize_function),
                    init_function: name(&t.init_function),
                    ..t.clone()
                })
                .collect()
        }),
        dictionaries: container.dictionaries.as_ref().map(|dictionaries| {
            dictionaries
                .iter()
                .map(|d| TextSearchDict {
                    template: name(&d.template),
                    options: canonical_options(&d.options),
                    ..d.clone()
                })
                .collect()
        }),
        configurations: container.configurations.as_ref().map(|configs| {
            configs
                .iter()
                .map(|c| TextSearchConfig {
                    parser: name(&c.parser),
                    source: None,
                    mappings: canonical_mappings(&c.mappings),
                    ..c.clone()
                })
                .collect()
        }),
    }
}

/// The database container as the project states it. A configuration
/// that the project copies from another (`source`) states only the
/// mappings that it changes, and the source is often in `pg_catalog`,
/// which pg_dump does not dump. Thus for a copy, deploy compares only
/// the token types that the project gives, and not the parser.
pub(crate) fn copied_view(repo: &TextSearch, db: &TextSearch) -> TextSearch {
    let (
        Some(Object::Configuration(repo)),
        Some(Object::Configuration(config)),
    ) = (only(repo), only(db))
    else {
        return db.clone();
    };
    if repo.source.is_none() {
        return db.clone();
    }
    let tokens: BTreeSet<String> = repo
        .mappings
        .iter()
        .flatten()
        .map(|(token, _)| token.to_ascii_lowercase())
        .collect();
    let mappings: BTreeMap<String, Vec<String>> = config
        .mappings
        .iter()
        .flatten()
        .filter(|(token, _)| tokens.contains(&token.to_ascii_lowercase()))
        .map(|(token, dictionaries)| (token.clone(), dictionaries.clone()))
        .collect();
    TextSearch {
        configurations: Some(vec![TextSearchConfig {
            parser: None,
            mappings: (!mappings.is_empty()).then_some(mappings),
            ..config.clone()
        }]),
        ..db.clone()
    }
}

/// Reconcile a changed text search object
pub(super) fn text_search(repo: &TextSearch, db: &TextSearch) -> Resolution {
    let wanted = canonical(repo);
    let existing = canonical(&copied_view(repo, db));
    let (Some(object), Some(current)) = (only(&wanted), only(&existing))
    else {
        return Resolution::Replace;
    };
    let name = qualified(&repo.schema, object.name());
    let desc = format!("TEXT SEARCH {}", object.kind());
    let mut alters = Vec::new();
    let comments = match (object, current) {
        (Object::Parser(r), Object::Parser(d)) => {
            let without = |p: &TextSearchParser| TextSearchParser {
                comment: None,
                ..p.clone()
            };
            if without(r) != without(d) {
                return Resolution::Replace;
            }
            (&r.comment, &d.comment)
        }
        (Object::Template(r), Object::Template(d)) => {
            let without = |t: &TextSearchTemplate| TextSearchTemplate {
                comment: None,
                ..t.clone()
            };
            if without(r) != without(d) {
                return Resolution::Replace;
            }
            (&r.comment, &d.comment)
        }
        (Object::Dictionary(r), Object::Dictionary(d)) => {
            if r.template != d.template {
                return Resolution::Replace;
            }
            let original = match only(repo) {
                Some(Object::Dictionary(original)) => original,
                _ => r,
            };
            if let Some(sql) = dictionary_options(&name, original, d) {
                alters.push(Alter::new(sql));
            }
            (&r.comment, &d.comment)
        }
        (Object::Configuration(r), Object::Configuration(d)) => {
            if r.parser != d.parser {
                return Resolution::Replace;
            }
            alters.extend(mappings(&name, r, d).into_iter().map(Alter::new));
            (&r.comment, &d.comment)
        }
        _ => return Resolution::Replace,
    };
    push_comment(&mut alters, &desc, &name, comments.0, comments.1);
    Resolution::Statements(alters)
}

/// `ALTER TEXT SEARCH DICTIONARY name (option = value, option)` for the
/// options that differ. An option given with no value removes it.
/// `repo` is the dictionary as the project writes it, so that a value
/// renders as the build renders it; `db` is canonical.
fn dictionary_options(
    name: &str,
    repo: &TextSearchDict,
    db: &TextSearchDict,
) -> Option<String> {
    let wanted = canonical_options(&repo.options).unwrap_or_default();
    let existing = db.options.clone().unwrap_or_default();
    let mut items: Vec<String> = repo
        .options
        .iter()
        .flatten()
        .filter(|(key, value)| {
            existing.get(&option_name(key)) != Some(&option_value(value))
        })
        .map(|(key, value)| {
            format!(
                "{} = {}",
                quote_ident(&option_name(key)),
                postgres_value(value)
            )
        })
        .collect();
    items.extend(
        existing
            .keys()
            .filter(|key| !wanted.contains_key(*key))
            .map(|key| quote_ident(key)),
    );
    (!items.is_empty()).then(|| {
        format!(
            "ALTER TEXT SEARCH DICTIONARY {name} ({});\n",
            items.join(", ")
        )
    })
}

/// The `ADD MAPPING`, `ALTER MAPPING` and `DROP MAPPING` statements
/// that change the mappings of `db` into those of `repo`, one for each
/// token type. Both are canonical.
fn mappings(
    name: &str,
    repo: &TextSearchConfig,
    db: &TextSearchConfig,
) -> Vec<String> {
    let wanted = repo.mappings.clone().unwrap_or_default();
    let existing = db.mappings.clone().unwrap_or_default();
    let tokens: BTreeSet<&String> =
        wanted.keys().chain(existing.keys()).collect();
    let alter = format!("ALTER TEXT SEARCH CONFIGURATION {name}");
    tokens
        .into_iter()
        .filter_map(|token| {
            let token_sql = quote_ident(token);
            match (wanted.get(token), existing.get(token)) {
                (Some(w), Some(e)) if w == e => None,
                (Some(w), Some(_)) => Some(format!(
                    "{alter} ALTER MAPPING FOR {token_sql} WITH {};\n",
                    w.join(", ")
                )),
                (Some(w), None) => Some(format!(
                    "{alter} ADD MAPPING FOR {token_sql} WITH {};\n",
                    w.join(", ")
                )),
                (None, Some(_)) => {
                    Some(format!("{alter} DROP MAPPING FOR {token_sql};\n"))
                }
                (None, None) => None,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn container(json: serde_json::Value) -> TextSearch {
        serde_json::from_value(json).expect("text search deserializes")
    }

    fn statements(resolution: Resolution) -> Vec<String> {
        match resolution {
            Resolution::Statements(alters) => {
                alters.into_iter().map(|a| a.sql).collect()
            }
            _ => panic!("expected in-place statements"),
        }
    }

    /// The container as pull reads it from pg_dump
    fn pulled() -> TextSearch {
        container(serde_json::json!({
            "schema": "test",
            "configurations": [{
                "name": "cfg",
                "parser": "pg_catalog.\"default\"",
                "mappings": {
                    "asciiword": ["test.gate_dict", "simple"],
                    "email": ["simple"],
                },
                "comment": "Gate",
            }],
            "dictionaries": [{
                "name": "dict",
                "template": "pg_catalog.simple",
                "options": {"stopwords": "english", "accept": "true"},
            }],
            "parsers": [{
                "name": "prs",
                "start_function": "prsd_start",
                "gettoken_function": "prsd_nexttoken",
                "end_function": "prsd_end",
                "lextypes_function": "prsd_lextype",
                "headline_function": "prsd_headline",
            }],
            "templates": [{
                "name": "tmpl",
                "init_function": "dsimple_init",
                "lexize_function": "dsimple_lexize",
                "comment": "Copy",
            }],
        }))
    }

    /// The same objects as a person writes them
    fn written() -> TextSearch {
        container(serde_json::json!({
            "schema": "test",
            "configurations": [{
                "name": "cfg",
                "parser": "\"default\"",
                "mappings": {
                    "EMAIL": ["pg_catalog.Simple"],
                    "AsciiWord": ["TEST.\"gate_dict\"", "SIMPLE"],
                },
                "comment": "Gate",
            }],
            "dictionaries": [{
                "name": "dict",
                "template": "SIMPLE",
                "options": {"Accept": true, "StopWords": "english"},
            }],
            "parsers": [{
                "name": "prs",
                "start_function": "PG_CATALOG.prsd_start",
                "gettoken_function": "prsd_nexttoken",
                "end_function": "pg_catalog.prsd_end",
                "lextypes_function": "prsd_lextype",
                "headline_function": "\"prsd_headline\"",
            }],
            "templates": [{
                "name": "tmpl",
                "init_function": "dsimple_init",
                "lexize_function": "pg_catalog . DSIMPLE_LEXIZE",
                "comment": "Copy",
            }],
        }))
    }

    fn keys(container: &TextSearch) -> Vec<String> {
        split(container).iter().map(key_name).collect()
    }

    #[test]
    fn split_gives_one_container_for_each_object() {
        assert_eq!(
            keys(&pulled()),
            [
                "PARSER prs",
                "TEMPLATE tmpl",
                "DICTIONARY dict",
                "CONFIGURATION cfg"
            ]
        );
        for one in split(&pulled()) {
            assert_eq!(one.schema, "test");
            assert!(one.sql.is_none());
            assert_eq!(split(&one).len(), 1);
        }
        // a container of more than one object keeps the schema as key
        assert_eq!(key_name(&pulled()), "test");
    }

    #[test]
    fn a_raw_object_makes_a_raw_container() {
        let raw = container(serde_json::json!({
            "schema": "test",
            "dictionaries": [
                {"name": "raw", "sql": "CREATE TEXT SEARCH DICTIONARY \
                                        test.raw (TEMPLATE = simple)"},
                {"name": "plain", "template": "simple"},
            ],
        }));
        let split = split(&raw);
        assert!(split[0].sql.is_some());
        assert!(Definition::TextSearch(split[0].clone()).raw_sql());
        assert!(split[1].sql.is_none());
    }

    #[test]
    fn split_inventory_keeps_ids_and_dependencies() {
        let table: crate::models::Table =
            serde_json::from_value(serde_json::json!({
                "name": "t", "schema": "test", "owner": "postgres",
            }))
            .expect("table deserializes");
        let mut inventory = vec![
            Item {
                id: 0,
                desc: crate::constants::ObjectType::Table,
                definition: Definition::Table(table.clone()),
                dependencies: BTreeSet::new(),
            },
            Item {
                id: 1,
                desc: crate::constants::ObjectType::TextSearch,
                definition: Definition::TextSearch(pulled()),
                dependencies: BTreeSet::from([0]),
            },
            Item {
                id: 2,
                desc: crate::constants::ObjectType::Table,
                definition: Definition::Table(table),
                dependencies: BTreeSet::from([1]),
            },
        ];
        split_inventory(&mut inventory);
        assert_eq!(inventory.len(), 6);
        for (position, item) in inventory.iter().enumerate() {
            assert_eq!(item.id, position);
        }
        for item in &inventory[3..] {
            assert_eq!(item.dependencies, BTreeSet::from([0]));
        }
        assert_eq!(inventory[1].dependencies, BTreeSet::from([0]));
        assert_eq!(inventory[2].dependencies, BTreeSet::from([1, 3, 4, 5]));
        let names: Vec<String> = inventory
            .iter()
            .filter_map(|item| match &item.definition {
                Definition::TextSearch(t) => Some(key_name(t)),
                _ => None,
            })
            .collect();
        assert_eq!(
            names,
            [
                "PARSER prs",
                "TEMPLATE tmpl",
                "DICTIONARY dict",
                "CONFIGURATION cfg"
            ]
        );
    }

    #[test]
    fn entry_keys_match_the_object_keys() {
        assert_eq!(
            entry_key_name("TEXT SEARCH CONFIGURATION", "cfg").as_deref(),
            Some("CONFIGURATION cfg")
        );
        assert_eq!(entry_key_name("TEXT SEARCH", "cfg"), None);
        assert_eq!(entry_key_name("TABLE", "cfg"), None);
        assert_eq!(
            split_key_name("DICTIONARY Stray Dict"),
            Some(("DICTIONARY", "Stray Dict"))
        );
        // the key of a container of more than one object is its schema
        assert_eq!(split_key_name("Quoted Schema"), None);
        assert_eq!(
            drop_sql("Quoted Schema", "DICTIONARY Stray Dict").as_deref(),
            Some(
                "DROP TEXT SEARCH DICTIONARY IF EXISTS \"Quoted Schema\".\
                 \"Stray Dict\";\n"
            )
        );
        assert_eq!(drop_sql("test", "test"), None);
    }

    #[test]
    fn names_are_canonical() {
        assert_eq!(canonical_name("simple"), "pg_catalog.simple");
        assert_eq!(canonical_name("PG_CATALOG.Simple"), "pg_catalog.simple");
        assert_eq!(
            canonical_name("pg_catalog.\"default\""),
            "pg_catalog.\"default\""
        );
        assert_eq!(canonical_name("\"default\""), "pg_catalog.\"default\"");
        assert_eq!(
            canonical_name("TEST.English_Simple"),
            "test.english_simple"
        );
        assert_eq!(
            canonical_name("\"Quoted Schema\".\"Quoted \"\"Dict\""),
            "\"Quoted Schema\".\"Quoted \"\"Dict\""
        );
        // a quoted name keeps its case, so it is another object
        assert_ne!(canonical_name("\"Simple\""), canonical_name("simple"));
    }

    #[test]
    fn short_forms_are_not_a_change() {
        for (written, pulled) in split(&written()).iter().zip(split(&pulled()))
        {
            assert_eq!(canonical(written), canonical(&pulled));
            assert_eq!(key_name(written), key_name(&pulled));
            assert!(
                statements(text_search(written, &pulled)).is_empty(),
                "{}",
                key_name(written)
            );
        }
    }

    #[test]
    fn option_values_compare_as_postgresql_stores_them() {
        let dict = |options: serde_json::Value| {
            container(serde_json::json!({
                "schema": "test",
                "dictionaries": [{
                    "name": "d", "template": "simple", "options": options,
                }],
            }))
        };
        // a number is stored as it is, and pull reads it as a string
        assert_eq!(
            canonical(&dict(serde_json::json!({"accept": 1}))),
            canonical(&dict(serde_json::json!({"accept": "1"})))
        );
        // a string in quotes keeps its case
        assert_ne!(
            canonical(&dict(serde_json::json!({"stopwords": "English"}))),
            canonical(&dict(serde_json::json!({"stopwords": "english"})))
        );
        // no options is the same as an empty list of options
        assert_eq!(
            canonical(&dict(serde_json::json!({}))),
            canonical(&container(serde_json::json!({
                "schema": "test",
                "dictionaries": [{"name": "d", "template": "simple"}],
            })))
        );
    }

    fn copy(mappings: serde_json::Value) -> TextSearch {
        container(serde_json::json!({
            "schema": "test",
            "configurations": [{
                "name": "english_urls",
                "source": "pg_catalog.english",
                "mappings": mappings,
            }],
        }))
    }

    fn expanded(url: &str) -> TextSearch {
        container(serde_json::json!({
            "schema": "test",
            "configurations": [{
                "name": "english_urls",
                "parser": "pg_catalog.\"default\"",
                "mappings": {
                    "asciiword": ["english_stem"],
                    "email": ["simple"],
                    "url": [url],
                },
            }],
        }))
    }

    #[test]
    fn a_copy_compares_the_mappings_that_it_gives() {
        let repo = copy(serde_json::json!({"URL": ["TEST.english_simple"]}));
        let db = expanded("test.english_simple");
        assert_eq!(canonical(&repo), canonical(&copied_view(&repo, &db)));
        assert!(statements(text_search(&repo, &db)).is_empty());
        // a changed mapping of the copy is set again
        let db = expanded("simple");
        assert_ne!(canonical(&repo), canonical(&copied_view(&repo, &db)));
        assert_eq!(
            statements(text_search(&repo, &db)),
            ["ALTER TEXT SEARCH CONFIGURATION test.english_urls ALTER \
              MAPPING FOR url WITH test.english_simple;\n"]
        );
        // a mapping that the copy gives and the database does not have
        // is added, and no mapping of the copy is dropped
        let repo = copy(serde_json::json!({"host": ["simple"]}));
        assert_eq!(
            statements(text_search(&repo, &db)),
            [
                "ALTER TEXT SEARCH CONFIGURATION test.english_urls ADD MAPPING \
              FOR host WITH pg_catalog.simple;\n"
            ]
        );
        // a configuration that is not a copy compares all its mappings
        let db = expanded("simple");
        assert_eq!(copied_view(&db, &db), db);
    }

    #[test]
    fn mappings_change_in_place() {
        let config = |mappings: serde_json::Value, comment: Option<&str>| {
            let mut value = serde_json::json!({
                "name": "Gate Cfg",
                "parser": "pg_catalog.\"default\"",
                "mappings": mappings,
            });
            if let Some(comment) = comment {
                value["comment"] = comment.into();
            }
            container(serde_json::json!({
                "schema": "test",
                "configurations": [value],
            }))
        };
        let repo = config(
            serde_json::json!({
                "asciiword": ["test.gate_dict", "simple"],
                "email": ["simple"],
            }),
            Some("Gate"),
        );
        let db = config(
            serde_json::json!({
                "asciiword": ["simple", "test.gate_dict"],
                "host": ["simple"],
            }),
            None,
        );
        assert_eq!(
            statements(text_search(&repo, &db)),
            [
                "ALTER TEXT SEARCH CONFIGURATION test.\"Gate Cfg\" ALTER \
                 MAPPING FOR asciiword WITH test.gate_dict, \
                 pg_catalog.simple;\n",
                "ALTER TEXT SEARCH CONFIGURATION test.\"Gate Cfg\" ADD \
                 MAPPING FOR email WITH pg_catalog.simple;\n",
                "ALTER TEXT SEARCH CONFIGURATION test.\"Gate Cfg\" DROP \
                 MAPPING FOR host;\n",
                "COMMENT ON TEXT SEARCH CONFIGURATION test.\"Gate Cfg\" IS \
                 $$Gate$$;\n",
            ]
        );
    }

    #[test]
    fn dictionary_options_change_in_place() {
        let dict = |options: serde_json::Value| {
            container(serde_json::json!({
                "schema": "test",
                "dictionaries": [{
                    "name": "d", "template": "simple", "options": options,
                }],
            }))
        };
        let repo = dict(serde_json::json!({
            "StopWords": "english", "Accept": false,
        }));
        let db = dict(serde_json::json!({
            "stopwords": "danish", "accept": "false", "extra": "1",
        }));
        assert_eq!(
            statements(text_search(&repo, &db)),
            [
                "ALTER TEXT SEARCH DICTIONARY test.d (stopwords = 'english', \
              extra);\n"
            ]
        );
    }

    #[test]
    fn changes_with_no_alter_form_are_rebuilt() {
        let replaced = |repo: &TextSearch, db: &TextSearch| {
            matches!(text_search(repo, db), Resolution::Replace)
        };
        let parts = |container: TextSearch| split(&container);
        let (written, pulled) = (parts(written()), parts(pulled()));
        // a parser function
        let mut parser = written[0].clone();
        parser.parsers.as_mut().unwrap()[0].headline_function = None;
        assert!(replaced(&parser, &pulled[0]));
        // a template function
        let mut template = written[1].clone();
        template.templates.as_mut().unwrap()[0].init_function = None;
        assert!(replaced(&template, &pulled[1]));
        // a dictionary template
        let mut dictionary = written[2].clone();
        dictionary.dictionaries.as_mut().unwrap()[0].template =
            Some(String::from("snowball"));
        assert!(replaced(&dictionary, &pulled[2]));
        // a configuration parser
        let mut configuration = written[3].clone();
        configuration.configurations.as_mut().unwrap()[0].parser =
            Some(String::from("test.prs"));
        assert!(replaced(&configuration, &pulled[3]));
        // a comment alone is set in place
        let mut commented = written[0].clone();
        commented.parsers.as_mut().unwrap()[0].comment =
            Some(String::from("New"));
        assert_eq!(
            statements(text_search(&commented, &pulled[0])),
            ["COMMENT ON TEXT SEARCH PARSER test.prs IS $$New$$;\n"]
        );
    }
}
