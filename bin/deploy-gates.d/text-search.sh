# Sourced by bin/deploy-gates (gate 2). Text search parsers,
# templates, dictionaries and configurations: deploy compares each
# object by its kind, schema and name, not by the container of its
# schema.
#
# 1. The objects written by hand in a short form: names in another
#    case, qualified and not, option names in mixed case, an option as
#    a boolean, and a configuration copied from another with only the
#    mapping it changes. New objects in a schema that has text search
#    objects are made. The plan is empty.
# 2. Drift in the mappings, the dictionary options and the comments
#    converges in place, without --allow-drop.
# 3. A changed dictionary template and a changed configuration parser
#    need a drop and a create, so they are withheld without
#    --allow-drop and applied with it.
# 4. A dictionary that a configuration maps cannot be dropped for a
#    rebuild. The deploy fails with a clear error, and the transaction
#    rolls back, so no other change of the script stays.
# 5. Objects that only the database has are withheld without
#    --allow-drop and dropped with it, each before what it uses.
# 6. A configuration whose token types are in uppercase is made, as
#    PostgreSQL folds them to lowercase. The plan is empty.

# $1 is a query that must return t, $2 says what the step checks
expect_text_search() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

# the mappings of configuration $1, as token=dict,dict;token=...
ts_mappings() {
    cat <<SQL
SELECT string_agg(token || '=' || dictionaries, ';' ORDER BY token)
  FROM (SELECT t.alias AS token,
               string_agg(m.mapdict::regdictionary::text, ','
                          ORDER BY m.mapseqno) AS dictionaries
          FROM pg_ts_config_map AS m
          JOIN pg_ts_config AS c ON c.oid = m.mapcfg
          JOIN LATERAL ts_token_type(c.cfgparser) AS t
            ON t.tokid = m.maptokentype
         WHERE c.oid = '$1'::regconfig
         GROUP BY t.alias) AS mappings
SQL
}

text_search="${WORKDIR}/project/text_search"
cat > "${text_search}/test.yaml" <<'YAML'
---
schema: test
configurations:
- name: english_urls
  source: english
  mappings:
    url:
    - TEST.English_Simple
- name: gate_cfg
  parser: '"default"'
  mappings:
    word:
    - pg_catalog.simple
    email:
    - simple
    asciiword:
    - Test.gate_dict
    - Simple
  comment: Gate configuration
dictionaries:
- name: english_simple
  template: SIMPLE
  options:
    StopWords: english
  comment: Simple, no stopwords
- name: gate_dict
  template: pg_catalog.Snowball
  options:
    Language: english
- name: gate_lone
  template: simple
  options:
    accept: true
parsers:
- name: default_copy
  start_function: PG_CATALOG.prsd_start
  gettoken_function: prsd_nexttoken
  end_function: pg_catalog.prsd_end
  lextypes_function: prsd_lextype
  headline_function: prsd_headline
  comment: Copy of default
templates:
- name: simple_copy
  init_function: dsimple_init
  lexize_function: pg_catalog.DSIMPLE_LEXIZE
  comment: Copy of simple
YAML
cat > "${text_search}/quoted.yaml" <<'YAML'
---
schema: Quoted Schema
dictionaries:
- name: Quoted Dict
  template: simple
  comment: A quoted dictionary
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_text_search "SELECT to_tsvector('test.gate_cfg', 'Running')
        = 'run:1'::tsvector
    AND ts_lexize('\"Quoted Schema\".\"Quoted Dict\"', 'X') = '{x}'
    AND EXISTS (SELECT FROM pg_ts_dict WHERE dictname = 'gate_lone'
                  AND dictinitoption = 'accept = ''true''')" \
    "the new text search objects were not made"
expect_empty_plan "text search short forms are unchanged"

# drift that deploy changes in place: a changed, a missing and a
# database-only mapping, the mapping that a copy changes, a changed
# and a database-only dictionary option, and comments
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER TEXT SEARCH CONFIGURATION test.gate_cfg
    ALTER MAPPING FOR asciiword WITH simple;
ALTER TEXT SEARCH CONFIGURATION test.gate_cfg DROP MAPPING FOR email;
ALTER TEXT SEARCH CONFIGURATION test.gate_cfg
    ADD MAPPING FOR host WITH simple;
ALTER TEXT SEARCH CONFIGURATION test.english_urls
    ALTER MAPPING FOR url WITH simple;
ALTER TEXT SEARCH DICTIONARY test.english_simple
    (stopwords = 'danish', accept = false);
COMMENT ON TEXT SEARCH DICTIONARY test.english_simple IS 'drift';
COMMENT ON TEXT SEARCH PARSER test.default_copy IS NULL;
COMMENT ON TEXT SEARCH TEMPLATE test.simple_copy IS 'drift';
COMMENT ON TEXT SEARCH CONFIGURATION test.gate_cfg IS NULL;
COMMENT ON TEXT SEARCH DICTIONARY "Quoted Schema"."Quoted Dict" IS 'drift';
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_text_search "SELECT ($(ts_mappings test.gate_cfg))
        = 'asciiword=test.gate_dict,simple;email=simple;word=simple'
    AND ($(ts_mappings test.english_urls)) LIKE '%url=test.english_simple%'
    AND (SELECT dictinitoption FROM pg_ts_dict
          WHERE oid = 'test.english_simple'::regdictionary)
        = 'stopwords = ''english'''
    AND obj_description('test.english_simple'::regdictionary,
                        'pg_ts_dict') = 'Simple, no stopwords'
    AND obj_description('test.gate_cfg'::regconfig, 'pg_ts_config')
        = 'Gate configuration'
    AND obj_description('\"Quoted Schema\".\"Quoted Dict\"'::regdictionary,
                        'pg_ts_dict') = 'A quoted dictionary'
    AND (SELECT obj_description(oid, 'pg_ts_parser') FROM pg_ts_parser
          WHERE prsname = 'default_copy') = 'Copy of default'
    AND (SELECT obj_description(oid, 'pg_ts_template') FROM pg_ts_template
          WHERE tmplname = 'simple_copy') = 'Copy of simple'" \
    "the text search drift did not converge in place"
expect_empty_plan "text search drift converges in place"

# a changed dictionary template (made in the database) and a changed
# configuration parser (made in the project) have no ALTER form
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP TEXT SEARCH DICTIONARY test.gate_lone;
CREATE TEXT SEARCH DICTIONARY test.gate_lone
    (TEMPLATE = snowball, language = english);
SQL
perl -0pi -e 's/parser: .\"default\".\n/parser: test.default_copy\n/' \
    "${text_search}/test.yaml"
grep -q '^  parser: test.default_copy$' "${text_search}/test.yaml"
# the two rebuilds, and the comment that the configuration gets again
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP TEXT SEARCH' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 3 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the text search rebuilds were not" \
        "withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_text_search "SELECT (SELECT t.tmplname FROM pg_ts_dict AS d
          JOIN pg_ts_template AS t ON t.oid = d.dicttemplate
         WHERE d.oid = 'test.gate_lone'::regdictionary) = 'simple'
    AND (SELECT p.prsname FROM pg_ts_config AS c
          JOIN pg_ts_parser AS p ON p.oid = c.cfgparser
         WHERE c.oid = 'test.gate_cfg'::regconfig) = 'default_copy'
    AND ($(ts_mappings test.gate_cfg))
        = 'asciiword=test.gate_dict,simple;email=simple;word=simple'" \
    "the text search objects were not made again"
expect_empty_plan "text search rebuilds converge"

# a rebuild of a dictionary that a configuration maps: DROP fails, as
# deploy does not use CASCADE, and the deploy rolls back. The comment
# drift comes before the drop in the script, so it shows that no
# statement of the script stays
perl -0pi -e 's/template: SIMPLE\n  options:\n    StopWords: english\n/template: snowball\n  options:\n    language: english\n/' \
    "${text_search}/test.yaml"
grep -q '^  template: snowball$' "${text_search}/test.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 -c \
    "COMMENT ON TEXT SEARCH TEMPLATE test.simple_copy IS 'rollback probe';"
if ./target/debug/pglifecycle deploy --apply --allow-drop \
        -d "${TARGET_DB}" "${WORKDIR}/project" \
        > /dev/null 2> "${WORKDIR}/text-search.err"; then
    echo "Convergence gate FAILED: the drop of a mapped dictionary" \
        "did not fail" >&2
    exit 1
fi
if ! grep -q 'cannot drop text search dictionary test.english_simple because other objects depend on it' \
        "${WORKDIR}/text-search.err" \
    || ! grep -q 'the transaction was rolled back' \
        "${WORKDIR}/text-search.err"; then
    echo "Convergence gate FAILED: the failed rebuild did not give a" \
        "clear error" >&2
    cat "${WORKDIR}/text-search.err" >&2
    exit 1
fi
expect_text_search "SELECT (SELECT t.tmplname FROM pg_ts_dict AS d
          JOIN pg_ts_template AS t ON t.oid = d.dicttemplate
         WHERE d.oid = 'test.english_simple'::regdictionary) = 'simple'
    AND (SELECT obj_description(oid, 'pg_ts_template') FROM pg_ts_template
          WHERE tmplname = 'simple_copy') = 'rollback probe'" \
    "the failed rebuild left a partial change"
echo "Convergence gate passed: a rebuild that a dependent object stops" \
    "rolls back"
perl -0pi -e 's/template: snowball\n  options:\n    language: english\n/template: SIMPLE\n  options:\n    StopWords: english\n/' \
    "${text_search}/test.yaml"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "text search converges after the failed rebuild"

# objects that only the database has, each of the four kinds, with a
# configuration that uses a parser and a dictionary that only the
# database has, and one in a schema whose name needs quoting
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE TEXT SEARCH PARSER test.stray_prs (START = prsd_start,
    GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);
CREATE TEXT SEARCH TEMPLATE test.stray_tmpl (LEXIZE = dsimple_lexize);
CREATE TEXT SEARCH DICTIONARY test.stray_dict (TEMPLATE = test.stray_tmpl);
CREATE TEXT SEARCH CONFIGURATION test.stray_cfg (PARSER = test.stray_prs);
ALTER TEXT SEARCH CONFIGURATION test.stray_cfg
    ADD MAPPING FOR word WITH test.stray_dict;
CREATE TEXT SEARCH DICTIONARY "Quoted Schema"."Stray Dict"
    (TEMPLATE = simple);
SQL
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP TEXT SEARCH' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 5 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the text search drops were not" \
        "withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_text_search "SELECT NOT EXISTS (SELECT FROM pg_ts_parser
                     WHERE prsname = 'stray_prs')
    AND NOT EXISTS (SELECT FROM pg_ts_template WHERE tmplname = 'stray_tmpl')
    AND NOT EXISTS (SELECT FROM pg_ts_dict
                     WHERE dictname IN ('stray_dict', 'Stray Dict'))
    AND NOT EXISTS (SELECT FROM pg_ts_config WHERE cfgname = 'stray_cfg')" \
    "the database-only text search objects were not dropped"
expect_empty_plan "database-only text search objects are dropped"

# token types in uppercase: the build writes each as PostgreSQL reads
# it, and deploy compares them in lowercase
cat > "${text_search}/upper.yaml" <<'YAML'
---
schema: public
configurations:
- name: gate_upper
  parser: pg_catalog.default
  mappings:
    URL:
    - simple
    ASCIIWord:
    - simple
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_text_search "SELECT ($(ts_mappings public.gate_upper))
        = 'asciiword=simple;url=simple'" \
    "the configuration with uppercase token types was not made"
expect_empty_plan "uppercase token types are unchanged"
