# Sourced by bin/deploy-gates (gate 2). A project can write a value in
# a form that PostgreSQL keeps in another form. deploy compares the two
# forms as equal, so the plan stays empty:
#
# 1. A storage parameter as a YAML number or boolean. PostgreSQL keeps
#    the text, and pull writes it as a string. A boolean compares by
#    its value, so false and off are equal.
# 2. A collation with or without the pg_catalog schema. pull writes
#    pg_catalog."C" for a column or a domain, and "C" for an index
#    column.
# 3. A type alias in a cast in an index expression. PostgreSQL keeps
#    the standard name of the type.
#
# A materialized view and its indexes compare as a table does.
# A real change to each of them is still a change.

# $1 is a pattern that the plan must have, $2 says what the step checks
expect_in_plan() {
    ./target/debug/pglifecycle deploy -o "${WORKDIR}/stored-forms.sql" \
        -d "${TARGET_DB}" "${WORKDIR}/project"
    if ! grep -Eq "$1" "${WORKDIR}/stored-forms.sql"; then
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/stored-forms.sql" >&2
        exit 1
    fi
    echo "Convergence gate passed: $2"
}

table="${WORKDIR}/project/tables/test/stored_forms.yaml"
domain="${WORKDIR}/project/domains/test/stored_label.yaml"
view="${WORKDIR}/project/materialized_views/test/stored_view.yaml"
mkdir -p "${view%/*}"

# the forms that pull writes
cat > "${table}" <<'YAML'
---
name: stored_forms
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  nullable: false
- name: label
  data_type: text
  collation: pg_catalog."C"
indexes:
- name: stored_forms_cast
  method: btree
  columns:
  - expression: (label)::character varying(20)
  storage_parameters:
    fillfactor: '80'
  comment: a cast to a type alias
- name: stored_forms_posix
  method: btree
  columns:
  - name: label
    collation: '"POSIX"'
primary_key:
- id
row_level_security:
  enabled: false
storage_parameters:
  fillfactor: '90'
  autovacuum_enabled: 'false'
YAML
cat > "${domain}" <<'YAML'
---
name: stored_label
schema: test
owner: postgres
data_type: text
collation: pg_catalog."C"
YAML
cat > "${view}" <<'YAML'
---
name: stored_view
schema: test
owner: postgres
storage_parameters:
  fillfactor: '90'
query: ' SELECT 1 AS n'
indexes:
- name: stored_view_n
  method: btree
  columns:
  - expression: (n)::bigint
  storage_parameters:
    fillfactor: '80'
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "the forms that pull writes are unchanged"

# 1. storage parameters as YAML numbers and booleans, and a boolean
#    that the database keeps as off
perl -pi -e "s/^(\s*fillfactor: )'(\d+)'\$/\$1\$2/; s/^(\s*autovacuum_enabled: )'false'\$/\$1false/" \
    "${table}" "${view}"
grep -q '^  fillfactor: 90$' "${table}"
grep -q '^    fillfactor: 80$' "${table}"
grep -q '^  fillfactor: 90$' "${view}"
grep -q '^    fillfactor: 80$' "${view}"
grep -q '^  autovacuum_enabled: false$' "${table}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "ALTER TABLE test.stored_forms SET (autovacuum_enabled = off);"
expect_empty_plan "storage parameters as numbers and booleans are unchanged"

# 2. the other form of each collation
perl -pi -e 's/collation: pg_catalog\.("\w+")$/collation: '"'"'$1'"'"'/' \
    "${table}" "${domain}"
perl -pi -e 's/^(    collation: )'"'"'"POSIX"'"'"'$/$1pg_catalog."POSIX"/' \
    "${table}"
grep -q "^  collation: '\"C\"'\$" "${table}"
grep -q '^    collation: pg_catalog."POSIX"$' "${table}"
grep -q "^collation: '\"C\"'\$" "${domain}"
expect_empty_plan "collations with and without pg_catalog are unchanged"

# 3. a type alias in a cast, with the parentheses around all of it
perl -pi -e 's/expression: \(label\)::character varying\(20\)$/expression: ((label)::VARCHAR(20))/' \
    "${table}"
perl -pi -e 's/expression: \(n\)::bigint$/expression: ((n)::INT8)/' "${view}"
grep -q 'expression: ((label)::VARCHAR(20))$' "${table}"
grep -q 'expression: ((n)::INT8)$' "${view}"
expect_empty_plan "a type alias in an index expression is unchanged"

# real changes: another fillfactor, collation and cast type
perl -pi -e 's/^  fillfactor: 90$/  fillfactor: 70/' "${table}"
expect_in_plan '^-- destructive statements: [1-9][0-9]* excluded' \
    "a changed table fillfactor is a change"
perl -pi -e 's/^  fillfactor: 70$/  fillfactor: 90/' "${table}"
perl -pi -e 's/^    fillfactor: 80$/    fillfactor: 70/' "${table}"
expect_in_plan '^CREATE INDEX stored_forms_cast .*WITH \(fillfactor=70\)' \
    "a changed index fillfactor is a change"
perl -pi -e 's/^    fillfactor: 70$/    fillfactor: 80/' "${table}"
perl -pi -e "s/^  collation: '\"C\"'\$/  collation: '\"POSIX\"'/" "${table}"
expect_in_plan '^-- destructive statements: [1-9][0-9]* excluded' \
    "a changed column collation is a change"
perl -pi -e "s/^  collation: '\"POSIX\"'\$/  collation: '\"C\"'/" "${table}"
perl -pi -e "s/^collation: '\"C\"'\$/collation: '\"POSIX\"'/" "${domain}"
expect_in_plan '^-- destructive statements: [1-9][0-9]* excluded' \
    "a changed domain collation is a change"
perl -pi -e "s/^collation: '\"POSIX\"'\$/collation: '\"C\"'/" "${domain}"
perl -pi -e 's/^    collation: pg_catalog."POSIX"$/    collation: pg_catalog."C"/' \
    "${table}"
expect_in_plan '^CREATE INDEX stored_forms_posix .*COLLATE (pg_catalog\.)?"C"' \
    "a changed index collation is a change"
perl -pi -e 's/^    collation: pg_catalog."C"$/    collation: pg_catalog."POSIX"/' \
    "${table}"
perl -pi -e 's/::VARCHAR\(20\)\)$/::VARCHAR(30))/' "${table}"
expect_in_plan '^CREATE INDEX stored_forms_cast .*::character varying\(30\)' \
    "a changed cast type is a change"
perl -pi -e 's/::VARCHAR\(30\)\)$/::VARCHAR(20))/' "${table}"
expect_empty_plan "the reverted project is unchanged"

rm "${table}" "${domain}" "${view}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.stored_forms;" -c "DROP DOMAIN test.stored_label;" \
    -c "DROP MATERIALIZED VIEW test.stored_view;"
expect_empty_plan "the stored-forms step leaves the database unchanged"
