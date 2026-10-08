# Sourced by bin/deploy-gates (gate 2). PostgreSQL adds casts and
# parentheses when it stores a view query: `name = 'x'` becomes
# `(name = 'x'::text)`. Formatting does not add them. Against a live
# database, deploy asks the server to deparse the project query, thus
# a query with no casts is not a change:
#
# 1. A view and a materialized view with hand-written queries give an
#    empty plan after one deploy.
# 2. A real change to each query is still a change.
# 3. A view query that uses a table that is new in the project cannot
#    be deparsed before the deploy. deploy compares the query as text,
#    gives a warning, and the deploy works.

table="${WORKDIR}/project/tables/test/deparse_items.yaml"
new_table="${WORKDIR}/project/tables/test/deparse_new.yaml"
view="${WORKDIR}/project/views/test/deparse_view.yaml"
matview="${WORKDIR}/project/materialized_views/test/deparse_matview.yaml"
mkdir -p "${table%/*}" "${view%/*}" "${matview%/*}"

# $1 is a pattern that the plan with --allow-drop must have, $2 says
# what the step checks
expect_deparse_change() {
    ./target/debug/pglifecycle deploy --allow-drop \
        -o "${WORKDIR}/deparse.sql" -d "${TARGET_DB}" \
        "${WORKDIR}/project"
    if ! grep -Eq "$1" "${WORKDIR}/deparse.sql"; then
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/deparse.sql" >&2
        exit 1
    fi
    echo "Convergence gate passed: $2"
}

cat > "${table}" <<'YAML'
---
name: deparse_items
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: name
  data_type: text
- name: price
  data_type: numeric
YAML
cat > "${view}" <<'YAML'
---
name: deparse_view
schema: test
owner: postgres
query: |
  SELECT id, name FROM test.deparse_items
   WHERE name = 'x' AND price > 1
dependencies:
  tables:
  - test.deparse_items
YAML
cat > "${matview}" <<'YAML'
---
name: deparse_matview
schema: test
owner: postgres
query: SELECT id FROM test.deparse_items WHERE id > 1.5
dependencies:
  tables:
  - test.deparse_items
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "view queries with no casts are unchanged"

# real changes: another constant in each query
perl -pi -e "s/'x'/'y'/" "${view}"
expect_deparse_change \
    "^CREATE OR REPLACE VIEW test\.deparse_view " \
    "a changed view query is a change"
perl -pi -e "s/'y'/'x'/" "${view}"
perl -pi -e 's/1\.5/2.5/' "${matview}"
expect_deparse_change \
    "^CREATE MATERIALIZED VIEW test\.deparse_matview " \
    "a changed materialized view query is a change"
perl -pi -e 's/2\.5/1.5/' "${matview}"
expect_empty_plan "the reverted queries are unchanged"

# a query that uses a new table: the deparse fails, deploy works
cat > "${new_table}" <<'YAML'
---
name: deparse_new
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
YAML
perl -pi -e \
    's/price > 1$/price > 1 AND id NOT IN (SELECT id FROM test.deparse_new)/' \
    "${view}"
perl -pi -e 's/^  - test\.deparse_items$/  - test.deparse_items\n  - test.deparse_new/' \
    "${view}"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project" 2>"${WORKDIR}/deparse.err"
if ! grep -q 'Cannot deparse the query of VIEW test\.deparse_view' \
        "${WORKDIR}/deparse.err"; then
    echo "Convergence gate FAILED: no warning for a failed deparse" >&2
    cat "${WORKDIR}/deparse.err" >&2
    exit 1
fi
expect_empty_plan "a view query that uses a new table converges"

rm "${table}" "${new_table}" "${view}" "${matview}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP VIEW test.deparse_view;" \
    -c "DROP MATERIALIZED VIEW test.deparse_matview;" \
    -c "DROP TABLE test.deparse_new;" \
    -c "DROP TABLE test.deparse_items;"
expect_empty_plan "the deparse step leaves the database unchanged"
