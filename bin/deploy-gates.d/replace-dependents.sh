# Sourced by bin/deploy-gates (gate 2). When deploy drops an object and
# makes it again, the objects that depend on it stop the DROP: a view
# whose columns change (CREATE OR REPLACE VIEW cannot rename a column),
# and a table that deploy makes again. Deploy drops the dependents
# before the object and makes them again after it. Without --allow-drop
# all of these statements are withheld together.

cat > "${WORKDIR}/project/views/test/gate_rd_base.yaml" <<'YAML'
---
name: gate_rd_base
schema: test
owner: postgres
query: " SELECT 1 AS a,\n    2 AS b"
YAML
cat > "${WORKDIR}/project/views/test/gate_rd_outer.yaml" <<'YAML'
---
name: gate_rd_outer
schema: test
owner: postgres
query: " SELECT a,\n    b\n   FROM test.gate_rd_base"
comment: reads test.gate_rd_base
dependencies:
  views:
  - test.gate_rd_base
YAML
cat > "${WORKDIR}/project/tables/test/gate_rd_table.yaml" <<'YAML'
---
name: gate_rd_table
schema: test
owner: postgres
columns:
- name: n
  data_type: integer
YAML
cat > "${WORKDIR}/project/views/test/gate_rd_table_view.yaml" <<'YAML'
---
name: gate_rd_table_view
schema: test
owner: postgres
query: " SELECT n\n   FROM test.gate_rd_table"
dependencies:
  tables:
  - test.gate_rd_table
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a view and a table with dependent views are made"

# $1 says what the step checks, $2 is the label of the dependent that
# the header lists: without --allow-drop the plan withholds the object
# and its dependents together, and the script has no statement for
# them; with --allow-drop deploy converges
expect_rebuilt_dependents() {
    ./target/debug/pglifecycle deploy -o "${WORKDIR}/replaced.sql" \
        -d "${TARGET_DB}" "${WORKDIR}/project"
    if ! grep -q '^-- destructive statements: [0-9]* excluded' \
            "${WORKDIR}/replaced.sql" \
        || ! grep -q "^-- dependents rebuilt with a replaced object: .*$2" \
            "${WORKDIR}/replaced.sql" \
        || grep -v '^--' "${WORKDIR}/replaced.sql" | grep -q gate_rd
    then
        echo "Convergence gate FAILED: the object and its dependents" \
            "were not withheld together ($1)" >&2
        cat "${WORKDIR}/replaced.sql" >&2
        exit 1
    fi
    ./target/debug/pglifecycle deploy --apply --allow-drop \
        -d "${TARGET_DB}" "${WORKDIR}/project"
    if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT
            (SELECT a + b FROM test.gate_rd_outer) = 3
            AND obj_description('test.gate_rd_outer'::regclass,
                'pg_class') = 'reads test.gate_rd_base'
            AND (SELECT relpersistence FROM pg_class
                WHERE oid = 'test.gate_rd_table'::regclass) = 'p'
            AND to_regclass('test.gate_rd_table_view') IS NOT NULL")" != t ]
    then
        echo "Convergence gate FAILED: the object or its dependents" \
            "were not made again ($1)" >&2
        exit 1
    fi
    expect_empty_plan "$1"
}

# a view column with another name: CREATE OR REPLACE VIEW cannot rename
# it, thus deploy drops the view, and first the view that reads it
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP VIEW test.gate_rd_outer;
DROP VIEW test.gate_rd_base;
CREATE VIEW test.gate_rd_base AS SELECT 1 AS x, 2 AS b;
CREATE VIEW test.gate_rd_outer AS SELECT x AS a, b FROM test.gate_rd_base;
COMMENT ON VIEW test.gate_rd_outer IS 'reads test.gate_rd_base';
REVOKE SELECT ON test.gate_rd_base, test.gate_rd_outer FROM PUBLIC;
SQL
expect_rebuilt_dependents "a replaced view with a dependent view converges" \
    'VIEW test.gate_rd_outer'

# an unlogged table: deploy makes the table again, and first drops the
# view that reads it
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c 'ALTER TABLE test.gate_rd_table SET UNLOGGED'
expect_rebuilt_dependents "a rebuilt table with a dependent view converges" \
    'VIEW test.gate_rd_table_view'
