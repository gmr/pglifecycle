# Sourced by bin/deploy-gates (gate 2). A table, a function and a
# procedure that the project writes as a raw sql statement: pull writes
# the structured fields, so deploy only checks that they exist. The raw
# procedure has no parameters, so deploy must match it by its name. The
# first deploy makes them, and the plan after it is empty.
mkdir -p "${WORKDIR}/project/tables/test" \
    "${WORKDIR}/project/functions/test" \
    "${WORKDIR}/project/procedures/test"
cat > "${WORKDIR}/project/tables/test/raw_sql_table.yaml" <<'YAML'
---
name: raw_sql_table
schema: test
owner: postgres
sql: CREATE TABLE test.raw_sql_table (id integer NOT NULL);
YAML
cat > "${WORKDIR}/project/functions/test/raw_sql_function.yaml" <<'YAML'
---
name: raw_sql_function
schema: test
owner: postgres
sql: >-
  CREATE FUNCTION test.raw_sql_function(n integer) RETURNS integer
  LANGUAGE sql AS $$SELECT n$$;
YAML
cat > "${WORKDIR}/project/procedures/test/raw_sql_procedure.yaml" <<'YAML'
---
name: raw_sql_procedure
schema: test
owner: postgres
sql: >-
  CREATE PROCEDURE test.raw_sql_procedure(n integer)
  LANGUAGE sql AS $$SELECT n$$;
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
raw_sql_objects="SELECT to_regclass('test.raw_sql_table') IS NOT NULL
    AND to_regprocedure('test.raw_sql_function(integer)') IS NOT NULL
    AND to_regprocedure('test.raw_sql_procedure(integer)') IS NOT NULL"
if [ "$(psql -d "${TARGET_DB}" -tAc "${raw_sql_objects}")" != t ]; then
    echo "Convergence gate FAILED: raw sql objects were not made" >&2
    exit 1
fi
expect_empty_plan "raw sql objects are unchanged"
