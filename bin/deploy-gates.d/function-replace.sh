# Sourced by bin/deploy-gates (gate 2). A function body that a person
# writes has no space at its start or end, and pull formats the body
# that PostgreSQL keeps. Deploy compares a SQL or PL/pgSQL body without
# the space at its start and end, so the plan stays empty. A change
# that CREATE OR REPLACE FUNCTION cannot do (remove a default, rename
# an input parameter, change the output row) drops the function and
# makes it again, only with --allow-drop.

# $1 is a query that must return t, $2 says what the step checks
expect_function() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

# $1 says what the step checks: the plan withholds one drop, and
# --allow-drop applies it
expect_rebuild() {
    ./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
        -d "${TARGET_DB}" "${WORKDIR}/project"
    if ! grep -q '^-- destructive statements: 1 excluded' \
            "${WORKDIR}/withheld.sql"; then
        echo "Convergence gate FAILED: the rebuild was not withheld" \
            "($1)" >&2
        cat "${WORKDIR}/withheld.sql" >&2
        exit 1
    fi
    ./target/debug/pglifecycle deploy --apply --allow-drop \
        -d "${TARGET_DB}" "${WORKDIR}/project"
}

# bodies with no space at the start, and one with a line break at the
# end
cat > "${WORKDIR}/project/functions/test/gate_body.yaml" <<'YAML'
---
name: gate_body
schema: test
owner: postgres
returns: integer
language: sql
definition: SELECT 1;
YAML
cat > "${WORKDIR}/project/functions/test/gate_pl_body.yaml" <<'YAML'
---
name: gate_pl_body
schema: test
owner: postgres
returns: integer
language: plpgsql
definition: |
  BEGIN
    RETURN 1;
  END;
YAML
cat > "${WORKDIR}/project/functions/test/gate_signature.yaml" <<'YAML'
---
name: gate_signature
schema: test
owner: postgres
parameters:
- mode: IN
  name: a
  data_type: integer
- mode: IN
  name: b
  data_type: integer
returns: integer
language: sql
definition: SELECT a + b;
YAML
cat > "${WORKDIR}/project/functions/test/gate_row.yaml" <<'YAML'
---
name: gate_row
schema: test
owner: postgres
parameters:
- mode: IN
  name: a
  data_type: integer
- mode: OUT
  name: b
  data_type: integer
- mode: OUT
  name: c
  data_type: integer
returns: record
language: plpgsql
definition: |
  BEGIN
    b := a;
    c := a;
  END;
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_function "SELECT to_regprocedure('test.gate_row(integer)')
    IS NOT NULL" "the new functions were not made"
expect_empty_plan "function bodies with no outer space are unchanged"

# a changed body is still a change
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE OR REPLACE FUNCTION test.gate_body() RETURNS integer
    LANGUAGE sql AS $$SELECT 2;$$;
CREATE OR REPLACE FUNCTION test.gate_pl_body() RETURNS integer
    LANGUAGE plpgsql AS $$BEGIN RETURN 2; END;$$;
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_function "SELECT bool_and(prosrc LIKE '%1;%') FROM pg_proc
    WHERE oid IN ('test.gate_body()'::regprocedure,
        'test.gate_pl_body()'::regprocedure)" \
    "the changed function bodies were not replaced"
expect_empty_plan "changed function bodies converge"

# a default that the project does not have
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP FUNCTION test.gate_signature(integer, integer);
CREATE FUNCTION test.gate_signature(a integer, b integer DEFAULT 0)
    RETURNS integer LANGUAGE sql AS $$SELECT a + b;$$;
SQL
expect_rebuild "a removed default"
expect_function "SELECT pronargdefaults = 0 FROM pg_proc
    WHERE oid = 'test.gate_signature(integer, integer)'::regprocedure" \
    "the function default was not removed"
expect_empty_plan "a removed function default converges"

# an input parameter with another name
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP FUNCTION test.gate_signature(integer, integer);
CREATE FUNCTION test.gate_signature(a integer, x integer)
    RETURNS integer LANGUAGE sql AS $$SELECT a + x;$$;
SQL
expect_rebuild "a renamed input parameter"
expect_function "SELECT pg_get_function_identity_arguments(
        'test.gate_signature(integer, integer)'::regprocedure)
    = 'a integer, b integer'" \
    "the function with a renamed parameter was not made again"
expect_empty_plan "a renamed function parameter converges"

# an INOUT parameter where the project has IN: the output row changes
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP FUNCTION test.gate_row(integer);
CREATE FUNCTION test.gate_row(INOUT a integer, OUT b integer,
    OUT c integer) LANGUAGE plpgsql AS $$BEGIN b := a; c := a; END;$$;
SQL
expect_rebuild "an INOUT parameter"
expect_function "SELECT pg_get_function_arguments(
        'test.gate_row(integer)'::regprocedure)
    = 'a integer, OUT b integer, OUT c integer'" \
    "the function with a changed output row was not made again"
expect_empty_plan "a changed function output row converges"
