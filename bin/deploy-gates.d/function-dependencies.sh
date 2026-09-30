# Sourced by bin/deploy-gates (gate 2). PostgreSQL examines a
# SQL-standard body (BEGIN ATOMIC or RETURN) when it makes the
# function, so the function has to come after each object that its
# body uses. A hand-written project states these in `dependencies`.
# A function entry gives the argument types of one overload, in any
# form that PostgreSQL resolves to the same types. A bare name is
# sufficient when only one overload has the name. Name order puts each
# `a*_gate_` function before the object that it uses, and type order
# puts a function before a table.

cat > "${WORKDIR}/project/functions/test/zz_gate_callee.yaml" <<'YAML'
---
name: zz_gate_callee
schema: test
owner: postgres
parameters:
- mode: IN
  name: n
  data_type: integer
returns: integer
language: sql
sql_body: RETURN (n + 1)
YAML
cat > "${WORKDIR}/project/functions/test/aa_gate_caller.yaml" <<'YAML'
---
name: aa_gate_caller
schema: test
owner: postgres
returns: integer
language: sql
sql_body: RETURN test.zz_gate_callee(1)
dependencies:
  functions:
  - test.zz_gate_callee(INT4)
YAML
cat > "${WORKDIR}/project/functions/test/ab_gate_bare_caller.yaml" <<'YAML'
---
name: ab_gate_bare_caller
schema: test
owner: postgres
returns: integer
language: sql
sql_body: RETURN test.zz_gate_callee(2)
dependencies:
  functions:
  - test.zz_gate_callee
YAML
cat > "${WORKDIR}/project/tables/test/zz_gate_table.yaml" <<'YAML'
---
name: zz_gate_table
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
YAML
cat > "${WORKDIR}/project/functions/test/aa_gate_reader.yaml" <<'YAML'
---
name: aa_gate_reader
schema: test
owner: postgres
returns: bigint
language: sql
sql_body: RETURN (SELECT count(*) AS count FROM test.zz_gate_table)
dependencies:
  tables:
  - test.zz_gate_table
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT test.aa_gate_caller()
        + test.ab_gate_bare_caller() + test.aa_gate_reader()")" != 5 ]
then
    echo "Convergence gate FAILED: the functions with dependencies" \
        "were not made" >&2
    exit 1
fi
expect_empty_plan "functions are made after the objects that they use"
