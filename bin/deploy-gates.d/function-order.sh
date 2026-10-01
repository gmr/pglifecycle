# Sourced by bin/deploy-gates (gate 2). A table that the database has
# gets a default, a new column with a default, a check, an index and a
# trigger, each of which calls a function that this deploy makes. Each
# function depends on the table, so the build puts it after the table.
# The statements that call a function must come after its CREATE.

cat > "${WORKDIR}/project/tables/test/gate_order.yaml" <<'YAML'
---
name: gate_order
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: v
  data_type: integer
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "the table for the function order step is made"

cat > "${WORKDIR}/project/functions/test/gate_order_count.yaml" <<'YAML'
---
name: gate_order_count
schema: test
owner: postgres
returns: bigint
language: sql
sql_body: RETURN (SELECT count(*) AS count FROM test.gate_order)
dependencies:
  tables:
  - test.gate_order
YAML
cat > "${WORKDIR}/project/functions/test/gate_order_key.yaml" <<'YAML'
---
name: gate_order_key
schema: test
owner: postgres
parameters:
- mode: IN
  data_type: integer
  name: n
returns: integer
language: sql
immutable: true
sql_body: RETURN (n + 1)
dependencies:
  tables:
  - test.gate_order
YAML
cat > "${WORKDIR}/project/functions/test/gate_order_touch.yaml" <<'YAML'
---
name: gate_order_touch
schema: test
owner: postgres
returns: trigger
language: plpgsql
definition: |-
  BEGIN
    RETURN NEW;
  END;
dependencies:
  tables:
  - test.gate_order
YAML
cat > "${WORKDIR}/project/tables/test/gate_order.yaml" <<'YAML'
---
name: gate_order
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  default: test.gate_order_count()
- name: v
  data_type: integer
- name: w
  data_type: bigint
  default: test.gate_order_count()
check_constraints:
- name: gate_order_check
  expression: (test.gate_order_count() >= 0)
indexes:
- name: gate_order_key_idx
  method: btree
  columns:
  - expression: test.gate_order_key(id)
triggers:
- name: gate_order_touch
  when: BEFORE
  events:
  - INSERT
  for_each: ROW
  function: test.gate_order_touch()
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a changed table comes after the new functions it calls"
