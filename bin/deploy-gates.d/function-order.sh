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

# A new column has a default that calls a new function with a
# SQL-standard body, which reads the column. The column comes before
# the function, and gets its default after it. A changed view calls a
# new function that depends on the view: CREATE OR REPLACE VIEW, and
# then the drop and the create of the view, come after the function.
mkdir -p "${WORKDIR}/project/views/test"
cat > "${WORKDIR}/project/views/test/gate_order_view.yaml" <<'YAML'
---
name: gate_order_view
schema: test
owner: postgres
query: " SELECT id\n   FROM test.gate_order"
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "the view for the function order step is made"

cat > "${WORKDIR}/project/functions/test/gate_order_max.yaml" <<'YAML'
---
name: gate_order_max
schema: test
owner: postgres
returns: integer
language: sql
sql_body: RETURN (SELECT max(gate_order.x) AS max FROM test.gate_order)
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
- name: x
  data_type: integer
  default: test.gate_order_max()
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
cat > "${WORKDIR}/project/functions/test/gate_order_label.yaml" <<'YAML'
---
name: gate_order_label
schema: test
owner: postgres
returns: text
language: plpgsql
definition: |-
  BEGIN
    RETURN 'x';
  END;
dependencies:
  views:
  - test.gate_order_view
YAML
cat > "${WORKDIR}/project/views/test/gate_order_view.yaml" <<'YAML'
---
name: gate_order_view
schema: test
owner: postgres
query: " SELECT id,\n    test.gate_order_label() AS label\n   FROM test.gate_order"
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a new column and a changed view come before and after \
the new functions that they need"

cat > "${WORKDIR}/project/functions/test/gate_order_tag.yaml" <<'YAML'
---
name: gate_order_tag
schema: test
owner: postgres
returns: text
language: plpgsql
definition: |-
  BEGIN
    RETURN 'y';
  END;
dependencies:
  views:
  - test.gate_order_view
YAML
cat > "${WORKDIR}/project/views/test/gate_order_view.yaml" <<'YAML'
---
name: gate_order_view
schema: test
owner: postgres
query: " SELECT id,\n    test.gate_order_tag() AS tag\n   FROM test.gate_order"
comment: calls test.gate_order_tag
YAML
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a replaced view comes after the new function that it \
calls"
