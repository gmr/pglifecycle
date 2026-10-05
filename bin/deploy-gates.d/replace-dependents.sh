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

# A rebuilt table loses what PostgreSQL drops with it and the plan does
# not make again, thus deploy refuses the rebuild. The target gets
# another column order or fillfactor than the project, so that deploy
# must make the table again. Then the step restores the target

# $1 says what the step checks, $2 is the SQL that must be true after
# deploy refuses
expect_kept() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT $2")" != t ]; then
        echo "Convergence gate FAILED: deploy changed the database" \
            "($1)" >&2
        exit 1
    fi
}

# a partition that is an item of its own: PostgreSQL drops it with its
# table, and the plan does not make it again
cat > "${WORKDIR}/project/tables/test/gate_rd_parted.yaml" <<'YAML'
---
name: gate_rd_parted
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: k
  data_type: integer
partition:
  type: RANGE
  columns:
  - k
partitions:
- name: gate_rd_part
  schema: test
  for_values_from: 0
  for_values_to: 10
  attached: true
YAML
cat > "${WORKDIR}/project/tables/test/gate_rd_part.yaml" <<'YAML'
---
name: gate_rd_part
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: k
  data_type: integer
indexes:
- name: gate_rd_part_id
  method: btree
  columns:
  - name: id
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a table with a partition of its own is made"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO test.gate_rd_parted VALUES (1, 1);
ALTER TABLE test.gate_rd_parted DETACH PARTITION test.gate_rd_part;
DROP TABLE test.gate_rd_parted;
CREATE TABLE test.gate_rd_parted (k integer, id integer)
    PARTITION BY RANGE (k);
REVOKE SELECT ON test.gate_rd_parted FROM PUBLIC;
ALTER TABLE test.gate_rd_parted ATTACH PARTITION test.gate_rd_part
    FOR VALUES FROM (0) TO (10);
SQL
expect_refused "a table with a partition of its own" \
    'TABLE test.gate_rd_part: it is a partition of TABLE test.gate_rd_parted'
expect_kept "a table with a partition of its own" \
    "(SELECT count(*) FROM test.gate_rd_parted) = 1"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER TABLE test.gate_rd_parted DETACH PARTITION test.gate_rd_part;
DROP TABLE test.gate_rd_parted;
CREATE TABLE test.gate_rd_parted (id integer, k integer)
    PARTITION BY RANGE (k);
REVOKE SELECT ON test.gate_rd_parted FROM PUBLIC;
ALTER TABLE test.gate_rd_parted ATTACH PARTITION test.gate_rd_part
    FOR VALUES FROM (0) TO (10);
SQL
expect_empty_plan "a table with a partition of its own is restored"

# a table in a publication: PostgreSQL drops it from the publication,
# and the plan does not add it again
mkdir -p "${WORKDIR}/project/publications"
cat > "${WORKDIR}/project/tables/test/gate_rd_pub.yaml" <<'YAML'
---
name: gate_rd_pub
schema: test
owner: postgres
columns:
- name: n
  data_type: integer
YAML
cat > "${WORKDIR}/project/publications/gate_rd_pub.yaml" <<'YAML'
---
name: gate_rd_pub
tables:
- test.gate_rd_pub
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a table in a publication is made"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c 'ALTER TABLE test.gate_rd_pub SET (fillfactor = 50)'
expect_refused "a table in a publication" \
    'gate_rd_pub: deploy would remove the table from the publication'
expect_kept "a table in a publication" \
    "EXISTS (SELECT FROM pg_publication_tables
        WHERE pubname = 'gate_rd_pub' AND tablename = 'gate_rd_pub')"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c 'ALTER TABLE test.gate_rd_pub RESET (fillfactor)'
expect_empty_plan "a table in a publication is restored"

# a disabled trigger: the project does not keep the enabled state of a
# trigger, thus the rebuild would enable it again
cat > "${WORKDIR}/project/functions/test/gate_rd_tf.yaml" <<'YAML'
---
name: gate_rd_tf
schema: test
owner: postgres
returns: trigger
language: plpgsql
definition: |-
  BEGIN
    RETURN NEW;
  END;
YAML
cat > "${WORKDIR}/project/tables/test/gate_rd_trig.yaml" <<'YAML'
---
name: gate_rd_trig
schema: test
owner: postgres
columns:
- name: n
  data_type: integer
triggers:
- name: gate_rd_tg
  when: BEFORE
  events:
  - UPDATE
  for_each: ROW
  function: test.gate_rd_tf()
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a table with a trigger is made"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER TABLE test.gate_rd_trig DISABLE TRIGGER gate_rd_tg;
ALTER TABLE test.gate_rd_trig SET (fillfactor = 50);
SQL
expect_refused "a table with a disabled trigger" \
    'TRIGGER test.gate_rd_trig gate_rd_tg has statements after its CREATE'
expect_kept "a table with a disabled trigger" \
    "(SELECT tgenabled FROM pg_trigger WHERE tgname = 'gate_rd_tg') = 'D'"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER TABLE test.gate_rd_trig ENABLE TRIGGER gate_rd_tg;
ALTER TABLE test.gate_rd_trig RESET (fillfactor);
SQL
expect_empty_plan "a table with a disabled trigger is restored"
