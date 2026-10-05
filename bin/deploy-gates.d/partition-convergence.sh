# Sourced by bin/deploy-gates (gate 2). CREATE TABLE ... PARTITION OF
# makes in the partition what the partitioned table has: the column
# defaults, the CHECK and NOT NULL constraints (with the names of the
# parent), and a primary key, unique and exclusion constraint and index
# for each one of the parent (with names of the partition). pg_dump
# writes the partition as a table of its own, thus pull makes it a
# table of its own, attached to its parent. deploy compares a partition
# that the project gives by its bounds only with that table, so the
# plan stays empty, and an index that only the partition has is still
# a change. A partition that the project gives as a table of its own,
# with no names for its NOT NULL constraints, has the names of its
# parent when PARTITION OF made it, and names of its own when deploy
# made it and attached it. PostgreSQL cannot rename either, and deploy
# accepts both.

tables="${WORKDIR}/project/tables/test"

# Plan the project against the target and require a plan with a line
# that matches $1; $2 says what the step checks
expect_planned() {
    ./target/debug/pglifecycle deploy -o "${WORKDIR}/replan.sql" \
        -d "${TARGET_DB}" "${WORKDIR}/project"
    if grep -q "$1" "${WORKDIR}/replan.sql"; then
        echo "Convergence gate passed: $2"
    else
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/replan.sql" >&2
        exit 1
    fi
}

cat > "${tables}/parted_bounds.yaml" <<'YAML'
---
name: parted_bounds
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: k
  data_type: integer
- name: v
  data_type: text
  default: '''x''::text'
- name: w
  data_type: integer
indexes:
- name: parted_bounds_w
  columns:
  - name: w
primary_key:
- id
- k
check_constraints:
- name: parted_bounds_k_check
  expression: (k >= 0)
unique_constraints:
- - v
  - k
partition:
  type: RANGE
  columns:
  - k
partitions:
- name: parted_bounds_1
  schema: test
  for_values_from: 0
  for_values_to: 10
- name: parted_bounds_2
  schema: test
  for_values_from: 10
  for_values_to: 20
  comment: the second partition
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "partitions given by their bounds only"

# an index that only the partition has is a change
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "CREATE INDEX parted_bounds_1_own ON test.parted_bounds_1 (v);"
expect_planned "^-- destructive statements: [1-9]" \
    "an index of a partition given by its bounds only is a change"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP INDEX test.parted_bounds_1_own;"
expect_empty_plan "a partition without its own index is unchanged"

# a partition table of its own, which PARTITION OF made: its NOT NULL
# constraints have the names of the parent
cat > "${tables}/parted_names.yaml" <<'YAML'
---
name: parted_names
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: k
  data_type: integer
primary_key:
- id
- k
partition:
  type: RANGE
  columns:
  - k
partitions:
- name: parted_names_1
  schema: test
  for_values_from: 0
  for_values_to: 10
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a partition of a primary key given by its bounds only"
perl -0pi -e 's/(  for_values_to: 10\n)/$1  attached: true\n/' \
    "${tables}/parted_names.yaml"
cat > "${tables}/parted_names_1.yaml" <<'YAML'
---
name: parted_names_1
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: k
  data_type: integer
indexes:
- name: parted_names_1_id
  columns:
  - name: id
primary_key:
- id
- k
YAML
expect_planned "^CREATE INDEX parted_names_1_id " \
    "an index of an attached partition is a change"
# the default privileges of the schema can give the partition a grant
# that the project does not give, and its REVOKE needs --allow-drop
./target/debug/pglifecycle deploy --apply --allow-drop \
    -d "${TARGET_DB}" "${WORKDIR}/project"
expect_empty_plan "a partition that PARTITION OF made has the NOT NULL \
names of its parent"

# a partition that deploy makes and attaches has NOT NULL names of
# its own
cat > "${tables}/parted_attach.yaml" <<'YAML'
---
name: parted_attach
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: k
  data_type: integer
primary_key:
- id
- k
partition:
  type: RANGE
  columns:
  - k
partitions:
- name: parted_attach_1
  schema: test
  for_values_from: 0
  for_values_to: 10
  attached: true
YAML
sed 's/parted_names_1/parted_attach_1/g' "${tables}/parted_names_1.yaml" \
    > "${tables}/parted_attach_1.yaml"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT string_agg(conname, ' '
        ORDER BY conname) FROM pg_constraint
        WHERE contype = 'n' AND conrelid IN (
            'test.parted_names_1'::regclass,
            'test.parted_attach_1'::regclass)")" != \
    "parted_attach_1_id_not_null parted_attach_1_k_not_null parted_names_id_not_null parted_names_k_not_null" ]
then
    echo "Convergence gate FAILED: a partition has other NOT NULL" \
        "names than PostgreSQL gives" >&2
    exit 1
fi
expect_empty_plan "a partition that deploy attached has NOT NULL names \
of its own"

rm "${tables}/parted_bounds.yaml" "${tables}/parted_names.yaml" \
    "${tables}/parted_names_1.yaml" "${tables}/parted_attach.yaml" \
    "${tables}/parted_attach_1.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.parted_bounds, test.parted_names, test.parted_attach;"
expect_empty_plan "the partition step leaves the database unchanged"
