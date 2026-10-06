# Sourced by bin/deploy-gates (gate 2). A new partition of a
# partitioned table that the database has is made with CREATE TABLE
# ... PARTITION OF, or attached with ATTACH PARTITION, and not with a
# rebuild of the table. The data of the table stays. An attached
# partition gets the NOT NULL of its parent, which ATTACH PARTITION
# needs, also when its file does not give it. A partition that the
# project does not have is dropped, or detached when the project keeps
# its table. Both are destructive.

parted="${WORKDIR}/project/tables/test/parted_add.yaml"
attached="${WORKDIR}/project/tables/test/parted_add_3.yaml"

# $1: the partitions of the table, as YAML lines
write_parted_add() {
    cat > "${parted}" <<YAML
---
name: parted_add
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: k
  data_type: integer
- name: w
  data_type: integer
  nullable: false
primary_key:
- id
- k
partition:
  type: RANGE
  columns:
  - k
partitions:
- name: parted_add_1
  schema: test
  for_values_from: 0
  for_values_to: 10
$1
YAML
}

# $1: the NOT NULL of column w, as a YAML line or empty
write_parted_add_3() {
    cat > "${attached}" <<YAML
---
name: parted_add_3
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: k
  data_type: integer
- name: w
  data_type: integer
$1
indexes:
- name: parted_add_3_w
  columns:
  - name: w
primary_key:
- id
- k
YAML
}

# Plan the project against the target, with the options in $3, and
# require a plan with a line that matches $1; $2 says what the step
# checks
expect_planned() {
    # shellcheck disable=SC2086
    ./target/debug/pglifecycle deploy ${3:-} -o "${WORKDIR}/replan.sql" \
        -d "${TARGET_DB}" "${WORKDIR}/project"
    if ! grep -q "$1" "${WORKDIR}/replan.sql"; then
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/replan.sql" >&2
        exit 1
    fi
}

# Fail when the last plan has a line that matches $1; $2 says what the
# step checks
expect_not_planned() {
    if grep -q "$1" "${WORKDIR}/replan.sql"; then
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/replan.sql" >&2
        exit 1
    fi
}

# Fail when the table does not have $1 rows; $2 says what the step checks
expect_rows() {
    if [ "$(psql -d "${TARGET_DB}" -tAc \
            "SELECT count(*) FROM test.parted_add")" != "$1" ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

two='- name: parted_add_2
  schema: test
  for_values_from: 10
  for_values_to: 20
  comment: the second partition'
three='- name: parted_add_3
  schema: test
  for_values_from: 20
  for_values_to: 30
  attached: true'

write_parted_add ""
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a partitioned table with one partition"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "INSERT INTO test.parted_add VALUES (1, 1, 1);"

# a new partition given by its bounds, and a new attached partition
# whose file does not give the NOT NULL of w
write_parted_add "${two}
${three}"
write_parted_add_3 ""
expect_planned "^CREATE TABLE test.parted_add_2 PARTITION OF test.parted_add" \
    "a new partition is made with PARTITION OF"
expect_not_planned "^-- destructive statements: [1-9]" \
    "a new partition is not destructive"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_rows 1 "a new partition keeps the rows of its table"
expect_empty_plan "new partitions are made and attached"
# the NOT NULL that the file gives is the one that deploy made
write_parted_add_3 "  nullable: false"
expect_empty_plan "an attached partition has the NOT NULL of its parent"

# an attached partition that the project does not have is dropped,
# not detached
write_parted_add "${two}"
rm "${attached}"
expect_planned "^DROP TABLE .*test.parted_add_3" \
    "a removed attached partition is dropped" --allow-drop
expect_not_planned "DETACH PARTITION" \
    "a dropped partition is not detached"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a removed attached partition is dropped"

# an attached partition whose table the project keeps is detached
write_parted_add "${two}
${three}"
write_parted_add_3 "  nullable: false"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a partition is attached again"
write_parted_add "${two}"
expect_planned "^-- destructive statements: 1 excluded" \
    "a detach is withheld without --allow-drop"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a partition is detached"

# a partition given by its bounds that the project does not have is
# dropped
write_parted_add ""
expect_planned "^-- destructive statements: 1 excluded" \
    "a partition drop is withheld without --allow-drop"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_rows 1 "a removed partition keeps the rows of the other partitions"
expect_empty_plan "a removed partition is dropped"

rm "${parted}" "${attached}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.parted_add, test.parted_add_3;"
expect_empty_plan "the partition change step leaves the database unchanged"
