# Sourced by bin/deploy-gates (gate 2). A partitioned table can give
# some partitions by their bounds only and others as tables of their
# own (`attached: true`). An index of the partitioned table without ON
# ONLY then has an index in each partition, which PostgreSQL makes,
# and the plan stays empty. A partition that the project gives as a
# table of its own, where pull folds it into its parent because it has
# nothing of its own, is equal to it too.

mixed="${WORKDIR}/project/tables/test/parted_mixed.yaml"
mixed_two="${WORKDIR}/project/tables/test/parted_mixed_2.yaml"

cat > "${mixed}" <<'YAML'
---
name: parted_mixed
schema: test
owner: postgres
columns:
- name: k
  data_type: integer
- name: v
  data_type: text
indexes:
- name: parted_mixed_v
  columns:
  - name: v
partition:
  type: LIST
  columns:
  - k
partitions:
- name: parted_mixed_1
  schema: test
  for_values_in:
  - 1
- name: parted_mixed_2
  schema: test
  for_values_in:
  - 2
  attached: true
YAML
cat > "${mixed_two}" <<'YAML'
---
name: parted_mixed_2
schema: test
owner: postgres
columns:
- name: k
  data_type: integer
- name: v
  data_type: text
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "partitions by bounds and as tables of their own"

# a partition that pull folds, given as a table of its own
cat > "${mixed}" <<'YAML'
---
name: parted_mixed
schema: test
owner: postgres
columns:
- name: k
  data_type: integer
- name: v
  data_type: text
  nullable: false
partition:
  type: LIST
  columns:
  - k
partitions:
- name: parted_mixed_1
  schema: test
  for_values_in:
  - 1
  comment: the first partition
YAML
rm "${mixed_two}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.parted_mixed;"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a partition given by its bounds only"
perl -0pi -e 's/  comment: the first partition\n/  attached: true\n/' \
    "${mixed}"
cat > "${WORKDIR}/project/tables/test/parted_mixed_1.yaml" <<'YAML'
---
name: parted_mixed_1
schema: test
owner: postgres
columns:
- name: k
  data_type: integer
- name: v
  data_type: text
comment: the first partition
YAML
# the default privileges of the schema can give the partition a grant
# that the project does not give, and its REVOKE needs --allow-drop.
# The plan does not change the table
./target/debug/pglifecycle deploy --allow-drop -o "${WORKDIR}/replan.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -Eq '^(CREATE|DROP|ALTER) TABLE|^COMMENT' "${WORKDIR}/replan.sql"
then
    echo "Convergence gate FAILED: a folded partition given as a table" \
        "of its own is a change" >&2
    cat "${WORKDIR}/replan.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop \
    -d "${TARGET_DB}" "${WORKDIR}/project"
expect_empty_plan "a folded partition given as a table of its own"

rm "${mixed}" "${WORKDIR}/project/tables/test/parted_mixed_1.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.parted_mixed;"
expect_empty_plan "the mixed partition step leaves the database unchanged"
