# Sourced by bin/deploy-gates (gate 2). A change to a column of a
# parent table must not change the inheritance children and the
# partitions, which the project gives values of their own. Without
# ONLY, PostgreSQL also changes the default, the statistics target and
# the storage of the column in each child (checked on PostgreSQL 18).
# On a partitioned table, DROP NOT NULL and SET EXPRESSION also change
# each partition, which the project models with all of its columns.

parent="${WORKDIR}/project/tables/test/gate_only_parent.yaml"
child="${WORKDIR}/project/tables/test/gate_only_child.yaml"
parted="${WORKDIR}/project/tables/test/gate_only_parted.yaml"
part="${WORKDIR}/project/tables/test/gate_only_part.yaml"

# $1: the parent's default, $2: the partitioned table's default, $3:
# its statistics target, $4: its storage, $5: its NOT NULL, $6: its
# generation expression
write_only_parents() {
    cat > "${parent}" <<YAML
---
name: gate_only_parent
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: c
  data_type: integer
  default: '$1'
YAML
    cat > "${parted}" <<YAML
---
name: gate_only_parted
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: c
  data_type: integer
  default: '$2'
- name: s
  data_type: text
  storage: $4
  statistics: $3
- name: n
  data_type: integer
  nullable: $5
- name: g
  data_type: integer
  generated:
    expression: $6
partition:
  type: RANGE
  columns:
  - id
partitions:
- name: gate_only_part
  schema: test
  for_values_from: 0
  for_values_to: 10
  attached: true
YAML
}
write_only_parents 1 1 50 EXTERNAL false '(id * 2)'
cat > "${child}" <<'YAML'
---
name: gate_only_child
schema: test
owner: postgres
parents:
- test.gate_only_parent
column_defaults:
- column: c
  default: '3'
dependencies:
  tables:
  - test.gate_only_parent
YAML
cat > "${part}" <<'YAML'
---
name: gate_only_part
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: c
  data_type: integer
  default: '3'
- name: s
  data_type: text
  storage: MAIN
  statistics: 30
- name: n
  data_type: integer
  nullable: false
  not_null_constraint:
    name: gate_only_parted_n_not_null
- name: g
  data_type: integer
  generated:
    expression: (id * 3)
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a parent and children with values of their own are made"

# the children's values, one row per child
only_children() {
    psql -d "${TARGET_DB}" -tA -F ' ' -c "
        SELECT a.attrelid::regclass, pg_get_expr(d.adbin, d.adrelid),
               a.attstattarget, a.attstorage, a.attnotnull
          FROM pg_attribute AS a
          LEFT JOIN pg_attrdef AS d
            ON (d.adrelid, d.adnum) = (a.attrelid, a.attnum)
         WHERE a.attrelid IN ('test.gate_only_child'::regclass,
                              'test.gate_only_part'::regclass)
           AND a.attnum > 0
         ORDER BY a.attrelid::regclass::text, a.attnum"
}
before="$(only_children)"
write_only_parents 2 2 70 PLAIN true '(id * 4)'
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
after="$(only_children)"
if [ "${before}" != "${after}" ]; then
    echo "Convergence gate FAILED: a change to a parent changed its" \
        "children" >&2
    diff <(echo "${before}") <(echo "${after}") >&2 || true
    exit 1
fi
expect_empty_plan "a change to a parent keeps the values of its children"

# DROP DEFAULT also keeps the defaults of the children
sed -i.bak '/^  default:/d' "${parent}" "${parted}"
rm "${parent}.bak" "${parted}.bak"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
after="$(only_children)"
if [ "${before}" != "${after}" ]; then
    echo "Convergence gate FAILED: DROP DEFAULT on a parent changed its" \
        "children" >&2
    diff <(echo "${before}") <(echo "${after}") >&2 || true
    exit 1
fi
expect_empty_plan "DROP DEFAULT on a parent keeps the children's defaults"

# the partition drops the NOT NULL that it kept from its parent
sed -i.bak -e '/^  nullable: false/d' -e '/^  not_null_constraint:/d' \
    -e '/^    name: gate_only_parted_n_not_null/d' "${part}"
rm "${part}.bak"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a partition drops the NOT NULL of its own"

rm "${parent}" "${child}" "${parted}" "${part}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.gate_only_child, test.gate_only_parent, \
        test.gate_only_parted;"
expect_empty_plan "the parent-only step leaves the database unchanged"
