# Sourced by bin/deploy-gates (gate 2). When the NOT NULL of a parent
# table changes to NO INHERIT, PostgreSQL keeps the inherited NOT NULL
# of each child as a NOT NULL of its own. A child that has no NOT NULL
# in the project drops it in the same deploy. A child with a NOT NULL
# of its own keeps it. A child of two parents drops it one time, after
# both parents change it.

parent="${WORKDIR}/project/tables/test/gate_ni_parent.yaml"
plain="${WORKDIR}/project/tables/test/gate_ni_plain.yaml"
own="${WORKDIR}/project/tables/test/gate_ni_own.yaml"
other="${WORKDIR}/project/tables/test/gate_ni_other.yaml"
both="${WORKDIR}/project/tables/test/gate_ni_both.yaml"

# $1: the not_null_constraint of the parent's id column
write_ni_parent() {
    write_ni_table "${parent}" gate_ni_parent "$1"
    write_ni_table "${other}" gate_ni_other "$1"
}
# $1: the file, $2: the table, $3: the not_null_constraint of id
write_ni_table() {
    cat > "$1" <<YAML
---
name: $2
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  nullable: false
$3
YAML
}
write_ni_parent ""
cat > "${plain}" <<'YAML'
---
name: gate_ni_plain
schema: test
owner: postgres
parents:
- test.gate_ni_parent
columns:
- name: x
  data_type: integer
dependencies:
  tables:
  - test.gate_ni_parent
YAML
cat > "${own}" <<'YAML'
---
name: gate_ni_own
schema: test
owner: postgres
parents:
- test.gate_ni_parent
columns:
- name: id
  data_type: integer
  nullable: false
dependencies:
  tables:
  - test.gate_ni_parent
YAML
cat > "${both}" <<'YAML'
---
name: gate_ni_both
schema: test
owner: postgres
parents:
- test.gate_ni_parent
- test.gate_ni_other
dependencies:
  tables:
  - test.gate_ni_other
  - test.gate_ni_parent
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a parent with a NOT NULL and its children are made"

write_ni_parent "  not_null_constraint:
    no_inherit: true"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "NO INHERIT on a parent's NOT NULL converges in one deploy"
if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT string_agg(
        attrelid::regclass::text || ' ' || attnotnull, ', '
        ORDER BY attrelid::regclass::text)
      FROM pg_attribute
     WHERE attrelid IN ('test.gate_ni_plain'::regclass,
                        'test.gate_ni_own'::regclass,
                        'test.gate_ni_both'::regclass)
       AND attname = 'id'")" \
        != "test.gate_ni_both false, test.gate_ni_own true, test.gate_ni_plain false" ]; then
    echo "Convergence gate FAILED: NO INHERIT did not keep the NOT NULL" \
        "of the child that has its own, and drop the other" >&2
    exit 1
fi

rm "${parent}" "${plain}" "${own}" "${other}" "${both}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.gate_ni_plain, test.gate_ni_own, \
        test.gate_ni_both, test.gate_ni_parent, test.gate_ni_other;"
expect_empty_plan "the no-inherit step leaves the database unchanged"
