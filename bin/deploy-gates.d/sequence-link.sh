# Sourced by bin/deploy-gates (gate 2). A sequence that the database
# links to a column, and that the project links to another column or
# to no column. The project also drops the old column. DROP COLUMN
# drops each sequence that the column owns, thus the plan unlinks the
# sequence before it. One sequence sorts before the table and one
# after it. A link to a new column comes after its ADD COLUMN.

expect_links() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "
        SELECT string_agg(c.relname || '=' || coalesce(a.attname, ''),
                          ',' ORDER BY c.relname)
          FROM pg_class AS c
          LEFT JOIN pg_depend AS d
                 ON d.objid = c.oid AND d.deptype = 'a'
                AND d.refclassid = 'pg_class'::regclass
          LEFT JOIN pg_attribute AS a
                 ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
         WHERE c.relkind = 'S' AND c.relname LIKE 'gate_relink_%'")" \
        != "$1" ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

mkdir -p "${WORKDIR}/project/sequences/test"
cat > "${WORKDIR}/project/tables/test/gate_relink_t.yaml" <<'YAML'
---
name: gate_relink_t
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: old
  data_type: integer
YAML
# the table comes first, as the project gives the sequences no
# dependency on it
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
for name in gate_relink_a gate_relink_z; do
    cat > "${WORKDIR}/project/sequences/test/${name}.yaml" <<YAML
---
schema: test
name: ${name}
owner: postgres
data_type: integer
owned_by: test.gate_relink_t.old
YAML
done
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "the table and the sequences that its column owns are made"

# drop the old column, link one sequence to another column, and unlink
# the other sequence
cat > "${WORKDIR}/project/tables/test/gate_relink_t.yaml" <<'YAML'
---
name: gate_relink_t
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
YAML
perl -pi -e 's/\.old$/.id/' \
    "${WORKDIR}/project/sequences/test/gate_relink_a.yaml"
perl -ni -e 'print unless /^owned_by:/' \
    "${WORKDIR}/project/sequences/test/gate_relink_z.yaml"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_links "gate_relink_a=id,gate_relink_z=" \
    "a sequence that the old column owned was not kept or linked"
expect_empty_plan "a sequence is unlinked before the drop of its column"

# link the sequence to a new column
cat >> "${WORKDIR}/project/tables/test/gate_relink_t.yaml" <<'YAML'
- name: new
  data_type: integer
YAML
perl -pi -e 's/\.id$/.new/' \
    "${WORKDIR}/project/sequences/test/gate_relink_a.yaml"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_links "gate_relink_a=new,gate_relink_z=" \
    "a sequence was not linked to a new column"
expect_empty_plan "a sequence is linked after the ADD COLUMN of its column"
