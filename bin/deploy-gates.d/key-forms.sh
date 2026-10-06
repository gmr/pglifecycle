# Sourced by bin/deploy-gates (gate 2). The columns of a primary key or
# a unique constraint written as one name, or in the detailed form with
# only `columns`, are the same as the plain list that pull writes, thus
# the plan stays empty.

tables="${WORKDIR}/project/tables/test"
cat > "${tables}/gate_key_forms.yaml" <<'YAML'
---
name: gate_key_forms
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  nullable: false
- name: v
  data_type: integer
- name: w
  data_type: integer
primary_key:
  columns:
  - id
unique_constraints:
- columns:
  - w
- v
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "the detailed form of key columns"

rm "${tables}/gate_key_forms.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.gate_key_forms;"
expect_empty_plan "the key-forms step leaves the database unchanged"
