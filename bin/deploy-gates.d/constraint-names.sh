# Sourced by bin/deploy-gates (gate 2). PostgreSQL gives an object
# that has no name of its own the name `<table>_<columns>_<label>`
# (`makeObjectName`), and cuts the name to 63 bytes. The project gives
# no name to these constraints and sequences, and the table and column
# names are long, so that PostgreSQL cuts each name. deploy compares
# each name with the name that PostgreSQL cut, so the plan stays empty.
# Two columns that cut to the same NOT NULL name get a number on the
# second name, and deploy compares with that name too.

tables="${WORKDIR}/project/tables/test"
t63="gate_names_tttttttttttttttttttttttttttttttttttttttttttttttttttt"
long="gate_names_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
multibyte="gate_names_éééééééééééééééééééééééééé"
child="gate_names_child_cccccccccccccccccccccccccccccccccccccccccccc"
twins="gate_names_dddddddddddddddddddddddddddddd"

cat > "${tables}/${t63}.yaml" <<YAML
---
name: ${t63}
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  nullable: false
- name: x
  data_type: integer
  nullable: false
primary_key:
- id
YAML
cat > "${tables}/${long}.yaml" <<YAML
---
name: ${long}
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  nullable: false
- name: cccccccccccccccccccccccccccccccccccccccc
  data_type: integer
  nullable: false
- name: gggggggggggggggggggggggggggggggggggggggg
  data_type: integer
  nullable: false
  generated:
    sequence_behavior: BY DEFAULT
- name: ssssssssssssssssssssssssssssssssssssssss
  data_type: serial
primary_key:
- id
unique_constraints:
- - cccccccccccccccccccccccccccccccccccccccc
foreign_keys:
- columns:
  - cccccccccccccccccccccccccccccccccccccccc
  references:
    name: test.${t63}
    columns:
    - id
YAML
cat > "${tables}/${multibyte}.yaml" <<YAML
---
name: ${multibyte}
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  nullable: false
- name: ŝŝŝŝŝŝŝŝŝŝŝŝŝŝŝŝŝŝŝŝ
  data_type: integer
  nullable: false
primary_key:
- id
YAML
# the two NOT NULL names cut to the same name, and PostgreSQL adds a
# number to the second
cat > "${tables}/${twins}.yaml" <<YAML
---
name: ${twins}
schema: test
owner: postgres
columns:
- name: cccccccccccccccccccccccccccccccccccccccc_1
  data_type: integer
  nullable: false
- name: cccccccccccccccccccccccccccccccccccccccc_2
  data_type: integer
  nullable: false
YAML
# a partition takes the NOT NULL names of its parent, and has its own
# primary key name
cat > "${tables}/gate_names_parent.yaml" <<YAML
---
name: gate_names_parent
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  nullable: false
- name: k
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
- name: ${child}
  schema: test
  for_values_from: 0
  for_values_to: 10
  attached: true
YAML
cat > "${tables}/${child}.yaml" <<YAML
---
name: ${child}
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  nullable: false
  not_null_constraint:
    name: gate_names_parent_id_not_null
- name: k
  data_type: integer
  nullable: false
  not_null_constraint:
    name: gate_names_parent_k_not_null
primary_key:
- id
- k
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
# each name is the one that PostgreSQL cut to 63 bytes
if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT count(*) FROM pg_constraint
        WHERE conname IN (
            'gate_names_ttttttttttttttttttttttttttttttttttttttttttttttt_pkey',
            'gate_names_ttttttttttttttttttttttttttttttttttttttttt_x_not_null',
            'gate_names_aaaaaaaaaaaaaaaa_cccccccccccccccccccccccccc_not_null',
            'gate_names_aaaaaaaaaaaaaaaaaa_ccccccccccccccccccccccccccccc_key',
            'gate_names_aaaaaaaaaaaaaaaaaa_cccccccccccccccccccccccccccc_fkey',
            'gate_names_ééééééééééééééééééééééé_pkey',
            'gate_names_éééééééé_ŝŝŝŝŝŝŝŝŝŝŝŝŝ_not_null',
            'gate_names_child_ccccccccccccccccccccccccccccccccccccccccc_pkey',
            'gate_names_dddddddddddddddd_cccccccccccccccccccccccccc_not_null',
            'gate_names_ddddddddddddddd_cccccccccccccccccccccccccc_not_null1'
        )")" != 10 ]; then
    echo "Convergence gate FAILED: the generated names are not cut" >&2
    psql -d "${TARGET_DB}" -tAc "SELECT conname FROM pg_constraint
        WHERE conname LIKE 'gate_names%'" >&2
    exit 1
fi
expect_empty_plan "generated names compare cut to 63 bytes"

rm "${tables}/${t63}.yaml" "${tables}/${long}.yaml" \
    "${tables}/${multibyte}.yaml" "${tables}/gate_names_parent.yaml" \
    "${tables}/${child}.yaml" "${tables}/${twins}.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.${long}, test.${t63}, test.\"${multibyte}\",
        test.gate_names_parent, test.${twins};"
expect_empty_plan "the constraint-names step leaves the database unchanged"
