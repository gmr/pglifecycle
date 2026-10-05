# Sourced by bin/deploy-gates (gate 2). A CHECK written on a column,
# as a person writes it: `check_constraint: ee > 0`. The CHECK renders
# as a constraint of the table with the name `<table>_<column>_check`,
# as pull reads it, and deploy compares the column CHECK with that
# constraint, thus the plan stays empty. The name is that of the
# column, also when the expression refers to another column (gg), and
# it has a number when a CHECK of the table has the name (hh).

tables="${WORKDIR}/project/tables/test"
cat > "${tables}/gate_column_check.yaml" <<'YAML'
---
name: gate_column_check
schema: test
owner: postgres
columns:
- name: ee
  data_type: integer
  check_constraint: ee > 0
- name: ff
  data_type: text
  check_constraint: length(ff) < 10
- name: gg
  data_type: integer
  check_constraint: ee < 100
- name: hh
  data_type: integer
  check_constraint: hh > 0
check_constraints:
- name: gate_column_check_hh_check
  expression: (hh < 100)
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT count(*) FROM pg_constraint
        WHERE conrelid = 'test.gate_column_check'::regclass
          AND contype = 'c'
          AND conname IN ('gate_column_check_ee_check',
                          'gate_column_check_ff_check',
                          'gate_column_check_gg_check',
                          'gate_column_check_hh_check',
                          'gate_column_check_hh_check1')")" != 5 ]; then
    echo "Convergence gate FAILED: the column CHECKs are not made" >&2
    exit 1
fi
expect_empty_plan "a column CHECK compares with the table CHECK"

rm "${tables}/gate_column_check.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.gate_column_check;"
expect_empty_plan "the column-check step leaves the database unchanged"
