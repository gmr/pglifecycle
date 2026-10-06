# Sourced by bin/deploy-gates (gate 2). A CHECK written on a column,
# as a person writes it: `check_constraint: ee > 0`. The CHECK renders
# as a constraint of the table with the name that PostgreSQL gives it,
# as pull reads it, and deploy compares the column CHECK with that
# constraint, thus the plan stays empty. The name is
# `<table>_<column>_check` with the column of the expression, also
# when that is another column (gg). It is `<table>_check` when the
# expression uses more than one column (ii), and it has a number when
# a CHECK of the table has the name (gg, hh).

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
- name: ii
  data_type: integer
  check_constraint: ii > ee
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
                          'gate_column_check_ee_check1',
                          'gate_column_check_hh_check',
                          'gate_column_check_hh_check1',
                          'gate_column_check_check')")" != 6 ]; then
    echo "Convergence gate FAILED: the column CHECKs are not made" >&2
    exit 1
fi
expect_empty_plan "a column CHECK compares with the table CHECK"

rm "${tables}/gate_column_check.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.gate_column_check;"
expect_empty_plan "the column-check step leaves the database unchanged"
