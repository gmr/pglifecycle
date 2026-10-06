# Sourced by bin/deploy-gates (gate 2). PostgreSQL gives a domain
# CHECK with no name the name `<domain>_check` (`ChooseConstraintName`),
# with a number when the domain has that name, cut to 63 bytes, and
# pg_dump writes that name. pg_dump writes the checks in the order of
# their names. deploy compares an unnamed check of the project with
# the name that PostgreSQL gives it, and compares the checks in the
# order of their names, so the plan stays empty. A NOT NULL with no
# name and a CHECK with the name `<domain>_not_null`, and a constraint
# name that must be quoted, are made with the domain.

domains="${WORKDIR}/project/domains/test"
long="a_long_gate_domain_name_that_cuts_the_generated_check_name_xx"

# $1 is the constraint list of the long domain
write_long() {
    cat > "${domains}/${long}.yaml" <<YAML
---
name: ${long}
schema: test
owner: postgres
data_type: integer
check_constraints:
- name: zz_upper
  expression: VALUE < 1000
- expression: VALUE > 0
$1
YAML
}

write_long "- expression: VALUE <> 5"
cat > "${domains}/gate_domain_chk_nn.yaml" <<YAML
---
name: gate_domain_chk_nn
schema: test
owner: postgres
data_type: integer
check_constraints:
- nullable: false
- name: gate_domain_chk_nn_not_null
  expression: VALUE > 0
YAML
cat > "${domains}/gate_domain_chk_quoted.yaml" <<YAML
---
name: gate_domain_chk_quoted
schema: test
owner: postgres
data_type: integer
check_constraints:
- name: Gate Check
  nullable: false
- name: Gate Positive
  expression: VALUE > 0
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "unnamed domain checks out of name order"

# the NOT NULL with no name gets the next name, as PostgreSQL names it
nn_names="SELECT string_agg(conname, ',' ORDER BY conname)
    FROM pg_constraint
    WHERE contypid = 'test.gate_domain_chk_nn'::regtype"
if [ "$(psql -d "${TARGET_DB}" -tAc "${nn_names}")" \
    != "gate_domain_chk_nn_not_null,gate_domain_chk_nn_not_null1" ]; then
    echo "Convergence gate FAILED: wrong NOT NULL name" >&2
    psql -d "${TARGET_DB}" -tAc "${nn_names}" >&2
    exit 1
fi

# an unnamed check added after the others is added in place, with the
# next name
write_long "- expression: VALUE <> 5
- expression: VALUE <> 7"
./target/debug/pglifecycle deploy -o "${WORKDIR}/domain-chk.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
added="ALTER DOMAIN test.${long} ADD CONSTRAINT \
a_long_gate_domain_name_that_cuts_the_generated_check_na_check2 \
CHECK ((VALUE <> 7));"
if ! grep -Fxq "${added}" "${WORKDIR}/domain-chk.sql"; then
    echo "Convergence gate FAILED: no ${added}" >&2
    cat "${WORKDIR}/domain-chk.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "an unnamed domain check is added in place"

# an unnamed check before the others moves their names: they are
# renamed in place, and the domain is not made again
write_long "- expression: VALUE <> 4
- expression: VALUE <> 5
- expression: VALUE <> 7"
./target/debug/pglifecycle deploy -o "${WORKDIR}/domain-chk.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
renamed="ALTER DOMAIN test.${long} RENAME CONSTRAINT \
a_long_gate_domain_name_that_cuts_the_generated_check_na_check2 TO \
a_long_gate_domain_name_that_cuts_the_generated_check_na_check3;"
if ! grep -Fxq "${renamed}" "${WORKDIR}/domain-chk.sql" \
    || grep -q "DROP DOMAIN" "${WORKDIR}/domain-chk.sql"; then
    echo "Convergence gate FAILED: no ${renamed}" >&2
    cat "${WORKDIR}/domain-chk.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a new unnamed domain check before the others"

# a changed check is still a change: the domain is made again
write_long "- expression: VALUE <> 6
- expression: VALUE <> 7"
./target/debug/pglifecycle deploy --allow-drop \
    -o "${WORKDIR}/domain-chk.sql" -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -Fxq "DROP DOMAIN IF EXISTS test.${long};" \
        "${WORKDIR}/domain-chk.sql"; then
    echo "Convergence gate FAILED: a changed domain check is not planned" >&2
    cat "${WORKDIR}/domain-chk.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a changed domain check"

rm "${domains}/${long}.yaml" "${domains}/gate_domain_chk_nn.yaml" \
    "${domains}/gate_domain_chk_quoted.yaml"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "the check domains are dropped"
