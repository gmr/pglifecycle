# Sourced by bin/deploy-gates (gate 2). A domain NOT NULL changes in
# place: ALTER DOMAIN SET NOT NULL or ADD CONSTRAINT name NOT NULL to
# add it, RENAME CONSTRAINT to rename it, and DROP NOT NULL to remove
# it. A NOT NULL with no name has the name that PostgreSQL makes for
# it, also when PostgreSQL cuts that name for a long domain name or
# adds a number to it.

domain="${WORKDIR}/project/domains/test/gate_domain_nn.yaml"
long="a_long_gate_domain_name_that_cuts_the_generated_not_null_name"
long_domain="${WORKDIR}/project/domains/test/${long}.yaml"
taken_domain="${WORKDIR}/project/domains/test/gate_domain_nn_taken.yaml"

# $1 is the constraint list of the domain
write_domain() {
    cat > "${domain}" <<YAML
---
name: gate_domain_nn
schema: test
owner: postgres
data_type: integer
$1
YAML
}

# Plan the project, require the statement $1 in the plan, apply the
# plan without --allow-drop, and require an empty plan; $2 says what
# the step checks
expect_alter() {
    ./target/debug/pglifecycle deploy -o "${WORKDIR}/domain-nn.sql" \
        -d "${TARGET_DB}" "${WORKDIR}/project"
    if ! grep -Fxq "$1" "${WORKDIR}/domain-nn.sql"; then
        echo "Convergence gate FAILED: no $1" >&2
        cat "${WORKDIR}/domain-nn.sql" >&2
        exit 1
    fi
    ./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
        "${WORKDIR}/project"
    expect_empty_plan "$2"
}

write_domain ""
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a domain with no NOT NULL"

write_domain "check_constraints:
- nullable: false"
expect_alter "ALTER DOMAIN test.gate_domain_nn SET NOT NULL;" \
    "a NOT NULL with no name is added"

write_domain "check_constraints:
- name: gate_nn
  nullable: false"
expect_alter "ALTER DOMAIN test.gate_domain_nn RENAME CONSTRAINT \
gate_domain_nn_not_null TO gate_nn;" "a NOT NULL gets a name"

write_domain ""
expect_alter "ALTER DOMAIN test.gate_domain_nn DROP NOT NULL;" \
    "a NOT NULL is removed"

write_domain "check_constraints:
- name: gate_nn
  nullable: false"
expect_alter "ALTER DOMAIN test.gate_domain_nn ADD CONSTRAINT gate_nn \
NOT NULL;" "a NOT NULL with a name is added"

# pg_dump writes the cut name of the NOT NULL, because it is not
# `<domain>_not_null`. The project has no name
cat > "${long_domain}" <<YAML
---
name: ${long}
schema: test
owner: postgres
data_type: integer
check_constraints:
- nullable: false
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a NOT NULL with no name on a long domain name"

# A CHECK has the name `<domain>_not_null`, thus PostgreSQL names the
# NOT NULL with no name `<domain>_not_null1`; $1 is the NOT NULL
write_taken() {
    cat > "${taken_domain}" <<YAML
---
name: gate_domain_nn_taken
schema: test
owner: postgres
data_type: integer
check_constraints:
- name: gate_domain_nn_taken_not_null
  expression: (VALUE > 0)
$1
YAML
}

write_taken "- nullable: false"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a NOT NULL with no name and a numbered name"

write_taken "- name: taken_nn
  nullable: false"
expect_alter "ALTER DOMAIN test.gate_domain_nn_taken RENAME CONSTRAINT \
gate_domain_nn_taken_not_null1 TO taken_nn;" "a numbered NOT NULL gets a name"

write_taken "- nullable: false"
expect_alter "ALTER DOMAIN test.gate_domain_nn_taken RENAME CONSTRAINT \
taken_nn TO gate_domain_nn_taken_not_null1;" \
    "a NOT NULL gets the numbered name"

rm "${domain}" "${long_domain}" "${taken_domain}"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "the NOT NULL domains are dropped"
