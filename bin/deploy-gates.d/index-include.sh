# Sourced by bin/deploy-gates (gate 2). The INCLUDE columns of an index
# and of a primary key or unique constraint are not key columns. A
# change to them is a change to the index, and an INCLUDE column that
# the project gives as a key column is a change too.

tables="${WORKDIR}/project/tables/test"
cp "${tables}/users.yaml" "${WORKDIR}/users.yaml.orig"

# Plan the project against the target and require a plan with a line
# that matches $1; $2 says what the step checks
expect_include_planned() {
    ./target/debug/pglifecycle deploy --allow-drop \
        -o "${WORKDIR}/replan.sql" -d "${TARGET_DB}" "${WORKDIR}/project"
    if grep -q "$1" "${WORKDIR}/replan.sql"; then
        echo "Convergence gate passed: $2"
    else
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/replan.sql" >&2
        exit 1
    fi
}

# fewer INCLUDE columns
perl -0pi -e 's/(  include:\n  - name\n)  - surname\n/$1/' \
    "${tables}/users.yaml"
expect_include_planned \
    '^CREATE INDEX users_email_names .* ( email ) INCLUDE (name);$' \
    "an index with other INCLUDE columns is a change"
./target/debug/pglifecycle deploy --apply --allow-drop \
    -d "${TARGET_DB}" "${WORKDIR}/project"
expect_empty_plan "an index with other INCLUDE columns converges"

# the INCLUDE columns as key columns
perl -0pi -e 's/(  - name: email\n)  include:\n  - name\n/$1  - name: name\n/' \
    "${tables}/users.yaml"
expect_include_planned \
    '^CREATE INDEX users_email_names .* ( email, name );$' \
    "an INCLUDE column as a key column is a change"
./target/debug/pglifecycle deploy --apply --allow-drop \
    -d "${TARGET_DB}" "${WORKDIR}/project"
expect_empty_plan "an INCLUDE column as a key column converges"

# a primary key and a unique constraint with INCLUDE columns, and then
# with other INCLUDE columns
cat > "${tables}/include_keys.yaml" <<'YAML'
---
name: include_keys
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: w
  data_type: integer
- name: v
  data_type: text
primary_key:
  columns:
  - id
  include:
  - v
unique_constraints:
- columns:
  - w
  include:
  - v
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a primary key and a unique constraint with INCLUDE \
columns"
perl -0pi -e 's/(  - id\n)  include:\n  - v\n/$1  include:\n  - w\n/' \
    "${tables}/include_keys.yaml"
perl -0pi -e 's/(  - w\n  include:\n)  - v\n/$1  - id\n/' \
    "${tables}/include_keys.yaml"
expect_include_planned 'PRIMARY KEY (id) INCLUDE (w)' \
    "a primary key with other INCLUDE columns is a change"
expect_include_planned 'UNIQUE (w) INCLUDE (id)' \
    "a unique constraint with other INCLUDE columns is a change"
./target/debug/pglifecycle deploy --apply --allow-drop \
    -d "${TARGET_DB}" "${WORKDIR}/project"
expect_empty_plan "keys with other INCLUDE columns converge"

rm "${tables}/include_keys.yaml"
mv "${WORKDIR}/users.yaml.orig" "${tables}/users.yaml"
./target/debug/pglifecycle deploy --apply --allow-drop \
    -d "${TARGET_DB}" "${WORKDIR}/project"
expect_empty_plan "the INCLUDE step leaves the database unchanged"
