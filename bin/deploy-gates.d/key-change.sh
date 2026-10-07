# Sourced by bin/deploy-gates (gate 2). A changed primary key or unique
# constraint is dropped and added again in place, not by a rebuild of
# the table, thus the rows stay. The foreign keys that reference the
# constraint, also one of the table itself, are dropped first and
# added again after it. Without --allow-drop all of it is withheld.

tables="${WORKDIR}/project/tables/test"

key_table() {
    cat > "${tables}/gate_key.yaml" <<YAML
---
name: gate_key
schema: test
owner: postgres
columns:
- name: a
  data_type: integer
  nullable: false
- name: b
  data_type: integer
- name: c
  data_type: integer
primary_key:
$1
unique_constraints:
$2
foreign_keys:
- name: gate_key_self
  columns:
  - c
  references:
    name: test.gate_key
    columns:
    - a
constraint_comments:
  gate_key_pkey: the key
YAML
}

key_table "- a" "- - b"
cat > "${tables}/gate_key_ref.yaml" <<'YAML'
---
name: gate_key_ref
schema: test
owner: postgres
columns:
- name: x
  data_type: integer
- name: y
  data_type: integer
foreign_keys:
- name: gate_key_ref_x_fkey
  columns:
  - x
  references:
    name: test.gate_key
    columns:
    - a
- name: gate_key_ref_y_fkey
  columns:
  - y
  references:
    name: test.gate_key
    columns:
    - b
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "INSERT INTO test.gate_key VALUES (1, 1, 1);" \
    -c "INSERT INTO test.gate_key_ref VALUES (1, 1);"
expect_empty_plan "a key that foreign keys reference"

# the primary key and the unique constraint get INCLUDE columns
key_table "  columns:
  - a
  include:
  - c" "- columns:
  - b
  include:
  - c"
./target/debug/pglifecycle deploy -o "${WORKDIR}/key-safe.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -Eq 'DROP (TABLE|CONSTRAINT)|ADD PRIMARY KEY' \
    "${WORKDIR}/key-safe.sql"; then
    echo "Convergence gate FAILED: a key change is in the script" \
        "without --allow-drop" >&2
    cat "${WORKDIR}/key-safe.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --allow-drop \
    -o "${WORKDIR}/key-change.sql" -d "${TARGET_DB}" "${WORKDIR}/project"
for pattern in \
    '^ALTER TABLE ONLY test\.gate_key_ref DROP CONSTRAINT gate_key_ref_x_fkey;' \
    '^ALTER TABLE test\.gate_key DROP CONSTRAINT gate_key_pkey;' \
    '^ALTER TABLE test\.gate_key ADD PRIMARY KEY \(a\) INCLUDE \(c\);' \
    '^ALTER TABLE test\.gate_key ADD UNIQUE \(b\) INCLUDE \(c\);' \
    '^ALTER TABLE test\.gate_key_ref ADD CONSTRAINT gate_key_ref_x_fkey '; do
    if ! grep -Eq "${pattern}" "${WORKDIR}/key-change.sql"; then
        echo "Convergence gate FAILED: no ${pattern}" >&2
        cat "${WORKDIR}/key-change.sql" >&2
        exit 1
    fi
done
if grep -Eq '^DROP TABLE' "${WORKDIR}/key-change.sql"; then
    echo "Convergence gate FAILED: a key change rebuilds the table" >&2
    cat "${WORKDIR}/key-change.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if [ "$(psql -d "${TARGET_DB}" -tAc \
        "SELECT count(*) FROM test.gate_key_ref")" != 1 ]; then
    echo "Convergence gate FAILED: a key change lost rows" >&2
    exit 1
fi
expect_empty_plan "a key change converges in place"

rm "${tables}/gate_key.yaml" "${tables}/gate_key_ref.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.gate_key_ref, test.gate_key;"
expect_empty_plan "the key-change step leaves the database unchanged"
