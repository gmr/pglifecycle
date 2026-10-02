# Sourced by bin/deploy-gates (gate 2). A new base type needs its shell
# type before its I/O functions. PostgreSQL makes the shell type for a
# function that returns the new type, but not for a function that takes
# it. Here the output function sorts before the input function, thus
# it comes first. The base type is made, and a re-deploy is an empty
# plan.
#
# A base type and its I/O functions depend on each other, thus a plain
# DROP TYPE fails. When only the database has them, the type drops with
# CASCADE, as pg_dump --clean does, and only with --allow-drop. When the
# project keeps an object that depends on the type, the drop does not
# cascade.

cat > "${WORKDIR}/project/types/gate_shell.yaml" <<'YAML'
---
schema: test
types:
- name: gate_shell
  schema: test
  owner: postgres
  type: base
  input: test.gate_shell_read
  output: test.gate_shell_emit
  internal_length: 4
  passed_by_value: true
  alignment: int4
  storage: plain
YAML
cat > "${WORKDIR}/project/functions/test/gate_shell_read.yaml" <<'YAML'
---
name: gate_shell_read
schema: test
owner: postgres
parameters:
- mode: IN
  data_type: cstring
returns: test.gate_shell
language: internal
immutable: true
strict: true
definition: int4in
YAML
cat > "${WORKDIR}/project/functions/test/gate_shell_emit.yaml" <<'YAML'
---
name: gate_shell_emit
schema: test
owner: postgres
parameters:
- mode: IN
  data_type: test.gate_shell
returns: cstring
language: internal
immutable: true
strict: true
definition: int4out
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT typtype FROM pg_type
        WHERE oid = 'test.gate_shell'::regtype")" != b ]; then
    echo "Convergence gate FAILED: the base type was not made" >&2
    exit 1
fi
expect_empty_plan "a new base type is made after its shell type"

# a table that the project keeps, with an index that depends on the
# type. The column is not of the type, thus the index is the only
# dependent. pg_dump writes the index as its own entry, which depends
# on the table and on the type
keeper="${WORKDIR}/project/tables/test/gate_keeper.yaml"
cat > "${keeper}" <<'YAML'
---
name: gate_keeper
schema: test
owner: postgres
columns:
- name: n
  data_type: integer
indexes:
- name: gate_keeper_shell
  method: btree
  columns:
  - expression: (((n)::text)::test.gate_shell)::text
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a table with an index that depends on the base type"

# only the type is removed from the project. CASCADE would drop the
# index of the table that the project keeps, thus the drop does not
# cascade, and it fails
mv "${WORKDIR}/project/types/gate_shell.yaml" "${WORKDIR}/gate_shell.yaml"
./target/debug/pglifecycle deploy --allow-drop -o "${WORKDIR}/kept.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project" 2> "${WORKDIR}/kept.log"
if ! grep -q '^DROP TYPE IF EXISTS test.gate_shell;' "${WORKDIR}/kept.sql" \
    || ! grep -q 'drop does not cascade.*test.gate_keeper' \
        "${WORKDIR}/kept.log"; then
    echo "Convergence gate FAILED: the base type drop cascades" >&2
    cat "${WORKDIR}/kept.sql" "${WORKDIR}/kept.log" >&2
    exit 1
fi
if ./target/debug/pglifecycle deploy --apply --allow-drop \
    -d "${TARGET_DB}" "${WORKDIR}/project"; then
    echo "Convergence gate FAILED: the base type drop did not fail" >&2
    exit 1
fi
if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT count(*) FROM pg_class
        WHERE oid = 'test.gate_keeper_shell'::regclass")" != 1 ]; then
    echo "Convergence gate FAILED: the kept index was dropped" >&2
    exit 1
fi
mv "${WORKDIR}/gate_shell.yaml" "${WORKDIR}/project/types/gate_shell.yaml"
expect_empty_plan "a failed base type drop does not change the database"


# the base type and its I/O functions that only the database has
rm "${keeper}" "${WORKDIR}/project/types/gate_shell.yaml" \
    "${WORKDIR}/project/functions/test/gate_shell_read.yaml" \
    "${WORKDIR}/project/functions/test/gate_shell_emit.yaml"
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 4 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the base type drop was not withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT count(*) FROM pg_type
        WHERE typname = 'gate_shell'")" != 0 ]; then
    echo "Convergence gate FAILED: the base type was not dropped" >&2
    exit 1
fi
expect_empty_plan "a database-only base type is dropped"
