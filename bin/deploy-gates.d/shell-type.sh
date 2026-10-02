# Sourced by bin/deploy-gates (gate 2). A new base type needs its shell
# type before its I/O functions. PostgreSQL makes the shell type for a
# function that returns the new type, but not for a function that takes
# it. Here the output function sorts before the input function, thus
# it comes first. The base type is made, and a re-deploy is an empty
# plan.
#
# A base type and its I/O functions depend on each other, thus a plain
# DROP TYPE fails. When only the database has them, the type drops with
# CASCADE, as pg_dump --clean does, and only with --allow-drop.

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


# the base type and its I/O functions that only the database has
rm "${WORKDIR}/project/types/gate_shell.yaml" \
    "${WORKDIR}/project/functions/test/gate_shell_read.yaml" \
    "${WORKDIR}/project/functions/test/gate_shell_emit.yaml"
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 3 excluded' \
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
