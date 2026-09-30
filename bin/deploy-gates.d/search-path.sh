# Sourced by bin/deploy-gates (gate 2). The deploy script runs with the
# empty search_path of pg_restore, so a name in it resolves as it does
# in a restore of the build, and not by the search_path of the session.
#
# 1. The script sets the search_path with the other session settings
#    before its first statement, and its header says so.
# 2. A bare commutator that does not exist fails, as it does in a
#    restore. With the search_path of the session, CREATE OPERATOR
#    made a shell operator in public. The transaction rolls back.

cat > "${WORKDIR}/project/operators/search_path.yaml" <<'YAML'
---
operators:
- name: '<~<'
  schema: test
  owner: postgres
  function: int4lt
  left_arg: integer
  right_arg: integer
  commutator: '>~>'
YAML
./target/debug/pglifecycle deploy -o "${WORKDIR}/search-path.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
# the session settings are the block after the header, before the
# first statement
settings="$(awk 'BEGIN { RS = "" } NR == 2' "${WORKDIR}/search-path.sql")"
if ! grep -q '^-- session settings, as pg_restore sets them: .*search_path' \
        "${WORKDIR}/search-path.sql" \
    || ! grep -qxF "SELECT pg_catalog.set_config('search_path', '', false);" \
        <<< "${settings}"
then
    echo "Convergence gate FAILED: the script does not set the" \
        "search_path first" >&2
    cat "${WORKDIR}/search-path.sql" >&2
    exit 1
fi
if ./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
        "${WORKDIR}/project" > /dev/null 2> "${WORKDIR}/search-path.err"; then
    echo "Convergence gate FAILED: a bare commutator did not fail" >&2
    exit 1
fi
if ! grep -q 'no schema has been selected to create in' \
        "${WORKDIR}/search-path.err"; then
    echo "Convergence gate FAILED: a bare commutator failed for an" \
        "unexpected reason" >&2
    cat "${WORKDIR}/search-path.err" >&2
    exit 1
fi
if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT count(*) FROM pg_operator
        WHERE oprname IN ('<~<', '>~>')")" != 0 ]; then
    echo "Convergence gate FAILED: the failed deploy made an operator" >&2
    exit 1
fi
echo "Convergence gate passed: a bare name resolves as in a restore"
rm "${WORKDIR}/project/operators/search_path.yaml"
expect_empty_plan "the search_path step leaves the database unchanged"
