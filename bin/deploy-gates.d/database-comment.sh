# Sourced by bin/deploy-gates (gate 2). The comment of the database
# (fixtures/schema.sql) is in the project.yaml of the project. Gate 1
# made it in the target.
#
# 1. Drift in the database: a changed comment. deploy sets the comment
#    of the project again.
# 2. When the project removes the comment, deploy removes it with
#    COMMENT ON DATABASE ... IS NULL. The removal is not destructive.
#    It loses no data, as for the comment of an object.
#
# Each time, the plan after --apply must be empty.

# the comment of the database $1
database_comment() {
    psql -d postgres -tA -v ON_ERROR_STOP=1 -c "
        SELECT shobj_description(oid, 'pg_database')
          FROM pg_database WHERE datname = '$1'"
}

# the target has the comment of the source, and the plan is empty; $1
# says what the step checks
expect_source_comment() {
    database_comment "${SOURCE_DB}" > "${WORKDIR}/comment-source.txt"
    database_comment "${TARGET_DB}" > "${WORKDIR}/comment-target.txt"
    if ! grep -q "The gate's database" "${WORKDIR}/comment-source.txt" \
        || ! diff -u "${WORKDIR}/comment-source.txt" \
            "${WORKDIR}/comment-target.txt"; then
        echo "Convergence gate FAILED: $1" >&2
        exit 1
    fi
    expect_empty_plan "$1"
}

expect_source_comment "deploy made the database comment"

psql -d postgres -q -v ON_ERROR_STOP=1 \
    -c "COMMENT ON DATABASE ${TARGET_DB} IS 'drift'"
./target/debug/pglifecycle deploy -o "${WORKDIR}/comment.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -q "^COMMENT ON DATABASE ${TARGET_DB} IS .*The gate's database" \
        "${WORKDIR}/comment.sql"; then
    echo "Convergence gate FAILED: the plan does not set the database" \
        "comment" >&2
    cat "${WORKDIR}/comment.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_source_comment "the database comment converges"

cp "${WORKDIR}/project/project.yaml" "${WORKDIR}/project.yaml.bak"
perl -0pi -e 's/^comment:.*?\n(?=\S)//ms' "${WORKDIR}/project/project.yaml"
if grep -q '^comment:' "${WORKDIR}/project/project.yaml"; then
    echo "Convergence gate FAILED: the step cannot remove the comment" >&2
    exit 1
fi
./target/debug/pglifecycle deploy -o "${WORKDIR}/comment.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -qxF "COMMENT ON DATABASE ${TARGET_DB} IS NULL;" \
        "${WORKDIR}/comment.sql" \
    || ! grep -q '^-- destructive statements: none$' \
        "${WORKDIR}/comment.sql"; then
    echo "Convergence gate FAILED: the plan does not remove the database" \
        "comment without a gate" >&2
    cat "${WORKDIR}/comment.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
if [ -n "$(database_comment "${TARGET_DB}")" ]; then
    echo "Convergence gate FAILED: the database comment is not removed" >&2
    exit 1
fi
expect_empty_plan "a removed database comment converges"
mv "${WORKDIR}/project.yaml.bak" "${WORKDIR}/project/project.yaml"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_source_comment "the database comment is set again"
