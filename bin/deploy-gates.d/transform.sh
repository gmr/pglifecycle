# Sourced by bin/deploy-gates (gate 2). Transforms compare by
# definition.
#
# 1. A transform written by hand in a short form: filed under another
#    schema than the one pull picks, with an uppercase type and an
#    uppercase function. The plan is empty.
# 2. Drift in the functions and the comment converges with CREATE OR
#    REPLACE TRANSFORM, without --allow-drop.
# 3. A transform that only the database has is dropped only with
#    --allow-drop.

# $1 is a query that must return t, $2 says what the step checks
expect_transform() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

# a FROM SQL function returns internal, so it can serve any type
cat > "${WORKDIR}/project/transforms/gate.yaml" <<'YAML'
---
schema: public
transforms:
- type: TEST.POINT_PAIR
  language: sql
  from_sql: TEST.BASE_INT_FROM_SQL(INTERNAL)
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_transform "SELECT EXISTS (SELECT FROM pg_transform
    WHERE trftype = 'test.point_pair'::regtype
      AND trflang = (SELECT oid FROM pg_language WHERE lanname = 'sql'))" \
    "the new transform was not made"
expect_empty_plan "transform short forms are unchanged"

# drift: a removed FROM SQL function, a cleared comment, and an added
# FROM SQL function
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE OR REPLACE TRANSFORM FOR test.base_int LANGUAGE sql (
    TO SQL WITH FUNCTION test.base_int_to_sql(internal));
COMMENT ON TRANSFORM FOR test.base_int LANGUAGE sql IS NULL;
CREATE OR REPLACE TRANSFORM FOR test.base_int LANGUAGE plpgsql (
    FROM SQL WITH FUNCTION test.base_int_from_sql(internal),
    TO SQL WITH FUNCTION test.base_int_to_sql(internal));
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_transform "SELECT bool_and(CASE l.lanname
        WHEN 'sql' THEN t.trffromsql = 'test.base_int_from_sql'::regproc
            AND obj_description(t.oid, 'pg_transform')
                = 'Converts base_int for SQL functions'
        ELSE t.trffromsql = 0 END)
    FROM pg_transform t JOIN pg_language l ON l.oid = t.trflang
    WHERE t.trftype = 'test.base_int'::regtype" \
    "the changed transforms were not replaced"
expect_empty_plan "changed transforms converge in place"

# a transform that only the database has
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "CREATE TRANSFORM FOR test.point_pair LANGUAGE plpgsql (
            FROM SQL WITH FUNCTION test.base_int_from_sql(internal));"
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP TRANSFORM' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 1 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the transform drop was not withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_transform "SELECT count(*) = 1 FROM pg_transform
    WHERE trftype = 'test.point_pair'::regtype" \
    "the database-only transform was not dropped"
expect_empty_plan "a database-only transform is dropped"
