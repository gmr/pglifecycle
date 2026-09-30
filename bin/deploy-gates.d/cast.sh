# Sourced by bin/deploy-gates (gate 2). Casts compare by definition.
#
# 1. Casts written by hand in a short form: filed under another schema
#    than the one pull picks, type aliases and uppercase types, an
#    uppercase function, and the explicit context and the method
#    written out at their defaults. The plan is empty.
# 2. A changed comment converges with COMMENT ON CAST, without
#    --allow-drop.
# 3. A changed context needs a drop and a create, so it is withheld
#    without --allow-drop and applied with it.
# 4. A cast that only the database has is dropped only with
#    --allow-drop.

# $1 is a query that must return t, $2 says what the step checks
expect_cast() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

# $1 is the number of statements that deploy must withhold without
# --allow-drop, $2 says what the step checks
expect_cast_withheld() {
    ./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
        -d "${TARGET_DB}" "${WORKDIR}/project"
    if grep -q '^DROP CAST' "${WORKDIR}/withheld.sql" \
        || ! grep -q "^-- destructive statements: $1 excluded" \
            "${WORKDIR}/withheld.sql"; then
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/withheld.sql" >&2
        exit 1
    fi
}

# the base type has the physical layout of integer, so the casts need
# no function
cat > "${WORKDIR}/project/casts/gate.yaml" <<'YAML'
---
schema: public
casts:
- source_type: TEST.BASE_INT
  target_type: INT4
  inout: false
  assignment: false
  implicit: false
- source_type: int
  target_type: test.base_int
  assignment: true
YAML
perl -pi -e 's/^  function: test\.point_pair_x\(test\.point_pair\)$/  function: TEST.POINT_PAIR_X( TEST.POINT_PAIR )/' \
    "${WORKDIR}/project/casts/test.yaml"
grep -q 'function: TEST.POINT_PAIR_X' "${WORKDIR}/project/casts/test.yaml"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_cast "SELECT count(*) = 2 FROM pg_cast
    WHERE (castsource, casttarget, castcontext, castmethod) IN (
        ('test.base_int'::regtype, 'integer'::regtype, 'e', 'b'),
        ('integer'::regtype, 'test.base_int'::regtype, 'a', 'b'))" \
    "the new casts were not made"
expect_empty_plan "cast short forms are unchanged"

# a changed comment and a database-only one
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
COMMENT ON CAST (test.point_pair AS text) IS 'drift';
COMMENT ON CAST ("gate.dotted"."pair.t" AS text) IS 'drift';
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_cast "SELECT bool_and(coalesce(obj_description(oid, 'pg_cast'), '')
        = CASE WHEN castsource = 'test.point_pair'::regtype
               THEN 'Text form of a pair' ELSE '' END)
    FROM pg_cast WHERE casttarget = 'text'::regtype
      AND castsource IN ('test.point_pair'::regtype,
                         '\"gate.dotted\".\"pair.t\"'::regtype)" \
    "the cast comments were not set back"
expect_empty_plan "changed cast comments converge in place"

# a changed context: there is no ALTER CAST
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP CAST (integer AS test.base_int);
CREATE CAST (integer AS test.base_int) WITHOUT FUNCTION AS IMPLICIT;
SQL
expect_cast_withheld 1 "the cast rebuild was not withheld"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_cast "SELECT castcontext = 'a' FROM pg_cast
    WHERE castsource = 'integer'::regtype
      AND casttarget = 'test.base_int'::regtype" \
    "the cast with a changed context was not made again"
expect_empty_plan "a cast with a changed context converges"

# a cast that only the database has
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "CREATE CAST (test.base_int AS bigint) WITH INOUT;"
expect_cast_withheld 1 "the cast drop was not withheld"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_cast "SELECT NOT EXISTS (SELECT FROM pg_cast
    WHERE castsource = 'test.base_int'::regtype
      AND casttarget = 'bigint'::regtype)" \
    "the database-only cast was not dropped"
expect_empty_plan "a database-only cast is dropped"
