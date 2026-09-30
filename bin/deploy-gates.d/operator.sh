# Sourced by bin/deploy-gates (gate 2). Operators compare by
# definition.
#
# 1. Operators written by hand in a short form: type aliases and
#    uppercase types, qualified pg_catalog functions, options at their
#    defaults, and a commutator and a negator given on one side only.
#    PostgreSQL can set the link on the other side too. The plan is
#    empty.
# 2. Drift in the estimators, HASHES, MERGES, the negator, the comment
#    and the owner converges with ALTER OPERATOR, without --allow-drop.
# 3. A changed function needs a drop and a create, so it is withheld
#    without --allow-drop and applied with it.
# 4. Operators that only the database has, one of them an overload of
#    an operator that the project has, are dropped only with
#    --allow-drop.
# 5. A prefix operator written with `left_arg: NONE` is made with no
#    left argument. The plan is empty.

# $1 is a query that must return t, $2 says what the step checks
expect_operator() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

# $1 is the number of statements that deploy must withhold without
# --allow-drop, $2 says what the step checks
expect_operator_withheld() {
    ./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
        -d "${TARGET_DB}" "${WORKDIR}/project"
    if grep -q '^DROP OPERATOR' "${WORKDIR}/withheld.sql" \
        || ! grep -q "^-- destructive statements: $1 excluded" \
            "${WORKDIR}/withheld.sql"; then
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/withheld.sql" >&2
        exit 1
    fi
}

psql -d postgres -q -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'Gate Owner') THEN
        CREATE ROLE "Gate Owner";
    END IF;
END
$$;
SQL

cat >> "${WORKDIR}/project/operators/test.yaml" <<'YAML'
- name: '<<<'
  schema: test
  owner: postgres
  function: PG_CATALOG.INT4LT
  left_arg: INT4
  right_arg: pg_catalog.int4
  commutator: OPERATOR(test.>>>)
  restrict: pg_catalog.scalarltsel
  join: SCALARLTJOINSEL
  hashes: false
  merges: false
- name: '>>>'
  schema: test
  owner: postgres
  function: int4gt
  left_arg: integer
  right_arg: integer
- name: '#@#'
  schema: test
  owner: postgres
  function: int4um
  right_arg: INTEGER
- name: '=%='
  schema: test
  owner: postgres
  function: int4eq
  left_arg: integer
  right_arg: integer
  negator: OPERATOR(test.<%>)
  restrict: eqsel
  join: eqjoinsel
  hashes: true
  merges: true
- name: '<%>'
  schema: test
  owner: postgres
  function: int4ne
  left_arg: integer
  right_arg: integer
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_operator "SELECT count(*) = 5 FROM pg_operator
    WHERE oprnamespace = 'test'::regnamespace
      AND oprname IN ('<<<', '>>>', '#@#', '=%=', '<%>')" \
    "the new operators were not made"
expect_empty_plan "operator short forms are unchanged"

# drift that ALTER OPERATOR reconciles: the estimators, a cleared
# comment and a changed one, and the owner of an overload. The equality
# operator is made again without HASHES, MERGES and its negator, which
# ALTER OPERATOR can set when they are not set
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER OPERATOR test.=~= (integer, integer) SET (RESTRICT = NONE, JOIN = NONE);
COMMENT ON OPERATOR test.=~= (integer, integer) IS 'drift';
COMMENT ON OPERATOR test.!!! (NONE, integer) IS 'drift';
ALTER OPERATOR test.!!! (NONE, bigint) OWNER TO "Gate Owner";
DROP OPERATOR test.=%= (integer, integer);
CREATE OPERATOR test.=%= (FUNCTION = int4eq, LEFTARG = integer,
    RIGHTARG = integer, RESTRICT = eqsel, JOIN = eqjoinsel);
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_operator "SELECT oprrest = 'eqsel'::regproc
        AND oprjoin = 'eqjoinsel'::regproc
        AND obj_description(oid, 'pg_operator') = 'Same parity'
    FROM pg_operator WHERE oid = 'test.=~=(integer, integer)'::regoperator" \
    "the operator estimators or comment were not set back"
expect_operator "SELECT obj_description(
        'test.!!!(NONE, integer)'::regoperator, 'pg_operator') IS NULL
    AND pg_get_userbyid(oprowner) = 'postgres'
    FROM pg_operator WHERE oid = 'test.!!!(NONE, bigint)'::regoperator" \
    "the operator comment or owner was not set back"
expect_operator "SELECT oprcanhash AND oprcanmerge
        AND oprnegate = 'test.<%>(integer, integer)'::regoperator
    FROM pg_operator WHERE oid = 'test.=%=(integer, integer)'::regoperator" \
    "HASHES, MERGES or the negator were not set in place"
expect_empty_plan "changed operators converge in place"

# a changed function: ALTER OPERATOR cannot change it
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP OPERATOR test.#@# (NONE, integer);
CREATE OPERATOR test.#@# (FUNCTION = int4abs, RIGHTARG = integer);
SQL
expect_operator_withheld 1 "the operator rebuild was not withheld"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_operator "SELECT oprcode = 'int4um'::regproc
    FROM pg_operator WHERE oid = 'test.#@#(NONE, integer)'::regoperator" \
    "the operator with a changed function was not made again"
expect_empty_plan "an operator with a changed function converges"

# operators that only the database has: a new one, and a third
# overload of !!!, whose archive tag is only its name
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE OPERATOR test.@@@ (FUNCTION = int4pl, LEFTARG = integer,
    RIGHTARG = integer);
CREATE OPERATOR test.!!! (FUNCTION = int2um, RIGHTARG = smallint);
SQL
expect_operator_withheld 2 "the operator drops were not withheld"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_operator "SELECT to_regoperator('test.@@@(integer, integer)') IS NULL
    AND to_regoperator('test.!!!(NONE, smallint)') IS NULL
    AND to_regoperator('test.!!!(NONE, bigint)') IS NOT NULL" \
    "the database-only operators were not dropped"
expect_empty_plan "database-only operators are dropped"

# a prefix operator whose left argument is NONE: the build writes no
# LEFTARG, as `LEFTARG = NONE` names a type that does not exist
cat >> "${WORKDIR}/project/operators/test.yaml" <<'YAML'
- name: '~#~'
  schema: test
  owner: postgres
  function: int4um
  left_arg: NONE
  right_arg: integer
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_operator "SELECT oprkind = 'l' AND oprleft = 0
    FROM pg_operator WHERE oid = 'test.~#~(NONE, integer)'::regoperator" \
    "the prefix operator was not made"
expect_empty_plan "a prefix operator with a NONE left argument is unchanged"
