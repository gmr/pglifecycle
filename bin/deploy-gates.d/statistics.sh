# Sourced by bin/deploy-gates (gate 2). Extended statistics compare by
# definition. The project writes them in the short forms that a person
# writes, and the plan stays empty. A changed statistics target,
# comment or owner is set in place, without --allow-drop. A changed
# kind, column, expression or table drops and makes the statistics
# again, only with --allow-drop. Statistics that only the database has
# are dropped, only with --allow-drop.

# $1 is a query that must return t, $2 says what the step checks
expect_statistics() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

# a role is a cluster object, so an earlier run can have made it
psql -d postgres -q -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'Gate Owner') THEN
        CREATE ROLE "Gate Owner";
    END IF;
END
$$;
SQL

# the short forms: kinds in another order, all the kinds written out,
# columns in uppercase and in another order, a table name in mixed
# case, expressions with other spaces, case and parentheses, and the
# default target written as -1
dir="${WORKDIR}/project/statistics/test"
cat > "${dir}/measurements_ab.yaml" <<'YAML'
---
name: measurements_ab
schema: test
owner: postgres
table: TEST.Measurements
kinds:
- dependencies
- ndistinct
elements:
- B
- a
YAML
cat > "${dir}/measurements_all.yaml" <<'YAML'
---
name: measurements_all
schema: test
owner: postgres
table: test.measurements
kinds:
- mcv
- ndistinct
- dependencies
elements:
- label
- a
- b
target: 500
comment: Every kind
YAML
cat > "${dir}/measurements_expr.yaml" <<'YAML'
---
name: measurements_expr
schema: test
owner: postgres
table: test.measurements
kinds:
- mcv
elements:
- (a+b)
- (LOWER( label ))
YAML
cat > "${dir}/quoted_stats.yaml" <<'YAML'
---
name: quoted_stats
schema: test
owner: postgres
table: test.quoted_cols
elements:
- '"select"'
- '"Id"'
YAML
cat > "${dir}/user_states_stats.yaml" <<'YAML'
---
name: user_states_stats
schema: test
owner: postgres
table: '"test"."user_states"'
elements:
- total
- state
target: -1
YAML
expect_empty_plan "statistics short forms are unchanged"

# drift that deploy sets in place: targets, a comment and an owner
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER STATISTICS test.measurements_all SET STATISTICS 50;
ALTER STATISTICS test.measurements_ab SET STATISTICS 10;
COMMENT ON STATISTICS test.measurements_all IS NULL;
COMMENT ON STATISTICS test.measurements_expr IS 'drift';
ALTER STATISTICS test.measurements_expr OWNER TO "Gate Owner";
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_statistics "SELECT string_agg(stxname || '=' ||
        coalesce(stxstattarget::text, 'default') || '/' ||
        pg_get_userbyid(stxowner) || '/' ||
        coalesce(obj_description(oid, 'pg_statistic_ext'), ''),
        ',' ORDER BY stxname)
    = 'measurements_ab=default/postgres/,'
      'measurements_all=500/postgres/Every kind,'
      'measurements_expr=default/postgres/'
    FROM pg_statistic_ext
    WHERE stxname IN ('measurements_ab', 'measurements_all',
                      'measurements_expr')" \
    "the target, comment or owner of statistics was not set back"
expect_empty_plan "changed statistics converge in place"

# changed kinds: no ALTER form, so deploy drops the statistics and
# makes them again, only with --allow-drop
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP STATISTICS test.measurements_ab;
CREATE STATISTICS test.measurements_ab (mcv) ON a, b FROM test.measurements;
SQL
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP STATISTICS' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 1 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the statistics rebuild was not" \
        "withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_statistics "SELECT stxkind = '{d,f}' FROM pg_statistic_ext
    WHERE stxname = 'measurements_ab'" \
    "the changed statistics were not made again"
expect_empty_plan "changed statistics converge with --allow-drop"

# statistics that only the database has, in a schema whose name needs
# quoting
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 -c \
    "CREATE STATISTICS \"Quoted Schema\".\"Stray Stats\" ON a, label
        FROM test.measurements;"
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP STATISTICS' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 1 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the statistics drop was not" \
        "withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_statistics "SELECT NOT EXISTS (SELECT FROM pg_statistic_ext
    WHERE stxname = 'Stray Stats')" \
    "the database-only statistics were not dropped"
expect_empty_plan "database-only statistics are dropped"
