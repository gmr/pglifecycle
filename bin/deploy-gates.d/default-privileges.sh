# Sourced by bin/deploy-gates (gate 2). Default privileges: deploy
# compares the grants of each role, schema and object type, and emits
# ALTER DEFAULT PRIVILEGES for the difference. pg_monitor is a role
# that each cluster has, so the step makes no role of its own.
dp_file="${WORKDIR}/project/default_privileges/postgres.yaml"
cp "${dp_file}" "${WORKDIR}/default-privileges.orig.yaml"

# the default ACLs of a database, one line for each role, schema and
# object type, with the items in a fixed order
default_acls() {
    psql -d "$1" -tAc "SELECT defaclrole::regrole,
            defaclnamespace::regnamespace, defaclobjtype,
            (SELECT string_agg(a::text, ',' ORDER BY a::text)
               FROM unnest(defaclacl) a)
        FROM pg_default_acl ORDER BY 1, 2, 3"
}

# --no-privileges leaves default privileges as the database has them,
# for a live database and for a dump
expect_no_default_privileges_with_x() {
    ./target/debug/pglifecycle deploy -x -o "${WORKDIR}/dp-x.sql" \
        -d "${TARGET_DB}" "${WORKDIR}/project"
    pg_dump -d "${TARGET_DB}" -Fc --schema-only -f "${WORKDIR}/dp-x.dump"
    ./target/debug/pglifecycle deploy -x -o "${WORKDIR}/dp-x-dump.sql" \
        --dump "${WORKDIR}/dp-x.dump" "${WORKDIR}/project"
    if grep -q 'DEFAULT PRIVILEGES' "${WORKDIR}/dp-x.sql" \
            "${WORKDIR}/dp-x-dump.sql"; then
        echo "Convergence gate FAILED: --no-privileges changed default" \
            "privileges" >&2
        cat "${WORKDIR}/dp-x.sql" "${WORKDIR}/dp-x-dump.sql" >&2
        exit 1
    fi
}

# a hand-written form of the fixture's default privileges: another
# case for the grantee and the privileges, ROUTINES for FUNCTIONS, and
# EXECUTE for the ALL that pg_dump writes
cat > "${dp_file}" <<'YAML'
---
name: postgres
grants:
  - schema: test
    object_type: TABLES
    grantee: Public
    privileges: [select]
  - schema: test
    object_type: SEQUENCES
    grantee: public
    privileges: [Usage]
revocations:
  - object_type: ROUTINES
    grantee: PUBLIC
    privileges: [EXECUTE]
YAML
expect_empty_plan "hand-written default privileges are unchanged"

# drift: a grant of the project taken away, a grant that the project
# does not have, and the built-in EXECUTE for PUBLIC given back. GRANT
# and REVOKE are in place, so deploy converges without --allow-drop
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER DEFAULT PRIVILEGES IN SCHEMA test REVOKE SELECT ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA test GRANT INSERT ON TABLES TO PUBLIC;
ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO PUBLIC;
SQL
expect_no_default_privileges_with_x
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
if [ "$(default_acls "${TARGET_DB}")" != "$(default_acls "${SOURCE_DB}")" ]
then
    echo "Convergence gate FAILED: default privilege drift remains" >&2
    default_acls "${TARGET_DB}" >&2
    exit 1
fi
expect_empty_plan "default privilege drift converged"

# PostgreSQL applies default privileges only when it creates an object.
# The database grants SELECT on new tables to PUBLIC, the project does
# not, and the same deploy creates a table: the REVOKE must come before
# the CREATE TABLE, or the new table stays readable by PUBLIC
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 -c \
    "ALTER DEFAULT PRIVILEGES IN SCHEMA public
         GRANT SELECT ON TABLES TO PUBLIC;"
mkdir -p "${WORKDIR}/project/tables/public"
cat > "${WORKDIR}/project/tables/public/dp_probe.yaml" <<'YAML'
---
name: dp_probe
schema: public
owner: postgres
sql: CREATE TABLE public.dp_probe (id integer NOT NULL);
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
public_select="SELECT has_table_privilege('public', 'public.dp_probe',
    'SELECT')"
if [ "$(psql -d "${TARGET_DB}" -tAc "${public_select}")" != f ] \
    || [ "$(default_acls "${TARGET_DB}")" \
        != "$(default_acls "${SOURCE_DB}")" ]; then
    echo "Convergence gate FAILED: a table made in the same deploy got" \
        "the default privileges of the database" >&2
    psql -d "${TARGET_DB}" -tAc "SELECT relacl FROM pg_class
        WHERE oid = 'public.dp_probe'::regclass" >&2
    default_acls "${TARGET_DB}" >&2
    exit 1
fi
rm "${WORKDIR}/project/tables/public/dp_probe.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 -c "DROP TABLE public.dp_probe;"
expect_empty_plan "default privileges changed before a new table"

# a changed grant names a schema that the same deploy creates. ALTER
# DEFAULT PRIVILEGES IN SCHEMA fails if the schema does not exist. The
# new table of that schema has the ACL of the project, which does not
# grant to pg_monitor: deploy takes away what the default privileges
# give it (bin/deploy-gates.d/privileges.sh)
cp "${dp_file}" "${WORKDIR}/default-privileges.step.yaml"
cat > "${dp_file}" <<'YAML'
---
name: postgres
grants:
  - schema: test
    object_type: TABLES
    grantee: PUBLIC
    privileges: [SELECT]
  - schema: test
    object_type: SEQUENCES
    grantee: PUBLIC
    privileges: [USAGE]
  - schema: dp_new
    object_type: TABLES
    grantee: pg_monitor
    privileges: [SELECT]
revocations:
  - object_type: FUNCTIONS
    grantee: PUBLIC
    privileges: [EXECUTE]
YAML
cat > "${WORKDIR}/project/schemata/dp_new.yaml" <<'YAML'
---
name: dp_new
owner: postgres
YAML
mkdir -p "${WORKDIR}/project/tables/dp_new"
cat > "${WORKDIR}/project/tables/dp_new/dp_probe.yaml" <<'YAML'
---
name: dp_probe
schema: dp_new
owner: postgres
sql: CREATE TABLE dp_new.dp_probe (id integer NOT NULL);
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
monitor_select="SELECT has_table_privilege('pg_monitor', 'dp_new.dp_probe',
    'SELECT') OR NOT EXISTS (SELECT FROM pg_default_acl
        WHERE defaclnamespace = 'dp_new'::regnamespace)"
if [ "$(psql -d "${TARGET_DB}" -tAc "${monitor_select}")" != f ]; then
    echo "Convergence gate FAILED: the default privileges in a new schema" \
        "were not set, or a table made in it does not have the ACL of" \
        "the project" >&2
    psql -d "${TARGET_DB}" -tAc "SELECT relacl FROM pg_class
        WHERE oid = 'dp_new.dp_probe'::regclass" >&2
    default_acls "${TARGET_DB}" >&2
    exit 1
fi
expect_empty_plan "default privileges in a new schema"
cp "${WORKDIR}/default-privileges.step.yaml" "${dp_file}"
rm -r "${WORKDIR}/project/schemata/dp_new.yaml" \
    "${WORKDIR}/project/tables/dp_new"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 -c "DROP SCHEMA dp_new CASCADE;"
expect_empty_plan "default privileges in a new schema removed"

# the project grants every table privilege to pg_monitor as a list,
# with the grant option, and takes TRUNCATE away from the owner.
# pg_dump writes the first as ALL, and the second as REVOKE ALL and a
# GRANT of the others
cat > "${dp_file}" <<'YAML'
---
name: postgres
grants:
  - schema: test
    object_type: TABLES
    grantee: PUBLIC
    privileges: [SELECT]
  - schema: test
    object_type: SEQUENCES
    grantee: PUBLIC
    privileges: [USAGE]
  - object_type: TABLES
    grantee: pg_monitor
    privileges: [SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES,
      TRIGGER, MAINTAIN]
    with_grant_option: true
revocations:
  - object_type: FUNCTIONS
    grantee: PUBLIC
    privileges: [EXECUTE]
  - object_type: TABLES
    grantee: postgres
    privileges: [TRUNCATE]
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
changed_acl="SELECT count(*) FROM pg_default_acl, unnest(defaclacl) a
    WHERE defaclrole = 'postgres'::regrole AND defaclnamespace = 0
      AND defaclobjtype = 'r'
      AND a::text IN ('pg_monitor=a*r*w*d*D*x*t*m*/postgres',
                      'postgres=arwdxtm/postgres')"
if [ "$(psql -d "${TARGET_DB}" -tAc "${changed_acl}")" != 2 ]; then
    echo "Convergence gate FAILED: changed default privileges were not" \
        "applied" >&2
    default_acls "${TARGET_DB}" >&2
    exit 1
fi
expect_empty_plan "changed default privileges converged"

# the default privileges of a role that the project does not have: the
# REVOKE is withheld without --allow-drop, and the script warns that
# the database grants more than the project
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 -c \
    "ALTER DEFAULT PRIVILEGES FOR ROLE pg_monitor
         GRANT SELECT ON TABLES TO PUBLIC;"
expect_no_default_privileges_with_x
./target/debug/pglifecycle deploy -o "${WORKDIR}/dp-gated.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -q '^-- WARNING: DEFAULT PRIVILEGES pg_monitor withheld' \
        "${WORKDIR}/dp-gated.sql" \
    || grep -q 'FOR ROLE pg_monitor' "${WORKDIR}/dp-gated.sql"; then
    echo "Convergence gate FAILED: database-only default privileges" \
        "were not withheld" >&2
    cat "${WORKDIR}/dp-gated.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
monitor_acl="SELECT count(*) FROM pg_default_acl
    WHERE defaclrole = 'pg_monitor'::regrole"
if [ "$(psql -d "${TARGET_DB}" -tAc "${monitor_acl}")" != 0 ]; then
    echo "Convergence gate FAILED: database-only default privileges" \
        "were not revoked" >&2
    exit 1
fi
expect_empty_plan "database-only default privileges revoked"

# a role of the project with no declarations makes no archive entry.
# deploy gives back the built-in defaults, without --allow-drop
printf -- '---\nname: postgres\n' > "${dp_file}"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
owner_acl="SELECT count(*) FROM pg_default_acl
    WHERE defaclrole = 'postgres'::regrole"
if [ "$(psql -d "${TARGET_DB}" -tAc "${owner_acl}")" != 0 ]; then
    echo "Convergence gate FAILED: built-in default privileges were not" \
        "given back" >&2
    default_acls "${TARGET_DB}" >&2
    exit 1
fi
expect_empty_plan "built-in default privileges given back"

# the fixture's default privileges again, for the steps after this one
cp "${WORKDIR}/default-privileges.orig.yaml" "${dp_file}"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
if [ "$(default_acls "${TARGET_DB}")" != "$(default_acls "${SOURCE_DB}")" ]
then
    echo "Convergence gate FAILED: fixture default privileges were not" \
        "restored" >&2
    default_acls "${TARGET_DB}" >&2
    exit 1
fi
expect_empty_plan "fixture default privileges restored"
