# Sourced by bin/deploy-gates (gate 2). pull and deploy dump the
# database with the session settings of pglifecycle, and not with the
# settings of the database, the role or the environment:
#
# 1. client_encoding is UTF8. A database with the LATIN1 server
#    encoding, a database that sets client_encoding to LATIN1, and a
#    PGCLIENTENCODING of LATIN1 give the same project as a UTF8
#    database. This is also true for the roles that pg_dumpall dumps.
# 2. standard_conforming_strings is on. A database that sets it to off,
#    and a PGOPTIONS that sets it to off, give the same project, so a
#    backslash in a string literal is not written two times.
#
# The step makes a reference database with the default settings, and
# three sources with other settings. It pulls each source, and the
# reference again with the other settings in the environment, and
# compares each project with the project of the reference. It deploys
# a project into a new LATIN1 database, and into a new UTF8 database
# with the other settings in the environment. The deployed schema must
# be the schema of the reference, and the plan after the deploy must be
# empty.
# pull and deploy refuse a dump file that is not in UTF8, or that was
# made with standard_conforming_strings off.
#
# The step does not use the project or the target of the other steps.

enc_dbs=(deploy_enc_plain deploy_enc_latin1 deploy_enc_client
    deploy_enc_scs deploy_enc_utf8_target deploy_enc_latin1_target)
enc_role='deploy_enc_rôle'
# the settings that a session must not use for the dump
enc_hostile=(PGCLIENTENCODING=LATIN1
    "PGOPTIONS=-c standard_conforming_strings=off")

# $1 is a message; the step fails with it
enc_fail() {
    echo "Convergence gate FAILED: $1" >&2
    exit 1
}

# Schema-only dump of $1 in UTF8 with standard_conforming_strings on,
# with the per-dump \restrict token stripped
enc_dump_schema() {
    PGOPTIONS='-c standard_conforming_strings=on' \
        pg_dump -E UTF8 -d "$1" --schema-only --no-owner \
        | grep -v '^\\\(un\)\{0,1\}restrict '
}

# Compare the project $1 with the project of the reference, without
# project.yaml (it has the database name), remaining.yaml (it has the
# settings of the database), and the roles and users (the step checks
# them only in the reference and in the environment step); $2 says
# what the step checks
enc_same_project() {
    if ! diff -ru -x project.yaml -x remaining.yaml -x roles -x users \
            "${WORKDIR}/enc-plain" "$1"; then
        enc_fail "$2: the project is not the project of the reference"
    fi
    if ! grep -qx 'encoding: UTF8' "$1/project.yaml" \
        || ! grep -qx 'stdstrings: true' "$1/project.yaml"; then
        cat "$1/project.yaml" >&2
        enc_fail "$2: project.yaml does not have UTF8 and stdstrings"
    fi
    echo "Convergence gate passed: $2"
}

for db in "${enc_dbs[@]}"; do
    bin/drop-database "${db}"
done
PGCLIENTENCODING=UTF8 psql -X -q -d postgres -v ON_ERROR_STOP=1 \
    -v role="${enc_role}" <<'SQL'
DROP ROLE IF EXISTS :"role";
CREATE ROLE :"role";
COMMENT ON ROLE :"role" IS 'rôle ü';
SQL

# the objects have names, comments and string constants with characters
# that are not ASCII, and string constants with a backslash, in a
# default, a check constraint, a comment, a view, a function body and a
# BEGIN ATOMIC body. All the characters are in LATIN1. The bodies have
# the form that pull writes, so that the deployed schema is the same.
cat > "${WORKDIR}/enc-schema.sql" <<'SQL'
CREATE SCHEMA "façade";
CREATE TABLE "façade"."café" (
    id integer PRIMARY KEY,
    "naïve" text DEFAULT 'crème',
    path text DEFAULT 'C:\new',
    CONSTRAINT path_ok CHECK (path <> 'D:\old')
);
COMMENT ON TABLE "façade"."café" IS 'Ça coûte £5, C:\temp';
COMMENT ON COLUMN "façade"."café"."naïve" IS 'déjà vu';
CREATE VIEW "façade".menu AS
    SELECT 'x\y'::text AS path, 'crêpe'::text AS dish;
CREATE FUNCTION "façade".quoted() RETURNS text LANGUAGE sql AS $$
 SELECT 'a\b ñ';
$$;
CREATE FUNCTION "façade".atomic() RETURNS text LANGUAGE sql
    BEGIN ATOMIC SELECT 'e\f ü'::text AS s; END;
SQL
# $1 is the database; the other arguments go to CREATE DATABASE
enc_create() {
    local db="$1"
    shift
    psql -X -q -d postgres -v ON_ERROR_STOP=1 \
        -c "CREATE DATABASE ${db} $*"
    PGCLIENTENCODING=UTF8 PGOPTIONS='-c standard_conforming_strings=on' \
        psql -X -q -d "${db}" -v ON_ERROR_STOP=1 \
        -f "${WORKDIR}/enc-schema.sql"
}
enc_create deploy_enc_plain
enc_create deploy_enc_latin1 \
    "ENCODING 'LATIN1' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0"
enc_create deploy_enc_client
psql -X -q -d postgres -v ON_ERROR_STOP=1 \
    -c "ALTER DATABASE deploy_enc_client SET client_encoding = 'LATIN1'"
enc_create deploy_enc_scs
psql -X -q -d postgres -v ON_ERROR_STOP=1 \
    -c "ALTER DATABASE deploy_enc_scs SET standard_conforming_strings = off"

# the reference; its text must be the text of the schema
./target/debug/pglifecycle pull -d deploy_enc_plain "${WORKDIR}/enc-plain"
enc_table="${WORKDIR}/enc-plain/tables/façade/café.yaml"
for text in "default: '''C:\\new''::text'" \
    "default: '''crème''::text'" \
    "expression: (path <> 'D:\\old'::text)" \
    'comment: Ça coûte £5, C:\temp' \
    'comment: déjà vu'; do
    if ! grep -qxF -e "${text}" -e "  ${text}" "${enc_table}"; then
        cat "${enc_table}" >&2
        enc_fail "the reference table does not have: ${text}"
    fi
done
grep -qF "'x\\y'::text" "${WORKDIR}/enc-plain/views/façade/menu.yaml" \
    || enc_fail "the reference view does not have 'x\\y'"
grep -qF "'e\\f ü'::text" "${WORKDIR}/enc-plain/functions/façade/atomic.yaml" \
    || enc_fail "the reference BEGIN ATOMIC body does not have 'e\\f ü'"
grep -qxF "comment: rôle ü" "${WORKDIR}/enc-plain/roles/${enc_role}.yaml" \
    || enc_fail "the reference role does not have its comment"

./target/debug/pglifecycle pull -d deploy_enc_latin1 --no-roles \
    "${WORKDIR}/enc-latin1"
enc_same_project "${WORKDIR}/enc-latin1" \
    "a database with the LATIN1 server encoding"
# the settings of the database are an entry that pull cannot model
./target/debug/pglifecycle pull -d deploy_enc_client --no-roles \
    --allow-unsupported "${WORKDIR}/enc-client"
enc_same_project "${WORKDIR}/enc-client" \
    "a database that sets client_encoding to LATIN1"
./target/debug/pglifecycle pull -d deploy_enc_scs --no-roles \
    --allow-unsupported "${WORKDIR}/enc-scs"
enc_same_project "${WORKDIR}/enc-scs" \
    "a database that sets standard_conforming_strings to off"
# the environment also changes the session of pg_dumpall
env "${enc_hostile[@]}" ./target/debug/pglifecycle pull \
    -d deploy_enc_plain "${WORKDIR}/enc-env"
grep -qxF "comment: rôle ü" "${WORKDIR}/enc-env/roles/${enc_role}.yaml" \
    || enc_fail "the roles are not pulled with LATIN1 in the environment"
enc_same_project "${WORKDIR}/enc-env" \
    "PGCLIENTENCODING and PGOPTIONS with other settings"

# deploy the project of the LATIN1 source into a LATIN1 database, and
# into a UTF8 database with the other settings in the environment
enc_deploy() {
    local target="$1"
    shift
    env "$@" ./target/debug/pglifecycle deploy --apply -d "${target}" \
        "${WORKDIR}/enc-latin1"
    env "$@" ./target/debug/pglifecycle deploy -o "${WORKDIR}/enc-replan.sql" \
        -d "${target}" "${WORKDIR}/enc-latin1"
    if ! grep -q '^-- no changes' "${WORKDIR}/enc-replan.sql"; then
        cat "${WORKDIR}/enc-replan.sql" >&2
        enc_fail "the re-deploy into ${target} is not empty"
    fi
    if ! diff -u <(enc_dump_schema deploy_enc_plain) \
            <(enc_dump_schema "${target}"); then
        enc_fail "the schema of ${target} is not the schema of the reference"
    fi
    echo "Convergence gate passed: deploy into ${target}"
}
psql -X -q -d postgres -v ON_ERROR_STOP=1 \
    -c "CREATE DATABASE deploy_enc_latin1_target ENCODING 'LATIN1'
        LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0" \
    -c "CREATE DATABASE deploy_enc_utf8_target"
enc_deploy deploy_enc_latin1_target
enc_deploy deploy_enc_utf8_target "${enc_hostile[@]}"

# a dump file in LATIN1, or with standard_conforming_strings off, is
# refused with a message that tells how to make it again
PGCLIENTENCODING=LATIN1 pg_dump -d deploy_enc_plain -Fc --schema-only \
    -f "${WORKDIR}/enc-latin1.dump"
PGOPTIONS='-c standard_conforming_strings=off' pg_dump -d deploy_enc_plain \
    -Fc --schema-only -f "${WORKDIR}/enc-scs.dump"
# $1 is the dump file, $2 is a pattern that the error must have
enc_refused() {
    rm -rf "${WORKDIR}/enc-refused"
    if ./target/debug/pglifecycle pull --dump "$1" \
            "${WORKDIR}/enc-refused" 2> "${WORKDIR}/enc-refused.err"; then
        enc_fail "pull did not refuse ${1##*/}"
    fi
    if ! grep -q "$2" "${WORKDIR}/enc-refused.err"; then
        cat "${WORKDIR}/enc-refused.err" >&2
        enc_fail "pull refused ${1##*/} for an unexpected reason"
    fi
    if ./target/debug/pglifecycle deploy --dump "$1" \
            -o "${WORKDIR}/enc-refused.sql" "${WORKDIR}/enc-plain" \
            2> "${WORKDIR}/enc-refused.err"; then
        enc_fail "deploy did not refuse ${1##*/}"
    fi
    if ! grep -q "$2" "${WORKDIR}/enc-refused.err"; then
        cat "${WORKDIR}/enc-refused.err" >&2
        enc_fail "deploy refused ${1##*/} for an unexpected reason"
    fi
    echo "Convergence gate passed: pull and deploy refuse ${1##*/}"
}
enc_refused "${WORKDIR}/enc-latin1.dump" "LATIN1 encoding.*pg_dump -E UTF8"
enc_refused "${WORKDIR}/enc-scs.dump" \
    "standard_conforming_strings off.*standard_conforming_strings=on"

for db in "${enc_dbs[@]}"; do
    bin/drop-database "${db}"
done
PGCLIENTENCODING=UTF8 psql -X -q -d postgres -v ON_ERROR_STOP=1 \
    -v role="${enc_role}" <<<'DROP ROLE :"role";'
