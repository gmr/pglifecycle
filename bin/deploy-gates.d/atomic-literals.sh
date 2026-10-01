# Sourced by bin/deploy-gates (gate 2). In a SQL-standard body (BEGIN
# ATOMIC ... END), PostgreSQL writes a string literal or a NULL that
# has no type with a cast, `'x'::text`. The output column of `'x'` has
# the name `?column?`, and PostgreSQL writes no `AS` for it. When deploy
# makes the routine from that body, the column of `'x'::text` has the
# name `text`, and PostgreSQL writes `'x'::text AS text`. deploy
# compares the two bodies as equal, so the re-plan is empty. A real
# change of the body is still a change.

literal="${WORKDIR}/project/functions/test/atomic_literal.yaml"
many="${WORKDIR}/project/functions/test/atomic_literal_many.yaml"
union="${WORKDIR}/project/functions/test/atomic_literal_union.yaml"
sub="${WORKDIR}/project/functions/test/atomic_literal_sub.yaml"
procedure="${WORKDIR}/project/procedures/test/atomic_literal_proc.yaml"

# each body has the form that pull writes
cat > "${literal}" <<'YAML'
---
name: atomic_literal
schema: test
owner: postgres
returns: text
language: sql
sql_body: |-
  BEGIN ATOMIC
   SELECT 'e\f'::text;
  END
YAML
cat > "${many}" <<'YAML'
---
name: atomic_literal_many
schema: test
owner: postgres
returns: record
language: sql
sql_body: |-
  BEGIN ATOMIC
   SELECT 'x'::text,
       2,
       NULL::text,
       ('y'::text COLLATE "C");
  END
YAML
cat > "${union}" <<'YAML'
---
name: atomic_literal_union
schema: test
owner: postgres
returns: SETOF text
language: sql
sql_body: |-
  BEGIN ATOMIC
   SELECT 'x'::text
   UNION
    SELECT DISTINCT 'y'::text
     WHERE ('z'::text = 'z'::text)
    ORDER BY 1;
  END
YAML
cat > "${sub}" <<'YAML'
---
name: atomic_literal_sub
schema: test
owner: postgres
returns: text
language: sql
sql_body: |-
  BEGIN ATOMIC
   SELECT 'q'::text;
   SELECT ( SELECT 'x'::text);
  END
YAML
cat > "${procedure}" <<'YAML'
---
name: atomic_literal_proc
schema: test
owner: postgres
language: sql
sql_body: |-
  BEGIN ATOMIC
   SELECT 'x'::text;
  END
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a literal in a BEGIN ATOMIC body converges"

# a real change of the body is still a change
perl -pi -e "s/^   SELECT 'e\\\\f'::text;\$/   SELECT 'g'::text;/" "${literal}"
./target/debug/pglifecycle deploy -o "${WORKDIR}/atomic.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -q "^ SELECT 'g'::text;" "${WORKDIR}/atomic.sql"; then
    echo "Convergence gate FAILED: a changed literal is a change" >&2
    cat "${WORKDIR}/atomic.sql" >&2
    exit 1
fi
echo "Convergence gate passed: a changed literal is a change"
perl -pi -e "s/^   SELECT 'g'::text;\$/   SELECT 'e\\\\f'::text;/" "${literal}"
expect_empty_plan "the reverted body is unchanged"

rm "${literal}" "${many}" "${union}" "${sub}" "${procedure}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP FUNCTION test.atomic_literal;" \
    -c "DROP FUNCTION test.atomic_literal_many;" \
    -c "DROP FUNCTION test.atomic_literal_union;" \
    -c "DROP FUNCTION test.atomic_literal_sub;" \
    -c "DROP PROCEDURE test.atomic_literal_proc;"
expect_empty_plan "the atomic-literals step leaves the database unchanged"
