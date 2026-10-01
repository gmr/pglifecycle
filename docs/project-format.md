# Project Format

A pglifecycle project is a directory of YAML files, one file per
database object, validated against the JSON-Schema definitions in
[`schemata/`](https://github.com/gmr/pglifecycle/tree/main/schemata).

```text
my-project/
├── project.yaml          # name, superuser, extensions, languages,
│                         # access methods
├── schemata/             # one file per schema
│   └── test.yaml
├── tables/               # <schema>/<table>.yaml
│   └── test/
│       └── users.yaml
├── views/                # <schema>/<view>.yaml
├── materialized_views/
├── functions/            # <schema>/<function>.yaml
├── sequences/
├── domains/
├── types/                # one container file per schema
├── roles/                # <role>.yaml
├── users/
├── groups/
└── ...                   # aggregates, casts, collations, conversions,
                          # event_triggers, operators,
                          # operator_classes, operator_families,
                          # publications,
                          # servers, subscriptions, tablespaces,
                          # text_search, transforms, user_mappings,
                          # dml
```

Objects are structured data, not SQL. A table file, for example:

```yaml
---
name: users
schema: test
owner: postgres
columns:
  - name: id
    data_type: uuid
    nullable: false
    default: uuid_generate_v4()
  - name: email
    data_type: test.email_address
    nullable: false
indexes:
  - name: users_unique_email
    unique: true
    method: btree
    columns:
      - name: email
primary_key:
  - id
```

## Conventions

- The file location implies `schema` and `name`; both may be omitted
  from the file body and are injected on load.
- A `dependencies` key (e.g. `dependencies: {tables: [test.users]}`)
  makes the build create the object after the objects that it names.
  Use it for a relationship that the definition does not state in a
  field, such as the relations of a view query or the objects of a
  SQL-standard function body (`BEGIN ATOMIC` or `RETURN`). PostgreSQL
  checks such a body when it creates the function, so the function
  must come after each function, aggregate, table, view or sequence
  that the body uses. pull writes these entries for views,
  materialized views, functions and procedures. A restore and a deploy
  do not check a string body (`AS $$ ... $$`), and PostgreSQL does not
  record what one uses, so pull writes no entries for it.
- An entry names an object as `schema.name`. A function or aggregate
  entry also gives the argument types of one overload, as
  pull writes them: `test.f(integer, text)`, or `test.agg(*)` for an
  aggregate with no arguments. The types compare as PostgreSQL
  resolves them, so `int4`, `INTEGER` and `pg_catalog.int4` are the
  same type. An entry with no argument list is correct only when one
  overload has the name. If more than one overload has the name, or no
  overload has the argument types, the load fails. An entry for an
  object that the project does not have gives a warning and orders
  nothing.
- A foreign key needs no entry: the build adds each foreign key after
  all tables. The load ignores a table-on-table entry unless the table
  inherits from, or is `LIKE`, the other table, and gives a warning.
  The definition already states those two relationships, so a table
  needs no entries for them.
- ACL grants and revocations live on the grantee's role, user, or
  group file under `grants:`/`revocations:`, keyed by object:

```yaml
---
name: PUBLIC
create: false
grants:
  schemata:
    test:
      - USAGE
```

- Role memberships live in the same place, as `roles:`/`groups:`
  arrays naming the roles the grantee is a member of. `build` emits
  them as `GRANT role TO grantee` (or `REVOKE ... FROM` under
  `revocations:`). The granted role does not have to be a project
  file — reserved roles like `pg_read_all_data` can only ever be
  referenced, never created, and this is how to express membership in
  them:

```yaml
---
name: alice
grants:
  roles:
    - developers
    - pg_read_all_data
```
- A membership that does not behave the way a plain `GRANT role TO
  member` grants it is written as a mapping instead of a bare name.
  `admin: true` emits `WITH ADMIN OPTION`; `inherit` and `set` emit the
  per-membership `WITH INHERIT` / `WITH SET` options, which need
  PostgreSQL 16 or later. Omit `inherit` to defer to the member role's
  own `inherit` option, which is what PostgreSQL does — so `pull` keeps
  an explicit `inherit: true` only for a `NOINHERIT` member, where it
  is the only thing making the membership inherit:

```yaml
---
name: alice
grants:
  roles:
    - developers
    - role: partition_writer
      inherit: false
    - role: analytics
      admin: true
```

  The grantor (`GRANTED BY`) is not carried: it records who granted a
  membership in one cluster, not what the schema is, and PostgreSQL 16
  and later writes one for every membership.

- A column's default belongs on the column. A table that inherits a
  column has no column entry to carry one, so a default on an
  inherited column lives at the table level under `column_defaults:`,
  which `build` emits as `ALTER TABLE ONLY <table> ALTER COLUMN
  <column> SET DEFAULT ...` the way `pg_dump` writes it:

```yaml
---
name: audit_events_archive
schema: test
parents:
  - test.audit_events
column_defaults:
  - column: recorded_at
    default: CURRENT_TIMESTAMP
```

- Row-level security lives on the table. `row_level_security` states
  whether it is enabled and forced, and `policies` lists the policies.
  Each policy keeps only what differs from `CREATE POLICY`'s defaults:
  `restrictive: true` for `AS RESTRICTIVE`, a `command` other than
  `ALL`, `roles` other than `PUBLIC`. `using` and `with_check` hold the
  expression inside the `USING (...)` / `WITH CHECK (...)` clause,
  written the way PostgreSQL reports it: an operator expression keeps
  its own parentheses, as in `(tenant = CURRENT_USER)`. Write it that
  way, or deploy sees a change on every run.

```yaml
---
name: tenant_notes
schema: test
row_level_security:
  enabled: true
  forced: true
policies:
  - name: tenant_notes_own
    using: (tenant = CURRENT_USER)
    with_check: (tenant = CURRENT_USER)
    comment: tenant isolation
  - name: tenant_notes_no_blank
    restrictive: true
    command: INSERT
    with_check: (body <> ''::text)
```

  `pull` writes `row_level_security` for every table, and a table with
  that key has exactly the policies listed. A table without either key,
  such as one pulled before pglifecycle modeled row security, is not
  managed: `deploy` leaves its row security and policies as the
  database has them. Pull the project again to record them.

- An exclusion constraint lists each element as an index column (a
  `name` or an `expression`, with its `collation`, `opclass`,
  `direction` and `null_placement`) and the `operator` two rows are
  compared with. `replica_identity` is `FULL`, `NOTHING`, or
  `{index: name}`; absent is `DEFAULT`, the primary key.

```yaml
exclude_constraints:
  - name: room_bookings_no_overlap
    method: gist
    elements:
      - name: room
        operator: =
      - name: during
        operator: '&&'
    where: (status <> 'cancelled'::text)
replica_identity: FULL
```

- Columns may carry `storage`, `compression`, `statistics` (the
  statistics target) and `options` (such as `n_distinct`), which
  `build` writes as `ALTER COLUMN ... SET` after `CREATE TABLE`, as
  `pg_dump` does. Comments on a table's primary key, unique, check,
  foreign key and NOT NULL constraints live in `constraint_comments`,
  keyed by constraint name.

- Tables and views carry `rules`. A rule's `commands` are absent for
  `DO INSTEAD NOTHING`. A view's internal `_RETURN` rule is its query,
  never a rule.

```yaml
rules:
  - name: ledger_audit_insert
    event: INSERT
    condition: (new.amount > (0)::numeric)
    commands:
      - |-
        INSERT INTO test.ledger_audit (id)
          VALUES (new.id)
  - name: ledger_no_delete
    event: DELETE
    instead: true
    comment: Append only
```

  Conditions and commands keep the form `pg_dump` writes them in.

- Extended statistics live in `statistics/<schema>/<name>.yaml`, not
  on the table: the name is schema-qualified, and the owner need not
  own the table.

```yaml
---
name: measurements_ab
schema: test
owner: postgres
table: test.measurements
kinds: [ndistinct, dependencies]
elements: [a, b]
```

- Default privileges live in `default_privileges/<role>.yaml`, one file
  for each role whose new objects they apply to (`ALTER DEFAULT
  PRIVILEGES FOR ROLE`). Global and per-schema declarations compose in
  PostgreSQL, so each is kept as written; `build` emits the
  revocations first, then the grants, as `pg_dump` does.

```yaml
---
name: app_owner
grants:
  - schema: reporting
    object_type: TABLES
    grantee: analyst
    privileges: [SELECT]
revocations:
  - object_type: FUNCTIONS
    grantee: PUBLIC
    privileges: [EXECUTE]
```

- A partition is listed in its parent's `partitions` by its bounds.
  A partition with properties of its own (indexes, constraints,
  defaults, triggers, a replica identity, …) is also a table file of
  its own, and its entry in the parent's list has `attached: true`:
  `build` creates it as an ordinary table and attaches it with `ALTER
  TABLE ... ATTACH PARTITION`, as `pg_dump` writes it.

- An index column that is an expression, such as a cast for an HNSW
  index, is written without parentheses around all of it:
  `expression: (embedding)::public.halfvec(1536)`. `build` adds them,
  because `CREATE INDEX` needs them for an expression that is not a
  function call. Parentheses around all of it are also accepted, and
  `deploy` compares the expression without them.

  The type of a cast can be a type alias, and it can be in uppercase:
  `(label)::VARCHAR(20)` compares equal to
  `(label)::character varying(20)`, which PostgreSQL writes. `deploy`
  compares the remaining text of the expression as it is, so write it
  as `pull` writes it. Give a type that is not a built-in type its
  schema, as `pull` writes it: `(embedding)::public.halfvec(1536)`.
  The deploy script runs with the empty `search_path` of `pg_restore`,
  so a type with no schema is not found and the script fails.

- A storage parameter (`storage_parameters` of a table, a
  materialized view or an index) can be a YAML number or boolean:
  `fillfactor: 90` and `autovacuum_enabled: false`. PostgreSQL keeps
  each value as text, and `pull` writes the text (`'90'`). `deploy`
  compares the value that PostgreSQL reads. A boolean and the words
  `true`, `false`, `on`, `off`, `yes` and `no` (in any case) are
  booleans, so `false` and `'off'` are equal. A number is its value,
  so `0.1` and `'0.10'` are equal. `1` and `0` compare as numbers, not
  as booleans.

- A `collation` (of a column, a domain, a type, an index column, an
  exclusion constraint or a partition key) can leave out the
  `pg_catalog` schema: `'"C"'` and `pg_catalog."C"` are one
  collation, because PostgreSQL always searches `pg_catalog`. A
  name that is not quoted is in lowercase, as in SQL: `C` is the
  collation `c`, which does not exist, so write `'"C"'`.

- A generated column's `expression` can leave out the parentheses
  around all of it, and a generated column with no `kind` is stored.
  `deploy` compares the expression as text otherwise, so write it as
  `pull` writes it, with the casts PostgreSQL adds: `lower('X' ||
  a::text)` is `lower(('X'::text || (a)::text))`. A different text is
  set again with `SET EXPRESSION` on each deploy.

- A function's or procedure's `sql_body` (a `RETURN` expression or a
  `BEGIN ATOMIC` block) is not kept as it is written. PostgreSQL
  parses it and keeps the parsed form, and `pull` writes that form.
  `deploy` compares the body as text, so a `sql_body` written in any
  other form gets a `CREATE OR REPLACE` on each deploy. This does no
  harm, but the plan is never empty. Write the body as `pull` writes
  it:

    | Written | As PostgreSQL keeps it and `pull` writes it |
    | --- | --- |
    | `RETURN 'a'` | `RETURN 'a'::text` |
    | `RETURN pg_catalog.lower(x)` | `RETURN lower(x)` |
    | <code>RETURN lower(x) &#124;&#124; s.g() &#124;&#124; 'y'</code> | <code>RETURN ((lower(x) &#124;&#124; s.g()) &#124;&#124; 'y'::text)</code> |
    | `SELECT pg_catalog.abs(x) + pg_catalog.int4(1);` | `SELECT (abs(x) + 1);` |

  PostgreSQL removes the `pg_catalog.` schema, adds the casts and
  parentheses it infers, and evaluates a cast of a constant. A name in
  any other schema keeps its schema (`s.g()`). A body in `definition`
  (`AS $$ ... $$`) is kept as it is written, so this does not apply to
  it. To get the form for a body, create the function in a scratch
  database and `pull` it.

- A function's or procedure's `configuration` sets each setting when
  the routine starts. A value is a string, a number or a boolean. A
  setting that PostgreSQL keeps as a list of names is a list, with one
  name for each item. These settings are `search_path`,
  `temp_tablespaces`, `local_preload_libraries`,
  `session_preload_libraries`, `shared_preload_libraries`,
  `oauth_validator_libraries`, `output_plugin_libraries` and
  `unix_socket_directories`. Write each name as it is, with no quotes
  in it (`$user`, `My Schema`). `build` writes each item as its own
  string constant, `SET search_path = 'pg_catalog', 'pg_temp'`, as
  `pg_dump` does, and `pull` writes a value with more than one name as
  a list:

```yaml
---
name: whoami
returns: text
language: sql
security: DEFINER
configuration:
  search_path: [pg_catalog, pg_temp]
definition: SELECT current_user::text;
```

  For one of these settings, one string with a comma is an error. As
  one string, `pg_catalog, pg_temp` names one schema, so PostgreSQL
  searches `pg_temp` first, which is not safe for a `SECURITY DEFINER`
  function. One name can be a string (`search_path: pg_catalog`). A
  list for any other setting is also an error: `pg_dump` writes the
  value of such a setting as one string, so write it as one string
  (`DateStyle: iso, mdy`). An empty list is an error, because SET
  needs a value. For an empty value, write an empty string
  (`search_path: ''`).

- A role's or user's `settings` sets each setting when the role starts
  a session (`ALTER ROLE ... SET`). It has one object for each
  setting, and each value has the forms of a routine's
  `configuration`. `build` writes each item of a list as its own
  string constant, `ALTER ROLE app SET search_path TO '$user',
  'public'`, as `pg_dumpall` does:

```yaml
---
name: app
settings:
- search_path: [$user, public]
- work_mem: 64MB
```

- A foreign key can leave out its `name`. The project then uses the
  name PostgreSQL generates, `<table>_<columns>_fkey`, cut to 63
  bytes. When that name is already in use, PostgreSQL adds a number,
  so give such a key its name.

- A primary key or unique constraint on one column can be the column
  name alone (`primary_key: id`), and an integer default can be a
  number (`default: 0`). Each is the same as the form `pull` writes.

- An index of a partitioned table has `recurse: false` when it is made
  `ON ONLY` the partitioned table, as `pg_dump` writes it. Each
  partition's index is then in the partition's table file, with the
  index it belongs to as `parent`, and `build` and `deploy` attach it
  with `ALTER INDEX parent ATTACH PARTITION`. The `parent` index must
  have `recurse: false`, or `build` stops with an error. The index of a
  unique or primary key constraint needs no `parent`: it attaches with
  its partition.

- A cast has no schema of its own. `pull` files it in the
  `casts/<schema>.yaml` of the first schema its function or types name,
  or `public` when they are all built-in.

- A transform has no schema and no owner. `pull` files it in the
  `transforms/<schema>.yaml` of its type, or of the first schema its
  functions name when the type is built-in, or `public`.

- A publication lists each table as its qualified name, or as a
  mapping that also limits the columns or rows published, and names
  whole schemas under `schemas:`. Each table is exactly the table
  named: `pull` lists an inheritance child on its own, as `pg_dump`
  does, and `build` adds every table with `ONLY`.

```yaml
---
name: replicated_positive
tables:
  - name: test.replicated
    columns: [id, amount]
    where: (amount > 0)
  - test.audit_log
schemas:
  - reporting
parameters:
  publish: [insert, update]
```

- Grants on views may be written under either `tables:` or `views:`.
  PostgreSQL grants on views with `TABLE` syntax, so both emit
  `GRANT ... ON TABLE` and coalesce into a single ACL entry; `pull`
  writes view grants under `tables:`.

- `create: false` defines a role without creating it — used for
  built-in pseudo-roles like `PUBLIC`.
