# Project Format

A pglifecycle project is a directory of YAML files, one file per
database object, validated against the JSON-Schema definitions in
[`schemata/`](https://github.com/gmr/pglifecycle/tree/main/schemata).

```text
my-project/
├── project.yaml          # name, encoding, extensions, languages,
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
                          # text_search, user_mappings, dml
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
  records relationships the topological sort cannot infer, such as
  foreign-key ordering between tables.
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

- An index of a partitioned table has `recurse: false` when it is made
  `ON ONLY` the partitioned table, as `pg_dump` writes it. Each
  partition's index is then in the partition's table file, with the
  index it belongs to as `parent`, and `build` and `deploy` attach it
  with `ALTER INDEX parent ATTACH PARTITION`. The index of a unique or
  primary key constraint needs no `parent`: it attaches with its
  partition.

- A cast has no schema of its own. `pull` files it in the
  `casts/<schema>.yaml` of the first schema its function or types name,
  or `public` when they are all built-in.

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
