# Project Format

A pglifecycle project is a directory of YAML files, one file per
database object, validated against the JSON-Schema definitions in
[`schemata/`](https://github.com/gmr/pglifecycle/tree/main/schemata).

```text
my-project/
├── project.yaml          # name, encoding, extensions, languages
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
                          # event_triggers, operators, publications,
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

- Grants on views may be written under either `tables:` or `views:`.
  PostgreSQL grants on views with `TABLE` syntax, so both emit
  `GRANT ... ON TABLE` and coalesce into a single ACL entry; `pull`
  writes view grants under `tables:`.

- `create: false` defines a role without creating it — used for
  built-in pseudo-roles like `PUBLIC`.
