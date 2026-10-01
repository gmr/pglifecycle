# Commands

All commands share the logging options `-L/--log-file FILE`,
`-v/--verbose`, and `--debug`.

## create

Create a skeleton project.

```bash
pglifecycle create [OPTIONS] DEST
```

| Option | Description |
| --- | --- |
| `--force` | Write to `DEST` even if it already exists |
| `--name NAME` | Override the default project name |
| `--no-gitkeep` | Do not create `.gitkeep` files in empty directories |
| `--superuser NAME` | Superuser name (default `postgres`) |
| `--include-mode-headers` | Prefix generated files with editor mode headers |

## build

Generate a `pg_restore`-compatible archive from a project. The project
is loaded and validated against the JSON-Schema contract before the
archive is written; entries are ordered with pg_dump's weighted
topological sort.

```bash
pglifecycle build PROJECT DEST
```

The archive is in UTF8 with `standard_conforming_strings` on, because
the text of a project is UTF-8 and its SQL is written with that
setting. pg_restore converts the text to the encoding of the database,
thus the same archive restores into a UTF8 and into a LATIN1 database.
Create the database with the encoding before the restore.

## deploy

Compare a live database (or an existing dump) against the project and
emit the DDL needed to make the database match: `CREATE` for objects
missing from the database, `DROP` for objects missing from the
project, and an in-place reconciliation (or a drop+recreate fallback)
for objects that exist in both but differ. The script goes to stdout
(or `-o FILE`) with a summary on stderr; by default nothing is
executed, so it can be applied as a separate CI step:

```bash
pglifecycle deploy -o deploy.sql PROJECT
psql --single-transaction -v ON_ERROR_STOP=1 -f deploy.sql
```

`--apply` runs the script directly instead, in a single transaction
via `psql` (it rolls back on the first error and refuses while gated
destructive statements are pending).

Before its first statement, the script sets the session settings of
pg_restore that can change the result of DDL, and its header names
them:

```sql
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
```

Thus the script runs as a restore of the `build` archive does, and not
by the settings of the session:

- `client_encoding`: the script is UTF-8, so a character that is not
  ASCII does not change when the database or the client uses another
  encoding.
- `standard_conforming_strings`: a backslash in a string literal is
  not an escape.
- `search_path`: a name resolves as it does in a restore. A name with
  no schema resolves only in `pg_catalog`, so qualify each other name
  in the project.
- `check_function_bodies`: PostgreSQL does not check the body of a
  `LANGUAGE sql` function when it makes the function, so the body can
  refer to a table that the script makes after it.
- `xmloption`: an `xml` constant that is not a document, such as a
  default of `'text'::xml`, is valid.

The script does not set the other settings that pg_restore sets.
`statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout`
and `transaction_timeout` can stop the script, but they do not change
what it makes, and on a live database they are limits that the operator
sets on purpose. `client_min_messages` changes only the messages that
the client gets. `row_security` changes only the rows that a query
reads. DDL does not use it, except the query that fills a materialized
view.

The settings are not local to the transaction, so they also apply when
the script runs outside a transaction block. They stay until the
session ends, or until a rollback of the transaction that set them.

| Option | Description |
| --- | --- |
| `-D, --dump FILE` | Compare against a `pg_dump -Fc` file instead of connecting |
| `-o, --output FILE` | Write the DDL script to FILE instead of stdout |
| `--apply` | Execute the script in one transaction via psql (conflicts with `--dump`) |
| `--allow-drop` | Include destructive statements in the script |
| `--allow-drop-indexes` | Drop indexes that the database has and the project does not (kept by default) |
| `-x, --no-privileges` | Do not include GRANT/REVOKE |
| `-O, --no-owner` | Do not set the owners of objects |
| `--error-file FILE` | Where to record failures and their DDL (default `pglifecycle-errors.log`) |
| `-T, --exclude-table PATTERN` | Exclude tables/views/sequences matching `PATTERN` (repeatable; conflicts with `--dump`) |
| `-N, --exclude-schema PATTERN` | Exclude schemas matching `PATTERN` (repeatable; conflicts with `--dump`) |
| `--exclude-extension PATTERN` | Exclude extensions matching `PATTERN` (repeatable; conflicts with `--dump`) |

The exclude patterns are the same as `pull`'s and are passed through to
`pg_dump`. Excluding the schemas the project does not manage keeps them
out of the snapshot, which both shortens the dump and silences the
"unmanaged" dependency warnings.

The connection options match `pull` (see below), and so do the session
settings of the dump and the rules for a `--dump` file. Like `pull`,
`deploy` snapshots and formats the database with libpgfmt, so DDL that fails to
parse or format — and the statement in flight if it is interrupted — is
recorded to the error report (`--error-file`); see
[Diagnosing parse and format failures](#diagnosing-parse-and-format-failures).

### Change reconciliation

Objects that differ between the project and the database are
reconciled in place where PostgreSQL can express it:

- **Tables** — add column, set/drop default, set/drop not-null,
  add/drop check constraints, foreign keys and exclusion constraints,
  primary-key and unique additions, index and trigger create/drop,
  `REPLICA IDENTITY`, column storage, compression, statistics target
  and options, rules (`CREATE OR REPLACE RULE`, their state and
  comment), and comment changes, including comments on constraints. A
  changed comment on a constraint is set alone, without rebuilding the
  constraint; a constraint that is re-added gets its comment again.
  A changed comment on an index is set alone, too. The indexes of a
  partitioned table and of its partitions change as one group:
  PostgreSQL does not drop an index that is attached to another, and
  dropping the partitioned table's index drops its partitions'. So when
  the definition of an index of the group changes, or an index of the
  group is removed, deploy drops the partitioned table's index, then
  makes all of them again and attaches them. A change to the comment
  only does not rebuild the group.
  A changed generated expression is set with `ALTER COLUMN ... SET
  EXPRESSION` (PostgreSQL 17 and later); a change between stored and
  virtual, or to or from a plain column, falls back.
  Identity columns are added, and their `ALWAYS`/`BY DEFAULT` behavior
  and sequence options changed, with `ALTER COLUMN`; renaming an
  identity's sequence falls back. A `NOT VALID` check, foreign key or
  NOT NULL constraint that the project marks valid is validated with
  `VALIDATE CONSTRAINT`, which avoids the full scan under a heavy lock
  that dropping and re-adding it would take. Dropping a column, changing a column
  type, reordering columns, and partitioning/storage changes fall back
  to drop+recreate.
- **Row-level security** — `ENABLE`/`DISABLE` and `[NO] FORCE ROW
  LEVEL SECURITY`, and one statement per policy, labeled with the
  policy's own name in the script. A new policy is created. A changed
  comment is set with `COMMENT ON POLICY`, and a role change that
  narrows access (fewer roles on a permissive policy, more on a
  restrictive one) with `ALTER POLICY ... TO`. Every other policy
  change drops and re-creates the policy. A table file without
  `row_level_security` or `policies`, such as one pulled before
  pglifecycle modeled them, leaves the table's row security as the
  database has it.
- **Functions and views** — `CREATE OR REPLACE` (a function whose
  return type changed must be dropped first, so it falls back). A
  view's rules are reconciled after it. A `sql_body` is compared as
  text with the form PostgreSQL keeps, so write it as `pull` writes
  it (see [Project format](project-format.md)).
- **Procedures** — `CREATE OR REPLACE PROCEDURE`, as for functions,
  with the comment set after it. A procedure is matched by its name
  and input types, so a changed input type makes the new procedure
  and drops the old one. PostgreSQL does not let `CREATE OR REPLACE`
  rename a parameter, change the `OUT` or `INOUT` parameters, or
  remove a default, so such a change falls back. A type alias, a type
  modifier (`varchar(20)` is `character varying`), the language name
  in upper case, `security: INVOKER` (the default), and a default or
  setting value written as a number are not changes. The body is
  compared as text in the form that `pull` writes.
- **Sequences** — a single `ALTER SEQUENCE` of the changed options.
- **Domains** — set/drop default; a base-type or constraint change
  falls back.
- **Enum types** — `ALTER TYPE ... ADD VALUE` for appended values;
  reordering or removing values falls back.
- **Extensions** — `ALTER EXTENSION ... UPDATE` / `SET SCHEMA`.
- **Foreign data wrappers** — handler, validator, and `OPTIONS`
  (`ADD`/`SET`/`DROP`) changes, plus comments.
- **Foreign servers** — `VERSION` and `OPTIONS` changes, plus comments;
  a wrapper or `TYPE` change (neither is alterable) falls back, as does
  clearing an existing `VERSION` (it cannot be removed in place).
- **User mappings** — per-server `OPTIONS` changes; a mapping the
  project adds or drops is created or dropped. A `password` the project
  does not carry is left untouched, so a redacted pull (the default)
  never strips a live credential.
- **Foreign tables** — `OPTIONS` changes and comments in place; a server
  or column change falls back to drop+recreate.
- Everything else falls back to drop+recreate.

### Destructive statements and limits

Destructive statements — `DROP` for database-only objects, data-losing
column changes, and every drop+recreate fallback — are excluded from
the script unless `--allow-drop` is given; each exclusion is reported
on stderr and counted in the script header, and `--apply` refuses while
any are pending. Trigger and constraint drops issued while
reconciling a table are *not* gated: they lose no data and the project
is authoritative. A changed index is also dropped and made again
without a gate.

An index that the database has and the project does not is **kept**
unless `--allow-drop-indexes` is given. Such an index is often made at
runtime, and one such as an HNSW vector index is slow to make again.
Each kept index is reported on stderr and listed in the script header,
and it does not stop `--apply`. `--allow-drop` does not drop it:
give `--allow-drop-indexes` to drop it. To keep an index for good,
add it to the project. A table that deploy drops and makes again
(with `--allow-drop`) loses every index that the project does not
have.

`DROP RULE` is gated although a rule holds no data. A `DO INSTEAD
NOTHING` rule can block writes, so dropping one can let through changes
the database refused before. A project pulled with a version of
pglifecycle that did not model rules has none on any table or view,
and the gate stops a deploy of that project from dropping every rule.

`DROP IDENTITY` is gated although it keeps every row: the sequence goes
with it, so adding the identity back restarts the numbering and
collides with existing keys. A project pulled with a version of
pglifecycle that did not model identity columns has none on any
column, so deploying it asks for exactly this on each one; the gate is
what stops that from stripping them. Pull the project again to record
them.

Row-security reconciliation is gated when it can give a role access to
rows it could not see before: `DISABLE` and `NO FORCE ROW LEVEL
SECURITY`, the drop of a restrictive policy, and every policy change
except a comment or a role change that narrows access. Whether an
edited `USING` or `WITH CHECK` expression allows more rows or fewer
cannot be decided in general, so every such edit is gated. Enabling
or forcing row security, dropping a permissive policy and adding a
policy are always included: a new policy is one the project adds
explicitly, and gating it while `ENABLE` runs would hide every row.

A withheld statement that opens access leaves the database stricter
than the project. Any other withheld policy change is different: the
database keeps the old policy, which can allow more than the project
does. The script header names each one with a `-- WARNING:` line, and
the stderr report says the same.

Default privileges compare by the privileges that they give to each
role, schema and object type, not by the statements as written. `ALL`
is the full list of the object type (for tables, the PostgreSQL 17
list with `MAINTAIN`), `ROUTINES` is `FUNCTIONS`, the case of a
privilege and of `PUBLIC` is not a difference, and neither is the
order. The built-in privileges count too: the owner has all of them,
and `PUBLIC` has `EXECUTE` on functions and `USAGE` on types, so
`REVOKE EXECUTE` matches the `REVOKE ALL` that pg_dump writes. deploy
emits `ALTER DEFAULT PRIVILEGES FOR ROLE … GRANT` or `… REVOKE` for
the difference only. They change only the objects that the role makes
later, so neither is gated. A role whose default privileges only the
database has gets the statements that give it the built-in privileges
again; like a drop, they are gated. When they revoke a grant, the
script header warns that the database can allow access the project
does not. With `-x`, deploy does not change default privileges.

deploy gives each object the owner that the project names, as
pg_restore does. The connecting role owns what the script creates, so
the script sets the owner with `ALTER … OWNER TO` directly after each
CREATE. An object that the database has with another owner gets the
same statement in place; it is not destructive. The owner is not part
of the definition comparison, so a changed owner never causes a
rebuild. Each owner role must exist, and each owner must have CREATE on
the schema of its objects. A connecting role that is not a superuser
must be a member of each owner role with the SET and INHERIT options
(`GRANT` gives both by default to a role that has the INHERIT
attribute). The statements after an owner change need the privileges of
the owner: for example, CREATE TABLE in a new schema, and the indexes,
comments and grants of a new table. To change the owner of an object
that the database has, the connecting role must also have the
privileges of its current owner. To change the owner of a schema, the
connecting role must have CREATE on the database. A type whose model
has no owner (for example publications, subscriptions and event
triggers) keeps the connecting role as owner. So does most of what the
project writes as raw `sql`: as pg_restore does, deploy sets no owner
for an archive entry with no DROP statement. A new object gets the
default privileges of the connecting role, not those of its owner; its
grants come from the project. With `-O`, deploy does not set or compare
owners (as `pg_restore --no-owner`).

Roles, users, groups, and tablespaces are skipped entirely — they are
cluster-level objects a single-database dump cannot capture.

A publication changes in place: one `ALTER PUBLICATION ... SET` gives
all its tables and schemas, and `SET (...)` gives its parameters. The
tables, their columns, the schemas and the `publish` operations are
compared as sets, a table name as PostgreSQL resolves it, and a
parameter written at its default is the same as none. A row filter is
compared without the parentheses that enclose all of it; other than
that, it is compared as text, and PostgreSQL writes it back in its own
form, so write it as `pull` does. A change to or from `all_tables`
drops and makes the publication again (with `--allow-drop`). A change
to `publish_via_partition_root` is applied with a warning: a subscriber
of a partitioned table can copy rows two times or lose rows.

A subscription changes in place: `CONNECTION`, `SET PUBLICATION ...
WITH (refresh = false)`, `SET (...)` and its comment. Deploy runs in
one transaction, and a refresh cannot, so after a change to the
publications, run `ALTER SUBSCRIPTION ... REFRESH PUBLICATION` by hand.
The options that only `CREATE SUBSCRIPTION` reads (`connect`,
`create_slot`, `copy_data`) are not compared, nor is `enabled`, which
`pg_dump` does not write; deploy never enables or disables a
subscription. A change to `slot_name`, `two_phase` or `failover` is
reported and not made, as it needs the subscription disabled, the
publisher, or a statement outside a transaction; the script carries
the statement to run as a comment. A connection that the project gives
without a password keeps the password the database has. The drop of a
subscription that only the database has (with `--allow-drop`) first
disables it and removes its slot name, as `DROP SUBSCRIPTION` cannot
drop a slot in a transaction: the publisher keeps the slot, and the
report gives the statement that drops it there.

Statistics, event triggers, collations, conversions and access methods
compare by definition. These changes are made in place, without
`--allow-drop`: the statistics target (`ALTER STATISTICS ... SET
STATISTICS`), the state of an event trigger (`ALTER EVENT TRIGGER ...
ENABLE [REPLICA | ALWAYS]` or `DISABLE`), the comment, and the owner of
statistics, a collation or a conversion. PostgreSQL has no ALTER for the
other settings, so a change to one of them drops and makes the object
again, only with `--allow-drop`. These objects hold no data, but an
object can depend on one: for example, a column that uses a collation.
Then the drop fails, and deploy rolls back all of its changes; deploy
does not use `CASCADE`. One that only the database has is dropped, also
only with `--allow-drop`. An event trigger or an access method needs a
superuser. Values that can be written two ways compare as the same:

- statistics: the table as PostgreSQL resolves the name, the kinds as a
  set (all three are the same as none), the columns as a set (a name in
  parentheses is a column), and a target of -1 as none. An expression
  loses the parentheses that enclose all of it, and compares with no
  spaces and in lowercase, other than in quotes; PostgreSQL writes an
  expression back in its own form (for example with casts), so write it
  as `pull` does.
- event triggers: the tags as a set, in any case; ORIGIN, the default
  state, as none; the function name as PostgreSQL resolves it.
- collations: `libc` and `deterministic: true`, the defaults, as none;
  the same `lc_collate` and `lc_ctype` as one `locale`; a simple ICU
  locale such as `en_US` in its standard form, `en-US`. The version is
  not compared. A collation made `FROM` another is only checked for
  existence, as `pull` writes the settings that it copied.
- conversions: `default: false` as none; an encoding by the name that
  PostgreSQL resolves it to (`utf-8` and `Unicode` are `UTF8`); the
  function name as PostgreSQL resolves it, with no `pg_catalog.`.
- access methods: the type in any case; the handler as for a
  conversion function.

Text search parsers, templates, dictionaries and configurations
compare one by one, by kind, schema and name. The project keeps the
text search objects of a schema in one file, but deploy adds, changes
and drops each object apart from the others in that file. A mapping of
a configuration changes in place with `ALTER TEXT SEARCH CONFIGURATION
… ADD MAPPING`, `… ALTER MAPPING` or `… DROP MAPPING`, the options of a
dictionary with `ALTER TEXT SEARCH DICTIONARY … (…)`, and a comment
with `COMMENT ON`. An option that only the database has is given with
no value, which removes it. A changed parser of a configuration,
template of a dictionary, or function of a parser or a template has no
ALTER form, so deploy drops the object and makes it again (with
`--allow-drop`). The drop fails when another object uses the object,
for example a configuration that maps the dictionary: deploy does not
use `CASCADE`, so the deploy stops and rolls back. Names compare as
PostgreSQL reads them: a part that is not in quotes is folded to
lowercase, and a name with no schema is in `pg_catalog`, as the build
runs with an empty `search_path`. Thus qualify a name in the object's
own schema. Token types compare in lowercase and in any order; the
dictionaries of a token type compare in their order. Option names
compare in lowercase, and a boolean or a number is the same as its
text. pg_dump writes a copied configuration (`source`) as its parser
and all its mappings, so deploy compares only the mappings that the
project gives for a copy, not its parser or its other mappings. Text
search dictionaries and configurations have an owner in PostgreSQL,
but the project model has none, so deploy does not compare or set it.

Aggregates, operators, casts, transforms, operator classes and
operator families compare by definition. An aggregate is matched by
its name and its input types, an operator by its name and its argument
types, a class or a family by its name and its index method, a cast by
its two types, and a transform by its type and its language. A name, a
type or a function
compares in the form that PostgreSQL keeps: a type alias or an
uppercase name is not a difference, nor is `pg_catalog.` or
`OPERATOR(...)`, an option at its default, or the order of the members
of a class or a family. A cast or a transform has no schema of its
own, so the schema that the project files it under is not compared.
PostgreSQL has almost no ALTER for these types:

- An aggregate changes with `CREATE OR REPLACE AGGREGATE`. A change to
  the state type, the final function, `FINALFUNC_EXTRA`,
  `HYPOTHETICAL` or an argument drops the aggregate and makes it again,
  as `CREATE OR REPLACE` cannot change them.
- An operator changes `RESTRICT` and `JOIN` in place with `ALTER
  OPERATOR ... SET`, and sets `COMMUTATOR`, `NEGATOR`, `HASHES` and
  `MERGES` in place when the database has none. To clear or change
  one of those four, or to change the function, deploy drops the
  operator and makes it again. PostgreSQL can link the commutator or
  the negator back to the operator that names it, so a project can
  give the link on one side only.
- A transform changes with `CREATE OR REPLACE TRANSFORM`.
- A family adds and drops its members with `ALTER OPERATOR FAMILY`. A
  member that only the database has is dropped only with
  `--allow-drop`. A member that a class does not have yet is added to
  the family of the class in place. PostgreSQL keeps some members of a
  class, for example a btree sort support function and each operator
  of a GiST class, in the family only, and pg_dump writes them there;
  written in the class, they compare equal. A class with no family
  gets a family of its own name, which deploy does not drop.
- Any other change to a cast or a class drops it and makes it again.

A drop and a create is destructive, so it needs `--allow-drop`. The
drop does not cascade: when an object depends on the object, for
example an index on an operator class or a view that uses a cast, the
drop fails and the transaction rolls back. A class whose members are
kept in its family cannot be made again while they are there, so a
rebuild of such a class also fails and rolls back. The comment of each
of these types changes in place. An aggregate, an operator, a class
and a family have an owner; a cast and a transform do not.

An object of a type that `pull` does not yet model (security labels,
…) is left as the database has it. An object that the project writes
as a raw `sql` statement is only checked for existence, whatever its
type: `pull` writes the structured fields, so the two never compare
equal.
A raw statement has no structured input types, so it is matched by
its type, schema and name, and any overload of that name counts. A
raw cast is the exception: it must have its source and target types,
and it is matched by them.

Privileges on created objects are emitted (unless `-x`); privilege
changes on objects that already exist are not yet diffed.

## pull

Create a project from a live database or an existing dump. Entry DDL is
parsed into structured YAML (columns, constraints, indexes, and ACLs as
data), view queries and function bodies are formatted, and child
objects are merged into their owners.

```bash
pglifecycle pull [OPTIONS] DEST
```

| Option | Description |
| --- | --- |
| `-D, --dump FILE` | Use an existing `pg_dump -Fc` file instead of connecting |
| `--no-roles` | Skip cluster role/user extraction (role/user extraction is enabled by default for live connections; always skipped with `--dump`) |
| `--include-password-hashes` | Include role password hashes in users (omitted by default via `pg_dumpall --no-role-passwords`), and the passwords of user mappings and subscription connections |
| `--include-mode-headers` | Prefix each generated file with editor mode headers (see below) |
| `-i, --ignore FILE` | File listing project paths to skip writing |
| `--force` | Write to `DEST` even if it already exists |
| `--update` | Merge into an existing project, rewriting only changed files |
| `--prune` | With `--update`, delete files whose objects left the database |
| `--gitkeep` | Create `.gitkeep` files in empty directories |
| `--remove-empty-dirs` | Remove empty directories after generation |
| `--allow-unsupported` | Accept a project that does not reproduce the source database (see below) |
| `--error-file FILE` | Where to record failures and their DDL (default `pglifecycle-errors.log`) |
| `--style STYLE` | libpgfmt style for view/materialized view queries and function bodies (default `pg_dump`) |
| `-T, --exclude-table PATTERN` | Exclude tables/views/sequences matching `PATTERN` (repeatable; ignored with `--dump`) |
| `-N, --exclude-schema PATTERN` | Exclude schemas matching `PATTERN` (repeatable; ignored with `--dump`) |
| `--exclude-extension PATTERN` | Exclude extensions matching `PATTERN` (repeatable; ignored with `--dump`) |

## Unsupported dump entries

`pull` parses every schema entry in the archive into the object model.
An entry it cannot model is written verbatim to `remaining.yaml` at the
project root, and `pull` then **fails**, because the project it produced
would not rebuild the database it came from — and nothing downstream
(`build`, `deploy`, or a review of the YAML) could tell that something
went missing.

```console
$ pglifecycle pull ./project -d mydb
...
error: 1 dump entry could not be modeled (SECURITY LABEL), so the
generated project would not reproduce the source database.
The entry was preserved in ./project/remaining.yaml; re-run with
--allow-unsupported to accept the project as it is.
```

A `COMMENT` entry counts as unmodeled when the model has no place for
it, such as a comment on an object type that `pull` does not model.

The project directory is written either way, so `remaining.yaml` is
there to inspect. `--allow-unsupported` downgrades the failure to a
warning for the cases where an incomplete project is what you want.

`deploy` reports the same condition from the other side: objects in the
database it cannot model are named in a warning and left untouched,
since they cannot be represented in the plan.

`--save-remaining` is accepted and ignored; `remaining.yaml` is now
always written when there is anything to put in it. It holds the only
copy of those entries, so the `--ignore` file cannot hold it back.

The exclude patterns are passed through to `pg_dump` (`--exclude-table`,
`--exclude-schema`, `--exclude-extension`) and use the same pattern
syntax. They apply only when connecting to a database; with `--dump` the
archive is already built, so they are rejected as conflicting.

`pull` runs `pg_dump` and `pg_dumpall` with `-E UTF8`, and with
`standard_conforming_strings` on (added to the end of `PGOPTIONS`). These
settings replace the settings of the database, the role and the
environment, thus a database with the LATIN1 encoding, or with
`standard_conforming_strings` off, gives the same project as a UTF8
database with the default settings. A `--dump` file must have the same
settings. `pull` refuses a file in another encoding, or a file made with
`standard_conforming_strings` off, and names the setting. Make the file
again with the settings:

```bash
PGOPTIONS='-c standard_conforming_strings=on' pg_dump -E UTF8 -Fc --schema-only -d mydb -f mydb.dump
```

The `--style` value is one of libpgfmt's styles — `river`, `mozilla`,
`aweber`, `dbt`, `gitlab`, `kickstarter`, `mattmc3`, or `pg_dump` — and
controls only how view/materialized view queries and function bodies are
formatted in the generated project. Note that `deploy` always re-formats the database
side with the default (`pg_dump`) to compare it against the project, so
pulling with a non-default style will make `deploy` report
formatting-only differences for every view and function.

With `--include-mode-headers`, each generated file is prefixed with two
comment lines above the `---` document marker — an Emacs modeline and a
`# pglifecycle: <kind>` type comment (`<kind>` is the object-type noun,
e.g. `table`, `materialized_view`, `function`) that editor extensions
can key off to detect pglifecycle files:

```yaml
# -*- mode: pglifecycle -*-
# pglifecycle: materialized_view
---
name: autoresponder_service_package_info
schema: public
```

Without the flag (the default), files begin directly at the `---`
marker. The same flag is available on `create`.

### Diagnosing parse and format failures

DDL that fails to parse, or SQL that fails to format, is logged and the
offending statement is written to the error report (`--error-file`,
default `pglifecycle-errors.log` in the working directory) alongside its
full DDL, so a failure can be correlated with the exact statement that
produced it. The file is created only when there is something to report.

If `pull` is interrupted with Ctrl-C — for example because the formatter
is stuck on a pathological statement — the statement in flight at that
moment is written to the same report before exiting, turning a hang into
a reproducer.

Connection options mirror the PostgreSQL client tools and honor the
standard `PGHOST`, `PGPORT`, `PGUSER`, and `PGDATABASE` environment
variables:

| Option | Description |
| --- | --- |
| `-d, --dbname NAME` | Database name to connect to |
| `-h, --host HOST` | Server host or socket directory (default `localhost`) |
| `-p, --port PORT` | Server port (default `5432`) |
| `-U, --username NAME` | Username to operate as |
| `-w, --no-password` | Never prompt for a password |
| `-W, --password` | Prompt for a password up front |
| `--role NAME` | Role to assume when connecting |

pglifecycle does its own password prompting rather than letting
`pg_dump`, `pg_dumpall`, and `psql` prompt: they write the prompt to
`/dev/tty`, where the progress bar overwrites it, and the command then
appears to hang with no prompt in sight. The client tools always run
with `-w`, and pglifecycle supplies the password in the environment.

A prompt appears when the server asks for a password that `PGPASSWORD`
or a pgpass file did not supply, and the failed attempt is retried with
it; `-W` prompts up front instead. The progress bars are hidden while
the prompt is on screen. `-w` suppresses prompting altogether, as does a
non-interactive stdin, so scripted runs fail with the error rather than
waiting for input.

DDL options:

| Option | Description |
| --- | --- |
| `-x, --no-privileges` | Do not include GRANT/REVOKE |
| `--no-security-labels` | Do not include security label assignments |
| `--no-tablespaces` | Do not include tablespace assignments |

With `--update`, `DEST` must be an existing project (it must contain
`project.yaml`). The pull is rendered as usual but only files whose
content actually changed are written, so `git diff` afterwards shows
exactly what changed in the database. Files for objects that no longer
exist in the database are reported as warnings and left in place;
`--prune` deletes them instead (confined to the directories `pull`
manages — `dml/` and other project content is never touched). Paths
listed in the `--ignore` file are neither rewritten nor pruned, except
`remaining.yaml`, which the file cannot hold back. Note
that overloaded function files are numbered in dump order
(`name.yaml`, `name_1.yaml`, …), so adding or removing an overload can
renumber a sibling's file.

Cluster roles and users are extracted via `pg_dumpall --roles-only`
whenever `pull` connects to a live database (use `--no-roles` to skip,
and note they cannot be extracted from a `--dump` file). They are
classified when written: a role with the `LOGIN` attribute becomes a
file in `users/`; everything else lands in `roles/`. Roles that appear
only as ACL grantees (such as `PUBLIC`) are written with
`create: false` so `build` defines but never creates them. The
reserved `pg_*` roles are cluster-managed (and uncreatable), so they
are excluded. Password hashes are omitted unless
`--include-password-hashes` is given. Reading hashes requires
`pg_authid`, which managed platforms (e.g. RDS) restrict; when it is
denied, `pull` falls back to a passwordless roles dump (warning that
hashes were unavailable) rather than dropping all roles.

A subscription connection string can contain a password, as a
`password` keyword or in a URI. `pull` removes it by default and
writes a warning. Add the password to the project before you `build`,
or pass `--include-password-hashes` to write it.

### Foreign data wrappers, servers, and foreign tables

Foreign objects round-trip through `pull` and `build`:

- **Foreign data wrappers** are written into `project.yaml` under
  `foreign_data_wrappers` (alongside extensions and languages), with
  their handler, validator, and options. Extension-owned wrappers (e.g.
  `postgres_fdw`'s own wrapper) are created by their extension and are
  not emitted here.
- **Foreign servers** get one file each in `servers/`, carrying the
  wrapper name, optional `type`/`version`, and connection `options`.
- **User mappings** get one file each in `user_mappings/`, grouping all
  of a user's server mappings. A mapping's `password` option is a
  secret and is **redacted by default**; pass `--include-password-hashes`
  to write it (the same flag that controls role password hashes).
- **Foreign tables** live in `tables/` like ordinary tables, with a
  `server` field and an open `options` map (keys depend on the wrapper —
  e.g. `schema_name`/`table_name` for `postgres_fdw`, `filename`/`format`
  for `file_fdw`). `build` renders them as `CREATE FOREIGN TABLE`.
