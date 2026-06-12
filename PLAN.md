# pglifecycle Rust Rewrite Plan

A plan for rewriting pglifecycle (~7,400 lines of Python) in Rust, built on the
existing gmr Rust ecosystem: [libpgdump](https://crates.io/crates/libpgdump),
[libpgfmt](https://crates.io/crates/libpgfmt),
[tree-sitter-postgres](https://crates.io/crates/tree-sitter-postgres), and
selected pieces of [postgres-lsp](https://crates.io/crates/postgres-lsp-parse).

## Goals

- Functional parity with the Python tool: `create`, `build`, and `pull`
  (which replaces the Python `generate`; see Phase 3) commands operating
  on the same YAML project layout.
- Two new capabilities beyond parity (see New Capabilities below):
  database-to-repo updates of an existing project (`pull`), and
  repo-to-database schema synchronization (`deploy`).
- Single static binary, no Python runtime, no libpg_query C dependency.
- The existing 37 JSON-Schema files in `pglifecycle/schemata/` and the
  project directory layout are the contract — they carry over unchanged
  (along with `test-project/` and `fixtures/`).
- **Clean-slate branch, same repository.** The rewrite happens on a
  long-lived `rust-rewrite` branch in this repo that starts by removing the
  Python implementation entirely (keeping `schemata/`, `test-project/`,
  `fixtures/`, and docs). Finishing is a PR that merges `rust-rewrite` into
  `main`, replacing the Python tree wholesale.
- **Incremental PRs.** `rust-rewrite` is the integration branch; work lands
  on it through small reviewable PRs from short-lived feature branches
  (one per phase, or smaller where a phase splits cleanly — e.g., the
  `ddl` module per statement family). CI must be green on every PR; the
  phase gates below are the merge criteria for the PR that completes each
  phase.
- The Python code is never present on the rewrite branch. Agents working on
  the rewrite reference it via a shallow clone of the `python-final` tag:

  ```bash
  git clone --depth 1 --branch python-final https://github.com/gmr/pglifecycle /tmp/pglifecycle-py
  ```

  The Python tool is the parity oracle (run from the clone via `uv run`)
  until the Phase 3 round-trip gates pass.

## Dependency Mapping

| Python dependency | Role today | Rust replacement |
|---|---|---|
| pgdumplib | Read/write pg_dump `-Fc` archives | **libpgdump 2.x** — direct functional superset (custom/directory/tar formats, 4 compression algorithms, archive versions 1.12–1.16, built-in weighted topological sort matching `pg_dump_sort.c`) |
| pgparse (libpg_query) + `tokenizer.py` (947 lines) | Parse DDL from dump entries into normalized structures | **tree-sitter-postgres 1.1** — CST walking in a new `ddl` module (see below) |
| — (new capability) | SQL/PL-pgSQL formatting for clean YAML diffs | **libpgfmt 1.1** — format view queries and function bodies during `generate` |
| ruamel.yaml | YAML I/O with literal block scalars | serde + a YAML emitter with scalar-style control (see Risks) |
| jsonschema | Validate YAML objects against `schemata/*.yml` | **jsonschema** crate (Stranger6667) — validate via `serde_json::Value` |
| toposort | Build-order linearization | **libpgdump's weighted topological sort** (the pg_dump_sort.c port run by `save()`) — inventory dependency edges are recorded on the archive entries and ordering is delegated entirely to it; no petgraph |
| argparse | CLI | **clap** (derive) |
| pg_dump subprocess (`pgdump.py`) | Extract schema from live database | unchanged — `std::process::Command` wrapping `pg_dump -Fc` |
| stringcase, arrow, python-dotenv, faker | Misc / dev-only | heck (case conversion); the rest are fixture-generation only and stay in `bin/` as-is |

## DDL Parsing: tree-sitter-postgres vs postgres-lsp vs pg_catalog

Three candidate ways to get at PostgreSQL DDL were evaluated:

1. **tree-sitter-postgres directly** (recommended for parsing).
   Grammar is generated from PostgreSQL 18's `gram.y` — all 69 DDL statement
   types pglifecycle cares about are covered (`CreateStmt`,
   `CreateFunctionStmt`, `DefineStmt` for types/aggregates/operators, etc.),
   plus a separate PL/pgSQL grammar with injection. The cost: tree-sitter
   produces a CST without named field accessors, so extraction code walks
   nodes by `kind()`. This replaces `tokenizer.py` and is the largest single
   piece of new work.

2. **postgres-lsp-analysis** (reuse selectively).
   Its `extract_symbols()` already maps `CreateStmt`/`CreateFunctionStmt`/
   type statements to a `Symbol` model with qualified names and column
   children — but it extracts *symbols for navigation*, not full semantic
   detail: constraints, indexes, ACLs, storage parameters, and trigger
   definitions are left as raw text. pglifecycle needs the deep model, so
   the `ddl` module will be written fresh, borrowing postgres-lsp-analysis's
   node-walking patterns (and `postgres-lsp-parse`'s parser pooling +
   PL/pgSQL injection detection if useful) rather than depending on the
   crates wholesale.

3. **postgres-lsp-schema** (pg_catalog introspection — not the primary path).
   Direct introspection could eventually replace the pg_dump subprocess for
   `generate --extract`, but pg_dump remains the authoritative DDL renderer:
   it handles ACL ordering, comments, security labels, extension membership,
   and version quirks that `pg_catalog` queries would have to re-derive.
   Recommendation: keep `pg_dump -Fc` → libpgdump as the extraction path for
   parity, and note postgres-lsp-schema's batched catalog queries as the
   basis for a future dump-free `extract` mode (post-parity, separate
   effort). This avoids re-implementing pg_dump's DDL rendering in v1.

## Architecture

Single binary crate at the repo root (no workspace — the Python code is
~7.4k lines; a workspace adds ceremony without payoff). Modules mirror the
Python layout where it's clean, and split `dump.py`/`generate_*.py` along
clearer seams:

```
src/
├── main.rs            # clap CLI: create / build / generate (cli.py)
├── constants.rs       # paths, read order, object-type tables (constants.py)
├── models/            # serde structs for all 27 object types (models.py)
│   ├── mod.rs         #   Definition enum + Item inventory wrapper
│   ├── table.rs, function.rs, ...
├── project/           # load, validate, dependency graph (project.py)
│   ├── load.rs        #   YAML walk in _READ_ORDER
│   ├── validate.rs    #   jsonschema against schemata/*.yml (validation.py)
├── build/             # project → libpgdump archive (dump.py, 1,555 lines)
│   ├── mod.rs         #   per-object SQL rendering
│   └── acls.rs        #   GRANT/REVOKE generation
├── pull/              # dump → project (generate_dump.py + generate_project.py)
│   ├── mod.rs         #   entry classification, inventory, role/ACL handling
│   ├── writer.rs      #   YAML file emission, .gitkeep, empty-dir cleanup
│   └── update.rs      #   update mode: merge into an existing project
├── diff/              # model-level schema diff (shared by pull/deploy)
│   ├── mod.rs         #   ObjectDiff: Added / Removed / Changed(field paths)
│   └── ...
├── deploy/            # diff → ordered DDL (CREATE/ALTER/DROP)
│   ├── mod.rs         #   plan assembly, topo ordering, drop gating
│   └── alter.rs       #   per-object ALTER renderers + fallback rules
├── ddl/               # tree-sitter CST → models (replaces tokenizer.py)
│   ├── mod.rs         #   parser setup, statement dispatch
│   ├── table.rs       #   CreateStmt → Table (columns, constraints, indexes)
│   ├── function.rs    #   CreateFunctionStmt → Function (+ libpgfmt body)
│   └── ...
├── pgdump.rs          # pg_dump / pg_dumpall subprocess wrapper (pgdump.py)
├── yamlio.rs          # YAML load/save with literal scalars (yaml.py, storage.py)
└── skeleton.rs        # `create` command (project scaffolding)
```

The `schemata/` YAML files are embedded with `include_dir!` so the binary
stays self-contained.

## New Capabilities (beyond Python parity)

Both features sit on the same foundation: once `build` can turn the repo
into models and `pull` can turn a live database into models (via
`pg_dump -Fc` → libpgdump → `ddl`), a shared `diff` module can compare the
two model sets. The diff drives both directions — database→repo (`pull`
update mode) and repo→database (`deploy`).

### `pull` — rename `generate`, add update mode

Recommendation: rename `generate` to **`pull`**. The Python command name
describes only the bootstrap case ("generate a project"); the new behavior
is bidirectional-sounding and pairs naturally with `deploy`
(`pull` = database → repo, `deploy` = repo → database). The `--extract`
flag goes away — connecting to a database *is* the normal mode, and
`--dump FILE` remains for offline use.

Behavior:

- **Bootstrap (today's behavior):** destination doesn't exist (or
  `--force`) — write the full project, exactly what `generate` does now.
- **Update (new):** destination is an existing project — load it, pull the
  database into models, diff, and rewrite only the YAML files whose objects
  changed. Files for objects dropped from the database are deleted only
  with `--prune` (default: warn and leave them). Untouched files are not
  rewritten, so `git diff` after a `pull` shows exactly what changed in the
  database.
- Update mode respects the existing `--ignore` file and never touches
  files outside the managed layout (`dml/`, ad-hoc docs, etc.).

### `deploy` — sync the database to the repo

Evaluates the live database against the repo and produces the DDL needed to
make the database match: `CREATE` for objects missing from the database,
`DROP` for objects missing from the repo, `ALTER` where the object exists
in both but differs.

- **Pipeline:** load + validate the repo (the `build` front half) → snapshot
  the live database into the same models (the `pull` front half) → `diff` →
  render an ordered DDL script. Creates are ordered by libpgdump's
  weighted toposort over the dependency edges; drops in reverse
  topological order.
- **Output first, execution second.** Default is a plan: the SQL script on
  stdout (or `-o FILE`) plus a human-readable summary. `--apply` executes
  it, wrapped in a single transaction (PostgreSQL DDL is transactional;
  exceptions like `CREATE INDEX CONCURRENTLY` are out of scope for v1).
- **Drops are gated.** Without `--allow-drop`, destructive statements
  (`DROP ...`, column drops, `ALTER ... TYPE` that rewrites data) are
  listed in the summary but excluded from the script, and `--apply` refuses
  if the database has objects the repo doesn't account for.
- **ALTER coverage is incremental.** Each object type gets an ALTER
  renderer for the changes PostgreSQL can express (`ALTER TABLE ADD/ALTER
  COLUMN`, `ALTER FUNCTION`, `CREATE OR REPLACE` for views/functions,
  re-grant for ACL changes, `COMMENT ON`). Where no in-place ALTER exists
  (e.g., changing a domain's base type), the fallback is drop + recreate —
  which is destructive and therefore gated behind `--allow-drop`.
- **No rename detection in v1.** A renamed table/column looks like drop +
  add; the drop gate is the safety net. Rename hints could come later via
  annotations in the YAML.

## Phases

Each phase ends with a verifiable gate. Order is chosen so every phase
ships something testable against the Python implementation.

### Phase 0 — Scaffolding
- Create the `rust-rewrite` branch from `main`; remove the Python package,
  tests, and Python tooling (keep `schemata/`, `test-project/`,
  `fixtures/`, `compose.yaml`, docs). `cargo init`, clap CLI skeleton with
  the same flags as `cli.py`, CI workflow (fmt, clippy, test), `create`
  command.
- **Gate:** `pglifecycle create /tmp/x` produces a directory tree identical
  to the Python tool's output (diff -r against a run from the shallow
  clone).

### Phase 1 — Models, YAML I/O, validation
- serde models for all object types, YAML load/save, JSON-Schema validation
  including the `$package_schema` composition used by `validation.py`.
- **Gate:** the full `test-project/` loads, validates, and round-trips
  through YAML save with semantically identical output (yaml-diff, not
  byte-diff — see Risks).

### Phase 2 — `build`
- Dependency caching/application, per-object SQL rendering, ACLs,
  comments; emit via `libpgdump::new()` + `add_entry()`, recording
  dependency edges so libpgdump's weighted toposort orders the archive.
- This ports `dump.py`, the largest and highest-complexity module.
- **Gate (parity):** build `test-project/` with both implementations;
  compare archives entry-by-entry (`libpgdump::load` both, diff
  namespace/tag/desc/defn after whitespace normalization).
- **Gate (correctness):** `pg_restore` the Rust artifact into the Docker
  PostgreSQL from `compose.yaml` and diff `pg_dump --schema-only` output
  against a restore of the Python artifact.

### Phase 3 — `pull` (bootstrap mode; replaces `generate`)
- Parse `pg_dump -Fc` with libpgdump, classify entries, extract structure
  from DDL via the `ddl` module, and write the YAML project **in the
  structured format** (the test-project/ shape: columns, constraints,
  indexes as data — not raw SQL). Use libpgfmt (AWeber style) for view
  queries and function bodies. Port `--extract-roles` (pg_dumpall
  subprocess) and the ignore/remaining-yaml options. The command ships as
  `pull` (see New Capabilities).
- **Decision (2026-06-11):** the Python `generate` emitted sql-passthrough
  YAML (raw DDL in `sql:` keys, children as nested sql entries). That
  output violates the schemata contract (`additionalProperties: false`)
  and cannot be loaded by `project.load` in either implementation — the
  Python round-trip never worked end-to-end, and `generate` on main
  crashes outright (`_mark_remaining_processed` lives on the wrong
  class). Output parity with Python `generate` is therefore dropped as a
  gate; the structured format honors the schemata contract and gives
  Phases 5–6 the model-level data the `diff` module requires.
- This replaces `generate_project.py` + `tokenizer.py`; lands as several
  PRs (the `ddl` module per statement family, then pgdump.rs + the pull
  assembly).
- **Gate (round-trip):** `fixtures/schema.sql` → Docker pg → `pg_dump` →
  Rust `pull` → load + validate against the schemata → Rust `build` →
  `pg_restore` → re-dump → schema diff is empty.

### Phase 4 — Cutover
- Port docs (mkdocs → keep, or switch to README + docs.rs), release
  workflow (`cargo dist` or GitHub release binaries).
- Open the cutover PR: `rust-rewrite` → `main`, replacing the Python tree
  wholesale. Tag the last Python commit (e.g., `python-final`) before
  merging so the old implementation stays reachable.
- **Gate:** CI green on the rewrite branch; `cargo test` + the Docker
  round-trip suite replace `ci/test`; PR merged.

The remaining phases are new features, developed post-cutover on `main`
through normal feature branches.

### Phase 5 — `pull` update mode
- The `diff` module (model-level comparison) and `pull/update.rs`: merge
  pulled models into an existing project, rewrite only changed files,
  `--prune` for deletions.
- **Gate:** pull into a fresh bootstrap of the fixtures database is a no-op
  (`git status` clean). Apply a known migration to the database
  (add a column, change a function, drop a view), pull again — `git diff`
  shows exactly those objects and nothing else; with `--prune` the dropped
  view's file is removed.

### Phase 6 — `deploy`
- Diff → ordered DDL plan, per-object ALTER renderers, `--allow-drop`
  gating, `--apply` execution in a transaction.
- **Gate (convergence):** mutate the fixtures database (drop / add / alter
  a representative object of each kind), `deploy --apply --allow-drop`,
  then `pull` — the repo is unchanged (`git status` clean) and a re-run of
  `deploy` produces an empty plan.
- **Gate (equivalence):** `deploy --apply` against an empty database yields
  the same schema as `build` + `pg_restore` (`pg_dump --schema-only` diff
  is empty).
- **Gate (safety):** without `--allow-drop`, the emitted script contains no
  destructive statements and `--apply` refuses when drops are pending.

## Effort Estimate (relative)

| Component | Python size | Expected effort |
|---|---|---|
| `ddl` module (replaces tokenizer.py) | 947 lines | **Largest** — CST walking is more verbose than libpg_query's AST; budget ~2× the rest of any phase |
| `build` (dump.py) | 1,555 lines | Large but mechanical — 30+ object renderers, mostly string assembly |
| `pull` bootstrap (generate_*, 2 modules) | 1,977 lines | Large; depends on `ddl` |
| models + project + validation | 1,500 lines | Moderate — serde does heavy lifting |
| CLI, yamlio, pgdump, skeleton | ~570 lines | Small |
| `diff` + `pull` update mode | — (new) | Moderate — mechanical once models exist |
| `deploy` (ALTER renderers) | — (new) | **Large** — second only to `ddl`; the ALTER coverage matrix across 27 object types grows incrementally |

## Risks & Open Questions

1. **YAML emission fidelity.** ruamel.yaml's literal block scalars and key
   ordering shape the on-disk project format; Rust YAML emitters vary in
   scalar-style control (`serde_yaml` is unmaintained; candidates:
   `serde_norway`, `serde_yaml_ng`, or emitting through `saphyr` directly).
   Parity gates use semantic YAML comparison, not byte equality, but
   generated projects should still produce clean git diffs. Spike this in
   Phase 1 before committing to an emitter.
2. **tree-sitter CST verbosity.** No named fields means `ddl` extraction is
   walk-by-`kind()` code. Mitigation: a `NodeExt` helper trait (libpgfmt
   already has one in `node_helpers.rs` worth lifting) and corpus-style
   fixture tests per statement type.
3. **Grammar edge cases.** tree-sitter-postgres is generated from PG 18's
   grammar and well-tested (190 corpus cases), but pg_dump emits some
   unusual constructs (e.g., `ALTER TABLE ONLY ... ATTACH PARTITION`,
   operator class definitions). The Phase 3 round-trip gate over real dumps
   is the safety net; grammar gaps get fixed upstream in
   tree-sitter-postgres rather than worked around.
4. **dml/ directory support.** The Python tool's DML/data handling is
   thin; confirm whether table data entries (COPY payloads via
   `set_entry_data`) are in scope for v1 or deferred.
5. **Diff normalization.** `deploy` and `pull` update mode compare models
   built from two sources: YAML written by a human and DDL rendered by
   pg_dump. PostgreSQL canonicalizes on ingest (type aliases like
   `int4`/`integer`, default expressions, qualified names in view bodies),
   so naive comparison produces false diffs. The `diff` module must compare
   normalized forms; the Phase 5 "pull is a no-op" gate exists precisely to
   flush these out before `deploy` builds on the same comparison.
6. **ALTER generation is a long tail.** Tools like migra/atlas exist
   because this is hard: dependent-object cascades (changing a column type
   under a view), constraint validation order, ACL re-grant ordering.
   Mitigation: drop+recreate is always a correct (if destructive) fallback
   and it's gated; ALTER renderers are added incrementally per object type
   with the Phase 6 convergence gate as the regression suite.
