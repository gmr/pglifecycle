# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# pglifecycle

A PostgreSQL schema management tool (Rust rewrite of a former Python
implementation). It keeps a database schema as a version-controlled
project of YAML files — one file per object, validated against a
JSON-Schema contract — and moves schema between the repository and live
databases.

## Commands

`just` (the Justfile) is the task runner; `just --list` shows all
recipes. The common ones:

```bash
just build          # cargo build
just check          # fmt-check + lint + test (the CI trio)
just fmt            # cargo fmt (max_width 79, see rustfmt.toml)
just lint           # cargo clippy --all-targets -- -D warnings
just test           # cargo test
just run -- pull --help   # cargo run -- <args>
```

Run a single test directly with cargo:

```bash
cargo test --test pull              # one integration test file in tests/
cargo test name_of_test             # a single test by name
```

## Architecture

The CLI (`src/cli.rs`, clap) dispatches four subcommands from
`src/main.rs`, each a top-level module:

- **`pull`** (`src/pull/`) — database (or `pg_dump` archive) → project.
  Runs `pg_dump`/`pg_dumpall` (`src/pgdump.rs`), parses each entry's DDL
  through `src/ddl/`, merges child entries (indexes, constraints,
  triggers, comments, ACLs) into their owning objects, and writes the
  structured YAML project.
- **`build`** (`src/build/`) — project → `pg_restore -Fc` archive.
- **`create`** (`src/skeleton.rs`) — scaffold an empty project.
- **`deploy`** (`src/deploy/`) — compare the project against a live
  database/dump and emit the DDL to make the database match. **Output
  first**: the script goes to stdout/`--output` to be applied
  separately; destructive statements (DROPs, data-losing changes,
  drop+recreate fallbacks) are excluded unless `--allow-drop`.

Shared layers used by the commands:

- `src/models/` — the object model. `Definition` is an untagged enum
  over every supported object type; `Item` wraps one with its inventory
  `id` and dependency set. Serde field order **is** the emitted YAML key
  order — declaration order is load-bearing, and `deny_unknown_fields`
  is intentional. A `Project` (`src/project/`) is `load`ed (`load.rs`)
  and `validate`d against the schemata before use.
- `src/ddl/` — tree-sitter-postgres CST → models. The grammar is
  generated from PostgreSQL's `gram.y`, so node kinds mirror grammar
  productions and carry no named fields; extraction walks the tree by
  kind via `NodeExt` helpers.
- `src/deploy/diff.rs` + `alter.rs` — compute the project↔database diff
  and resolve each change to in-place ALTER or drop+recreate.
- `src/pgdump.rs` — `pg_dump`/`pg_dumpall` subprocess wrappers.
- `src/progress.rs` — indicatif bars; logging is bridged through them so
  log records print above live bars and stdout stays clean (deploy
  writes its script there).

External crates do the heavy lifting: `libpgdump` (archive
read/write), `libpgfmt` (SQL formatting; `--style` selects a house
style, but `deploy` always compares with the `pg_dump` default),
`tree-sitter-postgres` (DDL parsing).

## Parity oracle

`build` rendering is deliberately bug-for-bug faithful to the old Python
implementation so archives compare entry-by-entry; intentional
deviations (places the Python emitted broken SQL) are enumerated at the
top of `src/build/mod.rs` and asserted in `tests/build_parity.rs`. The
Python implementation is preserved at the `python-final` tag:

```bash
git clone --depth 1 --branch python-final https://github.com/gmr/pglifecycle /tmp/pglifecycle-py
```

Module headers reference the Python file each replaces ("ports X.py").

## Testing & gates

Integration tests live in `tests/`. The round-trip and deploy gates need
PostgreSQL: they bring up `compose.yaml` (Postgres 17) on a
dynamically-mapped host port and run the scripts in `bin/`.

```bash
just db-up          # start the container, wait until healthy
just db-port        # print the mapped host port
just round-trip     # schema → pull → build → restore → diff
just deploy-gates   # deploy equivalence / convergence / safety
just gates          # both
```

The gate recipes discover the mapped port automatically. To run a gate
script by hand, set `PGHOST`/`PGPORT`/`PGUSER` (the recipes use
`PGUSER=postgres`).

## Key directories

- `schemata/` — JSON-Schema (YAML) definitions for PostgreSQL objects;
  the on-disk project contract, carried over from Python unchanged.
- `test-project/` — example project structure; a parity contract.
- `fixtures/` — test database schema.
- `bin/` — gate scripts and fixture-data generation.
- `docs/` — mkdocs site (`just docs` builds with `--strict`).
