# pglifecycle

A PostgreSQL schema management tool

[![Crates.io](https://img.shields.io/crates/v/pglifecycle.svg)](https://crates.io/crates/pglifecycle)
[![Testing](https://github.com/gmr/pglifecycle/workflows/Testing/badge.svg?)](https://github.com/gmr/pglifecycle/actions?workflow=Testing)
[![License](https://img.shields.io/github/license/gmr/pglifecycle.svg)](https://github.com/gmr/pglifecycle/blob/main/LICENSE)

pglifecycle keeps your database schema in a version-controlled project
of YAML files — one file per database object, validated against a
JSON-Schema contract — and moves schema between the repository and live
databases:

- **`pull`** reads a live database (or a `pg_dump` archive) and writes
  the project directory
- **`build`** turns the project into a `pg_restore`-compatible custom
  format archive
- **`create`** scaffolds an empty project

## Installation

### Homebrew (macOS / Linux)

```bash
brew tap gmr/postgres
brew install pglifecycle
```

> [!NOTE]
> Homebrew 6.0 added [tap trust](https://docs.brew.sh/Tap-Trust), and some
> versions fail to install third-party taps inside the build sandbox (the
> error mentions `build.rb ... exited with 1`). If you hit this, trust the
> formula first:
>
> ```bash
> brew trust --formula gmr/postgres/pglifecycle
> ```
>
> or, as a temporary workaround, set `HOMEBREW_NO_REQUIRE_TAP_TRUST=1` for
> the install.

### Cargo

```bash
cargo install pglifecycle
```

### Docker

The image bundles the PostgreSQL 17 client tools pglifecycle shells out to.
Mount the project directory at `/project`, the image's working directory:

```bash
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/project" \
  ghcr.io/gmr/pglifecycle:latest build my-project/ mydb.dump
```

The image itself runs as uid 65532, which a bind-mounted project directory
you own is not writable by, so `--user` above hands the container your own
uid. Drop it for a read-only project, or for a volume that uid 65532 owns.

Prebuilt binaries for Linux and macOS (x86_64 and aarch64) are attached
to each [release](https://github.com/gmr/pglifecycle/releases).

## Usage

```bash
# pull an existing database into a new project
pglifecycle pull -h localhost -d mydb my-project/

# build the project into a restorable archive
pglifecycle build my-project/ mydb.dump
pg_restore -d mydb_copy mydb.dump
```

See the [documentation](https://gmr.github.io/pglifecycle/) for the
full command reference and project format.
