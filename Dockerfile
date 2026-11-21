ARG RUST_VERSION=1.96
ARG ALPINE_VERSION=3.23
ARG POSTGRES_VERSION=19

FROM rust:${RUST_VERSION}-alpine${ALPINE_VERSION} AS builder

# musl-dev supplies the C runtime the linker needs; build-base supplies the C
# compiler tree-sitter-postgres builds its generated parser with.
RUN apk add --no-cache musl-dev build-base

WORKDIR /src

# Cache the dependency build: it only changes when the manifests do.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY schemata ./schemata
COPY src ./src
# Cargo skips a rebuild when only the mtime changed, so make the real sources newer.
RUN touch src/main.rs src/lib.rs \
    && cargo build --release --locked

# Alpine does not package the PostgreSQL 19 client yet, so the client
# tools and libpq come from the official PostgreSQL image of the same
# Alpine release. pg_dump refuses to dump a server newer than itself,
# so the client is the newest supported major; it dumps older servers.
FROM postgres:${POSTGRES_VERSION}-alpine${ALPINE_VERSION} AS pgclient

FROM alpine:${ALPINE_VERSION}

# pull, build, and deploy all shell out to pg_dump/pg_dumpall/pg_restore
# and psql. The packages are the shared libraries that these tools and
# libpq link to.
RUN apk add --no-cache krb5-libs keyutils-libs libcom_err libedit libldap \
    libsasl libssl3 lz4-libs zlib zstd-libs
COPY --from=pgclient /usr/local/lib/libpq.so.5* /usr/local/lib/
COPY --from=pgclient \
    /usr/local/bin/pg_dump /usr/local/bin/pg_dumpall \
    /usr/local/bin/pg_restore /usr/local/bin/psql \
    /usr/local/bin/

COPY --from=builder /src/target/release/pglifecycle /usr/local/bin/pglifecycle

# pglifecycle needs no privileges of its own. A mounted project directory has to be
# readable (writable, for `pull` and `create`) by this user; `docker run --user`
# overrides it.
RUN adduser -D -H -u 65532 pglifecycle
USER 65532:65532

WORKDIR /project

ENTRYPOINT ["/usr/local/bin/pglifecycle"]
