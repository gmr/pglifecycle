ARG RUST_VERSION=1.90
ARG ALPINE_VERSION=3.22

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

FROM alpine:${ALPINE_VERSION}

# pull, build, and deploy all shell out to pg_dump/pg_dumpall/pg_restore.
RUN apk add --no-cache postgresql17-client

COPY --from=builder /src/target/release/pglifecycle /usr/local/bin/pglifecycle

# pglifecycle needs no privileges of its own. A mounted project directory has to be
# readable (writable, for `pull` and `create`) by this user; `docker run --user`
# overrides it.
RUN adduser -D -H -u 65532 pglifecycle
USER 65532:65532

WORKDIR /project

ENTRYPOINT ["/usr/local/bin/pglifecycle"]
