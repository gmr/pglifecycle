# GitHub Actions

Two actions live in
[gmr/pglifecycle-action](https://github.com/gmr/pglifecycle-action):

| Action | What it does |
| --- | --- |
| `gmr/pglifecycle-action@v1` | Installs pglifecycle and puts it on `PATH` |
| `gmr/pglifecycle-action/deploy@v1` | Runs `pglifecycle deploy`, publishes the plan, and can fail on drift |

Linux and macOS runners on x86-64 and arm64 are supported; the release
publishes no Windows binary. `pull`, `build`, and `deploy` shell out to
`pg_dump`, `pg_dumpall`, `pg_restore`, and `psql`, which the
GitHub-hosted Linux and macOS images already carry.

## Installing the binary

```yaml
- uses: gmr/pglifecycle-action@v1
  with:
    version: 2.0.0-alpha.1
- run: pglifecycle build ./schema schema.dump
```

| Input | Default | Description |
| --- | --- | --- |
| `version` | `latest` | Release tag to install |
| `repository` | `gmr/pglifecycle` | Repository to install from |
| `github-token` | `${{ github.token }}` | Token for the release lookup |

It outputs `version` (the tag installed) and `path` (the binary).

!!! note
    `latest` resolves to the newest release. pglifecycle has published
    only prereleases so far, so `latest` picks the newest prerelease and
    logs a warning. Pin `version` to keep a workflow stable.

With the binary on `PATH`, any command from
[Commands](commands.md) runs as an ordinary step — `build` to produce an
archive as a build artifact, `pull --update` against a staging database
to see what a project would look like after a change.

## Planning a deploy

`deploy` compares a project against a database and writes the DDL that
would make the database match. It never applies anything: the script is
the output, which is what makes it safe to run against production from
a pull request.

```yaml
permissions:
  contents: read
  pull-requests: write

jobs:
  plan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: gmr/pglifecycle-action/deploy@v1
        with:
          project: ./schema
          host: db.example.net
          database: production
          username: schema_ci
          password: ${{ secrets.PGPASSWORD }}
          comment-on-pr: true
          fail-on-drift: true
```

| Input | Default | Description |
| --- | --- | --- |
| `project` | *(required)* | Path to the pglifecycle project |
| `version` | `latest` | pglifecycle release tag to install |
| `dump` | | Compare against this `pg_dump` file instead of a live database |
| `output` | `pglifecycle-plan.sql` | Path to write the DDL script to |
| `allow-drop` | `false` | Include destructive statements in the plan |
| `no-privileges` | `false` | Leave grants and revokes out of the plan |
| `role` | | Role to assume when connecting |
| `args` | | Extra flags appended to the invocation |
| `host` `port` `database` `username` `password` | | Connection settings (`PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`) |
| `comment-on-pr` | `false` | Post the plan as a sticky pull request comment |
| `fail-on-drift` | `false` | Fail the step when the plan holds any change |
| `github-token` | `${{ github.token }}` | Token for the release lookup and the comment |

| Output | Description |
| --- | --- |
| `drift` | `true` when the database does not match the project |
| `plan` | Path of the generated DDL script |
| `excluded` | Count of destructive statements withheld from the plan |

The plan always goes to the job summary. `comment-on-pr` also posts it
to the pull request, replacing the previous comment for the same project
on each run, and needs `pull-requests: write`.

Destructive changes stay out of the plan unless `allow-drop` is set, but
they still count as drift: a plan that withheld statements reports
`drift: true` with `excluded` above zero. See
[deploy](commands.md#deploy) for what counts as destructive.

## Applying a plan

The action has no apply mode, deliberately. Take the script from the
`plan` output and run it in the step or job that owns that decision:

```yaml
- id: plan
  uses: gmr/pglifecycle-action/deploy@v1
  with:
    project: ./schema
    host: db.example.net
    database: production
    username: schema_ci
    password: ${{ secrets.PGPASSWORD }}
- if: steps.plan.outputs.drift == 'true'
  env:
    PGPASSWORD: ${{ secrets.PGPASSWORD }}
  run: psql -h db.example.net -U schema_ci -d production -1 -f "${{ steps.plan.outputs.plan }}"
```

`psql -1` wraps the script in a single transaction, so a failure
partway through leaves the database as it was. Most of what `deploy`
emits is transactional; `CREATE INDEX CONCURRENTLY` and
`ALTER TYPE ... ADD VALUE` are not, and PostgreSQL will reject them
inside `-1`.

## Guarding the round trip

`build` is worth running on every pull request even without a database:
it loads and validates the whole project against the
[schemata](reference/index.md), so a bad file fails the build rather
than a deploy.

```yaml
- uses: gmr/pglifecycle-action@v1
- run: pglifecycle build ./schema /tmp/schema.dump
```

## Versioning

The action releases are tagged `vMAJOR.MINOR.PATCH`, and `v1` moves to
the newest release of that major version. Those tags track the actions,
not pglifecycle — the tool version is the `version` input.
