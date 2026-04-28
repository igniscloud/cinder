# cinder

`cinder` is a Rust library workspace for a server-side agent loop runtime:

- multi-provider abstraction through `Provider`
- custom tools through `Tool`
- custom skills through `Skill`
- Postgres checkpoints for runs, messages, and async tool tasks
- inline tools for short work and resumable async tools for long work
- run-level Postgres leases to prevent concurrent advancement of the same run
- retry/backoff/dead-letter handling for async tool tasks

## Crates

- `cinder-core`: stable types and traits
- `cinder-store-postgres`: Postgres schema and persistence
- `cinder-runtime`: `create_agent`, `create_run`, `run_agent`, worker loop, lock/retry policy

The framework crates do not contain business logic. Your axum service owns prompts,
providers, tools, skills, user auth, billing, and domain workflows. It embeds `cinder`
by registering those pieces and calling the runtime from handlers, workers, or schedulers.

## Test

Start Postgres with podman:

```bash
chmod +x scripts/start_postgres.sh scripts/run_e2e.sh
./scripts/start_postgres.sh
```

Run the library end-to-end tests:

```bash
./scripts/run_e2e.sh
```

## Axum Embedding Example

The example under `examples/axum-embedding` shows how an axum application can embed
the library runtime. It uses a tiny example provider and a generic async sleep tool;
it is not part of the framework API and contains no product/business workflow.

```bash
DATABASE_URL=postgres://cinder:cinder@127.0.0.1:55432/cinder \
  cargo run --manifest-path examples/axum-embedding/Cargo.toml
```

Create a run:

```bash
curl -sS http://127.0.0.1:3000/runs \
  -H 'content-type: application/json' \
  -d '{"input":"hello"}'
```

Trigger the async tool path:

```bash
curl -sS http://127.0.0.1:3000/runs \
  -H 'content-type: application/json' \
  -d '{"input":"sleep"}'
```

Your real axum service should define its own providers and tools in the business
crate, then depend on `cinder-core`, `cinder-runtime`, and `cinder-store-postgres`.
# cinder
