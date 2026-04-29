# cinder

`cinder` is a Rust library workspace for a server-side agent loop runtime:

- multi-provider abstraction through `Provider`
- custom tools through `Tool`
- custom skills through `Skill`
- SQLite or Postgres checkpoints for runs, messages, async tool tasks, and TaskPlans
- inline tools for short work and resumable async tools for long work
- run-level store leases to prevent concurrent advancement of the same run
- retry/backoff/dead-letter handling for async tool tasks
- TaskPlan DAG orchestration for multi-agent workflows

## Crates

- `cinder-core`: stable types and traits
- `cinder-store-postgres`: Postgres schema and persistence
- `cinder-store-sqlite`: SQLite schema and persistence
- `cinder-runtime`: `create_agent`, `create_run`, `run_agent`, worker loop, lock/retry policy, TaskPlan advancement

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
crate, then depend on `cinder-core`, `cinder-runtime`, and the store crate you need.

## Math Proof Solver Example

The example under `examples/math-proof-solver` ports the shape of Ignis
`math-proof-lab` to a dynamic Cinder solver. It starts a root TaskPlan whose
main math solver agent reads its task with `cinder.get_task`, dynamically
creates child TaskPlans when needed, and completes the root task with
`cinder.submit_task`. The example uses the high-level `Cinder` facade: callers
submit a `TaskPlan`, then wait for or query its result; they do not manually
advance execution.

It uses `examples/math-proof-solver/cinder.toml` for the database, provider,
model, and agent definitions. Edit the API key directly in that file before
running; environment variable config is intentionally not used in this phase.

```bash
cargo run --manifest-path examples/math-proof-solver/Cargo.toml \
  examples/math-proof-solver/cinder.toml
```

## Built-in Tools

`AgentRuntime::from_config` registers these tools automatically:

- `cinder.list_agents`: returns each configured agent's id, description, provider, model, tools, and skills.
- `cinder.get_task`: returns the current TaskPlan task prompt, input, output schema, and metadata for worker agents.
- `cinder.spawn_task_plan`: creates a TaskPlan from a normal agent run or a TaskPlan task and pauses the current agent until the plan completes.
- `cinder.submit_task`: submits the current TaskPlan task result or failure.

Cinder also injects a framework system prompt into every agent. It tells agents
to fetch assigned work with `cinder.get_task`, create child plans with
`cinder.spawn_task_plan` when needed, and complete TaskPlan work only by calling
`cinder.submit_task`.
