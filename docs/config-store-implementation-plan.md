# Config, Model, and Store Implementation Plan

## Goals

- Use one `cinder.toml` to configure store, providers, models, agents, and runtime defaults.
- Do not support environment-variable based config in this phase.
- Support `sqlite` and `postgres` stores from config.
- Agents reference model definitions by the `[models.<id>]` table name through the `model` field.
- Keep manual Rust registration available for advanced embedding.

## Config Shape

```toml
[store]
kind = "sqlite"

[store.sqlite]
path = "./data/cinder.db"

[store.postgres]
database_url = "postgres://cinder:cinder@127.0.0.1:55432/cinder"
max_connections = 8

[providers.hpcai]
kind = "openai_compatible"
base_url = "https://api.hpc-ai.com/inference/v1"
api_key = "..."

[models.kimi25]
provider = "hpcai"
model = "moonshotai/kimi-k2.5"
temperature = 0.2
context_tokens = 1048576
max_output_tokens = 4096

[agents.orchestrator]
model = "kimi25"
description = "Coordinates plans and delegates work to specialist agents."
system_prompt_file = "agents/orchestrator.md"
tools = ["taskplan.spawn_task_plan", "taskplan.submit_task"]
```

## Implementation Steps

1. Add config types and validation in `cinder-core`.
2. Add a `CinderStore` async trait in `cinder-core` for runtime persistence.
3. Implement `CinderStore` for the existing Postgres store.
4. Add `cinder-store-sqlite` with equivalent schema and `CinderStore` implementation.
5. Refactor `cinder-runtime` to depend on `Arc<dyn CinderStore>` instead of `PostgresStore`.
6. Add config loading helpers that build providers, models, agents, and stores from concrete config values.
7. Update examples so database/provider/model/agents come from config files.
8. Add tests for config validation, unknown model references, SQLite execution, and Postgres regression.
