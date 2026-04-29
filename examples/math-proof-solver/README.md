# Math Proof Solver Example

This example ports the shape of Ignis `math-proof-lab` to a dynamic Cinder solver.
It uses Cinder's embedded runtime, SQLite or Postgres checkpoints, and an
OpenAI-compatible HPC-AI provider.

Edit `cinder.toml` and set your database plus provider values directly in the
file. This phase intentionally does not read API keys or database URLs from
environment variables.

The included config defaults to SQLite and `moonshotai/kimi-k2.5` through
HPC-AI's OpenAI-compatible endpoint.

The example submits a root TaskPlan with one main `math-solver-agent` task to
the Cinder framework. Cinder manages the configured database, starts execution
in the background, and exposes the plan run id plus result lookup/wait APIs. The
main agent reads its task with `cinder.get_task`, inspects available specialist
agents with `cinder.list_agents`, dynamically creates one or more child
TaskPlans with `cinder.spawn_task_plan`, and submits the final root task result
with `cinder.submit_task`.

Specialist agents follow the same pattern: `cinder.get_task` to inspect their
assigned TaskPlan task, optional `cinder.spawn_task_plan` if allowed, and
`cinder.submit_task` when they complete it. All configured agent system prompts
are loaded from `prompts/agents/`.

Run:

```bash
cargo run --manifest-path examples/math-proof-solver/Cargo.toml \
  examples/math-proof-solver/cinder.toml
```

You can pass a custom question as the second argument.

The default request asks for a high-school-accessible explanation of Fermat's
Last Theorem while preserving the fact that Wiles/Ribet-level mathematics must
remain named black boxes for that audience.
