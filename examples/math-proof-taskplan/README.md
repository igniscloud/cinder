# Math Proof TaskPlan Example

This example ports the shape of Ignis `math-proof-lab` to Cinder TaskPlan.
It uses Cinder's embedded runtime, Postgres checkpoints, and an OpenAI-compatible
HPC-AI provider.

Edit `cinder.toml` and set your database plus provider values directly in the
file. This phase intentionally does not read API keys or database URLs from
environment variables.

The included config defaults to SQLite and `moonshotai/kimi-k2.5` through
HPC-AI's OpenAI-compatible endpoint.

Run:

```bash
cargo run --manifest-path examples/math-proof-taskplan/Cargo.toml \
  examples/math-proof-taskplan/cinder.toml
```

The default request asks for a high-school-accessible explanation of Fermat's
Last Theorem while preserving the fact that Wiles/Ribet-level mathematics must
remain named black boxes for that audience.
