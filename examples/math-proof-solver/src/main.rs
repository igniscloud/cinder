use anyhow::Result;
use cinder_core::{TaskPlan, TaskSpec};
use cinder_runtime::Cinder;
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;

const SOLVER_AGENT_ID: &str = "math-solver-agent";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("examples/math-proof-solver/cinder.toml"));
    let question = std::env::args().nth(2).unwrap_or_else(|| {
        "Prove that sqrt(2) is irrational. Use exactly one child TaskPlan task with the formal-verifier-agent to verify the proof before the final answer, and keep the final answer concise.".to_owned()
    });

    let cinder = Cinder::from_config_path(&config_path).await?;
    let plan_run_id = cinder.submit_task_plan(root_plan(&question)).await?;
    let result = cinder
        .wait_task_plan_result(plan_run_id, Duration::from_secs(600))
        .await?;
    println!("{}", serde_json::to_string_pretty(&result)?);

    Ok(())
}

fn root_plan(question: &str) -> TaskPlan {
    TaskPlan {
        id: format!("math-proof-solver-root-{}", uuid::Uuid::new_v4()),
        tasks: vec![TaskSpec {
            id: "solve".to_owned(),
            agent_id: SOLVER_AGENT_ID.to_owned(),
            prompt: "Solve the user's mathematics request. Use cinder.list_agents and cinder.spawn_task_plan when specialist sub-work is needed. When the final answer is ready, submit JSON with a markdown string field.".to_owned(),
            input: json!({
                "question": question,
                "audience": "high_school"
            }),
            output_schema: json!({
                "type": "object",
                "required": ["markdown"],
                "properties": {
                    "markdown": { "type": "string" }
                },
                "additionalProperties": true
            }),
        }],
        dependencies: vec![],
    }
}
