use anyhow::{anyhow, Context, Result};
use cinder_core::{
    CinderConfig, CinderStore, OutputBinding, StoreKind, TaskDependency, TaskPlan, TaskSpec,
};
use cinder_runtime::{AgentRuntime, CreateTaskPlanRun, TaskPlanOutcome, TaskPlanRuntime};
use cinder_store_postgres::PostgresStore;
use cinder_store_sqlite::SqliteStore;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("examples/math-proof-taskplan/cinder.toml"));
    let config = CinderConfig::load(&config_path)?;
    let base_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
    let question = "Explain why Fermat's Last Theorem is true for a high-school audience, without pretending that advanced mathematics can be removed.";

    match config.store.kind {
        StoreKind::Sqlite => {
            let sqlite = config
                .store
                .sqlite
                .as_ref()
                .context("store.sqlite is required")?;
            let store = SqliteStore::connect_from_config(sqlite).await?;
            run_with_store(store, &config, base_dir, question).await?;
        }
        StoreKind::Postgres => {
            let postgres = config
                .store
                .postgres
                .as_ref()
                .context("store.postgres is required")?;
            let store = PostgresStore::connect_from_config(postgres).await?;
            run_with_store(store, &config, base_dir, question).await?;
        }
    }

    Ok(())
}

async fn run_with_store<S>(
    store: S,
    config: &CinderConfig,
    base_dir: &Path,
    question: &str,
) -> Result<()>
where
    S: CinderStore + Clone + 'static,
{
    store.init_schema().await.map_err(|error| anyhow!(error))?;
    let agent_runtime = AgentRuntime::from_config(store.clone(), config, base_dir).await?;
    let plan_runtime = TaskPlanRuntime::new(store, agent_runtime);
    let plan_run_id = plan_runtime
        .create_plan_run(CreateTaskPlanRun {
            plan: math_proof_plan(question),
            user_id: None,
            target_id: Some("math-proof-taskplan-example".to_owned()),
            parent_plan_run_id: None,
            parent_task_id: None,
        })
        .await?;

    let deadline = Instant::now() + Duration::from_secs(600);
    loop {
        match plan_runtime.advance_plan(plan_run_id).await? {
            TaskPlanOutcome::Completed { result } => {
                println!("{}", serde_json::to_string_pretty(&result)?);
                break;
            }
            TaskPlanOutcome::Failed { error } => return Err(anyhow!(error)),
            TaskPlanOutcome::Running | TaskPlanOutcome::Busy => {
                if Instant::now() > deadline {
                    return Err(anyhow!("task plan did not finish before timeout"));
                }
                sleep(Duration::from_secs(2)).await;
            }
        }
    }

    Ok(())
}

fn math_proof_plan(question: &str) -> TaskPlan {
    let common_schema = json!({
        "type": "object",
        "required": ["markdown"],
        "properties": {
            "markdown": { "type": "string" }
        },
        "additionalProperties": true
    });

    TaskPlan {
        id: format!("math-proof-{}", Uuid::new_v4()),
        root_task_id: "rigor-review".to_owned(),
        tasks: vec![
            TaskSpec {
                id: "literature".to_owned(),
                agent_id: "literature-agent".to_owned(),
                prompt: "For the user request, identify rigorous references and named theorem dependencies. Include a short black-box ledger when needed.".to_owned(),
                input: json!({ "question": question, "audience": "high_school" }),
                output_schema: common_schema.clone(),
            },
            TaskSpec {
                id: "formal".to_owned(),
                agent_id: "formal-verifier-agent".to_owned(),
                prompt: "Create a proof dependency tree. For Fermat's Last Theorem, distinguish elementary reductions from Ribet/Wiles-level black boxes.".to_owned(),
                input: json!({ "question": question, "audience": "high_school" }),
                output_schema: common_schema.clone(),
            },
            TaskSpec {
                id: "curriculum".to_owned(),
                agent_id: "curriculum-agent".to_owned(),
                prompt: "Map the dependency tree to the target audience and decide which prerequisites can be taught now versus named black boxes.".to_owned(),
                input: json!({ "audience": "high_school" }),
                output_schema: common_schema.clone(),
            },
            TaskSpec {
                id: "pedagogy".to_owned(),
                agent_id: "pedagogy-agent".to_owned(),
                prompt: "Write the audience-facing explanation. It should be readable, layered, and strict: intuition first, definitions next, black boxes explicitly labeled.".to_owned(),
                input: json!({ "audience": "high_school" }),
                output_schema: common_schema.clone(),
            },
            TaskSpec {
                id: "rigor-review".to_owned(),
                agent_id: "rigor-critic-agent".to_owned(),
                prompt: "Review and produce the final approved response. If the explanation hides Wiles/Ribet black boxes, correct it before returning. Return final markdown.".to_owned(),
                input: json!({ "question": question, "audience": "high_school" }),
                output_schema: common_schema,
            },
        ],
        dependencies: vec![
            bind_markdown("literature", "curriculum", "/literature_markdown"),
            bind_markdown("formal", "curriculum", "/formal_markdown"),
            bind_markdown("literature", "pedagogy", "/literature_markdown"),
            bind_markdown("formal", "pedagogy", "/formal_markdown"),
            bind_markdown("curriculum", "pedagogy", "/curriculum_markdown"),
            bind_markdown("formal", "rigor-review", "/formal_markdown"),
            bind_markdown("curriculum", "rigor-review", "/curriculum_markdown"),
            bind_markdown("pedagogy", "rigor-review", "/draft_markdown"),
        ],
    }
}

fn bind_markdown(from_task: &str, to_task: &str, to_pointer: &str) -> TaskDependency {
    TaskDependency {
        from_task: from_task.to_owned(),
        to_task: to_task.to_owned(),
        bindings: vec![OutputBinding {
            from_pointer: "/markdown".to_owned(),
            to_pointer: to_pointer.to_owned(),
        }],
    }
}
