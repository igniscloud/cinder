use anyhow::Result;
use async_trait::async_trait;
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use cinder_core::{
    AgentSpec, CinderCoreError, Message, MessageRole, Provider, ProviderRequest, ProviderResponse,
    Tool, ToolCall, ToolContext, ToolExecutionMode, ToolResult, ToolSpec,
};
use cinder_runtime::{AgentRuntime, CreateRun, RunAgentOutcome};
use cinder_store_postgres::PostgresStore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::TcpListener, time::sleep};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://cinder:cinder@127.0.0.1:55432/cinder".to_owned());
    let bind: SocketAddr = std::env::var("BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3000".to_owned())
        .parse()?;

    let store = PostgresStore::connect(&database_url, 8).await?;
    store.init_schema().await?;

    let runtime = AgentRuntime::builder(store)
        .provider(ExampleProvider)
        .tool(SleepTool)
        .build();

    runtime
        .create_agent(AgentSpec {
            id: "example_agent".to_owned(),
            provider: "example".to_owned(),
            model: "example-model".to_owned(),
            description: "Example echo agent.".to_owned(),
            system_prompt: "You are a minimal framework embedding example.".to_owned(),
            tools: vec!["sleep_ms".to_owned()],
            skills: vec![],
        })
        .await?;
    runtime.spawn_worker("axum-example-worker", Duration::from_millis(250), 300);

    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/runs", post(create_run))
        .route("/runs/{run_id}/resume", post(resume_run))
        .with_state(AppState {
            runtime: Arc::new(runtime),
        });

    let listener = TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Clone)]
struct AppState {
    runtime: Arc<AgentRuntime>,
}

#[derive(Debug, Deserialize)]
struct CreateRunRequest {
    input: String,
    #[serde(default = "default_agent_id")]
    agent_id: String,
}

fn default_agent_id() -> String {
    "example_agent".to_owned()
}

#[derive(Debug, Serialize)]
struct RunResponse {
    run_id: Uuid,
    status: String,
    content: Option<String>,
    error: Option<String>,
}

async fn create_run(
    State(state): State<AppState>,
    Json(request): Json<CreateRunRequest>,
) -> Json<RunResponse> {
    let run_id = state
        .runtime
        .create_run(CreateRun {
            agent_id: request.agent_id,
            user_id: None,
            target_id: None,
        })
        .await
        .expect("create run");
    let outcome = state
        .runtime
        .run_agent(run_id, Some(request.input))
        .await
        .expect("run agent");
    Json(run_response(run_id, outcome))
}

async fn resume_run(State(state): State<AppState>, Path(run_id): Path<Uuid>) -> Json<RunResponse> {
    let outcome = state
        .runtime
        .run_agent(run_id, None)
        .await
        .expect("resume run");
    Json(run_response(run_id, outcome))
}

fn run_response(run_id: Uuid, outcome: RunAgentOutcome) -> RunResponse {
    match outcome {
        RunAgentOutcome::Completed { content } => RunResponse {
            run_id,
            status: "completed".to_owned(),
            content: Some(content),
            error: None,
        },
        RunAgentOutcome::SuspendedForTool => RunResponse {
            run_id,
            status: "suspended_for_tool".to_owned(),
            content: None,
            error: None,
        },
        RunAgentOutcome::WaitingForInput => RunResponse {
            run_id,
            status: "waiting_for_input".to_owned(),
            content: None,
            error: None,
        },
        RunAgentOutcome::Busy => RunResponse {
            run_id,
            status: "busy".to_owned(),
            content: None,
            error: None,
        },
        RunAgentOutcome::Failed { error } => RunResponse {
            run_id,
            status: "failed".to_owned(),
            content: None,
            error: Some(error),
        },
    }
}

struct ExampleProvider;

#[async_trait]
impl Provider for ExampleProvider {
    fn name(&self) -> &str {
        "example"
    }

    async fn chat(&self, request: ProviderRequest) -> Result<ProviderResponse, CinderCoreError> {
        if let Some(tool_message) = request
            .messages
            .last()
            .filter(|message| message.role == MessageRole::Tool)
        {
            return Ok(ProviderResponse {
                message: Message::assistant(
                    format!("tool completed: {}", tool_message.content),
                    vec![],
                ),
            });
        }

        let user = request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == MessageRole::User)
            .map(|message| message.content.as_str())
            .unwrap_or_default();

        if user.contains("sleep") {
            return Ok(ProviderResponse {
                message: Message::assistant(
                    "",
                    vec![ToolCall {
                        id: format!("call-{}", Uuid::new_v4()),
                        name: "sleep_ms".to_owned(),
                        arguments: json!({ "ms": 500 }),
                    }],
                ),
            });
        }

        Ok(ProviderResponse {
            message: Message::assistant(format!("echo: {user}"), vec![]),
        })
    }
}

struct SleepTool;

#[async_trait]
impl Tool for SleepTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "sleep_ms".to_owned(),
            description: "Example async tool that waits for a requested duration.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "ms": { "type": "integer", "minimum": 0, "maximum": 5000 }
                }
            }),
            execution_mode: ToolExecutionMode::Async,
        }
    }

    async fn execute(
        &self,
        _context: ToolContext,
        arguments: Value,
    ) -> Result<ToolResult, CinderCoreError> {
        let ms = arguments.get("ms").and_then(Value::as_u64).unwrap_or(0);
        sleep(Duration::from_millis(ms.min(5000))).await;
        Ok(ToolResult::text(json!({ "slept_ms": ms }).to_string()))
    }
}
