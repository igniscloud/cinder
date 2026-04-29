use async_trait::async_trait;
use cinder_core::{
    AgentSpec, CinderCoreError, Message, MessageRole, Provider, ProviderRequest, ProviderResponse,
    RunStatus, Skill, SkillSpec, TaskDependency, TaskPlan, TaskSpec, Tool, ToolCall, ToolContext,
    ToolExecutionMode, ToolResult, ToolSpec,
};
use cinder_runtime::{
    AgentRuntime, CreateRun, CreateTaskPlanRun, RunAgentOutcome, RuntimeOptions, SpawnTaskPlanTool,
    SubmitTaskTool, TaskPlanOutcome, TaskPlanRuntime, ToolRetryPolicy,
};
use cinder_store_postgres::PostgresStore;
use cinder_store_sqlite::SqliteStore;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

fn database_url() -> Option<String> {
    std::env::var("CINDER_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
}

fn runtime(store: PostgresStore) -> AgentRuntime {
    AgentRuntime::builder(store)
        .provider(TestProvider)
        .tool(AsyncEchoTool)
        .tool(UserConfigTool)
        .skill(AsyncEchoSkill)
        .build()
}

async fn reset_db(store: &PostgresStore) {
    sqlx::query(
        "TRUNCATE ai_task_plan_task_runs, ai_task_plan_runs, ai_tool_tasks, ai_messages, ai_runs, ai_agents RESTART IDENTITY CASCADE",
    )
        .execute(store.pool())
        .await
        .expect("truncate cinder tables");
}

#[tokio::test]
async fn e2e_sync_and_async_agent_runs_resume_from_postgres() {
    let Some(database_url) = database_url() else {
        eprintln!("skipping e2e test: set CINDER_DATABASE_URL");
        return;
    };

    let store = PostgresStore::connect(&database_url, 5)
        .await
        .expect("connect postgres");
    store.init_schema().await.expect("init schema");
    reset_db(&store).await;

    let runtime = runtime(store.clone());
    runtime
        .create_agent(AgentSpec {
            id: "sync_agent".to_owned(),
            provider: "test".to_owned(),
            model: "test-model".to_owned(),
            description: "".to_owned(),
            system_prompt: "sync test agent".to_owned(),
            tools: vec![],
            skills: vec![],
        })
        .await
        .expect("create sync agent");
    runtime
        .create_agent(AgentSpec {
            id: "async_agent".to_owned(),
            provider: "test".to_owned(),
            model: "test-model".to_owned(),
            description: "".to_owned(),
            system_prompt: "async test agent".to_owned(),
            tools: vec![],
            skills: vec!["async_echo".to_owned()],
        })
        .await
        .expect("create async agent");

    let sync_run = runtime
        .create_run(CreateRun {
            agent_id: "sync_agent".to_owned(),
            user_id: Some("u1".to_owned()),
            target_id: None,
        })
        .await
        .expect("create sync run");

    assert!(store
        .acquire_run_lock(sync_run, "test-owner", 300)
        .await
        .expect("acquire manual lock"));
    assert_eq!(
        runtime
            .run_agent(sync_run, Some("hello".to_owned()))
            .await
            .expect("busy run"),
        RunAgentOutcome::Busy
    );
    store
        .release_run_lock(sync_run, "test-owner")
        .await
        .expect("release manual lock");

    let sync_outcome = runtime
        .run_agent(sync_run, Some("hello".to_owned()))
        .await
        .expect("run sync agent");
    assert_eq!(
        sync_outcome,
        RunAgentOutcome::Completed {
            content: "echo: hello".to_owned()
        }
    );

    let async_run = runtime
        .create_run(CreateRun {
            agent_id: "async_agent".to_owned(),
            user_id: Some("u1".to_owned()),
            target_id: Some("target-1".to_owned()),
        })
        .await
        .expect("create async run");
    let first_outcome = runtime
        .run_agent(async_run, Some("use async tool".to_owned()))
        .await
        .expect("run async first turn");
    assert_eq!(first_outcome, RunAgentOutcome::SuspendedForTool);

    let suspended_run = store
        .get_run(async_run)
        .await
        .expect("load run")
        .expect("run exists");
    assert_eq!(suspended_run.status, RunStatus::SuspendedForTool);

    let messages = store
        .list_messages(async_run)
        .await
        .expect("list checkpoint messages");
    assert!(messages.iter().any(|message| {
        message.role == MessageRole::Assistant && !message.tool_calls.is_empty()
    }));

    assert!(
        runtime
            .work_once("e2e-worker", 300)
            .await
            .expect("work once"),
        "worker should claim one async task"
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let final_outcome = loop {
        let outcome = runtime
            .run_agent(async_run, None)
            .await
            .expect("resume async run");
        if matches!(outcome, RunAgentOutcome::Completed { .. }) {
            break outcome;
        }
        assert!(Instant::now() < deadline, "async run did not complete");
        sleep(Duration::from_millis(50)).await;
    };

    match final_outcome {
        RunAgentOutcome::Completed { content } => assert!(content.starts_with("tool completed:")),
        other => panic!("unexpected final outcome: {other:?}"),
    }

    let messages = store
        .list_messages(async_run)
        .await
        .expect("list final messages");
    assert!(messages
        .iter()
        .any(|message| message.role == MessageRole::Tool));
}

#[tokio::test]
async fn e2e_tool_failures_retry_then_dead_letter() {
    let Some(database_url) = database_url() else {
        eprintln!("skipping e2e test: set CINDER_DATABASE_URL");
        return;
    };

    let store = PostgresStore::connect(&database_url, 5)
        .await
        .expect("connect postgres");
    store.init_schema().await.expect("init schema");
    reset_db(&store).await;

    let runtime = AgentRuntime::builder(store.clone())
        .provider(TestProvider)
        .tool(AlwaysFailTool)
        .options(RuntimeOptions {
            tool_retry: ToolRetryPolicy {
                max_attempts: 2,
                base_delay: Duration::from_secs(1),
                max_delay: Duration::from_secs(1),
            },
            ..RuntimeOptions::default()
        })
        .build();

    runtime
        .create_agent(AgentSpec {
            id: "failing_agent".to_owned(),
            provider: "test".to_owned(),
            model: "test-model".to_owned(),
            description: "".to_owned(),
            system_prompt: "failing test agent".to_owned(),
            tools: vec!["always_fail".to_owned()],
            skills: vec![],
        })
        .await
        .expect("create failing agent");

    let run_id = runtime
        .create_run(CreateRun {
            agent_id: "failing_agent".to_owned(),
            user_id: None,
            target_id: None,
        })
        .await
        .expect("create run");

    assert_eq!(
        runtime
            .run_agent(run_id, Some("please use the tool".to_owned()))
            .await
            .expect("first run"),
        RunAgentOutcome::SuspendedForTool
    );

    assert!(runtime
        .work_once("retry-worker", 300)
        .await
        .expect("first try"));
    assert!(!runtime
        .work_once("retry-worker", 300)
        .await
        .expect("backoff should hide task"));

    sleep(Duration::from_secs(1)).await;
    assert!(runtime
        .work_once("retry-worker", 300)
        .await
        .expect("second try"));

    let run = store
        .get_run(run_id)
        .await
        .expect("load run")
        .expect("run exists");
    assert_eq!(run.status, RunStatus::Failed);
    assert!(run
        .last_error
        .unwrap_or_default()
        .contains("intentional failure"));
}

#[tokio::test]
async fn e2e_task_plan_runs_static_dag_with_dependency_inputs() {
    let Some(database_url) = database_url() else {
        eprintln!("skipping e2e test: set CINDER_DATABASE_URL");
        return;
    };

    let store = PostgresStore::connect(&database_url, 5)
        .await
        .expect("connect postgres");
    store.init_schema().await.expect("init schema");
    reset_db(&store).await;

    let agent_runtime = AgentRuntime::builder(store.clone())
        .provider(TaskPlanProvider)
        .tool(SubmitTaskTool)
        .build();
    for agent_id in ["research_agent", "summary_agent"] {
        agent_runtime
            .create_agent(AgentSpec {
                id: agent_id.to_owned(),
                provider: "taskplan".to_owned(),
                model: "test-model".to_owned(),
                description: "".to_owned(),
                system_prompt: "taskplan test agent".to_owned(),
                tools: vec![SubmitTaskTool::NAME.to_owned()],
                skills: vec![],
            })
            .await
            .expect("create taskplan agent");
    }

    let plan_runtime = TaskPlanRuntime::new(store, agent_runtime);
    let plan_run_id = plan_runtime
        .create_plan_run(CreateTaskPlanRun {
            plan: TaskPlan {
                id: "proof_test".to_owned(),
                tasks: vec![
                    TaskSpec {
                        id: "research".to_owned(),
                        agent_id: "research_agent".to_owned(),
                        prompt: "Return research findings.".to_owned(),
                        input: json!({ "topic": "fermat" }),
                        output_schema: json!({
                            "type": "object",
                            "required": ["findings"],
                            "properties": {
                                "findings": { "type": "array" }
                            }
                        }),
                    },
                    TaskSpec {
                        id: "summary".to_owned(),
                        agent_id: "summary_agent".to_owned(),
                        prompt: "Summarize findings.".to_owned(),
                        input: json!({}),
                        output_schema: json!({
                            "type": "object",
                            "required": ["markdown"],
                            "properties": {
                                "markdown": { "type": "string" }
                            }
                        }),
                    },
                ],
                dependencies: vec![TaskDependency {
                    from_task: "research".to_owned(),
                    to_task: "summary".to_owned(),
                }],
            },
            user_id: Some("u1".to_owned()),
            target_id: None,
            parent_plan_run_id: None,
            parent_task_id: None,
        })
        .await
        .expect("create plan run");

    let outcome = plan_runtime
        .advance_plan(plan_run_id)
        .await
        .expect("advance plan");
    assert_eq!(
        outcome,
        TaskPlanOutcome::Completed {
            result: json!({ "summary": { "markdown": "summary saw 1 findings" } })
        }
    );
}

#[tokio::test]
async fn sqlite_runs_agent_and_task_plan() {
    let store = SqliteStore::connect(":memory:")
        .await
        .expect("connect sqlite");
    store.init_schema().await.expect("init sqlite schema");

    let agent_runtime = AgentRuntime::builder(store.clone())
        .provider(TaskPlanProvider)
        .tool(SubmitTaskTool)
        .build();
    for agent_id in ["research_agent", "summary_agent"] {
        agent_runtime
            .create_agent(AgentSpec {
                id: agent_id.to_owned(),
                provider: "taskplan".to_owned(),
                model: "test-model".to_owned(),
                description: "".to_owned(),
                system_prompt: "taskplan test agent".to_owned(),
                tools: vec![SubmitTaskTool::NAME.to_owned()],
                skills: vec![],
            })
            .await
            .expect("create sqlite taskplan agent");
    }

    let run_id = agent_runtime
        .create_run(CreateRun {
            agent_id: "summary_agent".to_owned(),
            user_id: None,
            target_id: None,
        })
        .await
        .expect("create sqlite run");
    let outcome = agent_runtime
        .run_agent(run_id, Some("Task id: summary Ribet plus Wiles".to_owned()))
        .await
        .expect("run sqlite agent");
    assert_eq!(
        outcome,
        RunAgentOutcome::Completed {
            content: json!({ "markdown": "summary saw 1 findings" }).to_string()
        }
    );

    let plan_runtime = TaskPlanRuntime::new(store, agent_runtime);
    let plan_run_id = plan_runtime
        .create_plan_run(CreateTaskPlanRun {
            plan: TaskPlan {
                id: "sqlite_proof_test".to_owned(),
                tasks: vec![
                    TaskSpec {
                        id: "research".to_owned(),
                        agent_id: "research_agent".to_owned(),
                        prompt: "Return research findings.".to_owned(),
                        input: json!({ "topic": "fermat" }),
                        output_schema: json!({
                            "type": "object",
                            "required": ["findings"],
                            "properties": { "findings": { "type": "array" } }
                        }),
                    },
                    TaskSpec {
                        id: "summary".to_owned(),
                        agent_id: "summary_agent".to_owned(),
                        prompt: "Summarize findings.".to_owned(),
                        input: json!({}),
                        output_schema: json!({
                            "type": "object",
                            "required": ["markdown"],
                            "properties": { "markdown": { "type": "string" } }
                        }),
                    },
                ],
                dependencies: vec![TaskDependency {
                    from_task: "research".to_owned(),
                    to_task: "summary".to_owned(),
                }],
            },
            user_id: None,
            target_id: None,
            parent_plan_run_id: None,
            parent_task_id: None,
        })
        .await
        .expect("create sqlite plan run");

    let outcome = plan_runtime
        .advance_plan(plan_run_id)
        .await
        .expect("advance sqlite plan");
    assert_eq!(
        outcome,
        TaskPlanOutcome::Completed {
            result: json!({ "summary": { "markdown": "summary saw 1 findings" } })
        }
    );
}

#[tokio::test]
async fn sqlite_task_plan_can_spawn_child_plan_and_submit_task() {
    let store = SqliteStore::connect(":memory:")
        .await
        .expect("connect sqlite");
    store.init_schema().await.expect("init sqlite schema");

    let agent_runtime = AgentRuntime::builder(store.clone())
        .provider(RecursiveTaskPlanProvider)
        .tool(SpawnTaskPlanTool)
        .tool(SubmitTaskTool)
        .build();
    agent_runtime
        .create_agent(AgentSpec {
            id: "parent_agent".to_owned(),
            provider: "recursive_taskplan".to_owned(),
            model: "test-model".to_owned(),
            description: "Parent test agent.".to_owned(),
            system_prompt: "parent".to_owned(),
            tools: vec![
                SpawnTaskPlanTool::NAME.to_owned(),
                SubmitTaskTool::NAME.to_owned(),
            ],
            skills: vec![],
        })
        .await
        .expect("create parent agent");
    agent_runtime
        .create_agent(AgentSpec {
            id: "child_agent".to_owned(),
            provider: "recursive_taskplan".to_owned(),
            model: "test-model".to_owned(),
            description: "Child test agent.".to_owned(),
            system_prompt: "child".to_owned(),
            tools: vec![SubmitTaskTool::NAME.to_owned()],
            skills: vec![],
        })
        .await
        .expect("create child agent");

    let plan_runtime = TaskPlanRuntime::new(store, agent_runtime);
    let plan_run_id = plan_runtime
        .create_plan_run(CreateTaskPlanRun {
            plan: TaskPlan {
                id: "recursive_root".to_owned(),
                tasks: vec![TaskSpec {
                    id: "root".to_owned(),
                    agent_id: "parent_agent".to_owned(),
                    prompt: "Spawn child work and submit final result.".to_owned(),
                    input: json!({}),
                    output_schema: json!({
                        "type": "object",
                        "required": ["markdown"],
                        "properties": { "markdown": { "type": "string" } }
                    }),
                }],
                dependencies: vec![],
            },
            user_id: None,
            target_id: None,
            parent_plan_run_id: None,
            parent_task_id: None,
        })
        .await
        .expect("create recursive plan");

    let outcome = plan_runtime
        .advance_plan(plan_run_id)
        .await
        .expect("advance recursive plan");
    assert_eq!(
        outcome,
        TaskPlanOutcome::Completed {
            result: json!({ "root": { "markdown": "parent received child result" } })
        }
    );
}

#[tokio::test]
async fn sqlite_main_agent_can_spawn_task_plan_and_resume() {
    let store = SqliteStore::connect(":memory:")
        .await
        .expect("connect sqlite");
    store.init_schema().await.expect("init sqlite schema");

    let agent_runtime = AgentRuntime::builder(store.clone())
        .provider(RecursiveTaskPlanProvider)
        .tool(SpawnTaskPlanTool)
        .tool(SubmitTaskTool)
        .build();
    agent_runtime
        .create_agent(AgentSpec {
            id: "main_agent".to_owned(),
            provider: "recursive_taskplan".to_owned(),
            model: "test-model".to_owned(),
            description: "Main test agent.".to_owned(),
            system_prompt: "main".to_owned(),
            tools: vec![SpawnTaskPlanTool::NAME.to_owned()],
            skills: vec![],
        })
        .await
        .expect("create main agent");
    agent_runtime
        .create_agent(AgentSpec {
            id: "child_agent".to_owned(),
            provider: "recursive_taskplan".to_owned(),
            model: "test-model".to_owned(),
            description: "Child test agent.".to_owned(),
            system_prompt: "child".to_owned(),
            tools: vec![SubmitTaskTool::NAME.to_owned()],
            skills: vec![],
        })
        .await
        .expect("create child agent");

    let plan_runtime = TaskPlanRuntime::new(store, agent_runtime.clone());
    let run_id = agent_runtime
        .create_run(CreateRun {
            agent_id: "main_agent".to_owned(),
            user_id: None,
            target_id: None,
        })
        .await
        .expect("create main run");

    let outcome = agent_runtime
        .run_agent(run_id, Some("Root solver request".to_owned()))
        .await
        .expect("run main agent");
    assert_eq!(outcome, RunAgentOutcome::WaitingForInput);

    assert!(plan_runtime
        .advance_parent_run_child_plans(run_id)
        .await
        .expect("advance child plan"));

    let outcome = agent_runtime
        .run_agent(run_id, None)
        .await
        .expect("resume main agent");
    assert_eq!(
        outcome,
        RunAgentOutcome::Completed {
            content: "main received child result".to_owned()
        }
    );
}

#[derive(Default)]
struct TestProvider;

#[async_trait]
impl Provider for TestProvider {
    fn name(&self) -> &str {
        "test"
    }

    async fn chat(&self, request: ProviderRequest) -> Result<ProviderResponse, CinderCoreError> {
        let last = request.messages.last();
        if let Some(tool_message) = last.filter(|message| message.role == MessageRole::Tool) {
            return Ok(ProviderResponse {
                message: Message::assistant(final_from_tool_result(&tool_message.content), vec![]),
            });
        }

        let user = request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == MessageRole::User)
            .map(|message| message.content.as_str())
            .unwrap_or_default();

        if request.tools.iter().any(|tool| tool.name == "always_fail") {
            return Ok(ProviderResponse {
                message: Message::assistant(
                    "",
                    vec![ToolCall {
                        id: format!("call-{}", Uuid::new_v4()),
                        name: "always_fail".to_owned(),
                        arguments: json!({}),
                    }],
                ),
            });
        }

        if request.tools.iter().any(|tool| tool.name == "async_echo") {
            return Ok(ProviderResponse {
                message: Message::assistant(
                    "",
                    vec![ToolCall {
                        id: format!("call-{}", Uuid::new_v4()),
                        name: "async_echo".to_owned(),
                        arguments: json!({ "input": user }),
                    }],
                ),
            });
        }

        Ok(ProviderResponse {
            message: Message::assistant(format!("echo: {user}"), vec![]),
        })
    }
}

fn final_from_tool_result(content: &str) -> String {
    format!("tool completed: {content}")
}

#[derive(Default)]
struct AsyncEchoTool;

#[async_trait]
impl Tool for AsyncEchoTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "async_echo".to_owned(),
            description: "Async echo tool for framework tests.".to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["input"],
                "properties": {
                    "input": { "type": "string" }
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
        let input = arguments
            .get("input")
            .and_then(Value::as_str)
            .unwrap_or_default();
        Ok(ToolResult::text(json!({ "echo": input }).to_string()))
    }
}

#[derive(Default)]
struct UserConfigTool;

#[async_trait]
impl Tool for UserConfigTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "get_user_config".to_owned(),
            description: "Inline tool for framework tests.".to_owned(),
            input_schema: json!({"type": "object"}),
            execution_mode: ToolExecutionMode::Inline,
        }
    }

    async fn execute(
        &self,
        _context: ToolContext,
        _arguments: Value,
    ) -> Result<ToolResult, CinderCoreError> {
        Ok(ToolResult::text(json!({"locale": "test"}).to_string()))
    }
}

#[derive(Default)]
struct AlwaysFailTool;

#[async_trait]
impl Tool for AlwaysFailTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "always_fail".to_owned(),
            description: "Always fail for retry tests.".to_owned(),
            input_schema: json!({"type": "object"}),
            execution_mode: ToolExecutionMode::Async,
        }
    }

    async fn execute(
        &self,
        _context: ToolContext,
        _arguments: Value,
    ) -> Result<ToolResult, CinderCoreError> {
        Err(CinderCoreError::Tool("intentional failure".to_owned()))
    }
}

struct AsyncEchoSkill;

impl Skill for AsyncEchoSkill {
    fn spec(&self) -> SkillSpec {
        SkillSpec {
            name: "async_echo".to_owned(),
            description: "Adds async echo behavior for framework tests.".to_owned(),
            system_prompt: Some("Use async_echo when the input asks for async work.".to_owned()),
            tools: vec!["async_echo".to_owned()],
        }
    }
}

#[derive(Default)]
struct TaskPlanProvider;

#[async_trait]
impl Provider for TaskPlanProvider {
    fn name(&self) -> &str {
        "taskplan"
    }

    async fn chat(&self, request: ProviderRequest) -> Result<ProviderResponse, CinderCoreError> {
        let user = request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == MessageRole::User)
            .map(|message| message.content.as_str())
            .unwrap_or_default();

        let can_submit = request
            .tools
            .iter()
            .any(|tool| tool.name == SubmitTaskTool::NAME);
        if can_submit && user.contains("A Cinder TaskPlan task is ready") {
            let result = if user.contains("Task id: research") {
                json!({ "findings": ["Ribet plus Wiles"] })
            } else if user.contains("Task id: summary") {
                json!({ "markdown": "summary saw 1 findings" })
            } else {
                json!({ "markdown": "unknown task" })
            };
            return Ok(ProviderResponse {
                message: Message::assistant(
                    "",
                    vec![ToolCall {
                        id: format!("call-{}", Uuid::new_v4()),
                        name: SubmitTaskTool::NAME.to_owned(),
                        arguments: json!({
                            "status": "succeeded",
                            "result": result
                        }),
                    }],
                ),
            });
        }

        let content = if user.contains("Task id: research") {
            json!({ "findings": ["Ribet plus Wiles"] }).to_string()
        } else if user.contains("Task id: summary") {
            let count = user.matches("Ribet plus Wiles").count();
            json!({ "markdown": format!("summary saw {count} findings") }).to_string()
        } else {
            json!({ "markdown": "unknown task" }).to_string()
        };

        Ok(ProviderResponse {
            message: Message::assistant(content, vec![]),
        })
    }
}

#[derive(Default)]
struct RecursiveTaskPlanProvider;

#[async_trait]
impl Provider for RecursiveTaskPlanProvider {
    fn name(&self) -> &str {
        "recursive_taskplan"
    }

    async fn chat(&self, request: ProviderRequest) -> Result<ProviderResponse, CinderCoreError> {
        let last = request.messages.last();
        let user = request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == MessageRole::User)
            .map(|message| message.content.as_str())
            .unwrap_or_default();

        if user.contains("Root solver request") {
            if matches!(last.map(|message| message.role), Some(MessageRole::Tool)) {
                return Ok(ProviderResponse {
                    message: Message::assistant("main received child result", vec![]),
                });
            }

            return Ok(ProviderResponse {
                message: Message::assistant(
                    "",
                    vec![ToolCall {
                        id: format!("call-{}", Uuid::new_v4()),
                        name: SpawnTaskPlanTool::NAME.to_owned(),
                        arguments: json!({
                            "task_plan": {
                                "id": "main_child_plan",
                                "tasks": [{
                                    "id": "child",
                                    "agent_id": "child_agent",
                                    "prompt": "Submit child result.",
                                    "input": {},
                                    "output_schema": {
                                        "type": "object",
                                        "required": ["markdown"],
                                        "properties": {
                                            "markdown": { "type": "string" }
                                        }
                                    }
                                }],
                                "dependencies": []
                            }
                        }),
                    }],
                ),
            });
        }

        if user.contains("Task id: root") {
            if matches!(last.map(|message| message.role), Some(MessageRole::Tool)) {
                return Ok(ProviderResponse {
                    message: Message::assistant(
                        "",
                        vec![ToolCall {
                            id: format!("call-{}", Uuid::new_v4()),
                            name: SubmitTaskTool::NAME.to_owned(),
                            arguments: json!({
                                "status": "succeeded",
                                "result": { "markdown": "parent received child result" }
                            }),
                        }],
                    ),
                });
            }

            return Ok(ProviderResponse {
                message: Message::assistant(
                    "",
                    vec![ToolCall {
                        id: format!("call-{}", Uuid::new_v4()),
                        name: SpawnTaskPlanTool::NAME.to_owned(),
                        arguments: json!({
                            "task_plan": {
                                "id": "recursive_child",
                                "tasks": [{
                                    "id": "child",
                                    "agent_id": "child_agent",
                                    "prompt": "Submit child result.",
                                    "input": {},
                                    "output_schema": {
                                        "type": "object",
                                        "required": ["markdown"],
                                        "properties": {
                                            "markdown": { "type": "string" }
                                        }
                                    }
                                }],
                                "dependencies": []
                            }
                        }),
                    }],
                ),
            });
        }

        if user.contains("Task id: child") {
            return Ok(ProviderResponse {
                message: Message::assistant(
                    "",
                    vec![ToolCall {
                        id: format!("call-{}", Uuid::new_v4()),
                        name: SubmitTaskTool::NAME.to_owned(),
                        arguments: json!({
                            "status": "succeeded",
                            "result": { "markdown": "child result" }
                        }),
                    }],
                ),
            });
        }

        Ok(ProviderResponse {
            message: Message::assistant(json!({ "markdown": "unknown" }).to_string(), vec![]),
        })
    }
}
