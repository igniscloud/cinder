use anyhow::{anyhow, Context, Result};
use cinder_core::{
    AgentSpec, CinderConfig, CinderStore, Message, MessageRole, PlanRunStatus, Provider,
    ProviderRequest, RunStatus, Skill, SkillSpec, TaskPlan, Tool, ToolContext, ToolExecutionMode,
    ToolSpec,
};
use cinder_store_postgres::PostgresStore;
use cinder_store_sqlite::SqliteStore;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error};
use uuid::Uuid;

const CINDER_AGENT_SYSTEM_PROMPT: &str = include_str!("../system_prompts/cinder_agent.md");

pub mod config;
pub mod taskplan;
pub mod tools;
pub use config::OpenAiCompatibleProvider;
pub use taskplan::{CreateTaskPlanRun, TaskPlanOutcome, TaskPlanRuntime, TaskPlanRuntimeOptions};
pub use tools::{GetTaskTool, ListAgentsTool, SpawnTaskPlanTool, SubmitTaskTool};

#[derive(Clone)]
pub enum Cinder {
    Sqlite(CinderInner<SqliteStore>),
    Postgres(CinderInner<PostgresStore>),
}

#[derive(Clone)]
pub struct CinderInner<S>
where
    S: CinderStore + Clone + 'static,
{
    store: S,
    task_plan_runtime: TaskPlanRuntime,
    worker_poll_interval: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskPlanRunView {
    pub id: Uuid,
    pub status: PlanRunStatus,
    pub result: Option<Value>,
    pub last_error: Option<String>,
}

impl Cinder {
    pub async fn from_config_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let config = CinderConfig::load(path).map_err(|error| anyhow!(error))?;
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
        Self::from_config(config, base_dir).await
    }

    pub async fn from_config(config: CinderConfig, base_dir: impl AsRef<Path>) -> Result<Self> {
        match config.store.kind {
            cinder_core::StoreKind::Sqlite => {
                let sqlite = config
                    .store
                    .sqlite
                    .as_ref()
                    .ok_or_else(|| anyhow!("store.sqlite is required"))?;
                let store = SqliteStore::connect_from_config(sqlite).await?;
                Ok(Self::Sqlite(
                    CinderInner::from_store(store, &config, base_dir.as_ref()).await?,
                ))
            }
            cinder_core::StoreKind::Postgres => {
                let postgres = config
                    .store
                    .postgres
                    .as_ref()
                    .ok_or_else(|| anyhow!("store.postgres is required"))?;
                let store = PostgresStore::connect_from_config(postgres).await?;
                Ok(Self::Postgres(
                    CinderInner::from_store(store, &config, base_dir.as_ref()).await?,
                ))
            }
        }
    }

    pub async fn submit_task_plan(&self, plan: TaskPlan) -> Result<Uuid> {
        match self {
            Self::Sqlite(inner) => inner.submit_task_plan(plan).await,
            Self::Postgres(inner) => inner.submit_task_plan(plan).await,
        }
    }

    pub async fn get_task_plan(&self, plan_run_id: Uuid) -> Result<Option<TaskPlanRunView>> {
        match self {
            Self::Sqlite(inner) => inner.get_task_plan(plan_run_id).await,
            Self::Postgres(inner) => inner.get_task_plan(plan_run_id).await,
        }
    }

    pub async fn task_plan_result(&self, plan_run_id: Uuid) -> Result<Option<Value>> {
        Ok(self
            .get_task_plan(plan_run_id)
            .await?
            .and_then(|run| (run.status == PlanRunStatus::Completed).then_some(run.result))
            .flatten())
    }

    pub async fn wait_task_plan_result(
        &self,
        plan_run_id: Uuid,
        timeout: Duration,
    ) -> Result<Value> {
        match self {
            Self::Sqlite(inner) => inner.wait_task_plan_result(plan_run_id, timeout).await,
            Self::Postgres(inner) => inner.wait_task_plan_result(plan_run_id, timeout).await,
        }
    }
}

impl<S> CinderInner<S>
where
    S: CinderStore + Clone + 'static,
{
    async fn from_store(store: S, config: &CinderConfig, base_dir: &Path) -> Result<Self> {
        store.init_schema().await.map_err(|error| anyhow!(error))?;
        let agent_runtime = AgentRuntime::from_config(store.clone(), config, base_dir).await?;
        Ok(Self {
            store: store.clone(),
            task_plan_runtime: TaskPlanRuntime::new(store, agent_runtime),
            worker_poll_interval: Duration::from_secs(1),
        })
    }

    async fn submit_task_plan(&self, plan: TaskPlan) -> Result<Uuid> {
        let plan_run_id = self
            .task_plan_runtime
            .create_plan_run(CreateTaskPlanRun {
                plan,
                user_id: None,
                target_id: None,
                parent_plan_run_id: None,
                parent_task_id: None,
            })
            .await?;
        self.spawn_task_plan_worker(plan_run_id);
        Ok(plan_run_id)
    }

    async fn get_task_plan(&self, plan_run_id: Uuid) -> Result<Option<TaskPlanRunView>> {
        Ok(self
            .store
            .get_task_plan_run(plan_run_id)
            .await?
            .map(|run| TaskPlanRunView {
                id: run.id,
                status: run.status,
                result: run.result,
                last_error: run.last_error,
            }))
    }

    async fn wait_task_plan_result(&self, plan_run_id: Uuid, timeout: Duration) -> Result<Value> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let run = self
                .get_task_plan(plan_run_id)
                .await?
                .ok_or_else(|| anyhow!("task plan run not found: {plan_run_id}"))?;
            match run.status {
                PlanRunStatus::Completed => {
                    return run.result.ok_or_else(|| {
                        anyhow!("task plan `{plan_run_id}` completed without result")
                    });
                }
                PlanRunStatus::Failed | PlanRunStatus::Cancelled => {
                    return Err(anyhow!(
                        "task plan `{plan_run_id}` ended with status {}: {}",
                        run.status,
                        run.last_error
                            .unwrap_or_else(|| "no error detail".to_owned())
                    ));
                }
                PlanRunStatus::Running => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(anyhow!(
                            "task plan `{plan_run_id}` did not finish before timeout"
                        ));
                    }
                    sleep(self.worker_poll_interval).await;
                }
            }
        }
    }

    fn spawn_task_plan_worker(&self, plan_run_id: Uuid) {
        let runtime = self.task_plan_runtime.clone();
        let store = self.store.clone();
        let poll_interval = self.worker_poll_interval;
        tokio::spawn(async move {
            loop {
                match runtime.advance_plan(plan_run_id).await {
                    Ok(TaskPlanOutcome::Completed { .. } | TaskPlanOutcome::Failed { .. }) => {
                        break;
                    }
                    Ok(TaskPlanOutcome::Running | TaskPlanOutcome::Busy) => {
                        sleep(poll_interval).await;
                    }
                    Err(error) => {
                        error!(plan_run_id = %plan_run_id, error = %error, "task plan worker failed");
                        if let Err(fail_error) = store
                            .fail_task_plan_run(plan_run_id, &error.to_string())
                            .await
                        {
                            error!(plan_run_id = %plan_run_id, error = %fail_error, "failed to mark task plan failed");
                        }
                        break;
                    }
                }
            }
        });
    }
}

#[derive(Clone)]
pub struct AgentRuntime {
    store: Arc<dyn CinderStore>,
    providers: Arc<HashMap<String, Arc<dyn Provider>>>,
    tools: Arc<HashMap<String, Arc<dyn Tool>>>,
    skills: Arc<HashMap<String, Arc<dyn Skill>>>,
    options: Arc<RuntimeOptions>,
}

#[derive(Debug, Clone)]
pub struct RuntimeOptions {
    pub run_lease_seconds: i64,
    pub max_internal_turns: usize,
    pub tool_retry: ToolRetryPolicy,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            run_lease_seconds: 300,
            max_internal_turns: 12,
            tool_retry: ToolRetryPolicy::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ToolRetryPolicy {
    pub max_attempts: i32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for ToolRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateRun {
    pub agent_id: String,
    pub user_id: Option<String>,
    pub target_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunAgentOutcome {
    Completed { content: String },
    SuspendedForTool,
    WaitingForInput,
    Busy,
    Failed { error: String },
}

pub struct AgentRuntimeBuilder {
    store: Arc<dyn CinderStore>,
    providers: HashMap<String, Arc<dyn Provider>>,
    tools: HashMap<String, Arc<dyn Tool>>,
    skills: HashMap<String, Arc<dyn Skill>>,
    options: RuntimeOptions,
}

impl AgentRuntimeBuilder {
    pub fn new(store: impl CinderStore + 'static) -> Self {
        Self {
            store: Arc::new(store),
            providers: HashMap::new(),
            tools: HashMap::new(),
            skills: HashMap::new(),
            options: RuntimeOptions::default(),
        }
    }

    pub fn provider(mut self, provider: impl Provider + 'static) -> Self {
        self.providers
            .insert(provider.name().to_owned(), Arc::new(provider));
        self
    }

    pub fn tool(mut self, tool: impl Tool + 'static) -> Self {
        self.tools.insert(tool.spec().name, Arc::new(tool));
        self
    }

    pub fn skill(mut self, skill: impl Skill + 'static) -> Self {
        self.skills.insert(skill.spec().name, Arc::new(skill));
        self
    }

    pub fn options(mut self, options: RuntimeOptions) -> Self {
        self.options = options;
        self
    }

    pub fn build(self) -> AgentRuntime {
        AgentRuntime {
            store: self.store,
            providers: Arc::new(self.providers),
            tools: Arc::new(self.tools),
            skills: Arc::new(self.skills),
            options: Arc::new(self.options),
        }
    }
}

impl AgentRuntime {
    pub fn builder(store: impl CinderStore + 'static) -> AgentRuntimeBuilder {
        AgentRuntimeBuilder::new(store)
    }

    pub async fn from_config(
        store: impl CinderStore + 'static,
        config: &cinder_core::CinderConfig,
        base_dir: impl AsRef<std::path::Path>,
    ) -> Result<Self> {
        config.validate().map_err(|err| anyhow!(err))?;
        let mut builder = Self::builder(store)
            .options(RuntimeOptions {
                run_lease_seconds: config.runtime.run_lease_seconds,
                max_internal_turns: config.runtime.max_internal_turns,
                ..RuntimeOptions::default()
            })
            .tool(ListAgentsTool)
            .tool(GetTaskTool);
        builder = builder.tool(SpawnTaskPlanTool).tool(SubmitTaskTool);

        for (provider_id, provider_config) in &config.providers {
            match provider_config {
                cinder_core::ProviderConfig::OpenAiCompatible(provider_config) => {
                    builder = builder.provider(OpenAiCompatibleProvider::from_config(
                        provider_id.clone(),
                        provider_config.clone(),
                        config,
                    ));
                }
            }
        }

        let runtime = builder.build();
        for agent in config.agent_specs(base_dir).map_err(|err| anyhow!(err))? {
            runtime.create_agent(agent).await?;
        }
        Ok(runtime)
    }

    pub async fn create_agent(&self, spec: AgentSpec) -> Result<()> {
        for tool_name in &spec.tools {
            if !self.tools.contains_key(tool_name) {
                return Err(anyhow!(
                    "agent {} references unknown tool {}",
                    spec.id,
                    tool_name
                ));
            }
        }
        if !self.providers.contains_key(&spec.provider) {
            return Err(anyhow!(
                "agent {} references unknown provider {}",
                spec.id,
                spec.provider
            ));
        }
        for skill_name in &spec.skills {
            let skill = self.skills.get(skill_name).ok_or_else(|| {
                anyhow!("agent {} references unknown skill {}", spec.id, skill_name)
            })?;
            for tool_name in &skill.spec().tools {
                if !self.tools.contains_key(tool_name) {
                    return Err(anyhow!(
                        "skill {} references unknown tool {}",
                        skill_name,
                        tool_name
                    ));
                }
            }
        }
        self.store.upsert_agent(&spec).await?;
        Ok(())
    }

    pub async fn create_run(&self, input: CreateRun) -> Result<Uuid> {
        let run = self
            .store
            .create_run(
                &input.agent_id,
                input.user_id.as_deref(),
                input.target_id.as_deref(),
            )
            .await?;
        Ok(run.id)
    }

    pub async fn run_agent(
        &self,
        run_id: Uuid,
        input_message: Option<String>,
    ) -> Result<RunAgentOutcome> {
        let lock_owner = format!("run-agent-{}", Uuid::new_v4());
        let acquired = self
            .store
            .acquire_run_lock(run_id, &lock_owner, self.options.run_lease_seconds)
            .await?;
        if !acquired {
            return Ok(RunAgentOutcome::Busy);
        }

        let result = self.run_agent_locked(run_id, input_message).await;
        let release_result = self.store.release_run_lock(run_id, &lock_owner).await;
        match (result, release_result) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(err), Ok(())) => Err(err),
            (Ok(_), Err(err)) => Err(err.into()),
            (Err(err), Err(release_err)) => {
                Err(err).context(format!("also failed to release run lock: {release_err}"))
            }
        }
    }

    async fn run_agent_locked(
        &self,
        run_id: Uuid,
        input_message: Option<String>,
    ) -> Result<RunAgentOutcome> {
        if let Some(input) = input_message {
            self.store
                .append_message(run_id, &Message::user(input))
                .await
                .context("append user message")?;
        }

        let mut turns = 0usize;
        loop {
            turns += 1;
            if turns > self.options.max_internal_turns {
                let error = "agent exceeded max internal turns".to_owned();
                self.store
                    .update_run_status(run_id, RunStatus::Failed, Some(&error))
                    .await?;
                return Ok(RunAgentOutcome::Failed { error });
            }

            let run = self
                .store
                .get_run(run_id)
                .await?
                .ok_or_else(|| anyhow!("run not found: {run_id}"))?;

            match run.status {
                RunStatus::Completed => {
                    let messages = self.store.list_messages(run_id).await?;
                    let content = messages
                        .iter()
                        .rev()
                        .find(|message| message.role == MessageRole::Assistant)
                        .map(|message| message.content.clone())
                        .unwrap_or_default();
                    return Ok(RunAgentOutcome::Completed { content });
                }
                RunStatus::SuspendedForTool => return Ok(RunAgentOutcome::SuspendedForTool),
                RunStatus::WaitingForInput => return Ok(RunAgentOutcome::WaitingForInput),
                RunStatus::Failed => {
                    return Ok(RunAgentOutcome::Failed {
                        error: run.last_error.unwrap_or_else(|| "run failed".to_owned()),
                    });
                }
                RunStatus::Running => {}
            }

            let agent = self
                .store
                .get_agent(&run.agent_id)
                .await?
                .ok_or_else(|| anyhow!("agent not found: {}", run.agent_id))?;
            let provider = self
                .providers
                .get(&agent.provider)
                .ok_or_else(|| anyhow!("provider not registered: {}", agent.provider))?;

            let mut messages = vec![Message::system(self.system_prompt_for_agent(&agent)?)];
            messages.extend(
                self.store
                    .list_messages(run_id)
                    .await?
                    .into_iter()
                    .map(Message::from),
            );

            let tool_specs = self.tool_specs_for_agent(&agent)?;
            let response = provider
                .chat(ProviderRequest {
                    model: agent.model.clone(),
                    messages,
                    tools: tool_specs,
                })
                .await
                .map_err(|err| anyhow!(err))?;

            let assistant_message = response.message;
            let tool_calls = assistant_message.tool_calls.clone();
            self.store
                .append_message(run_id, &assistant_message)
                .await
                .context("append assistant message")?;

            if tool_calls.is_empty() {
                self.store
                    .update_run_status(run_id, RunStatus::Completed, None)
                    .await?;
                return Ok(RunAgentOutcome::Completed {
                    content: assistant_message.content,
                });
            }

            let mut has_async_tool = false;
            for tool_call in tool_calls {
                let tool = self
                    .tools
                    .get(&tool_call.name)
                    .ok_or_else(|| anyhow!("tool not registered: {}", tool_call.name))?;
                match tool.spec().execution_mode {
                    ToolExecutionMode::Inline => {
                        let result = tool
                            .execute(
                                self.tool_context(run_id, Some(&tool_call.id)).await?,
                                tool_call.arguments.clone(),
                            )
                            .await
                            .map_err(|err| anyhow!(err))?;
                        if result.append_message {
                            self.store
                                .append_message(
                                    run_id,
                                    &Message::tool(tool_call.id.clone(), result.content),
                                )
                                .await?;
                        }
                        if result.suspend_run {
                            return Ok(RunAgentOutcome::WaitingForInput);
                        }
                    }
                    ToolExecutionMode::Async => {
                        has_async_tool = true;
                        self.store.enqueue_tool_task(run_id, &tool_call).await?;
                    }
                }
            }

            if has_async_tool {
                self.store
                    .update_run_status(run_id, RunStatus::SuspendedForTool, None)
                    .await?;
                return Ok(RunAgentOutcome::SuspendedForTool);
            }
        }
    }

    pub async fn work_once(&self, worker_id: &str, lease_seconds: i64) -> Result<bool> {
        let Some(task) = self
            .store
            .claim_tool_task(worker_id, lease_seconds)
            .await
            .context("claim tool task")?
        else {
            return Ok(false);
        };

        debug!(task_id = %task.id, run_id = %task.run_id, tool = %task.tool_call.name, "claimed tool task");
        let tool = match self.tools.get(&task.tool_call.name) {
            Some(tool) => tool,
            None => {
                let error = format!("tool not registered: {}", task.tool_call.name);
                self.store
                    .dead_letter_tool_task(task.id, task.run_id, &error)
                    .await?;
                return Ok(true);
            }
        };

        match tool
            .execute(
                self.tool_context(task.run_id, Some(&task.tool_call.id))
                    .await?,
                task.tool_call.arguments.clone(),
            )
            .await
        {
            Ok(result) => {
                let should_wake = self
                    .store
                    .complete_tool_task(task.id, task.run_id, &task.tool_call.id, &result.content)
                    .await?;
                if should_wake {
                    let runtime = self.clone();
                    tokio::spawn(async move {
                        if let Err(err) = runtime.run_agent(task.run_id, None).await {
                            error!(run_id = %task.run_id, error = %err, "failed to wake agent");
                        }
                    });
                }
            }
            Err(err) => {
                self.handle_tool_failure(task.id, task.run_id, task.attempts, &err.to_string())
                    .await?;
            }
        }
        Ok(true)
    }

    pub fn spawn_worker(
        &self,
        worker_id: impl Into<String>,
        poll_interval: Duration,
        lease_seconds: i64,
    ) -> tokio::task::JoinHandle<()> {
        let runtime = self.clone();
        let worker_id = worker_id.into();
        tokio::spawn(async move {
            loop {
                match runtime.work_once(&worker_id, lease_seconds).await {
                    Ok(true) => {}
                    Ok(false) => sleep(poll_interval).await,
                    Err(err) => {
                        error!(worker_id = %worker_id, error = %err, "tool worker failed");
                        sleep(poll_interval).await;
                    }
                }
            }
        })
    }

    fn tool_specs_for_agent(&self, agent: &AgentSpec) -> Result<Vec<ToolSpec>> {
        self.tool_names_for_agent(agent)?
            .into_iter()
            .map(|name| {
                self.tools
                    .get(&name)
                    .map(|tool| tool.spec())
                    .ok_or_else(|| anyhow!("tool not registered: {name}"))
            })
            .collect()
    }

    async fn tool_context(&self, run_id: Uuid, tool_call_id: Option<&str>) -> Result<ToolContext> {
        Ok(ToolContext {
            agents: self.store.list_agents().await?,
            store: Some(self.store.clone()),
            run_id: Some(run_id),
            tool_call_id: tool_call_id.map(str::to_owned),
        })
    }

    fn system_prompt_for_agent(&self, agent: &AgentSpec) -> Result<String> {
        let mut parts = vec![
            CINDER_AGENT_SYSTEM_PROMPT.trim().to_owned(),
            agent.system_prompt.clone(),
        ];
        for skill in self.skill_specs_for_agent(agent)? {
            if let Some(prompt) = skill.system_prompt {
                parts.push(prompt);
            }
        }
        Ok(parts.join("\n\n"))
    }

    fn tool_names_for_agent(&self, agent: &AgentSpec) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for name in &agent.tools {
            push_unique(&mut names, name.clone());
        }
        for skill in self.skill_specs_for_agent(agent)? {
            for name in skill.tools {
                push_unique(&mut names, name);
            }
        }
        Ok(names)
    }

    fn skill_specs_for_agent(&self, agent: &AgentSpec) -> Result<Vec<SkillSpec>> {
        agent
            .skills
            .iter()
            .map(|name| {
                self.skills
                    .get(name)
                    .map(|skill| skill.spec())
                    .ok_or_else(|| anyhow!("skill not registered: {name}"))
            })
            .collect()
    }

    async fn handle_tool_failure(
        &self,
        task_id: Uuid,
        run_id: Uuid,
        attempts: i32,
        error: &str,
    ) -> Result<()> {
        if attempts >= self.options.tool_retry.max_attempts {
            self.store
                .dead_letter_tool_task(task_id, run_id, error)
                .await?;
            return Ok(());
        }

        let delay = retry_delay_seconds(attempts, &self.options.tool_retry);
        self.store.retry_tool_task(task_id, delay, error).await?;
        Ok(())
    }
}

fn push_unique(names: &mut Vec<String>, name: String) {
    if !names.iter().any(|existing| existing == &name) {
        names.push(name);
    }
}

fn retry_delay_seconds(attempts: i32, policy: &ToolRetryPolicy) -> i64 {
    let exponent = attempts.saturating_sub(1).min(30) as u32;
    let multiplier = 2_u64.saturating_pow(exponent);
    let delay = policy.base_delay.saturating_mul(multiplier as u32);
    delay.min(policy.max_delay).as_secs().max(1) as i64
}
