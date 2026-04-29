use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

pub mod config;
pub mod store;
pub mod taskplan;
pub use config::{
    AgentConfig, CinderConfig, ModelConfig, OpenAiCompatibleProviderConfig, PostgresStoreConfig,
    ProviderConfig, ProviderKind, RuntimeConfig, SqliteStoreConfig, StoreConfig, StoreKind,
};
pub use store::CinderStore;
pub use taskplan::{
    apply_output_bindings, ready_task_ids, validate_plan, validate_task_output, ChildPlanLink,
    OutputBinding, PlanRunStatus, TaskDependency, TaskPlan, TaskPlanRun, TaskRun, TaskSpec,
    TaskState,
};

pub type JsonMap = BTreeMap<String, Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    System,
    User,
    Assistant,
    Tool,
}

impl fmt::Display for MessageRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            MessageRole::System => "system",
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::Tool => "tool",
        };
        f.write_str(value)
    }
}

impl std::str::FromStr for MessageRole {
    type Err = CinderCoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "system" => Ok(MessageRole::System),
            "user" => Ok(MessageRole::User),
            "assistant" => Ok(MessageRole::Assistant),
            "tool" => Ok(MessageRole::Tool),
            other => Err(CinderCoreError::InvalidEnum {
                ty: "MessageRole",
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    SuspendedForTool,
    WaitingForInput,
    Completed,
    Failed,
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            RunStatus::Running => "running",
            RunStatus::SuspendedForTool => "suspended_for_tool",
            RunStatus::WaitingForInput => "waiting_for_input",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
        };
        f.write_str(value)
    }
}

impl std::str::FromStr for RunStatus {
    type Err = CinderCoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "running" => Ok(RunStatus::Running),
            "suspended_for_tool" => Ok(RunStatus::SuspendedForTool),
            "waiting_for_input" => Ok(RunStatus::WaitingForInput),
            "completed" => Ok(RunStatus::Completed),
            "failed" => Ok(RunStatus::Failed),
            other => Err(CinderCoreError::InvalidEnum {
                ty: "RunStatus",
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolExecutionMode {
    Inline,
    Async,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSpec {
    pub id: String,
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub description: String,
    pub system_prompt: String,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub skills: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillSpec {
    pub name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub tools: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub role: MessageRole,
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

impl Message {
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: MessageRole::System,
            content: content.into(),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: MessageRole::User,
            content: content.into(),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    pub fn assistant(content: impl Into<String>, tool_calls: Vec<ToolCall>) -> Self {
        Self {
            role: MessageRole::Assistant,
            content: content.into(),
            tool_calls,
            tool_call_id: None,
        }
    }

    pub fn tool(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: MessageRole::Tool,
            content: content.into(),
            tool_calls: Vec::new(),
            tool_call_id: Some(tool_call_id.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub arguments: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub execution_mode: ToolExecutionMode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderRequest {
    pub model: String,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderResponse {
    pub message: Message,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    pub content: String,
    #[serde(default)]
    pub data: Value,
    #[serde(default = "default_true")]
    pub append_message: bool,
    #[serde(default)]
    pub suspend_run: bool,
}

#[derive(Clone)]
pub struct ToolContext {
    pub agents: Vec<AgentSpec>,
    pub store: Option<Arc<dyn CinderStore>>,
    pub run_id: Option<Uuid>,
    pub tool_call_id: Option<String>,
}

impl Default for ToolContext {
    fn default() -> Self {
        Self {
            agents: Vec::new(),
            store: None,
            run_id: None,
            tool_call_id: None,
        }
    }
}

impl ToolResult {
    pub fn text(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            data: Value::Null,
            append_message: true,
            suspend_run: false,
        }
    }

    pub fn control(
        content: impl Into<String>,
        data: Value,
        append_message: bool,
        suspend_run: bool,
    ) -> Self {
        Self {
            content: content.into(),
            data,
            append_message,
            suspend_run,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRun {
    pub id: Uuid,
    pub agent_id: String,
    pub user_id: Option<String>,
    pub target_id: Option<String>,
    pub status: RunStatus,
    pub last_error: Option<String>,
    pub locked_at: Option<DateTime<Utc>>,
    pub locked_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunMessage {
    pub id: i64,
    pub run_id: Uuid,
    pub role: MessageRole,
    pub content: String,
    pub tool_calls: Vec<ToolCall>,
    pub tool_call_id: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl From<RunMessage> for Message {
    fn from(value: RunMessage) -> Self {
        Message {
            role: value.role,
            content: value.content,
            tool_calls: value.tool_calls,
            tool_call_id: value.tool_call_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolTask {
    pub id: Uuid,
    pub run_id: Uuid,
    pub tool_call: ToolCall,
    pub status: ToolTaskStatus,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub available_at: DateTime<Utc>,
    pub locked_at: Option<DateTime<Utc>>,
    pub locked_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolTaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Dead,
}

impl fmt::Display for ToolTaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            ToolTaskStatus::Pending => "pending",
            ToolTaskStatus::Running => "running",
            ToolTaskStatus::Completed => "completed",
            ToolTaskStatus::Failed => "failed",
            ToolTaskStatus::Dead => "dead",
        };
        f.write_str(value)
    }
}

impl std::str::FromStr for ToolTaskStatus {
    type Err = CinderCoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(ToolTaskStatus::Pending),
            "running" => Ok(ToolTaskStatus::Running),
            "completed" => Ok(ToolTaskStatus::Completed),
            "failed" => Ok(ToolTaskStatus::Failed),
            "dead" => Ok(ToolTaskStatus::Dead),
            other => Err(CinderCoreError::InvalidEnum {
                ty: "ToolTaskStatus",
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CinderCoreError {
    #[error("invalid {ty} value: {value}")]
    InvalidEnum { ty: &'static str, value: String },
    #[error("invalid task plan: {0}")]
    TaskPlan(String),
    #[error("invalid config: {0}")]
    Config(String),
    #[error("provider failed: {0}")]
    Provider(String),
    #[error("tool failed: {0}")]
    Tool(String),
    #[error("serialization failed: {0}")]
    Serde(String),
    #[error("store failed: {0}")]
    Store(String),
}

impl From<serde_json::Error> for CinderCoreError {
    fn from(value: serde_json::Error) -> Self {
        CinderCoreError::Serde(value.to_string())
    }
}

#[async_trait]
pub trait Provider: Send + Sync {
    fn name(&self) -> &str;

    async fn chat(&self, request: ProviderRequest) -> Result<ProviderResponse, CinderCoreError>;
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn spec(&self) -> ToolSpec;

    async fn execute(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<ToolResult, CinderCoreError>;
}

pub trait Skill: Send + Sync {
    fn spec(&self) -> SkillSpec;
}
