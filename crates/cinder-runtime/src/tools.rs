use async_trait::async_trait;
use cinder_core::{
    validate_plan, validate_task_output, CinderCoreError, TaskPlan, Tool, ToolContext,
    ToolExecutionMode, ToolResult, ToolSpec,
};
use serde_json::{json, Value};

const LIST_AGENTS_DESCRIPTION: &str = include_str!("../tool_descriptions/list_agents.md");
const LIST_AGENTS_SCHEMA: &str = include_str!("../tool_descriptions/list_agents.schema.json");
const GET_TASK_DESCRIPTION: &str = include_str!("../tool_descriptions/get_task.md");
const GET_TASK_SCHEMA: &str = include_str!("../tool_descriptions/get_task.schema.json");
const SPAWN_TASK_PLAN_DESCRIPTION: &str = include_str!("../tool_descriptions/spawn_task_plan.md");
const SPAWN_TASK_PLAN_SCHEMA: &str =
    include_str!("../tool_descriptions/spawn_task_plan.schema.json");
const SUBMIT_TASK_DESCRIPTION: &str = include_str!("../tool_descriptions/submit_task.md");
const SUBMIT_TASK_SCHEMA: &str = include_str!("../tool_descriptions/submit_task.schema.json");

#[derive(Debug, Clone, Default)]
pub struct ListAgentsTool;

impl ListAgentsTool {
    pub const NAME: &'static str = "cinder.list_agents";
}

#[async_trait]
impl Tool for ListAgentsTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Self::NAME.to_owned(),
            description: LIST_AGENTS_DESCRIPTION.trim().to_owned(),
            input_schema: serde_json::from_str(LIST_AGENTS_SCHEMA)
                .expect("valid list_agents schema"),
            execution_mode: ToolExecutionMode::Inline,
        }
    }

    async fn execute(
        &self,
        context: ToolContext,
        _arguments: serde_json::Value,
    ) -> Result<ToolResult, CinderCoreError> {
        let agents = context
            .agents
            .into_iter()
            .map(|agent| {
                json!({
                    "id": agent.id,
                    "description": agent.description,
                    "provider": agent.provider,
                    "model": agent.model,
                    "tools": agent.tools,
                    "skills": agent.skills,
                })
            })
            .collect::<Vec<_>>();
        Ok(ToolResult {
            content: serde_json::to_string(&json!({ "agents": agents }))?,
            data: json!({ "agents": agents }),
            append_message: true,
            suspend_run: false,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct GetTaskTool;

impl GetTaskTool {
    pub const NAME: &'static str = "cinder.get_task";
}

#[async_trait]
impl Tool for GetTaskTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Self::NAME.to_owned(),
            description: GET_TASK_DESCRIPTION.trim().to_owned(),
            input_schema: serde_json::from_str(GET_TASK_SCHEMA).expect("valid get_task schema"),
            execution_mode: ToolExecutionMode::Inline,
        }
    }

    async fn execute(
        &self,
        context: ToolContext,
        _arguments: Value,
    ) -> Result<ToolResult, CinderCoreError> {
        let store = context
            .store
            .ok_or_else(|| CinderCoreError::Tool("get_task requires store context".to_owned()))?;
        let run_id = context
            .run_id
            .ok_or_else(|| CinderCoreError::Tool("get_task requires run_id context".to_owned()))?;
        let Some(task) = store.get_task_run_by_agent_run(run_id).await? else {
            let data = json!({
                "has_task": false,
                "message": "This run is not currently executing a Cinder TaskPlan task."
            });
            return Ok(ToolResult::control(
                serde_json::to_string(&data)?,
                data,
                true,
                false,
            ));
        };
        let plan_run = store
            .get_task_plan_run(task.plan_run_id)
            .await?
            .ok_or_else(|| CinderCoreError::Tool("task plan run not found".to_owned()))?;
        let task_spec = plan_run
            .plan
            .tasks
            .iter()
            .find(|spec| spec.id == task.task_id)
            .ok_or_else(|| CinderCoreError::Tool("task spec not found".to_owned()))?;
        let data = json!({
            "has_task": true,
            "plan_id": plan_run.plan.id,
            "plan_run_id": task.plan_run_id,
            "task_id": task.task_id,
            "agent_id": task_spec.agent_id,
            "prompt": task_spec.prompt,
            "input": task.input,
            "output_schema": task_spec.output_schema,
            "state": task.state.to_string()
        });
        Ok(ToolResult::control(
            serde_json::to_string(&data)?,
            data,
            true,
            false,
        ))
    }
}

#[derive(Debug, Clone, Default)]
pub struct SpawnTaskPlanTool;

impl SpawnTaskPlanTool {
    pub const NAME: &'static str = "cinder.spawn_task_plan";
}

#[async_trait]
impl Tool for SpawnTaskPlanTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Self::NAME.to_owned(),
            description: SPAWN_TASK_PLAN_DESCRIPTION.trim().to_owned(),
            input_schema: serde_json::from_str(SPAWN_TASK_PLAN_SCHEMA)
                .expect("valid spawn_task_plan schema"),
            execution_mode: ToolExecutionMode::Inline,
        }
    }

    async fn execute(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<ToolResult, CinderCoreError> {
        let store = context.store.ok_or_else(|| {
            CinderCoreError::Tool("spawn_task_plan requires store context".to_owned())
        })?;
        let run_id = context.run_id.ok_or_else(|| {
            CinderCoreError::Tool("spawn_task_plan requires run_id context".to_owned())
        })?;
        let tool_call_id = context.tool_call_id.ok_or_else(|| {
            CinderCoreError::Tool("spawn_task_plan requires tool_call_id context".to_owned())
        })?;
        let child_plan: TaskPlan = serde_json::from_value(
            arguments
                .get("task_plan")
                .cloned()
                .ok_or_else(|| CinderCoreError::Tool("missing task_plan".to_owned()))?,
        )?;
        validate_plan(&child_plan)?;
        validate_agent_ids(&context.agents, &child_plan)?;
        let parent_task = store.get_task_run_by_agent_run(run_id).await?;
        let child_run = if let Some(parent_task) = parent_task {
            let parent_plan = store
                .get_task_plan_run(parent_task.plan_run_id)
                .await?
                .ok_or_else(|| {
                    CinderCoreError::Tool("parent task plan run not found".to_owned())
                })?;
            let child_run = store
                .create_task_plan_run(
                    &child_plan,
                    parent_plan.user_id.as_deref(),
                    parent_plan.target_id.as_deref(),
                    Some(parent_task.plan_run_id),
                    Some(&parent_task.task_id),
                    None,
                    None,
                )
                .await?;
            store
                .wait_task_run_for_child_plan(
                    parent_task.plan_run_id,
                    &parent_task.task_id,
                    child_run.id,
                    &tool_call_id,
                )
                .await?;
            child_run
        } else {
            let parent_run = store
                .get_run(run_id)
                .await?
                .ok_or_else(|| CinderCoreError::Tool("parent run not found".to_owned()))?;
            store
                .create_task_plan_run(
                    &child_plan,
                    parent_run.user_id.as_deref(),
                    parent_run.target_id.as_deref(),
                    None,
                    None,
                    Some(run_id),
                    Some(&tool_call_id),
                )
                .await?
        };
        store
            .update_run_status(run_id, cinder_core::RunStatus::WaitingForInput, None)
            .await?;

        let content = serde_json::to_string(&json!({
            "status": "waiting_child_plan",
            "child_plan_run_id": child_run.id
        }))?;
        Ok(ToolResult::control(
            content,
            json!({ "child_plan_run_id": child_run.id }),
            false,
            true,
        ))
    }
}

#[derive(Debug, Clone, Default)]
pub struct SubmitTaskTool;

impl SubmitTaskTool {
    pub const NAME: &'static str = "cinder.submit_task";
}

#[async_trait]
impl Tool for SubmitTaskTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Self::NAME.to_owned(),
            description: SUBMIT_TASK_DESCRIPTION.trim().to_owned(),
            input_schema: serde_json::from_str(SUBMIT_TASK_SCHEMA)
                .expect("valid submit_task schema"),
            execution_mode: ToolExecutionMode::Inline,
        }
    }

    async fn execute(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<ToolResult, CinderCoreError> {
        let store = context.store.ok_or_else(|| {
            CinderCoreError::Tool("submit_task requires store context".to_owned())
        })?;
        let run_id = context.run_id.ok_or_else(|| {
            CinderCoreError::Tool("submit_task requires run_id context".to_owned())
        })?;
        let task = store
            .get_task_run_by_agent_run(run_id)
            .await?
            .ok_or_else(|| {
                CinderCoreError::Tool(
                    "submit_task can only be called from a TaskPlan task".to_owned(),
                )
            })?;
        let plan_run = store
            .get_task_plan_run(task.plan_run_id)
            .await?
            .ok_or_else(|| CinderCoreError::Tool("task plan run not found".to_owned()))?;
        let task_spec = plan_run
            .plan
            .tasks
            .iter()
            .find(|spec| spec.id == task.task_id)
            .ok_or_else(|| CinderCoreError::Tool("task spec not found".to_owned()))?;
        let status = arguments
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("succeeded");

        match status {
            "succeeded" => {
                let result = arguments.get("result").cloned().ok_or_else(|| {
                    CinderCoreError::Tool("submit_task missing result".to_owned())
                })?;
                validate_task_output(task_spec, &result)?;
                store
                    .succeed_task_run(task.plan_run_id, &task.task_id, &result)
                    .await?;
                store
                    .update_run_status(run_id, cinder_core::RunStatus::Completed, None)
                    .await?;
                let content = serde_json::to_string(&json!({
                    "status": "succeeded",
                    "task_id": task.task_id
                }))?;
                Ok(ToolResult::control(content, result, false, false))
            }
            "failed" => {
                let error = arguments
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("task failed");
                store
                    .fail_task_run(task.plan_run_id, &task.task_id, error)
                    .await?;
                store
                    .update_run_status(run_id, cinder_core::RunStatus::Failed, Some(error))
                    .await?;
                let content = serde_json::to_string(&json!({
                    "status": "failed",
                    "task_id": task.task_id,
                    "error": error
                }))?;
                Ok(ToolResult::control(
                    content,
                    json!({ "error": error }),
                    false,
                    false,
                ))
            }
            other => Err(CinderCoreError::Tool(format!(
                "unsupported submit_task status `{other}`"
            ))),
        }
    }
}

fn validate_agent_ids(
    agents: &[cinder_core::AgentSpec],
    task_plan: &TaskPlan,
) -> Result<(), CinderCoreError> {
    for task in &task_plan.tasks {
        if !agents.iter().any(|agent| agent.id == task.agent_id) {
            return Err(CinderCoreError::Tool(format!(
                "task `{}` references unknown agent `{}`",
                task.id, task.agent_id
            )));
        }
    }
    Ok(())
}
