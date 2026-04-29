use crate::{
    taskplan::{TaskPlan, TaskPlanRun, TaskRun},
    AgentRun, AgentSpec, CinderCoreError, Message, RunMessage, RunStatus, ToolCall, ToolTask,
};
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

#[async_trait]
pub trait CinderStore: Send + Sync {
    async fn init_schema(&self) -> Result<(), CinderCoreError>;

    async fn upsert_agent(&self, spec: &AgentSpec) -> Result<(), CinderCoreError>;
    async fn get_agent(&self, agent_id: &str) -> Result<Option<AgentSpec>, CinderCoreError>;
    async fn list_agents(&self) -> Result<Vec<AgentSpec>, CinderCoreError>;

    async fn create_run(
        &self,
        agent_id: &str,
        user_id: Option<&str>,
        target_id: Option<&str>,
    ) -> Result<AgentRun, CinderCoreError>;
    async fn get_run(&self, run_id: Uuid) -> Result<Option<AgentRun>, CinderCoreError>;
    async fn update_run_status(
        &self,
        run_id: Uuid,
        status: RunStatus,
        last_error: Option<&str>,
    ) -> Result<(), CinderCoreError>;
    async fn acquire_run_lock(
        &self,
        run_id: Uuid,
        owner: &str,
        lease_seconds: i64,
    ) -> Result<bool, CinderCoreError>;
    async fn release_run_lock(&self, run_id: Uuid, owner: &str) -> Result<(), CinderCoreError>;

    async fn append_message(
        &self,
        run_id: Uuid,
        message: &Message,
    ) -> Result<RunMessage, CinderCoreError>;
    async fn list_messages(&self, run_id: Uuid) -> Result<Vec<RunMessage>, CinderCoreError>;

    async fn enqueue_tool_task(
        &self,
        run_id: Uuid,
        tool_call: &ToolCall,
    ) -> Result<ToolTask, CinderCoreError>;
    async fn claim_tool_task(
        &self,
        worker_id: &str,
        lease_seconds: i64,
    ) -> Result<Option<ToolTask>, CinderCoreError>;
    async fn complete_tool_task(
        &self,
        task_id: Uuid,
        run_id: Uuid,
        tool_call_id: &str,
        result_content: &str,
    ) -> Result<bool, CinderCoreError>;
    async fn fail_tool_task(
        &self,
        task_id: Uuid,
        run_id: Uuid,
        error: &str,
    ) -> Result<(), CinderCoreError>;
    async fn retry_tool_task(
        &self,
        task_id: Uuid,
        delay_seconds: i64,
        error: &str,
    ) -> Result<(), CinderCoreError>;
    async fn dead_letter_tool_task(
        &self,
        task_id: Uuid,
        run_id: Uuid,
        error: &str,
    ) -> Result<(), CinderCoreError>;

    async fn create_task_plan_run(
        &self,
        plan: &TaskPlan,
        user_id: Option<&str>,
        target_id: Option<&str>,
        parent_plan_run_id: Option<Uuid>,
        parent_task_id: Option<&str>,
    ) -> Result<TaskPlanRun, CinderCoreError>;
    async fn get_task_plan_run(
        &self,
        plan_run_id: Uuid,
    ) -> Result<Option<TaskPlanRun>, CinderCoreError>;
    async fn list_task_runs(&self, plan_run_id: Uuid) -> Result<Vec<TaskRun>, CinderCoreError>;
    async fn get_task_run_by_agent_run(
        &self,
        agent_run_id: Uuid,
    ) -> Result<Option<TaskRun>, CinderCoreError>;
    async fn acquire_task_plan_lock(
        &self,
        plan_run_id: Uuid,
        owner: &str,
        lease_seconds: i64,
    ) -> Result<bool, CinderCoreError>;
    async fn release_task_plan_lock(
        &self,
        plan_run_id: Uuid,
        owner: &str,
    ) -> Result<(), CinderCoreError>;
    async fn start_task_run(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
        agent_run_id: Uuid,
        input: &Value,
    ) -> Result<(), CinderCoreError>;
    async fn wait_task_run_for_child_plan(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
        child_plan_run_id: Uuid,
        tool_call_id: &str,
    ) -> Result<(), CinderCoreError>;
    async fn resume_task_run_after_child_plan(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
    ) -> Result<(), CinderCoreError>;
    async fn succeed_task_run(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
        output: &Value,
    ) -> Result<(), CinderCoreError>;
    async fn fail_task_run(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
        error: &str,
    ) -> Result<(), CinderCoreError>;
    async fn complete_task_plan_run(
        &self,
        plan_run_id: Uuid,
        result: &Value,
    ) -> Result<(), CinderCoreError>;
    async fn fail_task_plan_run(
        &self,
        plan_run_id: Uuid,
        error: &str,
    ) -> Result<(), CinderCoreError>;
}
