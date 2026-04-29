use crate::{AgentRuntime, CreateRun, RunAgentOutcome};
use anyhow::{anyhow, Context, Result};
use cinder_core::{
    apply_dependency_inputs, ready_task_ids, CinderStore, Message, PlanRunStatus, TaskPlan,
    TaskState,
};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::error;
use uuid::Uuid;

#[derive(Clone)]
pub struct TaskPlanRuntime {
    store: Arc<dyn CinderStore>,
    agent_runtime: AgentRuntime,
    options: TaskPlanRuntimeOptions,
}

#[derive(Debug, Clone)]
pub struct TaskPlanRuntimeOptions {
    pub plan_lease_seconds: i64,
    pub max_advance_iterations: usize,
}

impl Default for TaskPlanRuntimeOptions {
    fn default() -> Self {
        Self {
            plan_lease_seconds: 300,
            max_advance_iterations: 32,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateTaskPlanRun {
    pub plan: TaskPlan,
    pub user_id: Option<String>,
    pub target_id: Option<String>,
    pub parent_plan_run_id: Option<Uuid>,
    pub parent_task_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskPlanOutcome {
    Running,
    Completed { result: Value },
    Busy,
    Failed { error: String },
}

impl TaskPlanRuntime {
    pub fn new(store: impl CinderStore + 'static, agent_runtime: AgentRuntime) -> Self {
        Self::with_options(store, agent_runtime, TaskPlanRuntimeOptions::default())
    }

    pub fn with_options(
        store: impl CinderStore + 'static,
        agent_runtime: AgentRuntime,
        options: TaskPlanRuntimeOptions,
    ) -> Self {
        Self {
            store: Arc::new(store),
            agent_runtime,
            options,
        }
    }

    pub async fn create_plan_run(&self, input: CreateTaskPlanRun) -> Result<Uuid> {
        let run = self
            .store
            .create_task_plan_run(
                &input.plan,
                input.user_id.as_deref(),
                input.target_id.as_deref(),
                input.parent_plan_run_id,
                input.parent_task_id.as_deref(),
                None,
                None,
            )
            .await?;
        Ok(run.id)
    }

    pub async fn advance_plan(&self, plan_run_id: Uuid) -> Result<TaskPlanOutcome> {
        let lock_owner = format!("task-plan-{}", Uuid::new_v4());
        let acquired = self
            .store
            .acquire_task_plan_lock(plan_run_id, &lock_owner, self.options.plan_lease_seconds)
            .await?;
        if !acquired {
            return Ok(TaskPlanOutcome::Busy);
        }

        let result = self.advance_plan_locked(plan_run_id).await;
        let release_result = self
            .store
            .release_task_plan_lock(plan_run_id, &lock_owner)
            .await;
        match (result, release_result) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(err), Ok(())) => Err(err),
            (Ok(_), Err(err)) => Err(err.into()),
            (Err(err), Err(release_err)) => Err(err).context(format!(
                "also failed to release task plan lock: {release_err}"
            )),
        }
    }

    async fn advance_plan_locked(&self, plan_run_id: Uuid) -> Result<TaskPlanOutcome> {
        for _ in 0..self.options.max_advance_iterations {
            let plan_run = self
                .store
                .get_task_plan_run(plan_run_id)
                .await?
                .ok_or_else(|| anyhow!("task plan run not found: {plan_run_id}"))?;

            match plan_run.status {
                PlanRunStatus::Completed => {
                    return Ok(TaskPlanOutcome::Completed {
                        result: plan_run.result.unwrap_or(Value::Null),
                    });
                }
                PlanRunStatus::Failed => {
                    return Ok(TaskPlanOutcome::Failed {
                        error: plan_run
                            .last_error
                            .unwrap_or_else(|| "task plan failed".to_owned()),
                    });
                }
                PlanRunStatus::Cancelled => {
                    return Ok(TaskPlanOutcome::Failed {
                        error: "task plan cancelled".to_owned(),
                    });
                }
                PlanRunStatus::Running => {}
            }

            let tasks = self.store.list_task_runs(plan_run_id).await?;
            let mut progressed = self.poll_waiting_child_tasks(plan_run_id, &tasks).await?;
            let tasks = if progressed {
                self.store.list_task_runs(plan_run_id).await?
            } else {
                tasks
            };
            progressed |= self
                .poll_running_tasks(plan_run_id, &plan_run.plan, &tasks)
                .await?;
            let tasks = if progressed {
                self.store.list_task_runs(plan_run_id).await?
            } else {
                tasks
            };

            let states = states_by_task(&tasks);
            let outputs = outputs_by_task(&tasks);
            if tasks.iter().all(|task| task.state == TaskState::Succeeded) {
                let result = plan_result(&plan_run.plan, &outputs)?;
                self.store
                    .complete_task_plan_run(plan_run_id, &result)
                    .await?;
                return Ok(TaskPlanOutcome::Completed { result });
            }

            for task_id in ready_task_ids(&plan_run.plan, &states)? {
                self.start_ready_task(plan_run_id, &plan_run.plan, &task_id, &outputs)
                    .await?;
                progressed = true;
            }

            if !progressed {
                return Ok(TaskPlanOutcome::Running);
            }
        }

        let error = "task plan exceeded max advance iterations";
        self.store.fail_task_plan_run(plan_run_id, error).await?;
        Ok(TaskPlanOutcome::Failed {
            error: error.to_owned(),
        })
    }

    async fn poll_running_tasks(
        &self,
        plan_run_id: Uuid,
        _plan: &TaskPlan,
        tasks: &[cinder_core::TaskRun],
    ) -> Result<bool> {
        let mut progressed = false;
        for task in tasks.iter().filter(|task| task.state == TaskState::Running) {
            let Some(agent_run_id) = task.agent_run_id else {
                let error = format!("task `{}` is running without an agent_run_id", task.task_id);
                self.store
                    .fail_task_run(plan_run_id, &task.task_id, &error)
                    .await?;
                return Ok(true);
            };

            match self.agent_runtime.run_agent(agent_run_id, None).await? {
                RunAgentOutcome::Completed { .. } => {
                    if self.task_is_succeeded(plan_run_id, &task.task_id).await? {
                        progressed = true;
                        continue;
                    }
                    self.remind_submit_task(agent_run_id).await?;
                    progressed = true;
                }
                RunAgentOutcome::Failed { error } => {
                    self.store
                        .fail_task_run(plan_run_id, &task.task_id, &error)
                        .await?;
                    return Ok(true);
                }
                RunAgentOutcome::SuspendedForTool | RunAgentOutcome::WaitingForInput => {}
                RunAgentOutcome::Busy => {}
            }
        }
        Ok(progressed)
    }

    async fn poll_waiting_child_tasks(
        &self,
        plan_run_id: Uuid,
        tasks: &[cinder_core::TaskRun],
    ) -> Result<bool> {
        let mut progressed = false;
        for task in tasks
            .iter()
            .filter(|task| task.state == TaskState::WaitingChildPlan)
        {
            let child_plan_run_id = task.child_plan_run_id.ok_or_else(|| {
                anyhow!(
                    "task `{}` is waiting_child_plan without child_plan_run_id",
                    task.task_id
                )
            })?;
            match Box::pin(self.advance_plan(child_plan_run_id)).await? {
                TaskPlanOutcome::Completed { result } => {
                    let agent_run_id = task
                        .agent_run_id
                        .ok_or_else(|| anyhow!("task `{}` has no agent_run_id", task.task_id))?;
                    let tool_call_id = task.child_tool_call_id.as_ref().ok_or_else(|| {
                        anyhow!("task `{}` has no child tool_call_id", task.task_id)
                    })?;
                    self.store
                        .append_message(
                            agent_run_id,
                            &Message::tool(tool_call_id.clone(), serde_json::to_string(&result)?),
                        )
                        .await?;
                    self.store
                        .update_run_status(agent_run_id, cinder_core::RunStatus::Running, None)
                        .await?;
                    self.store
                        .resume_task_run_after_child_plan(plan_run_id, &task.task_id)
                        .await?;
                    progressed = true;
                }
                TaskPlanOutcome::Failed { error } => {
                    self.store
                        .fail_task_run(plan_run_id, &task.task_id, &error)
                        .await?;
                    return Ok(true);
                }
                TaskPlanOutcome::Running | TaskPlanOutcome::Busy => {}
            }
        }
        Ok(progressed)
    }

    pub async fn advance_parent_run_child_plans(&self, parent_run_id: Uuid) -> Result<bool> {
        let plans = self
            .store
            .list_undelivered_task_plan_runs_by_parent_run(parent_run_id)
            .await?;
        let mut progressed = false;
        for plan in plans {
            let tool_call_id = plan
                .parent_tool_call_id
                .clone()
                .ok_or_else(|| anyhow!("child plan `{}` has no parent tool call id", plan.id))?;
            match self.advance_plan(plan.id).await? {
                TaskPlanOutcome::Completed { result } => {
                    self.store
                        .append_message(
                            parent_run_id,
                            &Message::tool(tool_call_id, serde_json::to_string(&result)?),
                        )
                        .await?;
                    self.store.mark_task_plan_parent_delivered(plan.id).await?;
                    self.store
                        .update_run_status(parent_run_id, cinder_core::RunStatus::Running, None)
                        .await?;
                    let _ = self.agent_runtime.run_agent(parent_run_id, None).await?;
                    progressed = true;
                }
                TaskPlanOutcome::Failed { error } => {
                    let result = serde_json::json!({
                        "status": "failed",
                        "plan_run_id": plan.id,
                        "error": error
                    });
                    self.store
                        .append_message(
                            parent_run_id,
                            &Message::tool(tool_call_id, serde_json::to_string(&result)?),
                        )
                        .await?;
                    self.store.mark_task_plan_parent_delivered(plan.id).await?;
                    self.store
                        .update_run_status(parent_run_id, cinder_core::RunStatus::Running, None)
                        .await?;
                    let _ = self.agent_runtime.run_agent(parent_run_id, None).await?;
                    progressed = true;
                }
                TaskPlanOutcome::Running | TaskPlanOutcome::Busy => {}
            }
        }
        Ok(progressed)
    }

    async fn start_ready_task(
        &self,
        plan_run_id: Uuid,
        plan: &TaskPlan,
        task_id: &str,
        outputs: &BTreeMap<String, Value>,
    ) -> Result<()> {
        let task_spec = find_task(plan, task_id)?;
        let input = apply_dependency_inputs(plan, task_id, &task_spec.input, outputs)?;
        let agent_run_id = self
            .agent_runtime
            .create_run(CreateRun {
                agent_id: task_spec.agent_id.clone(),
                user_id: None,
                target_id: Some(plan_run_id.to_string()),
            })
            .await?;
        self.store
            .start_task_run(plan_run_id, task_id, agent_run_id, &input)
            .await?;

        match self
            .agent_runtime
            .run_agent(agent_run_id, Some(task_bootstrap_prompt(plan, task_spec)))
            .await?
        {
            RunAgentOutcome::Completed { .. } => {
                if self.task_is_succeeded(plan_run_id, task_id).await? {
                    return Ok(());
                }
                self.remind_submit_task(agent_run_id).await?;
            }
            RunAgentOutcome::Failed { error } => {
                self.store
                    .fail_task_run(plan_run_id, task_id, &error)
                    .await?;
            }
            RunAgentOutcome::SuspendedForTool | RunAgentOutcome::WaitingForInput => {}
            RunAgentOutcome::Busy => {}
        }

        Ok(())
    }

    async fn task_is_succeeded(&self, plan_run_id: Uuid, task_id: &str) -> Result<bool> {
        Ok(self
            .store
            .list_task_runs(plan_run_id)
            .await?
            .into_iter()
            .any(|task| task.task_id == task_id && task.state == TaskState::Succeeded))
    }

    async fn remind_submit_task(&self, agent_run_id: Uuid) -> Result<()> {
        self.store
            .append_message(
                agent_run_id,
                &Message::user(
                    "Your Cinder TaskPlan task is still open. Call cinder.get_task if you need the task details, then complete it with cinder.submit_task. A normal assistant message does not complete the task.",
                ),
            )
            .await?;
        self.store
            .update_run_status(agent_run_id, cinder_core::RunStatus::Running, None)
            .await?;
        Ok(())
    }

    pub fn spawn_worker(
        &self,
        plan_run_id: Uuid,
        poll_interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            loop {
                match runtime.advance_plan(plan_run_id).await {
                    Ok(TaskPlanOutcome::Completed { .. }) | Ok(TaskPlanOutcome::Failed { .. }) => {
                        break;
                    }
                    Ok(TaskPlanOutcome::Running) | Ok(TaskPlanOutcome::Busy) => {
                        sleep(poll_interval).await;
                    }
                    Err(err) => {
                        error!(plan_run_id = %plan_run_id, error = %err, "task plan worker failed");
                        sleep(poll_interval).await;
                    }
                }
            }
        })
    }
}

fn find_task<'a>(plan: &'a TaskPlan, task_id: &str) -> Result<&'a cinder_core::TaskSpec> {
    plan.tasks
        .iter()
        .find(|task| task.id == task_id)
        .ok_or_else(|| anyhow!("task `{task_id}` not found in plan `{}`", plan.id))
}

fn states_by_task(tasks: &[cinder_core::TaskRun]) -> BTreeMap<String, TaskState> {
    tasks
        .iter()
        .map(|task| (task.task_id.clone(), task.state))
        .collect()
}

fn outputs_by_task(tasks: &[cinder_core::TaskRun]) -> BTreeMap<String, Value> {
    tasks
        .iter()
        .filter_map(|task| {
            task.output
                .as_ref()
                .map(|output| (task.task_id.clone(), output.clone()))
        })
        .collect()
}

fn plan_result(plan: &TaskPlan, outputs: &BTreeMap<String, Value>) -> Result<Value> {
    let terminal_task_ids = plan
        .tasks
        .iter()
        .filter(|task| {
            !plan
                .dependencies
                .iter()
                .any(|dependency| dependency.from_task == task.id)
        })
        .map(|task| task.id.as_str())
        .collect::<Vec<_>>();

    if terminal_task_ids.is_empty() {
        return Err(anyhow!("task plan `{}` has no terminal task", plan.id));
    }

    let mut result = Map::new();
    for task_id in terminal_task_ids {
        let output = outputs
            .get(task_id)
            .cloned()
            .ok_or_else(|| anyhow!("terminal task `{task_id}` has no output"))?;
        result.insert(task_id.to_owned(), output);
    }
    Ok(Value::Object(result))
}

fn task_bootstrap_prompt(plan: &TaskPlan, task: &cinder_core::TaskSpec) -> String {
    format!(
        r#"A Cinder TaskPlan task is ready for you.

Plan id: {plan_id}
Task id: {task_id}

Call `cinder.get_task` to fetch the task prompt, input JSON, and output schema. When finished, call `cinder.submit_task`; do not treat a normal assistant message as task completion."#,
        plan_id = plan.id,
        task_id = task.id,
    )
}
