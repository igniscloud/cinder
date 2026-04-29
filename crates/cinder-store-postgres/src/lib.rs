use cinder_core::{
    taskplan::{PlanRunStatus, TaskPlan, TaskPlanRun, TaskRun, TaskState},
    AgentRun, AgentSpec, CinderCoreError, CinderStore, Message, MessageRole, RunMessage, RunStatus,
    ToolCall, ToolTask, ToolTaskStatus,
};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::str::FromStr;
use uuid::Uuid;

const INIT_SQL: &str = include_str!("../../../migrations/0001_init.sql");

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Core(#[from] cinder_core::CinderCoreError),
}

impl From<StoreError> for CinderCoreError {
    fn from(value: StoreError) -> Self {
        CinderCoreError::Store(value.to_string())
    }
}

#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    pub async fn connect_from_config(
        config: &cinder_core::PostgresStoreConfig,
    ) -> Result<Self, StoreError> {
        Self::connect(&config.database_url, config.max_connections).await
    }

    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn init_schema(&self) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(2948471201::bigint)")
            .execute(&mut *tx)
            .await?;
        for statement in INIT_SQL.split(';') {
            let statement = statement.trim();
            if !statement.is_empty() {
                sqlx::query(statement).execute(&mut *tx).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_agent(&self, spec: &AgentSpec) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            INSERT INTO ai_agents (id, provider, model, description, system_prompt, tools, skills)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO UPDATE SET
                provider = EXCLUDED.provider,
                model = EXCLUDED.model,
                description = EXCLUDED.description,
                system_prompt = EXCLUDED.system_prompt,
                tools = EXCLUDED.tools,
                skills = EXCLUDED.skills,
                updated_at = now()
            "#,
        )
        .bind(&spec.id)
        .bind(&spec.provider)
        .bind(&spec.model)
        .bind(&spec.description)
        .bind(&spec.system_prompt)
        .bind(serde_json::to_value(&spec.tools)?)
        .bind(serde_json::to_value(&spec.skills)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn create_task_plan_run(
        &self,
        plan: &TaskPlan,
        user_id: Option<&str>,
        target_id: Option<&str>,
        parent_plan_run_id: Option<Uuid>,
        parent_task_id: Option<&str>,
        parent_agent_run_id: Option<Uuid>,
        parent_tool_call_id: Option<&str>,
    ) -> Result<TaskPlanRun, StoreError> {
        cinder_core::validate_plan(plan)?;
        let plan_run_id = Uuid::new_v4();
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"
            INSERT INTO ai_task_plan_runs (
                id, plan, status, parent_plan_run_id,
                parent_task_id, parent_agent_run_id, parent_tool_call_id,
                user_id, target_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id, plan, status, result, last_error, parent_plan_run_id,
                parent_task_id, parent_agent_run_id, parent_tool_call_id,
                parent_delivered_at, user_id, target_id, locked_at, locked_by,
                created_at, updated_at
            "#,
        )
        .bind(plan_run_id)
        .bind(serde_json::to_value(plan)?)
        .bind(PlanRunStatus::Running.to_string())
        .bind(parent_plan_run_id)
        .bind(parent_task_id)
        .bind(parent_agent_run_id)
        .bind(parent_tool_call_id)
        .bind(user_id)
        .bind(target_id)
        .fetch_one(&mut *tx)
        .await?;

        for task in &plan.tasks {
            sqlx::query(
                r#"
                INSERT INTO ai_task_plan_task_runs (plan_run_id, task_id, state, input)
                VALUES ($1, $2, $3, $4)
                "#,
            )
            .bind(plan_run_id)
            .bind(&task.id)
            .bind(TaskState::Queued.to_string())
            .bind(&task.input)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        task_plan_run_from_row(&row)
    }

    pub async fn get_task_plan_run(
        &self,
        plan_run_id: Uuid,
    ) -> Result<Option<TaskPlanRun>, StoreError> {
        let row = sqlx::query(
            r#"
            SELECT id, plan, status, result, last_error, parent_plan_run_id,
                parent_task_id, parent_agent_run_id, parent_tool_call_id,
                parent_delivered_at, user_id, target_id, locked_at, locked_by,
                created_at, updated_at
            FROM ai_task_plan_runs
            WHERE id = $1
            "#,
        )
        .bind(plan_run_id)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(task_plan_run_from_row).transpose()
    }

    pub async fn list_task_runs(&self, plan_run_id: Uuid) -> Result<Vec<TaskRun>, StoreError> {
        let rows = sqlx::query(
            r#"
            SELECT plan_run_id, task_id, agent_run_id, child_plan_run_id,
                child_tool_call_id, state, input, output, last_error,
                created_at, updated_at
            FROM ai_task_plan_task_runs
            WHERE plan_run_id = $1
            ORDER BY created_at ASC, task_id ASC
            "#,
        )
        .bind(plan_run_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(task_run_from_row).collect()
    }

    pub async fn get_task_run_by_agent_run(
        &self,
        agent_run_id: Uuid,
    ) -> Result<Option<TaskRun>, StoreError> {
        let row = sqlx::query(
            r#"
            SELECT plan_run_id, task_id, agent_run_id, child_plan_run_id,
                child_tool_call_id, state, input, output, last_error,
                created_at, updated_at
            FROM ai_task_plan_task_runs
            WHERE agent_run_id = $1
            "#,
        )
        .bind(agent_run_id)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(task_run_from_row).transpose()
    }

    pub async fn list_undelivered_task_plan_runs_by_parent_run(
        &self,
        parent_agent_run_id: Uuid,
    ) -> Result<Vec<TaskPlanRun>, StoreError> {
        let rows = sqlx::query(
            r#"
            SELECT id, plan, status, result, last_error, parent_plan_run_id,
                parent_task_id, parent_agent_run_id, parent_tool_call_id,
                parent_delivered_at, user_id, target_id, locked_at, locked_by,
                created_at, updated_at
            FROM ai_task_plan_runs
            WHERE parent_agent_run_id = $1
              AND parent_delivered_at IS NULL
            ORDER BY created_at ASC
            "#,
        )
        .bind(parent_agent_run_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(task_plan_run_from_row).collect()
    }

    pub async fn mark_task_plan_parent_delivered(
        &self,
        plan_run_id: Uuid,
    ) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            UPDATE ai_task_plan_runs
            SET parent_delivered_at = now(), updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(plan_run_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn acquire_task_plan_lock(
        &self,
        plan_run_id: Uuid,
        owner: &str,
        lease_seconds: i64,
    ) -> Result<bool, StoreError> {
        let row: Option<(Uuid,)> = sqlx::query_as(
            r#"
            UPDATE ai_task_plan_runs
            SET locked_by = $2, locked_at = now(), updated_at = now()
            WHERE id = $1
              AND (
                locked_at IS NULL
                OR locked_at < now() - ($3::text || ' seconds')::interval
                OR locked_by = $2
              )
            RETURNING id
            "#,
        )
        .bind(plan_run_id)
        .bind(owner)
        .bind(lease_seconds)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    pub async fn release_task_plan_lock(
        &self,
        plan_run_id: Uuid,
        owner: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            UPDATE ai_task_plan_runs
            SET locked_by = NULL, locked_at = NULL, updated_at = now()
            WHERE id = $1 AND locked_by = $2
            "#,
        )
        .bind(plan_run_id)
        .bind(owner)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn start_task_run(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
        agent_run_id: Uuid,
        input: &Value,
    ) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            UPDATE ai_task_plan_task_runs
            SET state = $3, agent_run_id = $4, input = $5,
                child_plan_run_id = NULL,
                child_tool_call_id = NULL,
                last_error = NULL, updated_at = now()
            WHERE plan_run_id = $1 AND task_id = $2
            "#,
        )
        .bind(plan_run_id)
        .bind(task_id)
        .bind(TaskState::Running.to_string())
        .bind(agent_run_id)
        .bind(input)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn wait_task_run_for_child_plan(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
        child_plan_run_id: Uuid,
        tool_call_id: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            UPDATE ai_task_plan_task_runs
            SET state = $3,
                child_plan_run_id = $4,
                child_tool_call_id = $5,
                updated_at = now()
            WHERE plan_run_id = $1 AND task_id = $2
            "#,
        )
        .bind(plan_run_id)
        .bind(task_id)
        .bind(TaskState::WaitingChildPlan.to_string())
        .bind(child_plan_run_id)
        .bind(tool_call_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn resume_task_run_after_child_plan(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            UPDATE ai_task_plan_task_runs
            SET state = $3,
                child_plan_run_id = NULL,
                child_tool_call_id = NULL,
                updated_at = now()
            WHERE plan_run_id = $1 AND task_id = $2
            "#,
        )
        .bind(plan_run_id)
        .bind(task_id)
        .bind(TaskState::Running.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn succeed_task_run(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
        output: &Value,
    ) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            UPDATE ai_task_plan_task_runs
            SET state = $3,
                output = $4,
                child_plan_run_id = NULL,
                child_tool_call_id = NULL,
                last_error = NULL,
                updated_at = now()
            WHERE plan_run_id = $1 AND task_id = $2
            "#,
        )
        .bind(plan_run_id)
        .bind(task_id)
        .bind(TaskState::Succeeded.to_string())
        .bind(output)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fail_task_run(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
        error: &str,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            UPDATE ai_task_plan_task_runs
            SET state = $3,
                child_plan_run_id = NULL,
                child_tool_call_id = NULL,
                last_error = $4,
                updated_at = now()
            WHERE plan_run_id = $1 AND task_id = $2
            "#,
        )
        .bind(plan_run_id)
        .bind(task_id)
        .bind(TaskState::Failed.to_string())
        .bind(error)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE ai_task_plan_runs
            SET status = $2, last_error = $3, updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(plan_run_id)
        .bind(PlanRunStatus::Failed.to_string())
        .bind(error)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn complete_task_plan_run(
        &self,
        plan_run_id: Uuid,
        result: &Value,
    ) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            UPDATE ai_task_plan_runs
            SET status = $2, result = $3, last_error = NULL, updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(plan_run_id)
        .bind(PlanRunStatus::Completed.to_string())
        .bind(result)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fail_task_plan_run(
        &self,
        plan_run_id: Uuid,
        error: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            UPDATE ai_task_plan_runs
            SET status = $2, last_error = $3, updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(plan_run_id)
        .bind(PlanRunStatus::Failed.to_string())
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_agent(&self, agent_id: &str) -> Result<Option<AgentSpec>, StoreError> {
        let row = sqlx::query(
            r#"
            SELECT id, provider, model, description, system_prompt, tools, skills
            FROM ai_agents
            WHERE id = $1
            "#,
        )
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(agent_from_row).transpose()
    }

    pub async fn list_agents(&self) -> Result<Vec<AgentSpec>, StoreError> {
        let rows = sqlx::query(
            r#"
            SELECT id, provider, model, description, system_prompt, tools, skills
            FROM ai_agents
            ORDER BY id ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(agent_from_row).collect()
    }

    pub async fn create_run(
        &self,
        agent_id: &str,
        user_id: Option<&str>,
        target_id: Option<&str>,
    ) -> Result<AgentRun, StoreError> {
        let run_id = Uuid::new_v4();
        let row = sqlx::query(
            r#"
            INSERT INTO ai_runs (id, agent_id, user_id, target_id, status)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, agent_id, user_id, target_id, status, last_error,
                locked_at, locked_by, created_at, updated_at
            "#,
        )
        .bind(run_id)
        .bind(agent_id)
        .bind(user_id)
        .bind(target_id)
        .bind(RunStatus::Running.to_string())
        .fetch_one(&self.pool)
        .await?;

        run_from_row(&row)
    }

    pub async fn get_run(&self, run_id: Uuid) -> Result<Option<AgentRun>, StoreError> {
        let row = sqlx::query(
            r#"
            SELECT id, agent_id, user_id, target_id, status, last_error,
                locked_at, locked_by, created_at, updated_at
            FROM ai_runs
            WHERE id = $1
            "#,
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(run_from_row).transpose()
    }

    pub async fn update_run_status(
        &self,
        run_id: Uuid,
        status: RunStatus,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            UPDATE ai_runs
            SET status = $2, last_error = $3, updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(run_id)
        .bind(status.to_string())
        .bind(last_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn acquire_run_lock(
        &self,
        run_id: Uuid,
        owner: &str,
        lease_seconds: i64,
    ) -> Result<bool, StoreError> {
        let row: Option<(Uuid,)> = sqlx::query_as(
            r#"
            UPDATE ai_runs
            SET locked_by = $2, locked_at = now(), updated_at = now()
            WHERE id = $1
              AND (
                locked_at IS NULL
                OR locked_at < now() - ($3::text || ' seconds')::interval
                OR locked_by = $2
              )
            RETURNING id
            "#,
        )
        .bind(run_id)
        .bind(owner)
        .bind(lease_seconds)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    pub async fn release_run_lock(&self, run_id: Uuid, owner: &str) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            UPDATE ai_runs
            SET locked_by = NULL, locked_at = NULL, updated_at = now()
            WHERE id = $1 AND locked_by = $2
            "#,
        )
        .bind(run_id)
        .bind(owner)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn append_message(
        &self,
        run_id: Uuid,
        message: &Message,
    ) -> Result<RunMessage, StoreError> {
        let row = sqlx::query(
            r#"
            INSERT INTO ai_messages (run_id, role, content, tool_calls, tool_call_id, provider_metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, run_id, role, content, tool_calls, tool_call_id, provider_metadata, created_at
            "#,
        )
        .bind(run_id)
        .bind(message.role.to_string())
        .bind(&message.content)
        .bind(serde_json::to_value(&message.tool_calls)?)
        .bind(&message.tool_call_id)
        .bind(&message.provider_metadata)
        .fetch_one(&self.pool)
        .await?;

        message_from_row(&row)
    }

    pub async fn list_messages(&self, run_id: Uuid) -> Result<Vec<RunMessage>, StoreError> {
        let rows = sqlx::query(
            r#"
            SELECT id, run_id, role, content, tool_calls, tool_call_id, provider_metadata, created_at
            FROM ai_messages
            WHERE run_id = $1
            ORDER BY id ASC
            "#,
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(message_from_row).collect()
    }

    pub async fn enqueue_tool_task(
        &self,
        run_id: Uuid,
        tool_call: &ToolCall,
    ) -> Result<ToolTask, StoreError> {
        let task_id = Uuid::new_v4();
        let row = sqlx::query(
            r#"
            INSERT INTO ai_tool_tasks (id, run_id, tool_call, status)
            VALUES ($1, $2, $3, $4)
            RETURNING id, run_id, tool_call, status, attempts, last_error,
                available_at, locked_at, locked_by, created_at, updated_at
            "#,
        )
        .bind(task_id)
        .bind(run_id)
        .bind(serde_json::to_value(tool_call)?)
        .bind(ToolTaskStatus::Pending.to_string())
        .fetch_one(&self.pool)
        .await?;

        task_from_row(&row)
    }

    pub async fn claim_tool_task(
        &self,
        worker_id: &str,
        lease_seconds: i64,
    ) -> Result<Option<ToolTask>, StoreError> {
        let row = sqlx::query(
            r#"
            WITH candidate AS (
                SELECT id
                FROM ai_tool_tasks
                WHERE
                    (
                        status = 'pending'
                        AND available_at <= now()
                    )
                    OR (
                        status = 'running'
                        AND locked_at < now() - ($2::text || ' seconds')::interval
                    )
                ORDER BY available_at ASC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE ai_tool_tasks task
            SET status = 'running',
                attempts = attempts + 1,
                locked_at = now(),
                locked_by = $1,
                updated_at = now()
            FROM candidate
            WHERE task.id = candidate.id
            RETURNING task.id, task.run_id, task.tool_call, task.status, task.attempts,
                task.last_error, task.available_at, task.locked_at, task.locked_by,
                task.created_at, task.updated_at
            "#,
        )
        .bind(worker_id)
        .bind(lease_seconds)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(task_from_row).transpose()
    }

    pub async fn complete_tool_task(
        &self,
        task_id: Uuid,
        run_id: Uuid,
        tool_call_id: &str,
        result_content: &str,
    ) -> Result<bool, StoreError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            INSERT INTO ai_messages (run_id, role, content, tool_calls, tool_call_id, provider_metadata)
            VALUES ($1, $2, $3, '[]'::jsonb, $4, '{}'::jsonb)
            "#,
        )
        .bind(run_id)
        .bind(MessageRole::Tool.to_string())
        .bind(result_content)
        .bind(tool_call_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE ai_tool_tasks
            SET status = 'completed', locked_at = NULL, locked_by = NULL, updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(task_id)
        .execute(&mut *tx)
        .await?;

        let pending_count: i64 = sqlx::query_scalar(
            r#"
            SELECT count(*)
            FROM ai_tool_tasks
            WHERE run_id = $1 AND status IN ('pending', 'running')
            "#,
        )
        .bind(run_id)
        .fetch_one(&mut *tx)
        .await?;

        if pending_count == 0 {
            sqlx::query(
                r#"
                UPDATE ai_runs
                SET status = $2, updated_at = now()
                WHERE id = $1 AND status = $3
                "#,
            )
            .bind(run_id)
            .bind(RunStatus::Running.to_string())
            .bind(RunStatus::SuspendedForTool.to_string())
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(pending_count == 0)
    }

    pub async fn fail_tool_task(
        &self,
        task_id: Uuid,
        run_id: Uuid,
        error: &str,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            UPDATE ai_tool_tasks
            SET status = 'failed',
                last_error = $2,
                locked_at = NULL,
                locked_by = NULL,
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(task_id)
        .bind(error)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE ai_runs
            SET status = $2, last_error = $3, updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(run_id)
        .bind(RunStatus::Failed.to_string())
        .bind(error)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn retry_tool_task(
        &self,
        task_id: Uuid,
        delay_seconds: i64,
        error: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            UPDATE ai_tool_tasks
            SET status = 'pending',
                last_error = $3,
                available_at = now() + ($2::text || ' seconds')::interval,
                locked_at = NULL,
                locked_by = NULL,
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(task_id)
        .bind(delay_seconds)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn dead_letter_tool_task(
        &self,
        task_id: Uuid,
        run_id: Uuid,
        error: &str,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            UPDATE ai_tool_tasks
            SET status = 'dead',
                last_error = $2,
                locked_at = NULL,
                locked_by = NULL,
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(task_id)
        .bind(error)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE ai_runs
            SET status = $2, last_error = $3, updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(run_id)
        .bind(RunStatus::Failed.to_string())
        .bind(error)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

fn agent_from_row(row: sqlx::postgres::PgRow) -> Result<AgentSpec, StoreError> {
    let tools: Value = row.try_get("tools")?;
    let skills: Value = row.try_get("skills")?;
    Ok(AgentSpec {
        id: row.try_get("id")?,
        provider: row.try_get("provider")?,
        model: row.try_get("model")?,
        description: row.try_get("description")?,
        system_prompt: row.try_get("system_prompt")?,
        tools: serde_json::from_value(tools)?,
        skills: serde_json::from_value(skills)?,
    })
}

fn run_from_row(row: &sqlx::postgres::PgRow) -> Result<AgentRun, StoreError> {
    let status: String = row.try_get("status")?;
    Ok(AgentRun {
        id: row.try_get("id")?,
        agent_id: row.try_get("agent_id")?,
        user_id: row.try_get("user_id")?,
        target_id: row.try_get("target_id")?,
        status: RunStatus::from_str(&status)?,
        last_error: row.try_get("last_error")?,
        locked_at: row.try_get("locked_at")?,
        locked_by: row.try_get("locked_by")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn message_from_row(row: &sqlx::postgres::PgRow) -> Result<RunMessage, StoreError> {
    let role: String = row.try_get("role")?;
    let tool_calls: Value = row.try_get("tool_calls")?;
    Ok(RunMessage {
        id: row.try_get("id")?,
        run_id: row.try_get("run_id")?,
        role: MessageRole::from_str(&role)?,
        content: row.try_get("content")?,
        tool_calls: serde_json::from_value(tool_calls)?,
        tool_call_id: row.try_get("tool_call_id")?,
        provider_metadata: row.try_get("provider_metadata")?,
        created_at: row.try_get("created_at")?,
    })
}

fn task_from_row(row: &sqlx::postgres::PgRow) -> Result<ToolTask, StoreError> {
    let status: String = row.try_get("status")?;
    let tool_call: Value = row.try_get("tool_call")?;
    Ok(ToolTask {
        id: row.try_get("id")?,
        run_id: row.try_get("run_id")?,
        tool_call: serde_json::from_value(tool_call)?,
        status: ToolTaskStatus::from_str(&status)?,
        attempts: row.try_get("attempts")?,
        last_error: row.try_get("last_error")?,
        available_at: row.try_get("available_at")?,
        locked_at: row.try_get("locked_at")?,
        locked_by: row.try_get("locked_by")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn task_plan_run_from_row(row: &sqlx::postgres::PgRow) -> Result<TaskPlanRun, StoreError> {
    let status: String = row.try_get("status")?;
    let plan: Value = row.try_get("plan")?;
    Ok(TaskPlanRun {
        id: row.try_get("id")?,
        plan: serde_json::from_value(plan)?,
        status: PlanRunStatus::from_str(&status)?,
        result: row.try_get("result")?,
        last_error: row.try_get("last_error")?,
        parent_plan_run_id: row.try_get("parent_plan_run_id")?,
        parent_task_id: row.try_get("parent_task_id")?,
        parent_agent_run_id: row.try_get("parent_agent_run_id")?,
        parent_tool_call_id: row.try_get("parent_tool_call_id")?,
        parent_delivered_at: row.try_get("parent_delivered_at")?,
        user_id: row.try_get("user_id")?,
        target_id: row.try_get("target_id")?,
        locked_at: row.try_get("locked_at")?,
        locked_by: row.try_get("locked_by")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn task_run_from_row(row: &sqlx::postgres::PgRow) -> Result<TaskRun, StoreError> {
    let state: String = row.try_get("state")?;
    Ok(TaskRun {
        plan_run_id: row.try_get("plan_run_id")?,
        task_id: row.try_get("task_id")?,
        agent_run_id: row.try_get("agent_run_id")?,
        child_plan_run_id: row.try_get("child_plan_run_id")?,
        child_tool_call_id: row.try_get("child_tool_call_id")?,
        state: TaskState::from_str(&state)?,
        input: row.try_get("input")?,
        output: row.try_get("output")?,
        last_error: row.try_get("last_error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

#[async_trait::async_trait]
impl CinderStore for PostgresStore {
    async fn init_schema(&self) -> Result<(), CinderCoreError> {
        PostgresStore::init_schema(self).await.map_err(Into::into)
    }

    async fn upsert_agent(&self, spec: &AgentSpec) -> Result<(), CinderCoreError> {
        PostgresStore::upsert_agent(self, spec)
            .await
            .map_err(Into::into)
    }

    async fn get_agent(&self, agent_id: &str) -> Result<Option<AgentSpec>, CinderCoreError> {
        PostgresStore::get_agent(self, agent_id)
            .await
            .map_err(Into::into)
    }

    async fn list_agents(&self) -> Result<Vec<AgentSpec>, CinderCoreError> {
        PostgresStore::list_agents(self).await.map_err(Into::into)
    }

    async fn create_run(
        &self,
        agent_id: &str,
        user_id: Option<&str>,
        target_id: Option<&str>,
    ) -> Result<AgentRun, CinderCoreError> {
        PostgresStore::create_run(self, agent_id, user_id, target_id)
            .await
            .map_err(Into::into)
    }

    async fn get_run(&self, run_id: Uuid) -> Result<Option<AgentRun>, CinderCoreError> {
        PostgresStore::get_run(self, run_id)
            .await
            .map_err(Into::into)
    }

    async fn update_run_status(
        &self,
        run_id: Uuid,
        status: RunStatus,
        last_error: Option<&str>,
    ) -> Result<(), CinderCoreError> {
        PostgresStore::update_run_status(self, run_id, status, last_error)
            .await
            .map_err(Into::into)
    }

    async fn acquire_run_lock(
        &self,
        run_id: Uuid,
        owner: &str,
        lease_seconds: i64,
    ) -> Result<bool, CinderCoreError> {
        PostgresStore::acquire_run_lock(self, run_id, owner, lease_seconds)
            .await
            .map_err(Into::into)
    }

    async fn release_run_lock(&self, run_id: Uuid, owner: &str) -> Result<(), CinderCoreError> {
        PostgresStore::release_run_lock(self, run_id, owner)
            .await
            .map_err(Into::into)
    }

    async fn append_message(
        &self,
        run_id: Uuid,
        message: &Message,
    ) -> Result<RunMessage, CinderCoreError> {
        PostgresStore::append_message(self, run_id, message)
            .await
            .map_err(Into::into)
    }

    async fn list_messages(&self, run_id: Uuid) -> Result<Vec<RunMessage>, CinderCoreError> {
        PostgresStore::list_messages(self, run_id)
            .await
            .map_err(Into::into)
    }

    async fn enqueue_tool_task(
        &self,
        run_id: Uuid,
        tool_call: &ToolCall,
    ) -> Result<ToolTask, CinderCoreError> {
        PostgresStore::enqueue_tool_task(self, run_id, tool_call)
            .await
            .map_err(Into::into)
    }

    async fn claim_tool_task(
        &self,
        worker_id: &str,
        lease_seconds: i64,
    ) -> Result<Option<ToolTask>, CinderCoreError> {
        PostgresStore::claim_tool_task(self, worker_id, lease_seconds)
            .await
            .map_err(Into::into)
    }

    async fn complete_tool_task(
        &self,
        task_id: Uuid,
        run_id: Uuid,
        tool_call_id: &str,
        result_content: &str,
    ) -> Result<bool, CinderCoreError> {
        PostgresStore::complete_tool_task(self, task_id, run_id, tool_call_id, result_content)
            .await
            .map_err(Into::into)
    }

    async fn fail_tool_task(
        &self,
        task_id: Uuid,
        run_id: Uuid,
        error: &str,
    ) -> Result<(), CinderCoreError> {
        PostgresStore::fail_tool_task(self, task_id, run_id, error)
            .await
            .map_err(Into::into)
    }

    async fn retry_tool_task(
        &self,
        task_id: Uuid,
        delay_seconds: i64,
        error: &str,
    ) -> Result<(), CinderCoreError> {
        PostgresStore::retry_tool_task(self, task_id, delay_seconds, error)
            .await
            .map_err(Into::into)
    }

    async fn dead_letter_tool_task(
        &self,
        task_id: Uuid,
        run_id: Uuid,
        error: &str,
    ) -> Result<(), CinderCoreError> {
        PostgresStore::dead_letter_tool_task(self, task_id, run_id, error)
            .await
            .map_err(Into::into)
    }

    async fn create_task_plan_run(
        &self,
        plan: &TaskPlan,
        user_id: Option<&str>,
        target_id: Option<&str>,
        parent_plan_run_id: Option<Uuid>,
        parent_task_id: Option<&str>,
        parent_agent_run_id: Option<Uuid>,
        parent_tool_call_id: Option<&str>,
    ) -> Result<TaskPlanRun, CinderCoreError> {
        PostgresStore::create_task_plan_run(
            self,
            plan,
            user_id,
            target_id,
            parent_plan_run_id,
            parent_task_id,
            parent_agent_run_id,
            parent_tool_call_id,
        )
        .await
        .map_err(Into::into)
    }

    async fn get_task_plan_run(
        &self,
        plan_run_id: Uuid,
    ) -> Result<Option<TaskPlanRun>, CinderCoreError> {
        PostgresStore::get_task_plan_run(self, plan_run_id)
            .await
            .map_err(Into::into)
    }

    async fn list_task_runs(&self, plan_run_id: Uuid) -> Result<Vec<TaskRun>, CinderCoreError> {
        PostgresStore::list_task_runs(self, plan_run_id)
            .await
            .map_err(Into::into)
    }

    async fn get_task_run_by_agent_run(
        &self,
        agent_run_id: Uuid,
    ) -> Result<Option<TaskRun>, CinderCoreError> {
        PostgresStore::get_task_run_by_agent_run(self, agent_run_id)
            .await
            .map_err(Into::into)
    }

    async fn list_undelivered_task_plan_runs_by_parent_run(
        &self,
        parent_agent_run_id: Uuid,
    ) -> Result<Vec<TaskPlanRun>, CinderCoreError> {
        PostgresStore::list_undelivered_task_plan_runs_by_parent_run(self, parent_agent_run_id)
            .await
            .map_err(Into::into)
    }

    async fn mark_task_plan_parent_delivered(
        &self,
        plan_run_id: Uuid,
    ) -> Result<(), CinderCoreError> {
        PostgresStore::mark_task_plan_parent_delivered(self, plan_run_id)
            .await
            .map_err(Into::into)
    }

    async fn acquire_task_plan_lock(
        &self,
        plan_run_id: Uuid,
        owner: &str,
        lease_seconds: i64,
    ) -> Result<bool, CinderCoreError> {
        PostgresStore::acquire_task_plan_lock(self, plan_run_id, owner, lease_seconds)
            .await
            .map_err(Into::into)
    }

    async fn release_task_plan_lock(
        &self,
        plan_run_id: Uuid,
        owner: &str,
    ) -> Result<(), CinderCoreError> {
        PostgresStore::release_task_plan_lock(self, plan_run_id, owner)
            .await
            .map_err(Into::into)
    }

    async fn start_task_run(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
        agent_run_id: Uuid,
        input: &Value,
    ) -> Result<(), CinderCoreError> {
        PostgresStore::start_task_run(self, plan_run_id, task_id, agent_run_id, input)
            .await
            .map_err(Into::into)
    }

    async fn wait_task_run_for_child_plan(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
        child_plan_run_id: Uuid,
        tool_call_id: &str,
    ) -> Result<(), CinderCoreError> {
        PostgresStore::wait_task_run_for_child_plan(
            self,
            plan_run_id,
            task_id,
            child_plan_run_id,
            tool_call_id,
        )
        .await
        .map_err(Into::into)
    }

    async fn resume_task_run_after_child_plan(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
    ) -> Result<(), CinderCoreError> {
        PostgresStore::resume_task_run_after_child_plan(self, plan_run_id, task_id)
            .await
            .map_err(Into::into)
    }

    async fn succeed_task_run(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
        output: &Value,
    ) -> Result<(), CinderCoreError> {
        PostgresStore::succeed_task_run(self, plan_run_id, task_id, output)
            .await
            .map_err(Into::into)
    }

    async fn fail_task_run(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
        error: &str,
    ) -> Result<(), CinderCoreError> {
        PostgresStore::fail_task_run(self, plan_run_id, task_id, error)
            .await
            .map_err(Into::into)
    }

    async fn complete_task_plan_run(
        &self,
        plan_run_id: Uuid,
        result: &Value,
    ) -> Result<(), CinderCoreError> {
        PostgresStore::complete_task_plan_run(self, plan_run_id, result)
            .await
            .map_err(Into::into)
    }

    async fn fail_task_plan_run(
        &self,
        plan_run_id: Uuid,
        error: &str,
    ) -> Result<(), CinderCoreError> {
        PostgresStore::fail_task_plan_run(self, plan_run_id, error)
            .await
            .map_err(Into::into)
    }
}
