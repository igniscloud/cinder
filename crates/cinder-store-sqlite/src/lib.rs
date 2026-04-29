use chrono::{DateTime, Utc};
use cinder_core::{
    taskplan::{PlanRunStatus, TaskPlan, TaskPlanRun, TaskRun, TaskState},
    AgentRun, AgentSpec, CinderCoreError, CinderStore, Message, MessageRole, RunMessage, RunStatus,
    ToolCall, ToolTask, ToolTaskStatus,
};
use serde_json::Value;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
};
use std::path::Path;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Uuid(#[from] uuid::Error),
    #[error(transparent)]
    Chrono(#[from] chrono::ParseError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Core(#[from] cinder_core::CinderCoreError),
}

impl From<StoreError> for CinderCoreError {
    fn from(value: StoreError) -> Self {
        CinderCoreError::Store(value.to_string())
    }
}

#[derive(Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    pub async fn connect_from_config(
        config: &cinder_core::SqliteStoreConfig,
    ) -> Result<Self, StoreError> {
        Self::connect(&config.path).await
    }

    pub async fn connect(path: &str) -> Result<Self, StoreError> {
        if path != ":memory:" {
            if let Some(parent) = Path::new(path)
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent)?;
            }
        }

        let options = if path == ":memory:" {
            SqliteConnectOptions::from_str("sqlite::memory:")?
        } else if path.starts_with("sqlite:") {
            SqliteConnectOptions::from_str(path)?.create_if_missing(true)
        } else {
            SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true)
        };
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&pool)
            .await?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn init_schema(&self) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
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
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(id) DO UPDATE SET
                provider = excluded.provider,
                model = excluded.model,
                description = excluded.description,
                system_prompt = excluded.system_prompt,
                tools = excluded.tools,
                skills = excluded.skills,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            "#,
        )
        .bind(&spec.id)
        .bind(&spec.provider)
        .bind(&spec.model)
        .bind(&spec.description)
        .bind(&spec.system_prompt)
        .bind(serde_json::to_string(&spec.tools)?)
        .bind(serde_json::to_string(&spec.skills)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_agent(&self, agent_id: &str) -> Result<Option<AgentSpec>, StoreError> {
        let row = sqlx::query(
            "SELECT id, provider, model, description, system_prompt, tools, skills FROM ai_agents WHERE id = ?1",
        )
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(agent_from_row).transpose()
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
        rows.iter().map(agent_from_row).collect()
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
            VALUES (?1, ?2, ?3, ?4, ?5)
            RETURNING id, agent_id, user_id, target_id, status, last_error,
                locked_at, locked_by, created_at, updated_at
            "#,
        )
        .bind(run_id.to_string())
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
            WHERE id = ?1
            "#,
        )
        .bind(run_id.to_string())
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
            "UPDATE ai_runs SET status = ?2, last_error = ?3, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?1",
        )
        .bind(run_id.to_string())
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
        let row: Option<(String,)> = sqlx::query_as(
            r#"
            UPDATE ai_runs
            SET locked_by = ?2, locked_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'), updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?1
              AND (
                locked_at IS NULL
                OR locked_at < strftime('%Y-%m-%dT%H:%M:%fZ','now', '-' || ?3 || ' seconds')
                OR locked_by = ?2
              )
            RETURNING id
            "#,
        )
        .bind(run_id.to_string())
        .bind(owner)
        .bind(lease_seconds)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    pub async fn release_run_lock(&self, run_id: Uuid, owner: &str) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE ai_runs SET locked_by = NULL, locked_at = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?1 AND locked_by = ?2",
        )
        .bind(run_id.to_string())
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
            INSERT INTO ai_messages (run_id, role, content, tool_calls, tool_call_id)
            VALUES (?1, ?2, ?3, ?4, ?5)
            RETURNING id, run_id, role, content, tool_calls, tool_call_id, created_at
            "#,
        )
        .bind(run_id.to_string())
        .bind(message.role.to_string())
        .bind(&message.content)
        .bind(serde_json::to_string(&message.tool_calls)?)
        .bind(&message.tool_call_id)
        .fetch_one(&self.pool)
        .await?;
        message_from_row(&row)
    }

    pub async fn list_messages(&self, run_id: Uuid) -> Result<Vec<RunMessage>, StoreError> {
        let rows = sqlx::query(
            r#"
            SELECT id, run_id, role, content, tool_calls, tool_call_id, created_at
            FROM ai_messages
            WHERE run_id = ?1
            ORDER BY id ASC
            "#,
        )
        .bind(run_id.to_string())
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
            VALUES (?1, ?2, ?3, ?4)
            RETURNING id, run_id, tool_call, status, attempts, last_error,
                available_at, locked_at, locked_by, created_at, updated_at
            "#,
        )
        .bind(task_id.to_string())
        .bind(run_id.to_string())
        .bind(serde_json::to_string(tool_call)?)
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
            UPDATE ai_tool_tasks
            SET status = 'running',
                attempts = attempts + 1,
                locked_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                locked_by = ?1,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = (
                SELECT id
                FROM ai_tool_tasks
                WHERE
                    (
                        status = 'pending'
                        AND available_at <= strftime('%Y-%m-%dT%H:%M:%fZ','now')
                    )
                    OR (
                        status = 'running'
                        AND locked_at < strftime('%Y-%m-%dT%H:%M:%fZ','now', '-' || ?2 || ' seconds')
                    )
                ORDER BY available_at ASC, created_at ASC
                LIMIT 1
            )
            RETURNING id, run_id, tool_call, status, attempts, last_error,
                available_at, locked_at, locked_by, created_at, updated_at
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
            INSERT INTO ai_messages (run_id, role, content, tool_calls, tool_call_id)
            VALUES (?1, ?2, ?3, '[]', ?4)
            "#,
        )
        .bind(run_id.to_string())
        .bind(MessageRole::Tool.to_string())
        .bind(result_content)
        .bind(tool_call_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE ai_tool_tasks SET status = 'completed', locked_at = NULL, locked_by = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?1",
        )
        .bind(task_id.to_string())
        .execute(&mut *tx)
        .await?;

        let pending_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM ai_tool_tasks WHERE run_id = ?1 AND status IN ('pending', 'running')",
        )
        .bind(run_id.to_string())
        .fetch_one(&mut *tx)
        .await?;

        if pending_count == 0 {
            sqlx::query(
                "UPDATE ai_runs SET status = ?2, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?1 AND status = ?3",
            )
            .bind(run_id.to_string())
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
        self.dead_letter_tool_task(task_id, run_id, error).await
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
                last_error = ?3,
                available_at = strftime('%Y-%m-%dT%H:%M:%fZ','now', '+' || ?2 || ' seconds'),
                locked_at = NULL,
                locked_by = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?1
            "#,
        )
        .bind(task_id.to_string())
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
            "UPDATE ai_tool_tasks SET status = 'dead', last_error = ?2, locked_at = NULL, locked_by = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?1",
        )
        .bind(task_id.to_string())
        .bind(error)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE ai_runs SET status = ?2, last_error = ?3, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?1",
        )
        .bind(run_id.to_string())
        .bind(RunStatus::Failed.to_string())
        .bind(error)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn create_task_plan_run(
        &self,
        plan: &TaskPlan,
        user_id: Option<&str>,
        target_id: Option<&str>,
        parent_plan_run_id: Option<Uuid>,
        parent_task_id: Option<&str>,
    ) -> Result<TaskPlanRun, StoreError> {
        cinder_core::validate_plan(plan)?;
        let plan_run_id = Uuid::new_v4();
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            INSERT INTO ai_task_plan_runs (
                id, plan, root_task_id, status, parent_plan_run_id,
                parent_task_id, user_id, target_id
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            RETURNING id, plan, status, result, last_error, parent_plan_run_id,
                parent_task_id, user_id, target_id, locked_at, locked_by,
                created_at, updated_at
            "#,
        )
        .bind(plan_run_id.to_string())
        .bind(serde_json::to_string(plan)?)
        .bind(&plan.root_task_id)
        .bind(PlanRunStatus::Running.to_string())
        .bind(parent_plan_run_id.map(|id| id.to_string()))
        .bind(parent_task_id)
        .bind(user_id)
        .bind(target_id)
        .fetch_one(&mut *tx)
        .await?;

        for task in &plan.tasks {
            sqlx::query(
                r#"
                INSERT INTO ai_task_plan_task_runs (plan_run_id, task_id, state, input)
                VALUES (?1, ?2, ?3, ?4)
                "#,
            )
            .bind(plan_run_id.to_string())
            .bind(&task.id)
            .bind(TaskState::Queued.to_string())
            .bind(serde_json::to_string(&task.input)?)
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
                parent_task_id, user_id, target_id, locked_at, locked_by,
                created_at, updated_at
            FROM ai_task_plan_runs
            WHERE id = ?1
            "#,
        )
        .bind(plan_run_id.to_string())
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
            WHERE plan_run_id = ?1
            ORDER BY created_at ASC, task_id ASC
            "#,
        )
        .bind(plan_run_id.to_string())
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
            WHERE agent_run_id = ?1
            "#,
        )
        .bind(agent_run_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(task_run_from_row).transpose()
    }

    pub async fn acquire_task_plan_lock(
        &self,
        plan_run_id: Uuid,
        owner: &str,
        lease_seconds: i64,
    ) -> Result<bool, StoreError> {
        let row: Option<(String,)> = sqlx::query_as(
            r#"
            UPDATE ai_task_plan_runs
            SET locked_by = ?2, locked_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'), updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?1
              AND (
                locked_at IS NULL
                OR locked_at < strftime('%Y-%m-%dT%H:%M:%fZ','now', '-' || ?3 || ' seconds')
                OR locked_by = ?2
              )
            RETURNING id
            "#,
        )
        .bind(plan_run_id.to_string())
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
            "UPDATE ai_task_plan_runs SET locked_by = NULL, locked_at = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?1 AND locked_by = ?2",
        )
        .bind(plan_run_id.to_string())
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
            SET state = ?3, agent_run_id = ?4, input = ?5,
                child_plan_run_id = NULL,
                child_tool_call_id = NULL,
                last_error = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE plan_run_id = ?1 AND task_id = ?2
            "#,
        )
        .bind(plan_run_id.to_string())
        .bind(task_id)
        .bind(TaskState::Running.to_string())
        .bind(agent_run_id.to_string())
        .bind(serde_json::to_string(input)?)
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
            SET state = ?3,
                child_plan_run_id = ?4,
                child_tool_call_id = ?5,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE plan_run_id = ?1 AND task_id = ?2
            "#,
        )
        .bind(plan_run_id.to_string())
        .bind(task_id)
        .bind(TaskState::WaitingChildPlan.to_string())
        .bind(child_plan_run_id.to_string())
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
            SET state = ?3,
                child_plan_run_id = NULL,
                child_tool_call_id = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE plan_run_id = ?1 AND task_id = ?2
            "#,
        )
        .bind(plan_run_id.to_string())
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
            "UPDATE ai_task_plan_task_runs SET state = ?3, output = ?4, child_plan_run_id = NULL, child_tool_call_id = NULL, last_error = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE plan_run_id = ?1 AND task_id = ?2",
        )
        .bind(plan_run_id.to_string())
        .bind(task_id)
        .bind(TaskState::Succeeded.to_string())
        .bind(serde_json::to_string(output)?)
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
            "UPDATE ai_task_plan_task_runs SET state = ?3, child_plan_run_id = NULL, child_tool_call_id = NULL, last_error = ?4, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE plan_run_id = ?1 AND task_id = ?2",
        )
        .bind(plan_run_id.to_string())
        .bind(task_id)
        .bind(TaskState::Failed.to_string())
        .bind(error)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE ai_task_plan_runs SET status = ?2, last_error = ?3, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?1",
        )
        .bind(plan_run_id.to_string())
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
            "UPDATE ai_task_plan_runs SET status = ?2, result = ?3, last_error = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?1",
        )
        .bind(plan_run_id.to_string())
        .bind(PlanRunStatus::Completed.to_string())
        .bind(serde_json::to_string(result)?)
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
            "UPDATE ai_task_plan_runs SET status = ?2, last_error = ?3, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?1",
        )
        .bind(plan_run_id.to_string())
        .bind(PlanRunStatus::Failed.to_string())
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

const INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS ai_agents (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    system_prompt TEXT NOT NULL,
    tools TEXT NOT NULL DEFAULT '[]',
    skills TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ai_runs (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES ai_agents(id),
    user_id TEXT,
    target_id TEXT,
    status TEXT NOT NULL,
    last_error TEXT,
    locked_at TEXT,
    locked_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ai_runs_status ON ai_runs(status);
CREATE INDEX IF NOT EXISTS idx_ai_runs_agent_id ON ai_runs(agent_id);
CREATE INDEX IF NOT EXISTS idx_ai_runs_lock ON ai_runs(locked_at);

CREATE TABLE IF NOT EXISTS ai_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES ai_runs(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    tool_calls TEXT NOT NULL DEFAULT '[]',
    tool_call_id TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ai_messages_run_id_id ON ai_messages(run_id, id);

CREATE TABLE IF NOT EXISTS ai_tool_tasks (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ai_runs(id) ON DELETE CASCADE,
    tool_call TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    available_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    locked_at TEXT,
    locked_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ai_tool_tasks_claim
    ON ai_tool_tasks(status, available_at, locked_at);

CREATE TABLE IF NOT EXISTS ai_task_plan_runs (
    id TEXT PRIMARY KEY,
    plan TEXT NOT NULL,
    root_task_id TEXT NOT NULL,
    status TEXT NOT NULL,
    result TEXT,
    last_error TEXT,
    parent_plan_run_id TEXT REFERENCES ai_task_plan_runs(id) ON DELETE CASCADE,
    parent_task_id TEXT,
    user_id TEXT,
    target_id TEXT,
    locked_at TEXT,
    locked_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ai_task_plan_runs_status ON ai_task_plan_runs(status);
CREATE INDEX IF NOT EXISTS idx_ai_task_plan_runs_parent
    ON ai_task_plan_runs(parent_plan_run_id, parent_task_id);
CREATE INDEX IF NOT EXISTS idx_ai_task_plan_runs_lock ON ai_task_plan_runs(locked_at);

CREATE TABLE IF NOT EXISTS ai_task_plan_task_runs (
    plan_run_id TEXT NOT NULL REFERENCES ai_task_plan_runs(id) ON DELETE CASCADE,
    task_id TEXT NOT NULL,
    agent_run_id TEXT REFERENCES ai_runs(id) ON DELETE SET NULL,
    child_plan_run_id TEXT REFERENCES ai_task_plan_runs(id) ON DELETE SET NULL,
    child_tool_call_id TEXT,
    state TEXT NOT NULL,
    input TEXT NOT NULL DEFAULT '{}',
    output TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (plan_run_id, task_id)
);

CREATE INDEX IF NOT EXISTS idx_ai_task_plan_task_runs_state
    ON ai_task_plan_task_runs(plan_run_id, state);
CREATE INDEX IF NOT EXISTS idx_ai_task_plan_task_runs_agent_run
    ON ai_task_plan_task_runs(agent_run_id);
CREATE INDEX IF NOT EXISTS idx_ai_task_plan_task_runs_child_plan
    ON ai_task_plan_task_runs(child_plan_run_id);
"#;

fn agent_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<AgentSpec, StoreError> {
    Ok(AgentSpec {
        id: row.try_get("id")?,
        provider: row.try_get("provider")?,
        model: row.try_get("model")?,
        description: row.try_get("description")?,
        system_prompt: row.try_get("system_prompt")?,
        tools: serde_json::from_str(&row.try_get::<String, _>("tools")?)?,
        skills: serde_json::from_str(&row.try_get::<String, _>("skills")?)?,
    })
}

fn run_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<AgentRun, StoreError> {
    let status: String = row.try_get("status")?;
    Ok(AgentRun {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        agent_id: row.try_get("agent_id")?,
        user_id: row.try_get("user_id")?,
        target_id: row.try_get("target_id")?,
        status: RunStatus::from_str(&status)?,
        last_error: row.try_get("last_error")?,
        locked_at: parse_optional_time(row.try_get("locked_at")?)?,
        locked_by: row.try_get("locked_by")?,
        created_at: parse_time(row.try_get("created_at")?)?,
        updated_at: parse_time(row.try_get("updated_at")?)?,
    })
}

fn message_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<RunMessage, StoreError> {
    let role: String = row.try_get("role")?;
    Ok(RunMessage {
        id: row.try_get("id")?,
        run_id: parse_uuid(row.try_get::<String, _>("run_id")?)?,
        role: MessageRole::from_str(&role)?,
        content: row.try_get("content")?,
        tool_calls: serde_json::from_str(&row.try_get::<String, _>("tool_calls")?)?,
        tool_call_id: row.try_get("tool_call_id")?,
        created_at: parse_time(row.try_get("created_at")?)?,
    })
}

fn task_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<ToolTask, StoreError> {
    let status: String = row.try_get("status")?;
    Ok(ToolTask {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        run_id: parse_uuid(row.try_get::<String, _>("run_id")?)?,
        tool_call: serde_json::from_str(&row.try_get::<String, _>("tool_call")?)?,
        status: ToolTaskStatus::from_str(&status)?,
        attempts: row.try_get("attempts")?,
        last_error: row.try_get("last_error")?,
        available_at: parse_time(row.try_get("available_at")?)?,
        locked_at: parse_optional_time(row.try_get("locked_at")?)?,
        locked_by: row.try_get("locked_by")?,
        created_at: parse_time(row.try_get("created_at")?)?,
        updated_at: parse_time(row.try_get("updated_at")?)?,
    })
}

fn task_plan_run_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<TaskPlanRun, StoreError> {
    let status: String = row.try_get("status")?;
    Ok(TaskPlanRun {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        plan: serde_json::from_str(&row.try_get::<String, _>("plan")?)?,
        status: PlanRunStatus::from_str(&status)?,
        result: parse_optional_json(row.try_get("result")?)?,
        last_error: row.try_get("last_error")?,
        parent_plan_run_id: parse_optional_uuid(row.try_get("parent_plan_run_id")?)?,
        parent_task_id: row.try_get("parent_task_id")?,
        user_id: row.try_get("user_id")?,
        target_id: row.try_get("target_id")?,
        locked_at: parse_optional_time(row.try_get("locked_at")?)?,
        locked_by: row.try_get("locked_by")?,
        created_at: parse_time(row.try_get("created_at")?)?,
        updated_at: parse_time(row.try_get("updated_at")?)?,
    })
}

fn task_run_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<TaskRun, StoreError> {
    let state: String = row.try_get("state")?;
    Ok(TaskRun {
        plan_run_id: parse_uuid(row.try_get::<String, _>("plan_run_id")?)?,
        task_id: row.try_get("task_id")?,
        agent_run_id: parse_optional_uuid(row.try_get("agent_run_id")?)?,
        child_plan_run_id: parse_optional_uuid(row.try_get("child_plan_run_id")?)?,
        child_tool_call_id: row.try_get("child_tool_call_id")?,
        state: TaskState::from_str(&state)?,
        input: serde_json::from_str(&row.try_get::<String, _>("input")?)?,
        output: parse_optional_json(row.try_get("output")?)?,
        last_error: row.try_get("last_error")?,
        created_at: parse_time(row.try_get("created_at")?)?,
        updated_at: parse_time(row.try_get("updated_at")?)?,
    })
}

fn parse_uuid(value: String) -> Result<Uuid, StoreError> {
    Ok(Uuid::parse_str(&value)?)
}

fn parse_optional_uuid(value: Option<String>) -> Result<Option<Uuid>, StoreError> {
    value.map(parse_uuid).transpose()
}

fn parse_optional_json(value: Option<String>) -> Result<Option<Value>, StoreError> {
    value
        .map(|value| serde_json::from_str(&value).map_err(StoreError::from))
        .transpose()
}

fn parse_time(value: String) -> Result<DateTime<Utc>, StoreError> {
    let value = if value.contains('T') {
        value
    } else {
        format!("{}Z", value.replace(' ', "T"))
    };
    Ok(DateTime::parse_from_rfc3339(&value)?.with_timezone(&Utc))
}

fn parse_optional_time(value: Option<String>) -> Result<Option<DateTime<Utc>>, StoreError> {
    value.map(parse_time).transpose()
}

#[async_trait::async_trait]
impl CinderStore for SqliteStore {
    async fn init_schema(&self) -> Result<(), CinderCoreError> {
        SqliteStore::init_schema(self).await.map_err(Into::into)
    }

    async fn upsert_agent(&self, spec: &AgentSpec) -> Result<(), CinderCoreError> {
        SqliteStore::upsert_agent(self, spec)
            .await
            .map_err(Into::into)
    }

    async fn get_agent(&self, agent_id: &str) -> Result<Option<AgentSpec>, CinderCoreError> {
        SqliteStore::get_agent(self, agent_id)
            .await
            .map_err(Into::into)
    }

    async fn list_agents(&self) -> Result<Vec<AgentSpec>, CinderCoreError> {
        SqliteStore::list_agents(self).await.map_err(Into::into)
    }

    async fn create_run(
        &self,
        agent_id: &str,
        user_id: Option<&str>,
        target_id: Option<&str>,
    ) -> Result<AgentRun, CinderCoreError> {
        SqliteStore::create_run(self, agent_id, user_id, target_id)
            .await
            .map_err(Into::into)
    }

    async fn get_run(&self, run_id: Uuid) -> Result<Option<AgentRun>, CinderCoreError> {
        SqliteStore::get_run(self, run_id).await.map_err(Into::into)
    }

    async fn update_run_status(
        &self,
        run_id: Uuid,
        status: RunStatus,
        last_error: Option<&str>,
    ) -> Result<(), CinderCoreError> {
        SqliteStore::update_run_status(self, run_id, status, last_error)
            .await
            .map_err(Into::into)
    }

    async fn acquire_run_lock(
        &self,
        run_id: Uuid,
        owner: &str,
        lease_seconds: i64,
    ) -> Result<bool, CinderCoreError> {
        SqliteStore::acquire_run_lock(self, run_id, owner, lease_seconds)
            .await
            .map_err(Into::into)
    }

    async fn release_run_lock(&self, run_id: Uuid, owner: &str) -> Result<(), CinderCoreError> {
        SqliteStore::release_run_lock(self, run_id, owner)
            .await
            .map_err(Into::into)
    }

    async fn append_message(
        &self,
        run_id: Uuid,
        message: &Message,
    ) -> Result<RunMessage, CinderCoreError> {
        SqliteStore::append_message(self, run_id, message)
            .await
            .map_err(Into::into)
    }

    async fn list_messages(&self, run_id: Uuid) -> Result<Vec<RunMessage>, CinderCoreError> {
        SqliteStore::list_messages(self, run_id)
            .await
            .map_err(Into::into)
    }

    async fn enqueue_tool_task(
        &self,
        run_id: Uuid,
        tool_call: &ToolCall,
    ) -> Result<ToolTask, CinderCoreError> {
        SqliteStore::enqueue_tool_task(self, run_id, tool_call)
            .await
            .map_err(Into::into)
    }

    async fn claim_tool_task(
        &self,
        worker_id: &str,
        lease_seconds: i64,
    ) -> Result<Option<ToolTask>, CinderCoreError> {
        SqliteStore::claim_tool_task(self, worker_id, lease_seconds)
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
        SqliteStore::complete_tool_task(self, task_id, run_id, tool_call_id, result_content)
            .await
            .map_err(Into::into)
    }

    async fn fail_tool_task(
        &self,
        task_id: Uuid,
        run_id: Uuid,
        error: &str,
    ) -> Result<(), CinderCoreError> {
        SqliteStore::fail_tool_task(self, task_id, run_id, error)
            .await
            .map_err(Into::into)
    }

    async fn retry_tool_task(
        &self,
        task_id: Uuid,
        delay_seconds: i64,
        error: &str,
    ) -> Result<(), CinderCoreError> {
        SqliteStore::retry_tool_task(self, task_id, delay_seconds, error)
            .await
            .map_err(Into::into)
    }

    async fn dead_letter_tool_task(
        &self,
        task_id: Uuid,
        run_id: Uuid,
        error: &str,
    ) -> Result<(), CinderCoreError> {
        SqliteStore::dead_letter_tool_task(self, task_id, run_id, error)
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
    ) -> Result<TaskPlanRun, CinderCoreError> {
        SqliteStore::create_task_plan_run(
            self,
            plan,
            user_id,
            target_id,
            parent_plan_run_id,
            parent_task_id,
        )
        .await
        .map_err(Into::into)
    }

    async fn get_task_plan_run(
        &self,
        plan_run_id: Uuid,
    ) -> Result<Option<TaskPlanRun>, CinderCoreError> {
        SqliteStore::get_task_plan_run(self, plan_run_id)
            .await
            .map_err(Into::into)
    }

    async fn list_task_runs(&self, plan_run_id: Uuid) -> Result<Vec<TaskRun>, CinderCoreError> {
        SqliteStore::list_task_runs(self, plan_run_id)
            .await
            .map_err(Into::into)
    }

    async fn get_task_run_by_agent_run(
        &self,
        agent_run_id: Uuid,
    ) -> Result<Option<TaskRun>, CinderCoreError> {
        SqliteStore::get_task_run_by_agent_run(self, agent_run_id)
            .await
            .map_err(Into::into)
    }

    async fn acquire_task_plan_lock(
        &self,
        plan_run_id: Uuid,
        owner: &str,
        lease_seconds: i64,
    ) -> Result<bool, CinderCoreError> {
        SqliteStore::acquire_task_plan_lock(self, plan_run_id, owner, lease_seconds)
            .await
            .map_err(Into::into)
    }

    async fn release_task_plan_lock(
        &self,
        plan_run_id: Uuid,
        owner: &str,
    ) -> Result<(), CinderCoreError> {
        SqliteStore::release_task_plan_lock(self, plan_run_id, owner)
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
        SqliteStore::start_task_run(self, plan_run_id, task_id, agent_run_id, input)
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
        SqliteStore::wait_task_run_for_child_plan(
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
        SqliteStore::resume_task_run_after_child_plan(self, plan_run_id, task_id)
            .await
            .map_err(Into::into)
    }

    async fn succeed_task_run(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
        output: &Value,
    ) -> Result<(), CinderCoreError> {
        SqliteStore::succeed_task_run(self, plan_run_id, task_id, output)
            .await
            .map_err(Into::into)
    }

    async fn fail_task_run(
        &self,
        plan_run_id: Uuid,
        task_id: &str,
        error: &str,
    ) -> Result<(), CinderCoreError> {
        SqliteStore::fail_task_run(self, plan_run_id, task_id, error)
            .await
            .map_err(Into::into)
    }

    async fn complete_task_plan_run(
        &self,
        plan_run_id: Uuid,
        result: &Value,
    ) -> Result<(), CinderCoreError> {
        SqliteStore::complete_task_plan_run(self, plan_run_id, result)
            .await
            .map_err(Into::into)
    }

    async fn fail_task_plan_run(
        &self,
        plan_run_id: Uuid,
        error: &str,
    ) -> Result<(), CinderCoreError> {
        SqliteStore::fail_task_plan_run(self, plan_run_id, error)
            .await
            .map_err(Into::into)
    }
}
