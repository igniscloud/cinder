use cinder_core::{
    AgentRun, AgentSpec, Message, MessageRole, RunMessage, RunStatus, ToolCall, ToolTask,
    ToolTaskStatus,
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

#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
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
            INSERT INTO ai_agents (id, provider, model, system_prompt, tools, skills)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO UPDATE SET
                provider = EXCLUDED.provider,
                model = EXCLUDED.model,
                system_prompt = EXCLUDED.system_prompt,
                tools = EXCLUDED.tools,
                skills = EXCLUDED.skills,
                updated_at = now()
            "#,
        )
        .bind(&spec.id)
        .bind(&spec.provider)
        .bind(&spec.model)
        .bind(&spec.system_prompt)
        .bind(serde_json::to_value(&spec.tools)?)
        .bind(serde_json::to_value(&spec.skills)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_agent(&self, agent_id: &str) -> Result<Option<AgentSpec>, StoreError> {
        let row = sqlx::query(
            r#"
            SELECT id, provider, model, system_prompt, tools, skills
            FROM ai_agents
            WHERE id = $1
            "#,
        )
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(agent_from_row).transpose()
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
            INSERT INTO ai_messages (run_id, role, content, tool_calls, tool_call_id)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, run_id, role, content, tool_calls, tool_call_id, created_at
            "#,
        )
        .bind(run_id)
        .bind(message.role.to_string())
        .bind(&message.content)
        .bind(serde_json::to_value(&message.tool_calls)?)
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
            INSERT INTO ai_messages (run_id, role, content, tool_calls, tool_call_id)
            VALUES ($1, $2, $3, '[]'::jsonb, $4)
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
