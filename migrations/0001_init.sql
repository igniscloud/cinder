CREATE TABLE IF NOT EXISTS ai_agents (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    system_prompt TEXT NOT NULL,
    tools JSONB NOT NULL DEFAULT '[]'::jsonb,
    skills JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE ai_agents ADD COLUMN IF NOT EXISTS skills JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE TABLE IF NOT EXISTS ai_runs (
    id UUID PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES ai_agents(id),
    user_id TEXT,
    target_id TEXT,
    status TEXT NOT NULL,
    last_error TEXT,
    locked_at TIMESTAMPTZ,
    locked_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE ai_runs ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;
ALTER TABLE ai_runs ADD COLUMN IF NOT EXISTS locked_by TEXT;

CREATE INDEX IF NOT EXISTS idx_ai_runs_status ON ai_runs(status);
CREATE INDEX IF NOT EXISTS idx_ai_runs_agent_id ON ai_runs(agent_id);
CREATE INDEX IF NOT EXISTS idx_ai_runs_lock ON ai_runs(locked_at);

CREATE TABLE IF NOT EXISTS ai_messages (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES ai_runs(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    tool_calls JSONB NOT NULL DEFAULT '[]'::jsonb,
    tool_call_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ai_messages_run_id_id ON ai_messages(run_id, id);

CREATE TABLE IF NOT EXISTS ai_tool_tasks (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES ai_runs(id) ON DELETE CASCADE,
    tool_call JSONB NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at TIMESTAMPTZ,
    locked_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ai_tool_tasks_claim
    ON ai_tool_tasks(status, available_at, locked_at);
