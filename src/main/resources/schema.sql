CREATE TABLE IF NOT EXISTS tasks (
    task_id VARCHAR(36) PRIMARY KEY,
    prompt TEXT NOT NULL,
    target_node VARCHAR(64),
    status VARCHAR(24) NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    node_id VARCHAR(64),
    output TEXT,
    thread_id VARCHAR(36),
    failure_stage VARCHAR(16),
    last_error VARCHAR(160),
    claim_token VARCHAR(36),
    lease_until BIGINT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS tasks_status_updated ON tasks(status, updated_at);
-- Additive migration: existing job records remain readable with unknown historical timings.
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS batch_id VARCHAR(36);
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS queued_at BIGINT;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS started_at BIGINT;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS inference_completed_at BIGINT;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS finished_at BIGINT;
CREATE INDEX IF NOT EXISTS tasks_batch_created ON tasks(batch_id, created_at);
CREATE TABLE IF NOT EXISTS outbox (
    event_id VARCHAR(36) PRIMARY KEY,
    task_id VARCHAR(36) NOT NULL REFERENCES tasks(task_id),
    topic VARCHAR(249) NOT NULL,
    payload TEXT NOT NULL,
    created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS outbox_created ON outbox(created_at);
CREATE TABLE IF NOT EXISTS coordination_lock (id VARCHAR(32) PRIMARY KEY);
INSERT INTO coordination_lock(id) VALUES ('coral') ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS task_evaluations (
    evaluation_id VARCHAR(36) PRIMARY KEY,
    task_id VARCHAR(36) NOT NULL REFERENCES tasks(task_id),
    task_attempt INTEGER NOT NULL,
    generation VARCHAR(64) NOT NULL,
    trigger_kind VARCHAR(16) NOT NULL,
    status VARCHAR(16) NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    requested_model VARCHAR(128) NOT NULL,
    rubric_version VARCHAR(64) NOT NULL,
    threshold DOUBLE PRECISION NOT NULL,
    source_prompt TEXT NOT NULL,
    source_output TEXT NOT NULL,
    claim_token VARCHAR(36),
    lease_until BIGINT,
    last_error VARCHAR(64),
    report TEXT,
    created_at BIGINT NOT NULL,
    started_at BIGINT,
    completed_at BIGINT,
    next_attempt_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS evaluations_task_generation ON task_evaluations(task_id,generation,created_at);
CREATE INDEX IF NOT EXISTS evaluations_pending ON task_evaluations(status,next_attempt_at);
CREATE TABLE IF NOT EXISTS workflows (
    workflow_id VARCHAR(36) PRIMARY KEY,
    prompt TEXT NOT NULL,
    target_node VARCHAR(64),
    model VARCHAR(128) NOT NULL,
    created_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS workflow_steps (
    workflow_id VARCHAR(36) NOT NULL REFERENCES workflows(workflow_id),
    stage VARCHAR(16) NOT NULL,
    task_id VARCHAR(36) NOT NULL UNIQUE REFERENCES tasks(task_id),
    provided BOOLEAN NOT NULL DEFAULT FALSE,
    source_thread_id VARCHAR(36),
    source_reader VARCHAR(16),
    source_snapshot TEXT,
    inference_calls INTEGER NOT NULL DEFAULT 0,
    model VARCHAR(128) NOT NULL,
    inference_millis BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY(workflow_id,stage)
);
ALTER TABLE workflow_steps ADD COLUMN IF NOT EXISTS system_prompt TEXT;
