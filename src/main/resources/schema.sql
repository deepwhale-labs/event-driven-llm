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
