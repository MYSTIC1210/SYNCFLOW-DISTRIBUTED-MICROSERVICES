-- migrations/001_init.sql
-- Run once against the syncflow database before starting the services.

CREATE TABLE IF NOT EXISTS task_results (
    id           TEXT        PRIMARY KEY,
    type         TEXT        NOT NULL DEFAULT '',
    status       TEXT        NOT NULL DEFAULT 'pending',
    output       TEXT        NOT NULL DEFAULT '',
    duration_ms  BIGINT      NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_task_results_status     ON task_results (status);
CREATE INDEX IF NOT EXISTS idx_task_results_created_at ON task_results (created_at DESC);
