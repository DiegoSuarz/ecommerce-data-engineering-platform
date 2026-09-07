-- ---------------------------------------------------------------------------
-- Migration 012
-- Harden ETL run idempotency for orchestrated pipeline executions.
-- ---------------------------------------------------------------------------

CREATE UNIQUE INDEX IF NOT EXISTS ux_etl_run_orchestrated_run
ON audit.etl_run
(
    pipeline_name,
    orchestrator,
    orchestrator_run_id
)
WHERE orchestrator IS NOT NULL
  AND orchestrator_run_id IS NOT NULL;
