-- ============================================================
-- E-Commerce Data Engineering Platform
-- ETL Run Orchestration Metadata
-- ============================================================

ALTER TABLE audit.etl_run
    ADD COLUMN IF NOT EXISTS orchestrator VARCHAR(50),
    ADD COLUMN IF NOT EXISTS orchestrator_run_id VARCHAR(250),
    ADD COLUMN IF NOT EXISTS orchestrator_task_id VARCHAR(250),
    ADD COLUMN IF NOT EXISTS orchestrator_try_number INTEGER;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_etl_run_orchestrator_try_number'
            AND conrelid = 'audit.etl_run'::regclass
    ) THEN
        ALTER TABLE audit.etl_run
            ADD CONSTRAINT chk_etl_run_orchestrator_try_number
            CHECK (
                orchestrator_try_number IS NULL
                OR orchestrator_try_number > 0
            );
    END IF;
END
$$;
