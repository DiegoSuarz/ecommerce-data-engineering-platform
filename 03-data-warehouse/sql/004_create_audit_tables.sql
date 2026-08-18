-- ============================================================
-- E-Commerce Data Engineering Platform
-- ETL Audit Tables
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.etl_run
(
    run_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name   VARCHAR(100) NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at     TIMESTAMPTZ,
    status          VARCHAR(20) NOT NULL,
    rows_extracted  BIGINT NOT NULL DEFAULT 0,
    rows_loaded     BIGINT NOT NULL DEFAULT 0,
    rows_rejected   BIGINT NOT NULL DEFAULT 0,
    error_message   TEXT,

    CONSTRAINT chk_etl_run_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),

    CONSTRAINT chk_etl_run_rows_extracted
        CHECK (rows_extracted >= 0),

    CONSTRAINT chk_etl_run_rows_loaded
        CHECK (rows_loaded >= 0),

    CONSTRAINT chk_etl_run_rows_rejected
        CHECK (rows_rejected >= 0),

    CONSTRAINT chk_etl_run_finished_at
        CHECK (
            finished_at IS NULL
            OR finished_at >= started_at
        )
);
