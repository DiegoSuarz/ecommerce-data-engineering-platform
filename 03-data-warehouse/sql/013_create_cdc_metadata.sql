-- ============================================================
-- E-Commerce Data Engineering Platform
-- Migration 013
-- Change Data Capture metadata and event persistence
-- ============================================================


CREATE SCHEMA IF NOT EXISTS cdc;


-- ============================================================
-- CDC Checkpoint
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.cdc_checkpoint
(
    pipeline_name       VARCHAR(100) NOT NULL,
    checkpoint_name     VARCHAR(100) NOT NULL,
    binlog_file         VARCHAR(255) NOT NULL,
    binlog_position     BIGINT NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL
                        DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_cdc_checkpoint
        PRIMARY KEY (
            pipeline_name,
            checkpoint_name
        ),

    CONSTRAINT chk_cdc_checkpoint_position
        CHECK (binlog_position >= 0)
);


-- ============================================================
-- CDC Batch
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.cdc_batch
(
    batch_id                    BIGINT GENERATED ALWAYS
                                AS IDENTITY PRIMARY KEY,

    run_id                      BIGINT NOT NULL,

    start_binlog_file           VARCHAR(255) NOT NULL,
    start_binlog_position       BIGINT NOT NULL,

    end_binlog_file             VARCHAR(255),
    end_binlog_position         BIGINT,

    transactions_processed      BIGINT NOT NULL DEFAULT 0,
    events_processed            BIGINT NOT NULL DEFAULT 0,

    insert_events               BIGINT NOT NULL DEFAULT 0,
    update_events               BIGINT NOT NULL DEFAULT 0,
    delete_events               BIGINT NOT NULL DEFAULT 0,

    created_at                  TIMESTAMPTZ NOT NULL
                                DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_cdc_batch_etl_run
        FOREIGN KEY (run_id)
        REFERENCES audit.etl_run (run_id),

    CONSTRAINT uq_cdc_batch_run_id
        UNIQUE (run_id),

    CONSTRAINT chk_cdc_batch_start_position
        CHECK (start_binlog_position >= 0),

    CONSTRAINT chk_cdc_batch_end_position
        CHECK (
            end_binlog_position IS NULL
            OR end_binlog_position >= 0
        ),

    CONSTRAINT chk_cdc_batch_transactions
        CHECK (transactions_processed >= 0),

    CONSTRAINT chk_cdc_batch_events
        CHECK (events_processed >= 0),

    CONSTRAINT chk_cdc_batch_insert_events
        CHECK (insert_events >= 0),

    CONSTRAINT chk_cdc_batch_update_events
        CHECK (update_events >= 0),

    CONSTRAINT chk_cdc_batch_delete_events
        CHECK (delete_events >= 0),

    CONSTRAINT chk_cdc_batch_end_coordinate
        CHECK (
            (
                end_binlog_file IS NULL
                AND end_binlog_position IS NULL
            )
            OR
            (
                end_binlog_file IS NOT NULL
                AND end_binlog_position IS NOT NULL
            )
        )
);


-- ============================================================
-- CDC Change Events
-- ============================================================

CREATE TABLE IF NOT EXISTS cdc.change_event
(
    event_id                BIGINT GENERATED ALWAYS
                            AS IDENTITY PRIMARY KEY,

    event_key               VARCHAR(500) NOT NULL,
    batch_id                BIGINT NOT NULL,

    operation               VARCHAR(10) NOT NULL,

    source_schema           VARCHAR(128) NOT NULL,
    source_table            VARCHAR(128) NOT NULL,

    primary_key             JSONB NOT NULL,

    before_values           JSONB,
    after_values            JSONB,

    binlog_file             VARCHAR(255) NOT NULL,
    event_end_position      BIGINT NOT NULL,
    row_index               INTEGER NOT NULL,

    transaction_id          BIGINT NOT NULL,
    commit_position         BIGINT NOT NULL,

    event_timestamp         TIMESTAMPTZ NOT NULL,
    commit_timestamp        TIMESTAMPTZ NOT NULL,
    captured_at             TIMESTAMPTZ NOT NULL
                            DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_change_event_cdc_batch
        FOREIGN KEY (batch_id)
        REFERENCES audit.cdc_batch (batch_id),

    CONSTRAINT uq_change_event_event_key
        UNIQUE (event_key),

    CONSTRAINT chk_change_event_operation
        CHECK (
            operation IN (
                'INSERT',
                'UPDATE',
                'DELETE'
            )
        ),

    CONSTRAINT chk_change_event_event_position
        CHECK (event_end_position >= 0),

    CONSTRAINT chk_change_event_row_index
        CHECK (row_index >= 0),

    CONSTRAINT chk_change_event_commit_position
        CHECK (commit_position >= 0),

    CONSTRAINT chk_change_event_values
        CHECK (
            (
                operation = 'INSERT'
                AND before_values IS NULL
                AND after_values IS NOT NULL
            )
            OR
            (
                operation = 'UPDATE'
                AND before_values IS NOT NULL
                AND after_values IS NOT NULL
            )
            OR
            (
                operation = 'DELETE'
                AND before_values IS NOT NULL
                AND after_values IS NULL
            )
        )
);


CREATE INDEX IF NOT EXISTS ix_change_event_batch_id
ON cdc.change_event (batch_id);


CREATE INDEX IF NOT EXISTS ix_change_event_source
ON cdc.change_event (
    source_schema,
    source_table
);


CREATE INDEX IF NOT EXISTS ix_change_event_commit_coordinate
ON cdc.change_event (
    binlog_file,
    commit_position
);
