CREATE TABLE IF NOT EXISTS cdc.raw_change_event (
    raw_event_id BIGINT GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    batch_id BIGINT NOT NULL
        REFERENCES audit.cdc_batch(batch_id),

    event_key TEXT NOT NULL
        UNIQUE,

    operation TEXT NOT NULL
        CHECK (
            operation IN (
                'INSERT',
                'UPDATE',
                'DELETE'
            )
        ),

    source_schema TEXT NOT NULL,
    source_table TEXT NOT NULL,

    before_values JSONB,
    after_values JSONB,

    binlog_file TEXT NOT NULL,

    event_end_position BIGINT NOT NULL
        CHECK (
            event_end_position >= 0
        ),

    row_index INTEGER NOT NULL
        CHECK (
            row_index >= 0
        ),

    transaction_id BIGINT NOT NULL,

    commit_position BIGINT NOT NULL
        CHECK (
            commit_position >= 0
        ),

    event_timestamp TIMESTAMPTZ NOT NULL,
    commit_timestamp TIMESTAMPTZ NOT NULL,

    captured_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);


CREATE INDEX IF NOT EXISTS
    idx_raw_change_event_batch_id
ON cdc.raw_change_event (
    batch_id
);


CREATE INDEX IF NOT EXISTS
    idx_raw_change_event_transaction
ON cdc.raw_change_event (
    binlog_file,
    transaction_id
);


CREATE TABLE IF NOT EXISTS cdc.transformed_event (
    transformed_event_id BIGINT
        GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    batch_id BIGINT NOT NULL
        REFERENCES audit.cdc_batch(batch_id),

    raw_event_id BIGINT NOT NULL
        REFERENCES cdc.raw_change_event(
            raw_event_id
        ),

    event_key TEXT NOT NULL
        UNIQUE,

    operation TEXT NOT NULL
        CHECK (
            operation IN (
                'INSERT',
                'UPDATE',
                'DELETE'
            )
        ),

    source_schema TEXT NOT NULL,
    source_table TEXT NOT NULL,

    primary_key JSONB NOT NULL,

    before_values JSONB,
    after_values JSONB,

    binlog_file TEXT NOT NULL,

    event_end_position BIGINT NOT NULL
        CHECK (
            event_end_position >= 0
        ),

    row_index INTEGER NOT NULL
        CHECK (
            row_index >= 0
        ),

    transaction_id BIGINT NOT NULL,

    commit_position BIGINT NOT NULL
        CHECK (
            commit_position >= 0
        ),

    event_timestamp TIMESTAMPTZ NOT NULL,
    commit_timestamp TIMESTAMPTZ NOT NULL,

    transformed_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_transformed_event_raw
        UNIQUE (
            raw_event_id
        ),

    CONSTRAINT ck_transformed_event_values
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


CREATE INDEX IF NOT EXISTS
    idx_transformed_event_batch_id
ON cdc.transformed_event (
    batch_id
);
