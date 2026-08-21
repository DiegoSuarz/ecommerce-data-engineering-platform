-- ============================================================
-- E-Commerce Data Engineering Platform
-- Incremental Load Watermark Metadata
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.pipeline_watermark
(
    pipeline_name           VARCHAR(100) NOT NULL,
    watermark_name          VARCHAR(100) NOT NULL,
    watermark_timestamp     TIMESTAMPTZ NOT NULL,
    watermark_order_id      BIGINT NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_pipeline_watermark
        PRIMARY KEY (pipeline_name, watermark_name),

    CONSTRAINT chk_pipeline_watermark_order_id
        CHECK (watermark_order_id >= 0)
);
