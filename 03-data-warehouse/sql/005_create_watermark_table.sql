-- ============================================================
-- E-Commerce Data Engineering Platform
-- Incremental Load Watermark Metadata
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.pipeline_watermark
(
    pipeline_name    VARCHAR(100) NOT NULL,
    watermark_name   VARCHAR(100) NOT NULL,
    watermark_value  BIGINT NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_pipeline_watermark
        PRIMARY KEY (pipeline_name, watermark_name),

    CONSTRAINT chk_pipeline_watermark_value
        CHECK (watermark_value >= 0)
);
