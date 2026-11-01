/*
    # ============================================================================ #
        Migration File: GRAFANA ACCESS AND CONSUMER LAG
    # ============================================================================ #
    Script Purpose: Bring a database created with an older 01_init_schema.sql up to date
                    for the Grafana dashboards without dropping any data.

    Notes:
    - Every statement is idempotent, the file can be run more than once.
    - A fresh install with the current 01_init_schema.sql already contains these changes.
*/

-- Partition high watermark written by the consumer heartbeat, used for consumer lag
ALTER TABLE ops.consumer_heartbeat
    ADD COLUMN IF NOT EXISTS log_end_offset BIGINT;

-- Grafana panels and the no-data alert filter on recent inserts
CREATE INDEX IF NOT EXISTS idx_raw_transactions_inserted_at
    ON raw.transactions (inserted_at);

-- Schema usage only. dbt grants SELECT on the aggregated detection mart, other analytics tables stay hidden.
GRANT USAGE ON SCHEMA analytics TO app_grafana;
