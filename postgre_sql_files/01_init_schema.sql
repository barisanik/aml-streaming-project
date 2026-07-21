/*
    # ============================================================================ #
        DDL File: CREATE TABLES, SCHEMAS, ROLES AND GRANTS
    # ============================================================================ #
    Script Purpose: Initial DDL for AML/Fraud streaming platform

    WARNING:
    - Ensure that you have the necessary permissions to create databases and schemas on the PostgreSQL.
*/

/*  =========================================================
      1. SCHEMAS
    =========================================================
*/
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS ops;

/*  =========================================================
      2. TABLES
    =========================================================
*/
-- Transactions
CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id      UUID PRIMARY KEY,
    account_id          UUID NOT NULL,
    counterparty_id     UUID,
    amount              NUMERIC(18, 2) NOT NULL,
    currency            TEXT NOT NULL,
    txn_type            TEXT NOT NULL CHECK (
                             txn_type IN (
                                 'transfer_in', 'transfer_out',
                                 'card_payment', 'cash_deposit',
                                 'cash_withdrawal'
                             )
                         ),
    merchant_category   TEXT,
    channel             TEXT NOT NULL CHECK (
                             channel IN ('mobile', 'web', 'atm', 'branch')
                         ),
    city                TEXT,
    country             TEXT,
    event_time          TIMESTAMPTZ NOT NULL,
    produced_at         TIMESTAMPTZ NOT NULL,
    device_id           TEXT,
    inserted_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_transactions_account_event
    ON raw.transactions (account_id, event_time);

-- Alerts
CREATE TABLE IF NOT EXISTS raw.alerts (
    alert_id             UUID PRIMARY KEY,
    transaction_id       UUID NOT NULL,
    account_id           UUID NOT NULL,
    rule_id              TEXT NOT NULL,
    rule_name            TEXT NOT NULL,
    severity             TEXT NOT NULL CHECK (
                             severity IN ('low', 'medium', 'high')
                         ),
    window_summary       JSONB NOT NULL,
    event_time           TIMESTAMPTZ NOT NULL,
    alert_time           TIMESTAMPTZ NOT NULL,
    detection_latency_ms INTEGER NOT NULL,
    inserted_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_alerts_transaction_id
    ON raw.alerts (transaction_id);

-- Answer_key
CREATE TABLE IF NOT EXISTS public.answer_key (
    transaction_id  UUID PRIMARY KEY,
    scenario_id     UUID NOT NULL,
    scenario_type   TEXT NOT NULL CHECK (
                        scenario_type IN (
                            'structuring', 'smurfing', 'mule_fan_in',
                            'account_takeover', 'dormant_activation'
                        )
                    ),
    injected_at     TIMESTAMPTZ NOT NULL
);

-- Consumer Heartbeat
CREATE TABLE IF NOT EXISTS ops.consumer_heartbeat (
    consumer_group      TEXT NOT NULL,
    topic               TEXT NOT NULL,
    partition           INTEGER NOT NULL,
    committed_offset    BIGINT NOT NULL,
    messages_processed  BIGINT NOT NULL,
    heartbeat_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition)
);

/*  =========================================================
      3. ROLES
    =========================================================
    Passwords are set via set_role_password.py outside this file; store actual values only in .env.
*/

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_producer') THEN
        CREATE ROLE app_producer LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_consumer') THEN
        CREATE ROLE app_consumer LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_notifier') THEN
        CREATE ROLE app_notifier LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_dbt') THEN
        CREATE ROLE app_dbt LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_grafana') THEN
        CREATE ROLE app_grafana LOGIN;
    END IF;
END $$;

/*  =========================================================
      4. GRANTS (deny-by-default, then explicit grants)
    =========================================================
*/

-- Schema usage (needed just to "see" the schema)
GRANT USAGE ON SCHEMA raw, ops TO app_producer, app_consumer, app_notifier, app_dbt, app_grafana;
GRANT USAGE ON SCHEMA public TO app_producer, app_consumer, app_notifier, app_dbt, app_grafana;

-- app_producer: only writes answer_key
GRANT INSERT ON public.answer_key TO app_producer;

-- app_consumer: writes transactions/alerts/heartbeat
-- NO privilege of any kind on answer_key.
GRANT INSERT, SELECT ON raw.transactions, raw.alerts TO app_consumer;
GRANT INSERT, SELECT, UPDATE ON ops.consumer_heartbeat TO app_consumer;
REVOKE ALL ON public.answer_key FROM app_consumer;

-- app_notifier: only reads alerts to push webhook/SMTP
GRANT SELECT ON raw.alerts TO app_notifier;

-- app_dbt: needs read access to raw + answer_key (precision/recall model)
-- plus ability to build models in its own schemas.
GRANT SELECT ON raw.transactions, raw.alerts, public.answer_key, ops.consumer_heartbeat TO app_dbt;

-- app_grafana: read-only across everything dashboards need
GRANT SELECT ON raw.transactions, raw.alerts, ops.consumer_heartbeat TO app_grafana;
-- Grafana panels never expose answer_key
REVOKE ALL ON public.answer_key FROM app_grafana;
