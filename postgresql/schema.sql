-- ─────────────────────────────────────────────────────────────────
--  Intraday Liquidity Monitoring Pipeline — PostgreSQL Schema
--  Flink writes via TwoPhaseCommitSinkFunction (exactly-once)
-- ─────────────────────────────────────────────────────────────────

-- ──────────────────────────────────────────
--  1. LIQUIDITY POSITIONS
--     Running balance per (currency, settlement_account).
--     Flink LiquidityPositionEngine upserts here on every payment.
-- ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS liquidity_positions (
    id                  BIGSERIAL       PRIMARY KEY,
    currency            VARCHAR(3)      NOT NULL,
    settlement_account  VARCHAR(50)     NOT NULL,
    available_balance   NUMERIC(20, 2)  NOT NULL,
    reserved_balance    NUMERIC(20, 2)  NOT NULL DEFAULT 0,
    total_inbound       NUMERIC(20, 2)  NOT NULL DEFAULT 0,
    total_outbound      NUMERIC(20, 2)  NOT NULL DEFAULT 0,
    payment_count       INTEGER         NOT NULL DEFAULT 0,
    last_payment_id     VARCHAR(100),
    event_time          TIMESTAMPTZ     NOT NULL,
    last_updated        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_liquidity_positions UNIQUE (currency, settlement_account)
);

CREATE INDEX IF NOT EXISTS idx_lp_last_updated
    ON liquidity_positions (last_updated DESC);

CREATE INDEX IF NOT EXISTS idx_lp_currency
    ON liquidity_positions (currency);

COMMENT ON TABLE liquidity_positions IS
    'Real-time balance per currency/account. Upserted by Flink LiquidityPositionEngine.';

-- ──────────────────────────────────────────
--  2. LIQUIDITY FLOW SUMMARY
--     5-minute tumbling window aggregates per currency.
--     Written by the windowed branch of LiquidityPositionEngine.
-- ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS liquidity_flow_summary (
    id                  BIGSERIAL       PRIMARY KEY,
    window_start        TIMESTAMPTZ     NOT NULL,
    window_end          TIMESTAMPTZ     NOT NULL,
    currency            VARCHAR(3)      NOT NULL,
    settlement_account  VARCHAR(50),
    net_flow            NUMERIC(20, 2)  NOT NULL,
    total_inbound       NUMERIC(20, 2)  NOT NULL,
    total_outbound      NUMERIC(20, 2)  NOT NULL,
    payment_count       INTEGER         NOT NULL,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_flow_summary UNIQUE (window_start, currency, settlement_account)
);

CREATE INDEX IF NOT EXISTS idx_lfs_window_start
    ON liquidity_flow_summary (window_start DESC);

CREATE INDEX IF NOT EXISTS idx_lfs_currency
    ON liquidity_flow_summary (currency, window_start DESC);

COMMENT ON TABLE liquidity_flow_summary IS
    '5-minute tumbling window net flow per currency. Powers the Net Flow by Currency Grafana panel.';

-- ──────────────────────────────────────────
--  3. THRESHOLDS
--     WARNING and CRITICAL thresholds per (currency, settlement_account).
--     Seeded at startup; hot-reloaded at runtime via Kafka -> ThresholdManager -> BroadcastState.
-- ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS thresholds (
    id                  BIGSERIAL       PRIMARY KEY,
    currency            VARCHAR(3)      NOT NULL,
    settlement_account  VARCHAR(50)     NOT NULL,
    warning_threshold   NUMERIC(20, 2)  NOT NULL,
    critical_threshold  NUMERIC(20, 2)  NOT NULL,
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_by          VARCHAR(100)    DEFAULT 'seed',
    CONSTRAINT uq_thresholds UNIQUE (currency, settlement_account),
    CONSTRAINT chk_threshold_order CHECK (warning_threshold > critical_threshold)
);

COMMENT ON TABLE thresholds IS
    'Balance thresholds per currency/account. WARNING > CRITICAL. Hot-reloaded by Flink ThresholdManager via Kafka.';

-- ──────────────────────────────────────────
--  4. FEED HEALTH
--     One row per payment rail (RTGS, CHAPS, INTERNAL).
--     Updated by Flink FeedStaleAlert (EventTimeTimer, 60s window).
--     Powers the Feed Health panel in Grafana.
-- ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS feed_health (
    rail                VARCHAR(20)     PRIMARY KEY,   -- RTGS | CHAPS | INTERNAL
    last_event_time     TIMESTAMPTZ,
    last_check_time     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    is_stale            BOOLEAN         NOT NULL DEFAULT FALSE,
    stale_since         TIMESTAMPTZ,
    event_count_1min    INTEGER         NOT NULL DEFAULT 0
);

INSERT INTO feed_health (rail)
    VALUES ('RTGS'), ('CHAPS'), ('INTERNAL')
    ON CONFLICT (rail) DO NOTHING;

COMMENT ON TABLE feed_health IS
    'Liveness status per payment rail. Stale flag set by Flink after 60s of no events.';

-- ──────────────────────────────────────────
--  5. PENDING PAYMENTS
--     Payments expected to settle in the next 2 hours.
--     Populated by Airflow pending_payments_dag.
--     Powers the Pending Payments Queue panel in Grafana.
-- ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pending_payments (
    id                          BIGSERIAL       PRIMARY KEY,
    payment_id                  VARCHAR(100)    NOT NULL UNIQUE,
    currency                    VARCHAR(3)      NOT NULL,
    settlement_account          VARCHAR(50)     NOT NULL,
    amount                      NUMERIC(20, 2)  NOT NULL,
    direction                   VARCHAR(10)     NOT NULL,   -- DEBIT | CREDIT
    rail                        VARCHAR(20)     NOT NULL,   -- RTGS | CHAPS | INTERNAL
    counterparty                VARCHAR(100),
    expected_settlement_time    TIMESTAMPTZ     NOT NULL,
    status                      VARCHAR(20)     NOT NULL DEFAULT 'PENDING',
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_direction CHECK (direction IN ('DEBIT', 'CREDIT')),
    CONSTRAINT chk_pp_status  CHECK (status IN ('PENDING', 'SETTLED', 'FAILED', 'CANCELLED'))
);

CREATE INDEX IF NOT EXISTS idx_pp_settlement_time
    ON pending_payments (expected_settlement_time ASC)
    WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_pp_currency
    ON pending_payments (currency, expected_settlement_time ASC);

COMMENT ON TABLE pending_payments IS
    'Upcoming payments in next 2h window. Managed by Airflow pending_payments_dag.';

-- ──────────────────────────────────────────
--  6. ALERT LOG
--     Audit trail for every WARNING/CRITICAL alert fired by Flink.
--     Written alongside Kafka alert topics for compliance and history.
-- ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alert_log (
    id                  BIGSERIAL       PRIMARY KEY,
    alert_type          VARCHAR(20)     NOT NULL,   -- WARNING | CRITICAL
    currency            VARCHAR(3)      NOT NULL,
    settlement_account  VARCHAR(50)     NOT NULL,
    available_balance   NUMERIC(20, 2)  NOT NULL,
    threshold_value     NUMERIC(20, 2)  NOT NULL,
    breach_amount       NUMERIC(20, 2)  NOT NULL,   -- threshold - balance
    triggered_at        TIMESTAMPTZ     NOT NULL,
    payment_id          VARCHAR(100),
    acknowledged        BOOLEAN         NOT NULL DEFAULT FALSE,
    acknowledged_at     TIMESTAMPTZ,
    CONSTRAINT chk_alert_type CHECK (alert_type IN ('WARNING', 'CRITICAL'))
);

CREATE INDEX IF NOT EXISTS idx_al_triggered_at
    ON alert_log (triggered_at DESC);

CREATE INDEX IF NOT EXISTS idx_al_currency_type
    ON alert_log (currency, alert_type, triggered_at DESC);

COMMENT ON TABLE alert_log IS
    'Audit trail of all threshold breach alerts. Written by Flink alongside Kafka alert topics.';
