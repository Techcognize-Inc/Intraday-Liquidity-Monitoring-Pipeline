-- ─────────────────────────────────────────────────────────────────
--  Intraday Liquidity Monitoring Pipeline — Threshold Seeds
--
--  WARNING_THRESHOLD: balance falls below this → liquidity.warnings
--  CRITICAL_THRESHOLD: balance falls below this → liquidity.critical
--
--  Realistic RTGS/CHAPS daily settlement volumes (GBP billions):
--    GBP RTGS:      £2B warning / £500M critical
--    GBP CHAPS:     £500M warning / £100M critical
--    USD NOSTRO:    $1B warning / $250M critical
--    EUR NOSTRO:    €750M warning / €150M critical
-- ─────────────────────────────────────────────────────────────────

INSERT INTO thresholds (currency, settlement_account, warning_threshold, critical_threshold, updated_by)
VALUES
    -- GBP accounts
    ('GBP', 'RTGS-GBP-001',     2000000000.00,   500000000.00, 'seed'),
    ('GBP', 'CHAPS-GBP-001',     500000000.00,   100000000.00, 'seed'),
    ('GBP', 'INTERNAL-GBP-001',   50000000.00,    10000000.00, 'seed'),

    -- USD nostro accounts
    ('USD', 'NOSTRO-USD-001',   1000000000.00,   250000000.00, 'seed'),
    ('USD', 'NOSTRO-USD-002',    250000000.00,    50000000.00, 'seed'),

    -- EUR nostro accounts
    ('EUR', 'NOSTRO-EUR-001',    750000000.00,   150000000.00, 'seed'),
    ('EUR', 'NOSTRO-EUR-002',    200000000.00,    40000000.00, 'seed')

ON CONFLICT (currency, settlement_account)
DO UPDATE SET
    warning_threshold  = EXCLUDED.warning_threshold,
    critical_threshold = EXCLUDED.critical_threshold,
    updated_at         = NOW(),
    updated_by         = 'seed-reseed';
