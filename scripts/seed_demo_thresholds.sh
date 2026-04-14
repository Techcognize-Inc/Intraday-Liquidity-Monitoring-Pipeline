#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
#  Intraday Liquidity Monitoring Pipeline — Demo Threshold Seeder
#
#  Runs ONCE at startup (via threshold-init container) to publish
#  intentionally LOW thresholds to the liquidity.thresholds Kafka topic.
#
#  Why this is needed:
#    seed_thresholds.sql only writes to PostgreSQL (for audit display).
#    BroadcastState in LiquidityPositionEngine reads from Kafka, NOT PostgreSQL.
#    Without Kafka messages on liquidity.thresholds, BroadcastState is empty
#    and no alerts ever fire (the "if thresholds:" guard is False).
#
#  Why low values (warning=1000, critical=500)?
#    The running balance starts at 0 and drops quickly (55% DEBITs).
#    Any threshold above 0 will breach immediately → alerts fire on
#    the very first payment. This is intentional for demo purposes.
#
#  For production: replace with realistic values (e.g. 2_000_000_000).
#  Or run threshold_publisher.py manually from the treasury desk.
# ─────────────────────────────────────────────────────────────────

set -euo pipefail

GREEN='\033[0;32m'
AMBER='\033[0;33m'
NC='\033[0m'
info() { echo -e "${GREEN}[THRESHOLD-INIT]${NC} $*"; }
warn() { echo -e "${AMBER}[THRESHOLD-INIT]${NC} $*"; }

KAFKA="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
MAX_WAIT=60

# ── Wait for Kafka to accept connections ─────────────────────────
# threshold_publisher uses confluent_kafka which will throw on connect failure.
# We probe with a simple Python one-liner rather than adding kafka CLI tools.
info "Waiting for Kafka at ${KAFKA}..."
elapsed=0
until python3 -c "
from confluent_kafka.admin import AdminClient
c = AdminClient({'bootstrap.servers': '${KAFKA}', 'socket.timeout.ms': 3000})
md = c.list_topics(timeout=3)
" 2>/dev/null; do
    if [ "$elapsed" -ge "$MAX_WAIT" ]; then
        warn "Kafka not reachable after ${MAX_WAIT}s — continuing anyway"
        break
    fi
    echo "  ... not ready yet (${elapsed}s)"
    sleep 3
    elapsed=$((elapsed + 3))
done
info "Kafka is ready."

# ── Publish demo thresholds for every account ────────────────────
# warning=1000  → balance below £1,000  triggers WARNING
# critical=500  → balance below £500    triggers CRITICAL
#
# Since balance starts at 0 (below both), every account fires
# a WARNING alert on its very first payment. Perfect for demo.

ACCOUNTS=(
    "GBP RTGS-GBP-001"
    "GBP CHAPS-GBP-001"
    "GBP INTERNAL-GBP-001"
    "USD NOSTRO-USD-001"
    "USD NOSTRO-USD-002"
    "EUR NOSTRO-EUR-001"
    "EUR NOSTRO-EUR-002"
)

info "Publishing demo thresholds (warning=1000, critical=500) for all accounts..."
echo ""

for entry in "${ACCOUNTS[@]}"; do
    currency=$(echo "$entry" | cut -d' ' -f1)
    account=$(echo  "$entry" | cut -d' ' -f2)

    python3 -m producers.threshold_publisher \
        --currency   "$currency" \
        --account    "$account" \
        --warning    1000 \
        --critical   500 \
        --updated-by "demo-seed"

    info "  Published: ${currency}:${account}"
done

echo ""
info "All demo thresholds seeded to Kafka."
info "LiquidityPositionEngine will pick them up within ~30s (one checkpoint cycle)."
info "Alerts will start firing on the first payment per account."
