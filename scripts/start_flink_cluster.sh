#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
#  Intraday Liquidity Monitoring Pipeline — Flink Job Submitter
#
#  Runs once as a flink-init container after Flink, Kafka, and
#  PostgreSQL are ready. Submits both Flink jobs in order:
#    1. PaymentStreamMerger   — merges rtgs/chaps/internal → payments.all
#    2. LiquidityPositionEngine — positions, alerts, thresholds, stale detection
#
#  Called automatically by the flink-init container in docker-compose.yml.
# ─────────────────────────────────────────────────────────────────

set -uo pipefail

FLINK_JM_HOST="${FLINK_JM_HOST:-flink-jobmanager}"
FLINK_REST_PORT="${FLINK_REST_PORT:-8081}"
FLINK_REST="http://${FLINK_JM_HOST}:${FLINK_REST_PORT}"
JOBS_DIR="${JOBS_DIR:-/opt/flink/jobs}"
MAX_WAIT=120

GREEN='\033[0;32m'
AMBER='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${AMBER}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Wait for Flink REST API ───────────────────────────────────────
wait_for_flink() {
    info "Waiting for Flink JobManager REST API at ${FLINK_REST}..."
    local elapsed=0
    until curl -sf "${FLINK_REST}/overview" > /dev/null 2>&1; do
        if [ "$elapsed" -ge "$MAX_WAIT" ]; then
            error "Flink JobManager not ready after ${MAX_WAIT}s."
        fi
        echo "  ... not ready yet (${elapsed}s elapsed)"
        sleep 5
        elapsed=$((elapsed + 5))
    done
    info "Flink JobManager is ready."
}

# ── Submit a PyFlink job (skips if already running) ───────────────
submit_job() {
    local job_name="$1"
    local job_file="$2"

    info "Submitting ${job_name}..."

    # Skip if already running — safe to call on container restart
    local running
    running=$(curl -sf "${FLINK_REST}/jobs/overview" | \
              python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
print(len([j for j in jobs if j.get('name') == '${job_name}' and j.get('state') == 'RUNNING']))
" 2>/dev/null || echo "0")

    if [ "$running" -gt "0" ]; then
        warn "${job_name} is already running — skipping."
        return
    fi

    flink run \
        --python "${JOBS_DIR}/${job_file}" \
        2>&1 | tail -3

    info "${job_name} submitted."
}

# ── Main ──────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  Intraday Liquidity Monitoring Pipeline — Flink Startup"
echo "════════════════════════════════════════════════════════════"
echo ""

wait_for_flink

echo ""
info "Submitting Flink jobs..."
echo ""

# Job 1: merger must start first so payments.all is populated
# before the engine begins reading
submit_job "PaymentStreamMerger" "payment_stream_merger.py"

info "Waiting 15s for merger to start consuming..."
sleep 15

# Job 2: engine reads from payments.all + liquidity.thresholds
submit_job "LiquidityPositionEngine" "liquidity_position_engine.py"

echo ""
info "Both jobs submitted. Current status:"
echo ""

sleep 5
curl -sf "${FLINK_REST}/jobs/overview" | \
    python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
for j in jobs:
    print(f\"  [{j.get('state','?')}] {j.get('name','?')} (id={j.get('jid','?')})\")
"

echo ""
info "Flink Web UI:  http://localhost:18080"
info "Grafana:       http://localhost:13000"
echo "════════════════════════════════════════════════════════════"
