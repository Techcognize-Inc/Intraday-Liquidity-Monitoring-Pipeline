#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
#  Intraday Liquidity Monitoring Pipeline — Flink Job Submitter
#
#  This script:
#    1. Waits for Flink JobManager REST API to be ready
#    2. Waits for Kafka topics to exist (kafka-init must have run)
#    3. Waits for PostgreSQL to be accepting connections
#    4. Submits ThresholdManager job     (parallelism=1)
#    5. Submits LiquidityPositionEngine  (parallelism=16)
#    6. Prints job IDs and tails the Flink REST API for status
#
#  Usage (from project root):
#    bash scripts/start_flink_cluster.sh
#
#  Or inside docker-compose after the cluster is up:
#    docker exec flink-jobmanager bash /scripts/start_flink_cluster.sh
# ─────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────
FLINK_JM_HOST="${FLINK_JM_HOST:-flink-jobmanager}"
FLINK_REST_PORT="${FLINK_REST_PORT:-8081}"
FLINK_REST="http://${FLINK_JM_HOST}:${FLINK_REST_PORT}"

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
PG_HOST="${POSTGRES_HOST:-postgres}"
PG_PORT="${POSTGRES_PORT:-5432}"
PG_DB="${POSTGRES_DB:-liquidity}"
PG_USER="${POSTGRES_USER:-liquidity_user}"

JOBS_DIR="${JOBS_DIR:-/opt/flink/jobs}"

# How long to wait (seconds) before giving up on readiness checks
MAX_WAIT=120

# ── Colour helpers ────────────────────────────────────────────────
GREEN='\033[0;32m'
AMBER='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${AMBER}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ─────────────────────────────────────────────────────────────────
#  READINESS CHECKS
# ─────────────────────────────────────────────────────────────────

wait_for_flink() {
    info "Waiting for Flink JobManager REST API at ${FLINK_REST}..."
    local elapsed=0
    until curl -sf "${FLINK_REST}/overview" > /dev/null 2>&1; do
        if [ "$elapsed" -ge "$MAX_WAIT" ]; then
            error "Flink JobManager not ready after ${MAX_WAIT}s. Is flink-jobmanager running?"
        fi
        echo "  ... not ready yet (${elapsed}s elapsed)"
        sleep 5
        elapsed=$((elapsed + 5))
    done
    info "Flink JobManager is ready."
}

wait_for_kafka_topics() {
    info "Waiting for Kafka topic 'payments.all' to exist..."
    local elapsed=0
    until kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" --list 2>/dev/null | grep -q "^payments.all$"; do
        if [ "$elapsed" -ge "$MAX_WAIT" ]; then
            error "Kafka topic 'payments.all' not found after ${MAX_WAIT}s. Did kafka-init complete?"
        fi
        echo "  ... topic not found yet (${elapsed}s elapsed)"
        sleep 5
        elapsed=$((elapsed + 5))
    done
    info "Kafka topics are ready."
}

wait_for_postgres() {
    info "Waiting for PostgreSQL at ${PG_HOST}:${PG_PORT}..."
    local elapsed=0
    until pg_isready -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" > /dev/null 2>&1; do
        if [ "$elapsed" -ge "$MAX_WAIT" ]; then
            error "PostgreSQL not ready after ${MAX_WAIT}s."
        fi
        echo "  ... not ready yet (${elapsed}s elapsed)"
        sleep 5
        elapsed=$((elapsed + 5))
    done
    info "PostgreSQL is ready."
}

# ─────────────────────────────────────────────────────────────────
#  JOB SUBMISSION via Flink REST API
#  POST /jars/upload  → upload Python job as a JAR-equivalent
#  POST /jars/{id}/run → submit with entry class and parallelism
#
#  For PyFlink jobs we use the python entrypoint flag.
# ─────────────────────────────────────────────────────────────────

submit_pyflink_job() {
    local job_name="$1"
    local job_file="$2"
    local parallelism="$3"
    local extra_args="${4:-}"

    info "Submitting ${job_name} (parallelism=${parallelism})..."

    # Check if the job is already running (avoid duplicates on restart)
    local running
    running=$(curl -sf "${FLINK_REST}/jobs/overview" | \
              python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
running = [j for j in jobs if j.get('name','') == '${job_name}' and j.get('state','') == 'RUNNING']
print(len(running))
" 2>/dev/null || echo "0")

    if [ "$running" -gt "0" ]; then
        warn "${job_name} is already running — skipping submission."
        return
    fi

    # Submit using flink run (available inside the flink container)
    local job_id
    job_id=$(flink run \
        --jobmanager "${FLINK_JM_HOST}:6123" \
        --parallelism "$parallelism" \
        --python "${JOBS_DIR}/${job_file}" \
        $extra_args \
        2>&1 | grep -oP "Job has been submitted with JobID \K[a-f0-9]+" || true)

    if [ -z "$job_id" ]; then
        warn "Could not extract Job ID for ${job_name} — check Flink Web UI at ${FLINK_REST}"
    else
        info "${job_name} submitted successfully. Job ID: ${job_id}"
    fi
}

check_running_jobs() {
    info "Current running jobs:"
    curl -sf "${FLINK_REST}/jobs/overview" | \
        python3 -c "
import sys, json
data = json.load(sys.stdin)
jobs = data.get('jobs', [])
if not jobs:
    print('  (no jobs running)')
for j in jobs:
    state = j.get('state', 'UNKNOWN')
    name  = j.get('name', 'unknown')
    jid   = j.get('jid', '?')
    start = j.get('start-time', 0)
    print(f'  [{state}] {name} (id={jid})')
"
}

# ─────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  Intraday Liquidity Monitoring Pipeline — Flink Startup"
echo "════════════════════════════════════════════════════════════"
echo ""

# Step 1: Wait for all dependencies
wait_for_flink
wait_for_kafka_topics
wait_for_postgres

echo ""
info "All dependencies ready. Submitting Flink jobs..."
echo ""

# Step 2: Submit ThresholdManager FIRST (parallelism=1)
#   Must start before LiquidityPositionEngine so BroadcastState
#   is populated with thresholds before payments start flowing.
submit_pyflink_job \
    "ThresholdManager" \
    "threshold_manager.py" \
    "1"

# Small pause to let ThresholdManager start and replay compacted topic
info "Waiting 10s for ThresholdManager to replay threshold state..."
sleep 10

# Step 3: Submit LiquidityPositionEngine (parallelism=16)
submit_pyflink_job \
    "LiquidityPositionEngine" \
    "liquidity_position_engine.py" \
    "16"

echo ""
echo "════════════════════════════════════════════════════════════"
info "Both jobs submitted. Checking status..."
echo ""

sleep 5
check_running_jobs

echo ""
info "Flink Web UI: ${FLINK_REST}"
info "Grafana:      http://grafana:3000  (or localhost:3000 from host)"
info "Prometheus:   http://prometheus:9090"
echo ""
info "To verify acceptance criteria:"
echo "  1. Payment events:  docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic payments.all --from-beginning --max-messages 5"
echo "  2. Alert events:    docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic liquidity.warnings --from-beginning"
echo "  3. Hot-reload test: python -m producers.threshold_publisher --currency GBP --account RTGS-GBP-001 --warning 999999999 --critical 500000000"
echo "  4. Stale feed test: docker stop payment-producer && sleep 65"
echo "  5. Restart test:    docker restart flink-taskmanager  # balance should restore from checkpoint"
echo ""
echo "════════════════════════════════════════════════════════════"
