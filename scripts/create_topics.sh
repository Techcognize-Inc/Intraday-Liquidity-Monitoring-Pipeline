#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
#  Intraday Liquidity Monitoring Pipeline — Kafka Topic Init
#  Runs once inside the kafka-init container.
#
#  Topics:
#    payments.rtgs        — RTGS payment events          (16 partitions)
#    payments.chaps       — CHAPS payment events         (16 partitions)
#    payments.internal    — Internal transfer events     (16 partitions)
#    payments.all         — Fan-out of all rails merged  (16 partitions)
#    liquidity.warnings   — WARNING threshold alerts     (4 partitions)
#    liquidity.critical   — CRITICAL threshold alerts    (4 partitions)
#    liquidity.thresholds — Threshold update commands    (1 partition, compacted)
#    ops.feed.health      — Feed stale/recover alerts    (4 partitions)
# ─────────────────────────────────────────────────────────────────

# -u: error on unset vars, -o pipefail: catch pipe failures
# NOTE: no -e flag — we handle errors manually with retries
set -uo pipefail

BOOTSTRAP="kafka:9092"
REPLICATION=1          # single-broker dev cluster
RETENTION_MS=86400000  # 24 hours for payment events
ALERT_RETENTION_MS=604800000  # 7 days for alert topics

echo "Waiting for Kafka to be ready..."
until kafka-topics --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; do
    echo "  Kafka not ready yet, retrying in 3s..."
    sleep 3
done
echo "Kafka is ready. Waiting 10s for controller election..."
sleep 10

create_topic() {
    local topic=$1
    local partitions=$2
    local retention_ms=$3
    local cleanup_policy=${4:-delete}

    # Retry up to 5 times to handle transient broker errors
    for attempt in 1 2 3 4 5; do
        # Skip if topic already exists
        if kafka-topics --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null | grep -qF "$topic"; then
            echo "  [SKIP] Topic already exists: $topic"
            return 0
        fi

        if kafka-topics \
            --bootstrap-server "$BOOTSTRAP" \
            --create \
            --topic "$topic" \
            --partitions "$partitions" \
            --replication-factor "$REPLICATION" \
            --config retention.ms="$retention_ms" \
            --config cleanup.policy="$cleanup_policy" 2>/dev/null; then
            echo "  [OK]   Created: $topic (partitions=$partitions, retention=${retention_ms}ms, cleanup=$cleanup_policy)"
            return 0
        fi

        echo "  Attempt $attempt failed for '$topic', retrying in 5s..."
        sleep 5
    done

    echo "  [FAIL] Could not create topic after 5 attempts: $topic" >&2
    return 1
}

echo ""
echo "Creating payment topics..."
create_topic "payments.rtgs"       16 "$RETENTION_MS"
create_topic "payments.chaps"      16 "$RETENTION_MS"
create_topic "payments.internal"   16 "$RETENTION_MS"
create_topic "payments.all"        16 "$RETENTION_MS"

echo ""
echo "Creating alert topics..."
create_topic "liquidity.warnings"  4  "$ALERT_RETENTION_MS"
create_topic "liquidity.critical"  4  "$ALERT_RETENTION_MS"

echo ""
echo "Creating threshold topic (compacted — enables ThresholdManager replay on restart)..."
create_topic "liquidity.thresholds" 1 "$ALERT_RETENTION_MS" "compact"

echo ""
echo "Creating ops topic..."
create_topic "ops.feed.health"     4  "$ALERT_RETENTION_MS"

echo ""
echo "Creating Dead Letter Queue topic..."
create_topic "payments.dlq"        4  "$ALERT_RETENTION_MS"

echo ""
echo "All topics created. Current topic list:"
kafka-topics --bootstrap-server "$BOOTSTRAP" --list
echo ""
echo "Done."
