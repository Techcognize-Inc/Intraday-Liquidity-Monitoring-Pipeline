"""
Threshold Publisher — Treasury Desk CLI Tool
=============================================
This script is used by the treasury desk to change alert thresholds
without restarting any Flink jobs.

How it works:
  1. You run this from the command line with new threshold values
  2. It validates that WARNING > CRITICAL (a business rule)
  3. It publishes the update to the liquidity.thresholds Kafka topic
     using the currency:account as the Kafka message key
  4. It also writes the update to PostgreSQL for audit trail
  5. ThresholdManager picks up the Kafka message and updates BroadcastState
  6. All 16 LiquidityPositionEngine instances see the new threshold
     within ~30 seconds (one Flink checkpoint cycle)

Usage example:
  python -m producers.threshold_publisher \\
      --currency GBP \\
      --account RTGS-GBP-001 \\
      --warning 1500000000 \\
      --critical 300000000 \\
      --updated-by "treasury-desk"

List current thresholds:
  python -m producers.threshold_publisher --list
"""

import argparse     # Python's built-in CLI argument parser
import json         # for serialising the threshold message to JSON
import logging      # for logging confirmation and errors
import os           # for reading environment variables
import sys          # for sys.exit() when validation fails
from datetime import datetime, timezone   # for recording when the update was made

import psycopg2              # PostgreSQL Python driver
from confluent_kafka import Producer   # Kafka client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("ThresholdPublisher")

# Read connection details from environment (set in docker-compose)
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PG_HOST       = os.getenv("POSTGRES_HOST",     "postgres")
PG_PORT       = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB         = os.getenv("POSTGRES_DB",       "liquidity")
PG_USER       = os.getenv("POSTGRES_USER",     "liquidity_user")
PG_PASS       = os.getenv("POSTGRES_PASSWORD", "liquidity_pass")

THRESHOLD_TOPIC = "liquidity.thresholds"   # the compacted Kafka topic ThresholdManager reads


def get_pg_conn():
    """Opens and returns a new PostgreSQL connection."""
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS,
    )


def list_thresholds():
    """
    Queries PostgreSQL and prints all current thresholds in a formatted table.
    Useful for the treasury desk to see what thresholds are currently set.
    """
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT currency, settlement_account,
                       warning_threshold, critical_threshold,
                       updated_at, updated_by
                FROM thresholds
                ORDER BY currency, settlement_account   -- alphabetical for readability
            """)
            rows = cur.fetchall()   # fetch all results as a list of tuples

        # Print a formatted table header
        print(f"\n{'CURRENCY':<10} {'ACCOUNT':<22} {'WARNING':>20} {'CRITICAL':>20} {'UPDATED BY':<20} UPDATED AT")
        print("-" * 110)
        for row in rows:
            currency, account, warning, critical, updated_at, updated_by = row  # unpack tuple
            # :<10 = left-align in 10 chars, :>20 = right-align in 20 chars, :,.2f = with commas
            print(f"{currency:<10} {account:<22} {warning:>20,.2f} {critical:>20,.2f} {updated_by:<20} {updated_at}")
    finally:
        conn.close()   # always close the connection even if an error occurred


def publish_threshold(currency: str, account: str, warning: float, critical: float, updated_by: str):
    """
    Publishes a threshold update to Kafka AND writes it to PostgreSQL.

    The Kafka message key is "CURRENCY:account" (e.g. "GBP:RTGS-GBP-001").
    Because the topic is compacted, Kafka keeps only the LATEST message per key.
    So publishing a new threshold always overwrites the old one.
    """
    # Business rule: WARNING must always be greater than CRITICAL
    # (you warn before things go critical, not after)
    if warning <= critical:
        logger.error(f"WARNING threshold ({warning:,.0f}) must be > CRITICAL threshold ({critical:,.0f})")
        sys.exit(1)   # exit with error code 1 — don't publish invalid thresholds

    # Build the Kafka message payload
    message = {
        "currency":           currency.upper(),      # normalise to uppercase e.g. "gbp" → "GBP"
        "settlement_account": account,
        "warning_threshold":  warning,
        "critical_threshold": critical,
        "updated_by":         updated_by,            # who made this change (for audit)
        "updated_at":         datetime.now(tz=timezone.utc).isoformat(),   # timestamp of this update
    }

    kafka_key = f"{currency.upper()}:{account}"   # e.g. "GBP:RTGS-GBP-001"
                                                  # this is the compaction key — Kafka keeps latest per key

    # Create a Kafka producer just for this publish operation
    producer = Producer({
        "bootstrap.servers": KAFKA_SERVERS,
        "acks": "all",   # wait for all replicas to confirm delivery
    })

    producer.produce(
        topic=THRESHOLD_TOPIC,                   # the compacted threshold topic
        key=kafka_key,                           # compaction key — crucial for log compaction to work
        value=json.dumps(message),               # JSON-encoded threshold update
        callback=lambda err, msg: (
            logger.error(f"Delivery failed: {err}") if err   # log errors
            else logger.info(f"Published to {msg.topic()}[{msg.partition()}]@{msg.offset()}")  # log success
        ),
    )
    producer.flush(timeout=10)   # wait up to 10 seconds for the message to be delivered

    # Also write to PostgreSQL as a durable audit trail
    # (Kafka messages could be compacted/expired; the DB record is permanent)
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO thresholds (currency, settlement_account, warning_threshold, critical_threshold, updated_by)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (currency, settlement_account)    -- if row already exists for this account...
                DO UPDATE SET
                    warning_threshold  = EXCLUDED.warning_threshold,   -- overwrite with new values
                    critical_threshold = EXCLUDED.critical_threshold,
                    updated_at         = NOW(),
                    updated_by         = EXCLUDED.updated_by
            """, (currency.upper(), account, warning, critical, updated_by))   # %s = parameterised (safe from SQL injection)
        conn.commit()   # commit the transaction
        logger.info(f"Threshold saved to PostgreSQL: {kafka_key}")
    finally:
        conn.close()

    logger.info(
        f"Threshold update published:\n"
        f"  key={kafka_key}\n"
        f"  warning={warning:,.2f}\n"        # :,.2f = formatted with commas, 2 decimal places
        f"  critical={critical:,.2f}\n"
        f"  Flink will apply within ~30 seconds (no restart needed)"
    )


def main():
    """
    CLI entry point — parses command-line arguments and calls the right function.
    """
    parser = argparse.ArgumentParser(description="Push threshold updates to LiquidityPositionEngine via Kafka")
    # Each add_argument() defines one CLI flag
    parser.add_argument("--list",       action="store_true",   help="List current thresholds from PostgreSQL")
    parser.add_argument("--currency",   type=str,              help="Currency code (GBP, USD, EUR)")
    parser.add_argument("--account",    type=str,              help="Settlement account (e.g. RTGS-GBP-001)")
    parser.add_argument("--warning",    type=float,            help="New WARNING threshold")
    parser.add_argument("--critical",   type=float,            help="New CRITICAL threshold")
    parser.add_argument("--updated-by", type=str, default="cli", help="Who is making this change (for audit)")
    args = parser.parse_args()   # parse sys.argv and return Namespace object

    if args.list:                # if --list flag provided, just print thresholds and exit
        list_thresholds()
        return

    # Validate that all required arguments were provided before attempting publish
    required = ["currency", "account", "warning", "critical"]
    missing  = [f for f in required if not getattr(args, f.replace("-", "_"), None)]
    if missing:
        parser.error(f"Missing required arguments: {missing}")   # prints usage and exits

    publish_threshold(
        currency=args.currency,
        account=args.account,
        warning=args.warning,
        critical=args.critical,
        updated_by=args.updated_by,
    )


if __name__ == "__main__":
    main()
