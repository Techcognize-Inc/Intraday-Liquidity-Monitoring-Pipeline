"""
Pending Payments Generator
===========================
Generates synthetic upcoming payment obligations for the next 2 hours.

Purpose:
  The Flink engine handles payments as they ARRIVE (real-time).
  But treasury also needs to know what payments are SCHEDULED to arrive
  soon — e.g. a £200M RTGS settlement due at 10:30 — so they can
  pre-position liquidity before it hits.
  This generator simulates that forward-looking schedule.

Called by:  Airflow pending_payments_dag.py (every 30 minutes)
Writes to:  pending_payments PostgreSQL table
Powers:     Grafana "Pending Payments Queue" panel (next 2 hours view)
"""

import logging    # for status messages
import os         # for reading environment variables
import random     # for generating random amounts, currencies, counterparties
import uuid       # for generating unique payment IDs
from datetime import datetime, timedelta, timezone   # for calculating future settlement times

import psycopg2          # PostgreSQL Python driver
from faker import Faker  # library for generating realistic fake data

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("PendingPaymentsGenerator")

fake = Faker("en_GB")   # UK locale for realistic data

# Database connection settings from environment variables
PG_HOST = os.getenv("POSTGRES_HOST",     "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB",       "liquidity")
PG_USER = os.getenv("POSTGRES_USER",     "liquidity_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "liquidity_pass")

PAYMENT_COUNT = int(os.getenv("PENDING_PAYMENT_COUNT", "50"))   # how many to generate per run

# Same accounts as payment_producer.py and seed_thresholds.sql (must be consistent)
CURRENCIES = ["GBP", "USD", "EUR"]

ACCOUNTS = {
    "GBP": ["RTGS-GBP-001", "CHAPS-GBP-001", "INTERNAL-GBP-001"],
    "USD": ["NOSTRO-USD-001", "NOSTRO-USD-002"],
    "EUR": ["NOSTRO-EUR-001", "NOSTRO-EUR-002"],
}

COUNTERPARTIES = {
    "GBP": ["BARCLAYS", "HSBC", "LLOYDS", "NATWEST", "SANTANDER"],
    "USD": ["JPMORGAN", "CITIBANK", "BANK_OF_AMERICA", "WELLS_FARGO"],
    "EUR": ["DEUTSCHE_BANK", "BNP_PARIBAS", "SOCIETE_GENERALE", "ING"],
}

RAILS = ["RTGS", "CHAPS", "INTERNAL"]

AMOUNT_RANGES = {
    "RTGS":     (10_000_000,   500_000_000),   # £10M – £500M
    "CHAPS":    (1_000_000,    100_000_000),   # £1M  – £100M
    "INTERNAL": (100_000,      50_000_000),    # £100K – £50M
}


def get_pg_conn():
    """Opens and returns a new PostgreSQL database connection."""
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS,
    )


def generate_pending_payments(count: int) -> list[dict]:
    """
    Generates `count` synthetic pending payment records.
    Each has a future settlement time within the next 2 hours.
    Settlement times are clustered: most payments are due soon (next 30 min),
    fewer are scheduled further out — realistic for intraday liquidity management.
    """
    now = datetime.now(tz=timezone.utc)   # current time in UTC
    payments = []

    for _ in range(count):                                   # generate `count` payments
        rail     = random.choice(RAILS)                      # RTGS, CHAPS, or INTERNAL
        currency = random.choice(CURRENCIES)                 # GBP, USD, or EUR
        account  = random.choice(ACCOUNTS.get(currency, ACCOUNTS["GBP"]))   # pick valid account
        amount_min, amount_max = AMOUNT_RANGES[rail]

        # Settlement time: weighted random — 50% in next 30min, 30% in 30-90min, 20% in 90-120min
        # random.choices() picks one option using the given weights
        minutes_ahead = random.choices(
            [random.randint(5, 30),   # near-term: 5–30 minutes ahead
             random.randint(31, 90),  # medium:    31–90 minutes ahead
             random.randint(91, 120)],# far:       91–120 minutes ahead
            weights=[0.5, 0.3, 0.2], # 50% chance of near-term, 30% medium, 20% far
        )[0]                         # [0] because choices() returns a list; we want the single pick
        settlement_time = now + timedelta(minutes=minutes_ahead)   # future datetime

        payments.append({
            "payment_id":               str(uuid.uuid4()),       # unique ID
            "currency":                 currency,
            "settlement_account":       account,
            "amount":                   round(random.uniform(amount_min, amount_max), 2),   # random amount in range
            "direction":                random.choices(["DEBIT", "CREDIT"], weights=[0.6, 0.4])[0],  # 60% DEBIT
            "rail":                     rail,
            "counterparty":             random.choice(COUNTERPARTIES.get(currency, ["UNKNOWN"])),
            "expected_settlement_time": settlement_time.isoformat(),  # ISO timestamp
            "status":                   "PENDING",                    # all new payments start as PENDING
        })

    return payments   # return list of dicts (not yet in the database)


def load_to_postgres(payments: list[dict]) -> int:
    """
    Upserts generated pending payments into PostgreSQL.
    Also expires any payments that were PENDING but are now past their settlement time.
    Returns the count of newly inserted rows.
    """
    conn = get_pg_conn()
    inserted = 0
    try:
        with conn.cursor() as cur:
            # Step 1: Auto-expire payments whose settlement time has passed
            # These were PENDING but the time window has closed — mark as SETTLED
            cur.execute("""
                UPDATE pending_payments
                SET status = 'SETTLED'
                WHERE status = 'PENDING'
                  AND expected_settlement_time < NOW()   -- settlement time is in the past
            """)
            expired = cur.rowcount   # how many rows were updated
            if expired:
                logger.info(f"Marked {expired} past-due payments as SETTLED")

            # Step 2: Insert each new pending payment
            for p in payments:
                cur.execute("""
                    INSERT INTO pending_payments
                        (payment_id, currency, settlement_account, amount,
                         direction, rail, counterparty, expected_settlement_time, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::timestamptz, %s)
                    ON CONFLICT (payment_id) DO NOTHING   -- skip if same payment_id already exists
                """, (
                    p["payment_id"],                  # %s 1
                    p["currency"],                    # %s 2
                    p["settlement_account"],          # %s 3
                    p["amount"],                      # %s 4
                    p["direction"],                   # %s 5
                    p["rail"],                        # %s 6
                    p["counterparty"],                # %s 7
                    p["expected_settlement_time"],    # %s 8 — cast to timestamptz in SQL
                    p["status"],                      # %s 9 = "PENDING"
                ))
                inserted += cur.rowcount   # rowcount = 1 if inserted, 0 if skipped (conflict)

        conn.commit()   # commit the whole batch as one transaction
    finally:
        conn.close()    # always close connection

    return inserted


def run(count: int = PAYMENT_COUNT) -> dict:
    """
    Main entry point — called by Airflow DAG every 30 minutes.
    Returns a summary dict that Airflow stores in XCom for the summary task.
    """
    logger.info(f"Generating {count} pending payments...")
    payments = generate_pending_payments(count)    # create list of payment dicts in memory
    inserted = load_to_postgres(payments)          # write them to PostgreSQL
    logger.info(f"Loaded {inserted} new pending payments into PostgreSQL")
    return {"generated": count, "inserted": inserted}   # Airflow XCom payload


if __name__ == "__main__":
    result = run()    # run directly for testing
    print(result)
