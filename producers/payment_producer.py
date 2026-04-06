"""
Payment Producer
=================
Simulates a bank's payment processing system by generating fake
RTGS, CHAPS, and Internal payment events using the Faker library.

Publishes to 3 rail-specific Kafka topics only:
  - payments.rtgs      → only RTGS events
  - payments.chaps     → only CHAPS events
  - payments.internal  → only Internal transfers

payments.all is NOT written here. It is produced by the PaymentStreamMerger
Flink job (flink_jobs/payment_stream_merger.py), which reads all three rail
topics and unions them into payments.all. This mirrors real banking architecture
where each payment system owns its own topic and a separate merge layer
produces the unified stream.

Rate control (transactions per second per rail):
  RTGS_TPS=5      → 5 RTGS payments per second
  CHAPS_TPS=3     → 3 CHAPS payments per second
  INTERNAL_TPS=10 → 10 internal transfers per second

Business hours simulation:
  Inside  07:00–18:00 UTC → full rate (normal banking day)
  Outside 07:00–18:00 UTC → 10% of normal rate (overnight quiet period)

Each message looks like:
  {
      "payment_id":         "3f7c8a2e-...",      ← unique UUID
      "currency":           "GBP",
      "settlement_account": "RTGS-GBP-001",
      "amount":             15750000.00,          ← £15.75M
      "direction":          "DEBIT",              ← money leaving the bank
      "rail":               "RTGS",
      "counterparty":       "BARCLAYS",
      "event_time":         "2026-04-01T09:15:00.123Z"
  }
"""

import json       # for serialising payment dicts to JSON strings before publishing to Kafka
import logging    # for printing status and error messages
import os         # for reading environment variables set in docker-compose
import random     # for choosing random currencies, counterparties, amounts
import time       # for sleeping between events to control rate (TPS)
import uuid       # for generating unique payment IDs
from datetime import datetime, timezone   # for getting current UTC time for event_time

from confluent_kafka import Producer   # high-performance Kafka client (C-backed)
from faker import Faker                # library for generating realistic fake data

# Set up logging to show timestamps and log level
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("PaymentProducer")

fake = Faker("en_GB")   # UK locale — not heavily used but sets realistic context

# ── Read configuration from environment variables ──────────────────
# These are set in docker-compose.yml under payment-producer.environment
KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")   # Kafka broker host:port
RTGS_TPS       = float(os.getenv("RTGS_TPS",      "5"))               # RTGS payments per second
CHAPS_TPS      = float(os.getenv("CHAPS_TPS",     "3"))               # CHAPS payments per second
INTERNAL_TPS   = float(os.getenv("INTERNAL_TPS",  "10"))              # internal transfers per second
CURRENCIES     = os.getenv("CURRENCIES", "GBP,USD,EUR").split(",")    # comma-separated → list
HOURS_START    = os.getenv("BUSINESS_HOURS_START", "07:00")           # "HH:MM" format
HOURS_END      = os.getenv("BUSINESS_HOURS_END",   "18:00")           # "HH:MM" format
OVERNIGHT_RATE = 0.1   # multiply sleep time by 1/0.1 = 10x slower outside business hours

# ── Settlement accounts per currency ──────────────────────────────
# Must match the accounts in seed_thresholds.sql so Grafana gauges show correct thresholds
ACCOUNTS = {
    "GBP": ["RTGS-GBP-001", "CHAPS-GBP-001", "INTERNAL-GBP-001"],   # 3 GBP accounts
    "USD": ["NOSTRO-USD-001", "NOSTRO-USD-002"],                       # 2 USD nostro accounts
    "EUR": ["NOSTRO-EUR-001", "NOSTRO-EUR-002"],                       # 2 EUR nostro accounts
}

# ── Rail → Kafka topic mapping ─────────────────────────────────────
# Each rail writes only to its own topic. The merger Flink job handles payments.all.
RAILS = {
    "RTGS":     {"topic": "payments.rtgs",     "tps": RTGS_TPS},      # Bank of England RTGS system
    "CHAPS":    {"topic": "payments.chaps",    "tps": CHAPS_TPS},     # UK same-day sterling
    "INTERNAL": {"topic": "payments.internal", "tps": INTERNAL_TPS},  # internal fund transfers
}

# ── Counterparty banks per currency ───────────────────────────────
# Realistic counterparties for each currency — makes dashboards look authentic
COUNTERPARTIES = {
    "GBP": ["BARCLAYS", "HSBC", "LLOYDS", "NATWEST", "SANTANDER", "STANDARD_CHARTERED"],
    "USD": ["JPMORGAN", "CITIBANK", "BANK_OF_AMERICA", "WELLS_FARGO", "GOLDMAN_SACHS"],
    "EUR": ["DEUTSCHE_BANK", "BNP_PARIBAS", "SOCIETE_GENERALE", "ING", "UNICREDIT"],
}

# ── Typical payment amount ranges by rail ─────────────────────────
# RTGS handles the largest payments (systemically important, e.g. £100M bond settlements)
# CHAPS is medium-value (e.g. property purchases, corporate payments)
# INTERNAL is lower-value (internal liquidity management, FX sweeps)
AMOUNT_RANGES = {
    "RTGS":     (10_000_000,   500_000_000),   # £10M – £500M
    "CHAPS":    (1_000_000,    100_000_000),   # £1M  – £100M
    "INTERNAL": (100_000,      50_000_000),    # £100K – £50M
}


def is_business_hours() -> bool:
    """
    Returns True if current UTC time is within business hours window.
    Used to slow down event rate outside trading hours.
    """
    now = datetime.now(tz=timezone.utc).strftime("%H:%M")   # current UTC time as "HH:MM" string
    return HOURS_START <= now <= HOURS_END                   # simple string comparison works for "HH:MM"


def build_payment(rail: str, currency: str) -> dict:
    """
    Generates one synthetic payment event dict.
    Called every loop iteration in run_rail_producer().
    """
    # 55% DEBIT (more money going out than coming in — realistic for a bank with large obligations)
    direction = random.choices(["DEBIT", "CREDIT"], weights=[0.55, 0.45])[0]

    amount_min, amount_max = AMOUNT_RANGES[rail]                 # get range for this rail
    amount = round(random.uniform(amount_min, amount_max), 2)    # random float, 2 decimal places

    # Choose the settlement account
    accounts = ACCOUNTS.get(currency, ACCOUNTS["GBP"])           # fall back to GBP accounts if unknown currency
    if rail == "RTGS" and f"RTGS-{currency}-001" in accounts:    # RTGS events prefer the RTGS account
        account = f"RTGS-{currency}-001"
    elif rail == "CHAPS" and f"CHAPS-{currency}-001" in accounts: # CHAPS events prefer the CHAPS account
        account = f"CHAPS-{currency}-001"
    else:
        account = random.choice(accounts)                        # internal and other rails pick randomly

    return {
        "payment_id":         str(uuid.uuid4()),                 # globally unique ID (UUID v4)
        "currency":           currency,                          # e.g. "GBP"
        "settlement_account": account,                           # e.g. "RTGS-GBP-001"
        "amount":             amount,                            # e.g. 15750000.00
        "direction":          direction,                         # "DEBIT" or "CREDIT"
        "rail":               rail,                              # "RTGS", "CHAPS", or "INTERNAL"
        "counterparty":       random.choice(COUNTERPARTIES.get(currency, ["UNKNOWN"])),  # bank name
        "event_time":         datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        # ^ ISO-8601 UTC timestamp with millisecond precision, e.g. "2026-04-01T09:15:00.123Z"
        # [:-3] trims microseconds to milliseconds; "Z" means UTC
    }


def delivery_report(err, msg):
    """
    Kafka delivery callback — called after each produce() completes.
    Kafka calls this asynchronously to tell us if the message was delivered.
    We only log errors; successful deliveries are silent for performance.
    """
    if err:
        logger.error(f"Delivery failed: topic={msg.topic()} err={err}")


def run_rail_producer(producer: Producer, rail: str, config: dict):
    """
    Infinite loop that generates and publishes one payment per iteration.
    Each iteration:
      1. Build a random payment for this rail
      2. Publish ONLY to the rail's dedicated topic (e.g. payments.rtgs)
      3. Sleep to hit the target TPS

    payments.all is produced by PaymentStreamMerger (flink_jobs/payment_stream_merger.py),
    not here. Each rail producer is independent and unaware of the others.

    This function runs in its own thread (one thread per rail).
    """
    topic   = config["topic"]    # e.g. "payments.rtgs"
    tps     = config["tps"]      # e.g. 5.0 (events per second)
    sleep_s = 1.0 / tps          # e.g. 0.2s between events for 5 TPS

    logger.info(f"Starting {rail} producer → topic={topic} tps={tps}")

    while True:                                        # run forever until container stops
        # Apply overnight rate multiplier: during quiet hours, sleep 10x longer
        rate_multiplier = 1.0 if is_business_hours() else OVERNIGHT_RATE

        currency = random.choice(CURRENCIES)           # pick a random currency each event
        payment  = build_payment(rail, currency)       # generate the payment dict
        payload  = json.dumps(payment)                 # serialise dict → JSON string

        # Publish only to this rail's own topic — merger job handles payments.all
        producer.produce(topic, value=payload, callback=delivery_report)

        producer.poll(0)   # non-blocking poll: fires any pending delivery_report callbacks

        time.sleep(sleep_s / rate_multiplier)
        # sleep_s / rate_multiplier:
        #   business hours:  0.2 / 1.0 = 0.2s   → 5 TPS
        #   overnight:       0.2 / 0.1 = 2.0s   → 0.5 TPS (10x slower)


def main():
    """
    Entry point: creates one Kafka Producer and spawns one thread per rail.
    The main thread waits for all threads to finish (they run forever).
    """
    # Kafka producer configuration
    producer_conf = {
        "bootstrap.servers": KAFKA_SERVERS,  # where Kafka is running
        "acks":              "all",          # wait for all Kafka replicas to acknowledge
                                             # (strongest durability guarantee)
        "retries":           5,              # retry failed sends up to 5 times
        "linger.ms":         5,             # wait up to 5ms to batch small messages together
                                             # (improves throughput slightly)
        "compression.type":  "lz4",         # compress messages with LZ4 (fast compression)
    }
    producer = Producer(producer_conf)   # create the Kafka producer client

    logger.info(f"Payment Producer started | currencies={CURRENCIES} | kafka={KAFKA_SERVERS}")
    logger.info(f"Business hours: {HOURS_START} – {HOURS_END} UTC")

    import threading   # Python standard library for running code in parallel threads
    threads = []

    # Start one producer thread per rail (RTGS, CHAPS, INTERNAL)
    for rail, config in RAILS.items():
        t = threading.Thread(
            target=run_rail_producer,               # function to run in this thread
            args=(producer, rail, config),           # arguments to pass to the function
            daemon=True,                             # daemon=True means thread stops when main exits
            name=f"producer-{rail.lower()}",         # thread name (shows in logs/debugger)
        )
        t.start()             # launch the thread
        threads.append(t)     # keep a reference so we can join later

    try:
        for t in threads:
            t.join()          # block main thread until each producer thread finishes (they never do)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        producer.flush(timeout=10)   # wait up to 10s for in-flight messages to be delivered


if __name__ == "__main__":
    main()   # entry point when running as a module: python -m producers.payment_producer
