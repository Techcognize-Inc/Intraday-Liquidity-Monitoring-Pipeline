"""
Liquidity Position Engine — Core Flink Job
============================================
This is the most important file in the pipeline.
It reads every payment from Kafka, updates the running bank balance,
checks if the balance has breached a threshold, fires alerts if so,
and writes results to PostgreSQL — all in real time.

Parallelism: 16  ← 16 copies of this job run at the same time,
                    each handling one Kafka partition.

How data flows:
  payments.all (Kafka topic)
      │
      ▼
  parse_payment()          ← turns raw JSON string into a Python dict
      │
      ▼
  keyed by (currency, settlement_account)   ← e.g. "GBP:RTGS-GBP-001"
      │                                       Flink routes same key to same instance
      ├─► LiquidityPositionFunction   ← KeyedProcessFunction (one per key)
      │       • ValueState<balance>   ← stored in RocksDB, survives restarts
      │       • BroadcastState        ← receives live thresholds from ThresholdManager
      │       • EventTimeTimer        ← fires FeedStaleAlert after 60s silence
      │       • emits: position, alert, feed_health records
      │
      ├─► PostgreSQL sink             ← idempotent upsert (exactly-once via checkpoints)
      ├─► liquidity.warnings (Kafka)  ← WARNING alerts
      ├─► liquidity.critical (Kafka)  ← CRITICAL alerts
      ├─► ops.feed.health (Kafka)     ← stale/recover feed events
      │
      └─► 5-min TumblingWindow
              │
              ▼
          liquidity_flow_summary (PostgreSQL)
"""

import json                                    # for parsing Kafka JSON messages
import logging                                 # for printing log messages to stdout
import os                                      # for reading environment variables
from datetime import datetime, timezone        # for timestamp conversions

# ── Schema Registry note ───────────────────────────────────────────
# In production you would replace SimpleStringSchema (plain JSON strings)
# with Confluent Schema Registry + Avro or Protobuf serialisation:
#
#   from pyflink.datastream.formats.avro import AvroRowDeserializationSchema
#   SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
#
# Benefits:
#   • Schema is enforced at the Kafka broker level — malformed messages
#     are rejected before they ever reach Flink.
#   • Schema evolution (adding nullable fields) is tracked centrally.
#   • DLQ volume drops significantly because the registry acts as a
#     first line of defence before the stream even starts.
#
# For this project we use plain JSON + in-Flink schema validation (see
# validate_payment() below) and route failures to payments.dlq.
# ──────────────────────────────────────────────────────────────────

# PyFlink imports — these are the Flink streaming API classes
from pyflink.common import Time, WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema    # reads/writes plain strings
from pyflink.common.typeinfo import Types                      # tells Flink what type each state field is
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,                        # reads messages from a Kafka topic
    KafkaOffsetsInitializer,            # controls where to start reading (earliest/latest/committed)
    KafkaSink,                          # writes messages to a Kafka topic
    KafkaRecordSerializationSchema,     # tells Kafka sink how to serialise records
)
from pyflink.datastream.functions import (
    KeyedProcessFunction,               # base class for stateful per-key processing
    KeyedBroadcastProcessFunction,      # base class for keyed stream connected to broadcast stream
    BroadcastProcessFunction,           # base class for reading broadcast state
    RuntimeContext,                     # gives access to state backends at runtime
)
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
# ValueStateDescriptor — describes a single-value state variable (like a balance)
# MapStateDescriptor   — describes a key-value map state (like a thresholds dict)
from pyflink.datastream.window import TumblingEventTimeWindows  # fixed-size time windows
from pyflink.datastream.functions import ProcessWindowFunction, AggregateFunction
# AggregateFunction   — incrementally aggregates records as they arrive in a window
# ProcessWindowFunction — fires once when a window closes, accesses window metadata

logging.basicConfig(level=logging.INFO)                        # show INFO+ logs in stdout
logger = logging.getLogger("LiquidityPositionEngine")          # named logger for this job

# ── Config: read from environment variables set in docker-compose ──
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")  # Kafka broker address
PG_HOST       = os.getenv("POSTGRES_HOST", "postgres")              # PostgreSQL hostname
PG_PORT       = os.getenv("POSTGRES_PORT", "5432")                  # PostgreSQL port
PG_DB         = os.getenv("POSTGRES_DB", "liquidity")               # database name
PG_USER       = os.getenv("POSTGRES_USER", "liquidity_user")        # db username
PG_PASS       = os.getenv("POSTGRES_PASSWORD", "liquidity_pass")    # db password
PG_URL        = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"    # JDBC connection string

STALE_FEED_TIMEOUT_MS = 60_000   # 60,000ms = 60 seconds — if no payment arrives in this window, fire FeedStaleAlert

# ── Schema validation constants ────────────────────────────────────
# These define what a well-formed payment message must look like.
# Any message that fails these checks is routed to the DLQ instead
# of being silently dropped or causing a downstream KeyError.
REQUIRED_FIELDS = [
    "payment_id", "currency", "settlement_account",
    "amount", "direction", "rail", "event_time",
]
VALID_CURRENCIES = {"GBP", "USD", "EUR"}
VALID_DIRECTIONS = {"DEBIT", "CREDIT"}
VALID_RAILS      = {"RTGS", "CHAPS", "INTERNAL"}

# Expected amount ranges per rail (in GBP equivalent).
# Anything outside these bounds is almost certainly a producer bug —
# e.g. a £5 RTGS payment or a £900M INTERNAL transfer.
# These match the ranges defined in payment_producer.py AMOUNT_RANGES.
AMOUNT_RANGES = {
    "RTGS":     (10_000_000,   500_000_000),   # £10M  – £500M
    "CHAPS":    (1_000_000,    100_000_000),   # £1M   – £100M
    "INTERNAL": (100_000,       50_000_000),   # £100K – £50M
}

# ── BroadcastState descriptor ──────────────────────────────────────
# BroadcastState is a special Flink state that is SHARED across all
# parallel instances of an operator.
# ThresholdManager writes to it; all 16 LiquidityPositionFunction
# instances read from it. This is how hot-reload works.
THRESHOLD_STATE_DESCRIPTOR = MapStateDescriptor(
    "thresholds",                                   # state name — must match in ThresholdManager
    Types.STRING(),                                 # key type:   "GBP:RTGS-GBP-001"
    Types.MAP(Types.STRING(), Types.DOUBLE()),       # value type: {"warning": 2e9, "critical": 5e8}
)


# ─────────────────────────────────────────────────────────────────
#  STEP 1a: VALIDATE PAYMENT SCHEMA
#  Called by parse_payment() before any field access.
#  Returns None if the record is valid, or an error string describing
#  the first validation failure found.
# ─────────────────────────────────────────────────────────────────
def validate_payment(record: dict) -> str | None:
    """
    Checks that a parsed payment dict contains all required fields
    with the correct types and allowed values.

    Returns:
        None          — record is valid, safe to process
        str (error)   — human-readable reason the record is rejected
    """
    # 1. All required fields must be present
    for field in REQUIRED_FIELDS:
        if field not in record:
            return f"missing required field: '{field}'"

    # 2. currency must be one of the known settlement currencies
    if record["currency"] not in VALID_CURRENCIES:
        return f"invalid currency '{record['currency']}' — expected one of {VALID_CURRENCIES}"

    # 3. direction must be DEBIT or CREDIT — anything else would corrupt the balance
    if record["direction"] not in VALID_DIRECTIONS:
        return f"invalid direction '{record['direction']}' — expected DEBIT or CREDIT"

    # 4. rail must be a known payment system
    if record["rail"] not in VALID_RAILS:
        return f"invalid rail '{record['rail']}' — expected one of {VALID_RAILS}"

    # 5. amount must be a positive number — negative or zero amounts make no sense
    try:
        amount = float(record["amount"])
        if amount <= 0:
            return f"amount must be positive, got {amount}"
    except (ValueError, TypeError):
        return f"amount is not numeric: '{record['amount']}'"

    # 6. Amount must be within the expected range for this rail
    # Anything outside is almost certainly a producer bug, not a real payment.
    # Note: amount is already validated as a positive float by check 5 above.
    if rail in AMOUNT_RANGES:
        lo, hi = AMOUNT_RANGES[rail]
        if not (lo <= amount <= hi):
            return (
                f"amount {amount:,.0f} is outside expected range for {rail} "
                f"({lo:,.0f} – {hi:,.0f})"
            )

    return None   # all checks passed


# ─────────────────────────────────────────────────────────────────
#  STEP 1b: PARSE PAYMENT
#  Called once per message coming off the Kafka topic.
#  Converts a raw JSON string into a Python dict and adds event_time_ms
#  (milliseconds since epoch) so Flink can assign watermarks.
#
#  Returns a (tag, record) tuple so the stream can be split:
#    ("ok",  enriched_payment_dict)  — valid, routed to main pipeline
#    ("dlq", error_dict)             — invalid, routed to payments.dlq
#
#  Never raises — all failures are captured and tagged for the DLQ
#  instead of being silently dropped or crashing the Flink task.
# ─────────────────────────────────────────────────────────────────
def parse_payment(raw: str) -> tuple:
    """
    Stage 1 — JSON parse:
        Reject non-JSON messages immediately (e.g. corrupted bytes,
        wrong serialisation format).

    Stage 2 — Schema validation:
        Check required fields, allowed enum values, and numeric sanity.
        Any failure here means the message cannot be safely processed
        by LiquidityPositionFunction without risking a KeyError or
        incorrect balance update.

    Stage 3 — Timestamp enrichment:
        Parse event_time → epoch-ms so Flink watermarks work correctly.

    DLQ record format:
        {
            "raw":        <first 500 chars of the original message>,
            "payment_id": <if extractable, else null>,
            "error":      <human-readable reason>,
            "stage":      <"json_parse" | "schema_validation" | "timestamp_parse">,
            "detected_at": <ISO UTC timestamp when the error was caught>
        }
    """
    detected_at = datetime.now(tz=timezone.utc).isoformat()

    # ── Stage 1: JSON parse ───────────────────────────────────────
    try:
        record = json.loads(raw)                                   # parse JSON string → dict
    except Exception as e:
        logger.warning(f"DLQ | json_parse failed: {e} | raw={raw[:200]}")
        return ("dlq", {
            "raw":         raw[:500],              # truncate to avoid huge DLQ messages
            "payment_id":  None,
            "error":       f"JSON parse failed: {e}",
            "stage":       "json_parse",
            "detected_at": detected_at,
        })

    # ── Stage 2: Schema validation ────────────────────────────────
    error = validate_payment(record)
    if error:
        logger.warning(f"DLQ | schema_validation failed: {error} | payment_id={record.get('payment_id')}")
        return ("dlq", {
            "raw":         raw[:500],
            "payment_id":  record.get("payment_id"),   # include if present for traceability
            "error":       error,
            "stage":       "schema_validation",
            "detected_at": detected_at,
        })

    # ── Stage 3: Timestamp enrichment ────────────────────────────
    try:
        dt = datetime.fromisoformat(                               # parse ISO-8601 timestamp
            record["event_time"].replace("Z", "+00:00")            # Python needs +00:00 not Z
        )
        record["event_time_ms"] = int(dt.timestamp() * 1000)      # convert to epoch milliseconds
        return ("ok", record)                                      # valid — send to main pipeline
    except Exception as e:
        logger.warning(f"DLQ | timestamp_parse failed: {e} | payment_id={record.get('payment_id')}")
        return ("dlq", {
            "raw":         raw[:500],
            "payment_id":  record.get("payment_id"),
            "error":       f"event_time parse failed: {e}",
            "stage":       "timestamp_parse",
            "detected_at": detected_at,
        })


# ─────────────────────────────────────────────────────────────────
#  STEP 2: LIQUIDITY POSITION FUNCTION  (KeyedProcessFunction)
#
#  This class is instantiated once per (currency, settlement_account) key.
#  Flink guarantees: all payments for "GBP:RTGS-GBP-001" always go to
#  the SAME instance of this function. This means balance updates are
#  safe — no two threads ever touch the same account simultaneously.
#
#  State variables (stored in RocksDB, survive Flink restarts):
#    balance_state       — the current available balance for this account
#    total_in_state      — total money received today
#    total_out_state     — total money sent today
#    payment_count_state — number of payments processed
#    timer_registered    — timestamp of the currently registered stale-feed timer
# ─────────────────────────────────────────────────────────────────
class LiquidityPositionFunction(KeyedBroadcastProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        """
        Called once when the operator starts (or restores from checkpoint).
        This is where we register all our state variables with Flink.
        Flink manages the actual storage — we just declare what we need.
        """
        # Register the running balance as a ValueState<Double>
        # ValueState holds ONE value per key — perfect for a bank balance
        self.balance_state = runtime_context.get_state(
            ValueStateDescriptor("balance", Types.DOUBLE())        # name + type
        )
        # Cumulative inbound total (sum of all CREDIT amounts today)
        self.total_in_state = runtime_context.get_state(
            ValueStateDescriptor("total_inbound", Types.DOUBLE())
        )
        # Cumulative outbound total (sum of all DEBIT amounts today)
        self.total_out_state = runtime_context.get_state(
            ValueStateDescriptor("total_outbound", Types.DOUBLE())
        )
        # Running count of payments processed for this account
        self.payment_count_state = runtime_context.get_state(
            ValueStateDescriptor("payment_count", Types.INT())
        )
        # Stores the timestamp of the most recently registered stale-feed timer
        # We need this so we can DELETE the old timer when a new payment arrives
        self.timer_registered = runtime_context.get_state(
            ValueStateDescriptor("timer_ts", Types.LONG())         # LONG = 64-bit integer
        )
        # Tracks payment_ids already processed for this (currency, settlement_account) key.
        # Used for duplicate detection — if the same payment_id arrives twice (e.g. Kafka
        # at-least-once redelivery, producer retry), we skip the balance update to prevent
        # double-counting (e.g. a £50M DEBIT processed twice = £100M wrongly deducted).
        # Stored in RocksDB so dedup survives Flink restarts.
        # Production note: add state TTL (e.g. 24h) to prevent unbounded growth.
        self.seen_payments_state = runtime_context.get_map_state(
            MapStateDescriptor("seen_payments", Types.STRING(), Types.LONG())
            # key   = payment_id (UUID string)
            # value = event_time_ms (epoch-ms, kept for auditability)
        )
        # BroadcastState is accessed via ctx inside process_element (read-only)
        # and via ctx inside process_broadcast_element (read-write).
        # It is NOT accessible through RuntimeContext — do not call get_broadcast_state here.

    def process_broadcast_element(self, update: dict, ctx: KeyedBroadcastProcessFunction.Context, out):
        """
        Called for each threshold update on the broadcast stream.
        Writes the updated thresholds into BroadcastState so all parallel
        instances of process_element can read the latest values.
        """
        currency = update.get("currency")
        account  = update.get("settlement_account")
        if not currency or not account:
            return
        threshold_key = f"{currency}:{account}"
        threshold_value = {
            "warning":  float(update.get("warning_threshold",  float("inf"))),
            "critical": float(update.get("critical_threshold", float("inf"))),
        }
        ctx.get_broadcast_state(THRESHOLD_STATE_DESCRIPTOR).put(threshold_key, threshold_value)
        logger.info(f"BroadcastState updated: key={threshold_key} thresholds={threshold_value}")

    def process_element(self, payment: dict, ctx: KeyedBroadcastProcessFunction.ReadOnlyContext, out):
        """
        Called once per payment event.
        `payment` is the parsed dict from parse_payment().
        `ctx` gives access to timer service and current key.
        `out` is the output collector — we call out.collect() to emit records.
        """
        currency   = payment["currency"]              # e.g. "GBP"
        account    = payment["settlement_account"]   # e.g. "RTGS-GBP-001"
        amount     = float(payment["amount"])        # cast to float for maths
        direction  = payment["direction"]            # "DEBIT" or "CREDIT"
        event_ts   = payment["event_time_ms"]        # epoch-ms, used for timer registration
        payment_id = payment["payment_id"]           # UUID — used for dedup

        # ── 0. Duplicate detection ────────────────────────────────────
        # Check if we have already processed this payment_id for this key.
        # This guards against Kafka at-least-once redelivery and producer
        # retries, both of which can cause the same payment to arrive twice.
        # If duplicate: skip balance update entirely and route to DLQ so
        # ops can investigate the upstream cause.
        if self.seen_payments_state.contains(payment_id):
            logger.warning(
                f"Duplicate payment_id={payment_id} for key={currency}:{account} — "
                f"skipping balance update, routing to DLQ"
            )
            out.collect(("duplicate", {
                "payment_id":         payment_id,
                "currency":           currency,
                "settlement_account": account,
                "amount":             amount,
                "direction":          direction,
                "error":              f"duplicate payment_id already processed for {currency}:{account}",
                "stage":              "duplicate_detection",
                "detected_at":        datetime.now(tz=timezone.utc).isoformat(),
            }))
            return   # do NOT update balance — early exit

        # Mark this payment_id as seen so any future redelivery is caught
        self.seen_payments_state.put(payment_id, event_ts)

        # ── 1. Update the running balance ─────────────────────────────
        # .value() reads the current stored value; returns None if never set
        balance = self.balance_state.value() or 0.0   # default to 0 if no previous value

        if direction == "CREDIT":
            balance += amount                         # money coming IN increases balance
            total_in = (self.total_in_state.value() or 0.0) + amount   # accumulate inbound
            self.total_in_state.update(total_in)      # persist updated total back to RocksDB
        else:                                         # DEBIT
            balance -= amount                         # money going OUT decreases balance
            total_out = (self.total_out_state.value() or 0.0) + amount  # accumulate outbound
            self.total_out_state.update(total_out)    # persist updated total back to RocksDB

        self.balance_state.update(balance)            # persist new balance to RocksDB
        count = (self.payment_count_state.value() or 0) + 1   # increment payment count
        self.payment_count_state.update(count)        # persist updated count to RocksDB

        # ── 2. Check thresholds (read from BroadcastState) ────────────
        # Build the lookup key — must match the format ThresholdManager uses
        threshold_key = f"{currency}:{account}"       # e.g. "GBP:RTGS-GBP-001"
        thresholds = ctx.get_broadcast_state(THRESHOLD_STATE_DESCRIPTOR).get(threshold_key)   # returns dict or None

        if thresholds:                                # only check if thresholds are configured
            warning_threshold  = thresholds.get("warning",  float("inf"))   # default inf = never alert
            critical_threshold = thresholds.get("critical", float("inf"))   # default inf = never alert

            if balance < critical_threshold:          # CRITICAL check first (more severe)
                alert = self._build_alert("CRITICAL", currency, account, balance,
                                          critical_threshold, payment["payment_id"], event_ts)
                out.collect(("alert", alert))         # emit as (type, record) tuple

            elif balance < warning_threshold:         # only WARNING if NOT already critical
                alert = self._build_alert("WARNING", currency, account, balance,
                                          warning_threshold, payment["payment_id"], event_ts)
                out.collect(("alert", alert))         # emit WARNING alert

        # ── 3. Always emit a position record (goes to PostgreSQL) ─────
        # This runs regardless of whether an alert fired.
        # Grafana reads from liquidity_positions table to show live gauges.
        position = {
            "type":               "position",
            "currency":           currency,
            "settlement_account": account,
            "available_balance":  balance,                              # latest balance after this payment
            "total_inbound":      self.total_in_state.value()  or 0.0,  # cumulative today
            "total_outbound":     self.total_out_state.value() or 0.0,  # cumulative today
            "payment_count":      count,
            "last_payment_id":    payment["payment_id"],                # which payment triggered this update
            "event_time":         payment["event_time"],                # original ISO timestamp
        }
        out.collect(("position", position))           # emit as (type, record) tuple

        # ── 4. Manage the stale-feed detection timer ──────────────────
        # IDEA: Every payment resets a 60-second countdown.
        # If no payment arrives before 60s expires, on_timer() fires a FeedStaleAlert.
        # This detects payment feed outages (e.g. producer died, network issue).
        prev_timer = self.timer_registered.value()    # get timestamp of previously registered timer
        if prev_timer:
            ctx.timer_service().delete_event_time_timer(prev_timer)  # cancel the old countdown

        new_timer = event_ts + STALE_FEED_TIMEOUT_MS  # schedule timer 60s from THIS payment's time
        ctx.timer_service().register_event_time_timer(new_timer)     # register new countdown with Flink
        self.timer_registered.update(new_timer)       # remember this timer so we can cancel it next time

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext, out):
        """
        Flink calls this method when a registered EventTimeTimer fires.
        We get here only if NO payment arrived in the last 60 seconds.
        This means the payment feed for this account has gone silent — a problem!
        """
        stale_event = {
            "type":            "feed_stale",                              # type identifier
            "key":             ctx.get_current_key(),                     # e.g. "GBP:RTGS-GBP-001"
            "stale_since_ts":  timestamp - STALE_FEED_TIMEOUT_MS,        # when feed went silent
            "detected_at_ts":  timestamp,                                 # when we detected it
        }
        out.collect(("feed_health", stale_event))     # emit to ops.feed.health Kafka topic
        logger.warning(f"FeedStaleAlert: no payment for key={ctx.get_current_key()} in 60s")

    def _build_alert(self, alert_type, currency, account, balance, threshold, payment_id, event_ts):
        """Helper: constructs the alert dict emitted to Kafka and PostgreSQL."""
        return {
            "type":               "alert",
            "alert_type":         alert_type,           # "WARNING" or "CRITICAL"
            "currency":           currency,
            "settlement_account": account,
            "available_balance":  balance,              # current balance at time of breach
            "threshold_value":    threshold,            # the threshold that was breached
            "breach_amount":      threshold - balance,  # how far below the threshold we are
            "payment_id":         payment_id,           # which payment pushed us over the edge
            "triggered_at_ts":    event_ts,             # epoch-ms
            "triggered_at":       datetime.fromtimestamp(event_ts / 1000, tz=timezone.utc).isoformat(),  # human-readable UTC
        }


# ─────────────────────────────────────────────────────────────────
#  STEP 3: 5-MINUTE TUMBLING WINDOW (Net Flow Summary)
#
#  A tumbling window is a fixed-size, non-overlapping time bucket.
#  Every 5 minutes Flink closes the window and we emit one summary
#  record per (currency, account) with total inflow/outflow.
#  This powers the "Net Flow by Currency" Grafana panel.
#
#  We use TWO functions working together:
#    FlowAggregateFunction — processes each payment as it arrives
#                            (incrementally, memory-efficient)
#    FlowWindowFunction    — fires once when window closes,
#                            adds window timestamps to the result
# ─────────────────────────────────────────────────────────────────
class FlowAccumulator:
    """A simple accumulator that sums up flows within a window."""
    def __init__(self):
        self.net_flow  = 0.0   # inbound minus outbound — positive = net inflow
        self.inbound   = 0.0   # total credits in this window
        self.outbound  = 0.0   # total debits in this window
        self.count     = 0     # number of payments in this window


class FlowAggregateFunction(AggregateFunction):
    """
    Flink calls add() for each payment as it arrives in the window.
    Much more memory-efficient than storing all raw payments and aggregating at the end.
    """
    def create_accumulator(self):
        return FlowAccumulator()                       # start with zero accumulator for each new window

    def add(self, payment: dict, acc: FlowAccumulator):
        amount = float(payment["amount"])
        if payment["direction"] == "CREDIT":
            acc.inbound  += amount                     # money received
            acc.net_flow += amount                     # net flow goes up
        else:                                          # DEBIT
            acc.outbound += amount                     # money sent
            acc.net_flow -= amount                     # net flow goes down
        acc.count += 1                                 # count this payment
        return acc                                     # return updated accumulator

    def get_result(self, acc: FlowAccumulator):
        return acc                                     # pass the accumulator to FlowWindowFunction

    def merge(self, a: FlowAccumulator, b: FlowAccumulator):
        """Flink calls this when merging session windows — not used here but required."""
        a.net_flow += b.net_flow
        a.inbound  += b.inbound
        a.outbound += b.outbound
        a.count    += b.count
        return a


class FlowWindowFunction(ProcessWindowFunction):
    """
    Called once when the 5-minute window closes.
    Receives the single aggregated FlowAccumulator and adds window metadata.
    """
    def process(self, key, context, elements, out):
        acc = list(elements)[0]                        # only one element — the aggregated accumulator
        window = context.window()                      # the Window object with .start and .end (epoch-ms)
        currency, account = key.split(":", 1)          # split "GBP:RTGS-GBP-001" → ["GBP", "RTGS-GBP-001"]
        summary = {
            "type":               "flow_summary",
            "window_start":       datetime.fromtimestamp(window.start / 1000, tz=timezone.utc).isoformat(),  # e.g. "2026-04-01T09:00:00+00:00"
            "window_end":         datetime.fromtimestamp(window.end   / 1000, tz=timezone.utc).isoformat(),  # e.g. "2026-04-01T09:05:00+00:00"
            "currency":           currency,
            "settlement_account": account,
            "net_flow":           acc.net_flow,        # positive = net inflow, negative = net outflow
            "total_inbound":      acc.inbound,
            "total_outbound":     acc.outbound,
            "payment_count":      acc.count,
        }
        out.collect(json.dumps(summary))               # emit as JSON string to JDBC sink


# ─────────────────────────────────────────────────────────────────
#  SQL STATEMENTS used by the JDBC sinks
#  ON CONFLICT ... DO UPDATE = "upsert" — insert if new, update if exists
#  This gives idempotency: replaying the same payment twice gives the
#  same final result (important for checkpoint recovery).
# ─────────────────────────────────────────────────────────────────

# Upsert query for liquidity_positions table
# ? = positional parameter (filled in by the lambda in add_sink below)
UPSERT_POSITION_SQL = """
    INSERT INTO liquidity_positions
        (currency, settlement_account, available_balance,
         total_inbound, total_outbound, payment_count,
         last_payment_id, event_time, last_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?::timestamptz, NOW())
    ON CONFLICT (currency, settlement_account)        -- if this account already has a row...
    DO UPDATE SET
        available_balance = EXCLUDED.available_balance,  -- overwrite with new balance
        total_inbound     = EXCLUDED.total_inbound,
        total_outbound    = EXCLUDED.total_outbound,
        payment_count     = EXCLUDED.payment_count,
        last_payment_id   = EXCLUDED.last_payment_id,
        event_time        = EXCLUDED.event_time,
        last_updated      = NOW()                        -- always refresh timestamp
"""

# Upsert for 5-minute window summaries
UPSERT_FLOW_SUMMARY_SQL = """
    INSERT INTO liquidity_flow_summary
        (window_start, window_end, currency, settlement_account,
         net_flow, total_inbound, total_outbound, payment_count)
    VALUES (?::timestamptz, ?::timestamptz, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (window_start, currency, settlement_account)   -- idempotent re-delivery
    DO UPDATE SET
        net_flow       = EXCLUDED.net_flow,
        total_inbound  = EXCLUDED.total_inbound,
        total_outbound = EXCLUDED.total_outbound,
        payment_count  = EXCLUDED.payment_count
"""

# Insert for alerts — DO NOTHING on duplicate means replaying the same alert is safe
INSERT_ALERT_SQL = """
    INSERT INTO alert_log
        (alert_type, currency, settlement_account, available_balance,
         threshold_value, breach_amount, triggered_at, payment_id)
    VALUES (?, ?, ?, ?, ?, ?, ?::timestamptz, ?)
    ON CONFLICT DO NOTHING         -- don't error if we process the same alert twice
"""

# Update feed_health when stale event fires
UPDATE_FEED_HEALTH_SQL = """
    UPDATE feed_health
    SET is_stale = TRUE,
        stale_since = ?::timestamptz,
        last_check_time = NOW()
    WHERE rail = ?
"""


# ─────────────────────────────────────────────────────────────────
#  MAIN — Wires everything together into a Flink pipeline
# ─────────────────────────────────────────────────────────────────
def main():
    # Create the Flink streaming environment — this is the entry point for all Flink jobs
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)   # continuous real-time processing (not batch)
    env.set_parallelism(16)    # run 16 parallel instances — one per Kafka partition

    # ── Build Kafka source: reads from payments.all ────────────────
    # payments.all has 16 partitions (created in create_topics.sh)
    # Each of the 16 parallel Flink instances reads exactly one partition
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_SERVERS)           # where Kafka is running
        .set_topics("payments.all")                     # which topic to read
        .set_group_id("liquidity-position-engine")      # consumer group — Kafka tracks our offset
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(
            KafkaOffsetsInitializer.earliest()          # on first run, start from beginning
        ))                                              # on restart, resume from last committed offset
        .set_value_only_deserializer(SimpleStringSchema())  # read messages as plain strings
        .build()
    )

    # ── Build threshold Kafka source ───────────────────────────────
    # liquidity.thresholds is compacted (keeps only latest per key)
    # Always replay from earliest so ThresholdManager rebuilds full state on restart
    threshold_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_SERVERS)
        .set_topics("liquidity.thresholds")             # compacted threshold topic
        .set_group_id("liquidity-threshold-reader")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())  # always replay from start
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # ── Watermark strategy ─────────────────────────────────────────
    # Flink needs to know which field in each record is the "event time"
    # so it can track time progress and fire windows/timers correctly.
    # BoundedOutOfOrderness(1s) means: wait up to 1 second for late-arriving events
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(1))   # tolerate 1s of out-of-order events
        .with_timestamp_assigner(
            lambda event, _: event.get("event_time_ms", 0) if isinstance(event, dict) else 0
            # ^ this lambda tells Flink: "the event time is the event_time_ms field"
        )
    )

    # ── Create the raw payment stream from Kafka ───────────────────
    raw_payments = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),   # no watermarks yet — we assign them after parsing
        "payments-all-source"                # operator name (shows in Flink Web UI)
    )

    # ── Parse, validate, and split into ok vs DLQ ─────────────────
    # parse_payment() returns ("ok", record) or ("dlq", error_dict).
    # We split the stream here so bad messages go to payments.dlq
    # instead of being silently dropped or crashing downstream operators.
    parsed = raw_payments.map(parse_payment)   # ("ok"|"dlq", dict)

    # Valid payments — strip the tag, then assign watermarks for window/timer operations
    payments = (
        parsed
        .filter(lambda x: x[0] == "ok")                        # keep only valid records
        .map(lambda x: x[1])                                   # unwrap the tuple → plain dict
        .assign_timestamps_and_watermarks(watermark_strategy)  # tell Flink which field is event time
    )

    # Dead Letter Queue stream — invalid records routed to payments.dlq for ops inspection
    # Ops can consume this topic to debug schema issues or upstream producer bugs
    dlq_stream = (
        parsed
        .filter(lambda x: x[0] == "dlq")                      # keep only failed records
        .map(lambda x: json.dumps(x[1]))                       # serialise error dict → JSON string
    )

    # ── Create the broadcast threshold stream ─────────────────────
    # We broadcast threshold updates to ALL 16 parallel instances
    threshold_stream = (
        env.from_source(
            threshold_source,
            WatermarkStrategy.no_watermarks(),
            "threshold-source"
        )
        .map(lambda raw: json.loads(raw))               # parse JSON string → dict
        .broadcast(THRESHOLD_STATE_DESCRIPTOR)          # broadcast this stream to all parallel operators
    )

    # ── Key the payment stream by (currency:account) ───────────────
    # keyBy() routes all events with the same key to the same parallel instance
    # This is what guarantees our balance state is correct — same account, same machine
    keyed_payments = payments.key_by(
        lambda p: f"{p['currency']}:{p['settlement_account']}"   # e.g. "GBP:RTGS-GBP-001"
    )

    # ── Connect keyed payments with broadcast thresholds ──────────
    # connect() merges two streams so LiquidityPositionFunction can
    # see both payments AND threshold updates
    processed = (
        keyed_payments
        .connect(threshold_stream)                       # combine keyed stream + broadcast stream
        .process(LiquidityPositionFunction())            # apply our stateful processing function
    )

    # ── Split the output stream by record type ─────────────────────
    # LiquidityPositionFunction emits (type, record) tuples
    # We filter by type to route each record to the right sink
    positions   = processed.filter(lambda x: x[0] == "position").map(lambda x: x[1])           # → PostgreSQL
    alerts      = processed.filter(lambda x: x[0] == "alert").map(lambda x: x[1])             # → Kafka alerts
    feed_health = processed.filter(lambda x: x[0] == "feed_health").map(lambda x: x[1])       # → Kafka ops topic
    duplicates  = processed.filter(lambda x: x[0] == "duplicate").map(lambda x: json.dumps(x[1]))  # → payments.dlq

    # ── Build Kafka sinks for alerts ───────────────────────────────
    # WARNING alerts go to liquidity.warnings topic
    warnings_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("liquidity.warnings")             # destination topic
            .set_value_serialization_schema(SimpleStringSchema())   # write as plain string
            .build()
        )
        .build()
    )

    # CRITICAL alerts go to liquidity.critical topic (separate for severity routing)
    critical_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("liquidity.critical")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # Feed stale/recover events go to ops.feed.health
    feed_health_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("ops.feed.health")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # Route WARNING alerts to warnings topic, CRITICAL to critical topic
    (alerts.filter(lambda a: a["alert_type"] == "WARNING")   # only WARNING records
           .map(json.dumps)                                   # dict → JSON string
           .sink_to(warnings_sink))                           # write to Kafka

    (alerts.filter(lambda a: a["alert_type"] == "CRITICAL")  # only CRITICAL records
           .map(json.dumps)
           .sink_to(critical_sink))

    feed_health.map(json.dumps).sink_to(feed_health_sink)      # stale events → ops topic

    # ── 5-minute tumbling window for flow summary ──────────────────
    # Re-key the payments stream by currency:account
    # Group into 5-minute windows, aggregate flows, emit one record per window per key
    flow_summary_stream = (
        payments
        .key_by(lambda p: f"{p['currency']}:{p['settlement_account']}")   # group by account
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))              # 5-minute fixed windows
        .aggregate(FlowAggregateFunction(), FlowWindowFunction())          # aggregate then format
    )

    # ── PostgreSQL JDBC sinks ──────────────────────────────────────
    from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions

    # Connection config — tells JDBC how to connect to PostgreSQL
    jdbc_options = (
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(PG_URL)                              # jdbc:postgresql://postgres:5432/liquidity
        .with_driver_name("org.postgresql.Driver")     # PostgreSQL JDBC driver class
        .with_user_name(PG_USER)
        .with_password(PG_PASS)
        .build()
    )

    # Execution config — controls batching behaviour
    exec_options = (
        JdbcExecutionOptions.builder()
        .with_batch_interval_ms(1000)   # flush accumulated records to DB every 1 second
        .with_batch_size(200)           # or flush when 200 records are buffered, whichever first
        .with_max_retries(3)            # retry failed DB writes up to 3 times
        .build()
    )

    # Sink 1: liquidity_positions — one upsert per payment
    # The lambda maps field positions to the ? placeholders in UPSERT_POSITION_SQL
    positions.map(json.dumps).add_sink(
        JdbcSink.sink(
            UPSERT_POSITION_SQL,
            lambda stmt, pos: (
                stmt.set_string(1, pos["currency"]),             # ? 1 = currency
                stmt.set_string(2, pos["settlement_account"]),   # ? 2 = account
                stmt.set_double(3, pos["available_balance"]),    # ? 3 = balance
                stmt.set_double(4, pos["total_inbound"]),        # ? 4
                stmt.set_double(5, pos["total_outbound"]),       # ? 5
                stmt.set_int(6,    pos["payment_count"]),        # ? 6
                stmt.set_string(7, pos["last_payment_id"]),      # ? 7
                stmt.set_string(8, pos["event_time"]),           # ? 8
            ),
            jdbc_options,
            exec_options,
        )
    )

    # Sink 2: liquidity_flow_summary — one upsert per 5-min window
    flow_summary_stream.map(json.loads).add_sink(
        JdbcSink.sink(
            UPSERT_FLOW_SUMMARY_SQL,
            lambda stmt, s: (
                stmt.set_string(1, s["window_start"]),           # ? 1 = window start timestamp
                stmt.set_string(2, s["window_end"]),             # ? 2 = window end timestamp
                stmt.set_string(3, s["currency"]),               # ? 3
                stmt.set_string(4, s["settlement_account"]),     # ? 4
                stmt.set_double(5, s["net_flow"]),               # ? 5 (positive=inflow, negative=outflow)
                stmt.set_double(6, s["total_inbound"]),          # ? 6
                stmt.set_double(7, s["total_outbound"]),         # ? 7
                stmt.set_int(8,    s["payment_count"]),          # ? 8
            ),
            jdbc_options,
            exec_options,
        )
    )

    # Sink 3: alert_log — audit trail for every threshold breach
    alerts.add_sink(
        JdbcSink.sink(
            INSERT_ALERT_SQL,
            lambda stmt, a: (
                stmt.set_string(1, a["alert_type"]),             # ? 1 = "WARNING" or "CRITICAL"
                stmt.set_string(2, a["currency"]),               # ? 2
                stmt.set_string(3, a["settlement_account"]),     # ? 3
                stmt.set_double(4, a["available_balance"]),      # ? 4 = balance at time of breach
                stmt.set_double(5, a["threshold_value"]),        # ? 5 = threshold that was crossed
                stmt.set_double(6, a["breach_amount"]),          # ? 6 = how much below threshold
                stmt.set_string(7, a["triggered_at"]),           # ? 7 = ISO timestamp
                stmt.set_string(8, a["payment_id"]),             # ? 8 = which payment caused it
            ),
            jdbc_options,
            exec_options,
        )
    )

    # ── Dead Letter Queue (DLQ) Kafka sink ────────────────────────
    # payments.dlq receives every message that failed parse_payment().
    # Unlike silently dropping bad records, this gives ops a durable,
    # replayable record of every rejection with its reason and stage.
    # Ops workflow: consume payments.dlq → fix upstream producer →
    # republish corrected messages to payments.all → Flink reprocesses.
    dlq_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("payments.dlq")                         # dedicated DLQ topic
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    # Union both DLQ sources into the single payments.dlq topic:
    #   dlq_stream  — records rejected by parse_payment() (bad JSON, schema, timestamp)
    #   duplicates  — records rejected by LiquidityPositionFunction (duplicate payment_id)
    # Both use the same DLQ error format so ops has one topic to monitor.
    dlq_stream.union(duplicates).sink_to(dlq_sink)

    # Start the job — this call blocks until the job is cancelled or fails
    env.execute("LiquidityPositionEngine")


if __name__ == "__main__":
    main()   # entry point when running directly or via `flink run`
