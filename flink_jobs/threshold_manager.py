"""
Threshold Manager — Flink Job
==============================
Parallelism: 1  (MUST be 1 — only one writer is allowed for BroadcastState)

What this job does:
  Reads threshold update messages published by the treasury desk (via threshold_publisher.py),
  and makes them available to ALL 16 parallel instances of LiquidityPositionEngine
  via Flink BroadcastState — without restarting any job.

Why BroadcastState?
  LiquidityPositionEngine runs at parallelism=16.
  Each instance needs to know the thresholds for its accounts.
  BroadcastState is a Flink feature that lets you push the SAME data
  to ALL parallel instances simultaneously. Think of it like a
  "shared config" that all workers can read.

Why a compacted Kafka topic?
  The liquidity.thresholds topic uses Kafka log compaction.
  Log compaction means: for a given key (e.g. "GBP:RTGS-GBP-001"),
  Kafka only keeps the LATEST message. Older messages are deleted.
  This means on restart, ThresholdManager replays ALL keys from offset=0
  and rebuilds the full threshold map in seconds — no database needed.

Hot-reload flow:
  treasury desk runs threshold_publisher.py
      → message published to liquidity.thresholds (Kafka)
      → ThresholdManager reads it (parallelism=1)
      → ThresholdManager writes to BroadcastState
      → all 16 LiquidityPositionEngine instances see new threshold
      → applied within one Flink checkpoint cycle (~10 seconds)
  Total latency: < 30 seconds. No job restart required.
"""

import json          # for parsing threshold update messages from Kafka
import logging       # for printing status messages
import os            # for reading environment variables

from pyflink.common import WatermarkStrategy                  # needed for Kafka source setup
from pyflink.common.serialization import SimpleStringSchema   # read Kafka messages as plain strings
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,                 # reads from a Kafka topic
    KafkaOffsetsInitializer,     # controls where to start reading in the topic
)
from pyflink.datastream.functions import BroadcastProcessFunction, RuntimeContext
# BroadcastProcessFunction — special Flink function that handles two input streams:
#   1. A regular stream (non-broadcast side)
#   2. A broadcast stream (same data sent to ALL parallel instances)
from pyflink.datastream.state import MapStateDescriptor        # describes a key-value map state
from pyflink.common.typeinfo import Types                      # Flink type system

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ThresholdManager")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")   # read from docker-compose env

# ── BroadcastState descriptor ──────────────────────────────────────
# IMPORTANT: This descriptor must be IDENTICAL to the one in
# liquidity_position_engine.py — same name ("thresholds") and same types.
# Flink uses the name to match them up and share the state between jobs.
THRESHOLD_STATE_DESCRIPTOR = MapStateDescriptor(
    "thresholds",                                    # name — must match LiquidityPositionEngine
    Types.STRING(),                                  # key type:   "GBP:RTGS-GBP-001"
    Types.MAP(Types.STRING(), Types.DOUBLE()),        # value type: {"warning": 2e9, "critical": 5e8}
)


# ─────────────────────────────────────────────────────────────────
#  THRESHOLD BROADCAST FUNCTION
#  This class handles incoming threshold update messages.
#  It has two methods:
#    process_broadcast_element — called for the broadcast side (threshold updates)
#    process_element           — called for the regular side (not used here)
# ─────────────────────────────────────────────────────────────────
class ThresholdBroadcastFunction(BroadcastProcessFunction):
    """
    Reads threshold updates from the broadcast stream and writes them
    into BroadcastState so every LiquidityPositionEngine instance has
    access to the latest thresholds.

    Expected input message from threshold_publisher.py:
    {
        "currency":           "GBP",
        "settlement_account": "RTGS-GBP-001",
        "warning_threshold":  2000000000.0,    ← balance below this → WARNING alert
        "critical_threshold":  500000000.0,    ← balance below this → CRITICAL alert
        "updated_by":         "treasury-desk"
    }
    """

    def process_broadcast_element(self, update: dict, ctx: BroadcastProcessFunction.Context, out):
        """
        Called once per threshold update message.
        Writes the new threshold values into BroadcastState.
        Because this job runs at parallelism=1, there is exactly ONE
        instance writing to BroadcastState — no race conditions.
        """
        currency = update.get("currency")              # e.g. "GBP"
        account  = update.get("settlement_account")    # e.g. "RTGS-GBP-001"

        # Validate: both fields must be present
        if not currency or not account:
            logger.warning(f"Invalid threshold update (missing fields): {update}")
            return                                     # drop malformed messages silently

        # Build the lookup key in the same format LiquidityPositionEngine uses
        threshold_key = f"{currency}:{account}"        # e.g. "GBP:RTGS-GBP-001"

        # Build the threshold dict with float values
        threshold_value = {
            "warning":  float(update.get("warning_threshold",  float("inf"))),   # default inf = never alert
            "critical": float(update.get("critical_threshold", float("inf"))),   # default inf = never alert
        }

        # Write into BroadcastState — this immediately makes the new threshold
        # available to ALL 16 parallel LiquidityPositionFunction instances
        broadcast_state = ctx.get_broadcast_state(THRESHOLD_STATE_DESCRIPTOR)   # get the shared map
        broadcast_state.put(threshold_key, threshold_value)                      # write new value

        logger.info(
            f"Threshold updated: key={threshold_key} "
            f"warning={threshold_value['warning']:,.0f} "    # :,.0f = formatted with commas, no decimals
            f"critical={threshold_value['critical']:,.0f}"
        )

        # Emit a confirmation record downstream (optional — useful for audit sink)
        out.collect({
            "key":      threshold_key,
            "warning":  threshold_value["warning"],
            "critical": threshold_value["critical"],
        })

    def process_element(self, element, ctx: BroadcastProcessFunction.Context, out):
        """
        Called for records on the non-broadcast (regular) side.
        ThresholdManager has no regular input stream, so this is never called.
        Required by BroadcastProcessFunction interface — left empty.
        """
        pass


# ─────────────────────────────────────────────────────────────────
#  MAIN — Wire the ThresholdManager Flink job
# ─────────────────────────────────────────────────────────────────
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)   # continuous streaming, not batch
    env.set_parallelism(1)   # CRITICAL: must be 1 — BroadcastState requires a single writer
                             # If parallelism > 1, multiple instances would write to BroadcastState
                             # simultaneously, causing inconsistency

    # ── Kafka source: reads from the compacted threshold topic ─────
    threshold_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_SERVERS)            # Kafka broker address
        .set_topics("liquidity.thresholds")              # compacted topic (keeps latest per key)
        .set_group_id("threshold-manager")               # consumer group
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())   # ALWAYS start from beginning
        # ^ earliest means: on every restart, replay all messages from offset 0
        # Because the topic is compacted, this only replays the LATEST message per key
        # So we rebuild the full threshold map in seconds without hitting the database
        .set_value_only_deserializer(SimpleStringSchema())   # read as plain strings
        .build()
    )

    # Create the raw Kafka stream
    raw_thresholds = env.from_source(
        threshold_source,
        WatermarkStrategy.no_watermarks(),   # no time-based operations needed here
        "threshold-topic-source",            # operator name (visible in Flink Web UI)
    )

    # Parse JSON strings → Python dicts, filter out any None values
    threshold_updates = (
        raw_thresholds
        .map(lambda raw: json.loads(raw))    # JSON string → dict
        .filter(lambda x: x is not None)    # drop any parse failures
    )

    # Mark this stream as "broadcast" — Flink will send every record to ALL
    # downstream parallel instances instead of just one
    broadcast_stream = threshold_updates.broadcast(THRESHOLD_STATE_DESCRIPTOR)

    # Process the broadcast stream and write into BroadcastState
    # The .print() at the end logs every confirmed threshold update to stdout
    (broadcast_stream.process(ThresholdBroadcastFunction())   # write updates to BroadcastState
                     .map(json.dumps)                         # convert confirmation dict → JSON string
                     .print())                                # print confirmations to Flink logs

    env.execute("ThresholdManager")   # start the job (blocks until cancelled)


if __name__ == "__main__":
    main()
