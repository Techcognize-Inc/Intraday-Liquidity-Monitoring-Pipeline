"""
Payment Stream Merger — Flink Job
===================================
Reads from the three rail-specific Kafka topics and unions them into
the single payments.all topic that LiquidityPositionEngine consumes.

Why this exists (real-world pattern):
  In production each payment rail (RTGS, CHAPS, Internal) is a separate
  system owned by a separate team. Each publishes only to its own Kafka
  topic. No single producer has write access to every rail's topic.
  A dedicated merge job is responsible for producing the unified stream,
  applying consistent partitioning and normalisation before downstream
  consumers see the data.

What this job does:
  1. Opens a KafkaSource for each of the three rail topics
  2. Unions all three into a single DataStream
  3. Partitions by currency:settlement_account (consistent key routing)
  4. Writes to payments.all via KafkaSink

Key design decisions:
  - Partitioning by currency:settlement_account ensures that all payments
    for the same account always land on the same payments.all partition,
    so LiquidityPositionEngine's KeyedProcessFunction sees a coherent
    ordered stream per key without cross-partition coordination.
  - No state is held here — this job is stateless. All business logic
    (dedup, threshold checks, balance tracking) lives in LiquidityPositionEngine.
  - Schema normalisation: each rail message already uses the canonical
    payment schema from payment_producer.py. In production you would add
    a normalise() step here to map rail-specific schemas to the canonical one.

Data flow:
  payments.rtgs     ─┐
  payments.chaps    ─┼──► union ──► key by currency:account ──► payments.all
  payments.internal ─┘

Parallelism: 3 (one task slot per source topic is sufficient here,
              since the merge itself is lightweight)
"""

import json
import logging
import os

from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("PaymentStreamMerger")

# ── Configuration ──────────────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Source topics — one per rail, each owned by an independent producer
RAIL_TOPICS = [
    "payments.rtgs",
    "payments.chaps",
    "payments.internal",
]

# Sink topic — consumed by LiquidityPositionEngine
OUTPUT_TOPIC = "payments.all"

# Consumer group for this merger job
GROUP_ID = "payment-stream-merger"


def extract_partition_key(raw: str) -> str:
    """
    Derives the Kafka message key from the payment JSON.
    Key format: "currency:settlement_account"  e.g. "GBP:RTGS-GBP-001"

    Why this matters:
      Kafka routes messages with the same key to the same partition.
      LiquidityPositionEngine is keyed by (currency, settlement_account),
      so if all payments for a given account land on the same partition
      they are consumed by the same Flink task instance — no cross-task
      state sharing needed.

    If JSON is malformed we fall back to None (Kafka round-robins keyless
    messages), which is safe — LiquidityPositionEngine's parse_payment()
    will route the record to the DLQ anyway.
    """
    try:
        record = json.loads(raw)
        currency = record.get("currency", "")
        account  = record.get("settlement_account", "")
        if currency and account:
            return f"{currency}:{account}"
    except (json.JSONDecodeError, AttributeError):
        pass
    return None   # keyless → Kafka round-robins across partitions


def build_kafka_source(topic: str) -> KafkaSource:
    """
    Builds a KafkaSource for a single rail topic.
    Called once per rail in build_pipeline().

    Offset strategy: EARLIEST on first start so we don't miss messages
    published before this merger job was deployed. On restarts Flink
    resumes from the last committed offset (stored in Kafka __consumer_offsets).
    """
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_SERVERS)
        .set_topics(topic)
        .set_group_id(GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_pipeline(env: StreamExecutionEnvironment) -> None:
    """
    Wires together the full merge pipeline and registers the sink.

    Steps:
      1. Build one DataStream per rail topic
      2. Union all three into a single stream
      3. Map each raw JSON string to a (key, value) tuple for the sink
      4. Write to payments.all with partition key so downstream
         consumers get consistent key-based routing
    """

    # ── Step 1: one source stream per rail ────────────────────────
    # WatermarkStrategy.no_watermarks() because this merger is stateless —
    # we don't do any time-based operations here. Event time watermarks are
    # applied by LiquidityPositionEngine after it reads from payments.all.
    rail_streams = []
    for topic in RAIL_TOPICS:
        source = build_kafka_source(topic)
        stream = env.from_source(
            source,
            WatermarkStrategy.no_watermarks(),
            f"KafkaSource-{topic}",          # operator name shown in Flink UI
        )
        rail_streams.append(stream)
        logger.info(f"Registered source: {topic}")

    # ── Step 2: union all three into one stream ────────────────────
    # DataStream.union() merges N streams into one. Order is not guaranteed
    # across streams (depends on which Kafka partitions deliver messages first),
    # which is fine — LiquidityPositionEngine uses event time, not arrival order.
    merged = rail_streams[0].union(*rail_streams[1:])

    # ── Step 3: attach partition key for the sink ──────────────────
    # We derive the key here so the KafkaSink can use it as the Kafka
    # message key. The map produces (key_or_None, raw_json) tuples.
    keyed = merged.map(
        lambda raw: (extract_partition_key(raw), raw),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()]),
    ).name("ExtractPartitionKey")

    # ── Step 4: KafkaSink → payments.all ──────────────────────────
    # KafkaRecordSerializationSchema lets us set both the key and value.
    # Key  → currency:settlement_account  (routes to consistent partition)
    # Value → raw JSON string (unchanged — no transformation in the merger)
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_key_serialization_schema(SimpleStringSchema())      # tuple[0] → key
            .set_value_serialization_schema(SimpleStringSchema())    # tuple[1] → value
            .build()
        )
        # AT_LEAST_ONCE: if the job restarts before a checkpoint, some messages
        # may be re-delivered to payments.all. LiquidityPositionEngine deduplicates
        # by payment_id so duplicate delivery is safe end-to-end.
        .set_delivery_guarantee(
            __import__("pyflink.datastream.connectors.kafka", fromlist=["DeliveryGuarantee"])
            .DeliveryGuarantee.AT_LEAST_ONCE
        )
        .build()
    )

    keyed.sink_to(sink).name(f"KafkaSink-{OUTPUT_TOPIC}")
    logger.info(f"Sink registered: {OUTPUT_TOPIC}")


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    # 3 parallel tasks — one per source topic is sufficient for a stateless merge job.
    # Increase if rail volume grows beyond what 3 tasks can handle.
    env.set_parallelism(3)

    # Checkpoint every 30 seconds so that on restart we resume from a recent
    # committed offset rather than replaying from the beginning.
    env.enable_checkpointing(30_000)   # milliseconds

    build_pipeline(env)

    logger.info("Submitting PaymentStreamMerger job...")
    env.execute("PaymentStreamMerger")


if __name__ == "__main__":
    main()
