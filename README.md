# Intraday Liquidity Monitoring Pipeline

**Project Code:** DE-PRJ-004  
**Division:** Enterprise Banking — Treasury Operations  
**Status:** In Development  
**Last Updated:** March 2026

---

## Table of Contents

- [Executive Summary](#executive-summary)
- [Business Problem](#business-problem)
- [Business Objectives](#business-objectives)
- [Business Value](#business-value)
- [Key Stakeholders](#key-stakeholders)
- [High-Level Architecture](#high-level-architecture)
- [Pipeline Components](#pipeline-components)
- [Technology Stack](#technology-stack)
- [Advanced Flink Features](#advanced-flink-features)
- [Scalability & Latency Targets](#scalability--latency-targets)
- [Dashboard Design](#dashboard-design)
- [Data Sources](#data-sources)
- [Acceptance Criteria](#acceptance-criteria)

---

## Executive Summary

This project delivers a **Flink-native real-time liquidity position engine** that replaces the bank's current overnight batch-based liquidity calculation with a continuous streaming pipeline. The system updates available liquidity across all currencies and settlement accounts as payments flow in and out, alerts the treasury desk within seconds of a threshold breach, and provides a live Grafana command dashboard for intraday liquidity management.

---

## Business Problem

The Treasury team currently calculates the bank's liquidity position **once per day** using overnight batch files. This creates a critical visibility gap during trading hours:

- Large unexpected payment obligations mid-morning leave the treasury desk operating without accurate position data.
- The bank risks drawing on costly **intraday overdraft credit facilities** due to lack of real-time visibility.
- In extreme cases, a missed **RTGS payment deadline** constitutes a direct regulatory breach, carrying penalties of **£1M+** per incident.
- The earliest visibility into a developing liquidity shortfall is the next scheduled batch run — often hours too late for corrective action.

**Current State:** Batch-computed position (stale by market open)  
**Target State:** Continuous streaming position (updated within seconds of every payment event)

---

## Business Objectives

| # | Objective | Success Measure |
|---|-----------|-----------------|
| 1 | Continuously update liquidity position as payments flow | Position reflects all settled payments within 5 seconds |
| 2 | Alert treasury desk on threshold breaches in real time | Warning/Critical alerts delivered within 5 seconds of breach |
| 3 | Detect silent payment feed failures automatically | Stale feed alert fires within 60 seconds of feed dropout |
| 4 | Enable treasury to adjust thresholds without system restart | New thresholds active in Flink within 30 seconds of publish |
| 5 | Provide a live command dashboard for trading hours | Grafana dashboard with 5-second auto-refresh on wall monitors |
| 6 | Address Basel III LCR intraday monitoring requirements | Full audit trail of position changes in time-series database |

---

## Business Value

| Value Driver | Impact |
|--------------|--------|
| Prevent missed RTGS settlement penalties | Avoids **£1M+** regulatory fines per incident |
| Reduce intraday overdraft credit line usage | Saves **$50K–$200K** per avoided drawdown |
| Proactive liquidity management | Visibility **30–60 minutes earlier** than any batch approach |
| Basel III regulatory compliance | Directly addresses LCR intraday monitoring expectations |
| Operational risk reduction | Eliminates blind spots during peak settlement windows |

---

## Key Stakeholders

| Role | Responsibility |
|------|---------------|
| **Treasury Desk** | Primary user — monitors live position, adjusts thresholds, takes corrective action |
| **Treasury Head** | Owns liquidity risk policy, sets warning/critical threshold levels |
| **IT Operations** | Monitors Flink job health, feed connectivity, infrastructure |
| **Regulatory Reporting** | Consumes position time-series for Basel III LCR reporting |
| **Data Engineering** | Builds, deploys, and maintains the streaming pipeline |

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          PAYMENT SOURCES                                │
│                                                                         │
│   ┌──────────┐      ┌──────────────┐      ┌───────────────────┐        │
│   │   RTGS   │      │    CHAPS     │      │ Internal Transfers│        │
│   │ Gateway  │      │   Payments   │      │                   │        │
│   └────┬─────┘      └──────┬───────┘      └────────┬──────────┘        │
│        │                   │                        │                   │
└────────┼───────────────────┼────────────────────────┼───────────────────┘
         │                   │                        │
         ▼                   ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        APACHE KAFKA                                     │
│                                                                         │
│   payments.rtgs       payments.chaps       payments.internal            │
│   (16 partitions)     (16 partitions)      (16 partitions)              │
│        │                   │                        │                   │
│        └───────────┬───────┘────────────────────────┘                   │
│                    ▼                                                     │
│             payments.all  (merged, keyed by currency)                   │
│                                                                         │
│   liquidity.thresholds    liquidity.warnings    liquidity.critical      │
│   (treasury updates)      (warning alerts)      (critical alerts)       │
└────────────┬──────────────────────┬─────────────────────────────────────┘
             │                      │
             ▼                      ▼
┌────────────────────────┐  ┌────────────────────────────────────────────┐
│  FLINK JOB 2           │  │  FLINK JOB 1                               │
│  ThresholdManager      │  │  LiquidityPositionEngine                   │
│  (parallelism=1)       │  │  (parallelism=16, exactly-once)            │
│                        │  │                                            │
│  Reads threshold       │  │  KeyBy(currency + settlement_account)      │
│  updates from Kafka    │──│  ValueState: running balance per key       │
│  Distributes via       │  │  Threshold comparison on every update      │
│  Broadcast State       │  │  EventTimeTimer: stale feed detection      │
│                        │  │  Tumbling 5-min window: net flow summary   │
│  No restart required   │  │  TwoPhaseCommit JDBC Sink                 │
│  for threshold changes │  │  RocksDB + Incremental Checkpointing      │
└────────────────────────┘  └──────────────────┬─────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         POSTGRESQL                                      │
│                                                                         │
│   liquidity_positions (time-series)    liquidity_flow_summary           │
│   pending_payments                     feed_health_status               │
└────────────────────────┬────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     MONITORING & DASHBOARD                              │
│                                                                         │
│   ┌──────────────┐          ┌──────────────────────────────────┐       │
│   │  Prometheus   │          │  GRAFANA LIVE DASHBOARD           │       │
│   │  (Flink       │─────────▶│  (Treasury Desk, 5-sec refresh)  │       │
│   │   metrics     │          │                                   │       │
│   │   port 9249)  │          │  • Live Position Gauge            │       │
│   └──────────────┘          │  • Intraday Timeline              │       │
│                              │  • Net Flow by Currency           │       │
│   ┌──────────────┐          │  • Pending Payments Queue         │       │
│   │  Airflow      │          │  • Feed Health Panel              │       │
│   │  (morning DAG │          └──────────────────────────────────┘       │
│   │   pending     │                                                     │
│   │   payments)   │                                                     │
│   └──────────────┘                                                      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Pipeline Components

### 1. Payment Ingestion Layer

Three upstream payment systems (RTGS, CHAPS, Internal Transfers) publish events to dedicated Kafka topics. A Kafka Streams topology merges these into a unified `payments.all` topic, keyed by currency, ensuring all events for a given currency are processed by the same Flink subtask.

### 2. Flink Job 1 — LiquidityPositionEngine

The core streaming engine. Maintains a **running balance** per (currency, settlement_account) pair using `ValueState` backed by RocksDB. On every payment event:

- **Inflow** → balance increases
- **Outflow** → balance decreases
- Compares updated balance against warning and critical thresholds
- Emits alerts to dedicated Kafka topics on threshold breach
- Fires a stale feed alert if no event arrives for 60 seconds (silent feed failure detection)
- Writes position snapshots to PostgreSQL via TwoPhaseCommit exactly-once sink
- Computes 5-minute net flow summaries via tumbling window aggregation

### 3. Flink Job 2 — ThresholdManager

A lightweight job (parallelism=1) that reads threshold updates published by the treasury desk to a Kafka topic. Distributes updated thresholds to all LiquidityPositionEngine subtasks via **Broadcast State**. Enables treasury to change warning/critical levels multiple times per day without any pipeline restart.

### 4. Batch Layer — Airflow

A morning Airflow DAG loads the day's scheduled payment file into a `pending_payments` table. The Grafana dashboard reads this to show the treasury desk the 10 largest pending outbound payments in the next 2 hours — enabling pre-positioning before large payments fall due.

### 5. Dashboard — Grafana

A wall-monitor-style command dashboard with 5-second auto-refresh, designed for continuous display on the treasury floor during trading hours. See [Dashboard Design](#dashboard-design) for panel details.

---

## Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Stream Processing | Apache Flink (PyFlink) | Real-time position engine, threshold monitoring, stale feed detection |
| Message Broker | Apache Kafka | Event transport, partitioned by currency, 7-day retention |
| State Backend | RocksDB | Persistent state for running balances, survives job restarts |
| Database | PostgreSQL | Position time-series, flow summaries, pending payments |
| Orchestration | Apache Airflow | Morning pending payments load, SLA monitoring |
| Dashboard | Grafana | Live treasury command dashboard, 5-second refresh |
| Metrics | Prometheus | Flink job metrics, backpressure monitoring, feed health |
| Data Generation | Faker (Python) | Simulated RTGS/CHAPS/SWIFT payment events |

---

## Advanced Flink Features

| Feature | Usage in This Project | Enterprise Rationale |
|---------|----------------------|---------------------|
| **ValueState + RocksDB** | Running balance per (currency, account) key. State survives job restarts without reprocessing history. | A running balance must be exact — any state loss means treasury sees incorrect position. RocksDB-backed ValueState with exactly-once guarantees correctness. |
| **Broadcast State** | ThresholdManager distributes threshold values to all subtasks. Treasury updates thresholds via Kafka with immediate effect. | Treasury adjusts thresholds multiple times per day based on expected payment flows. Broadcast State allows this without interrupting the position engine. |
| **EventTimeTimers** | Fire if no payment event arrives for 60 seconds per currency — indicating a stale or disconnected payment feed. | A silent feed failure is as dangerous as a real liquidity shortage. Timers provide operational watchdog functionality that batch approaches cannot replicate. |
| **Tumbling Window** | 5-minute event-time windows computing net inflow/outflow per currency. Feeds the Grafana net flow trend panel. | Tumbling windows on event time produce accurate 5-minute net flow figures even if payment events arrive slightly late. |
| **TwoPhaseCommit Sink** | Exactly-once writes to `liquidity_positions` table. Prevents duplicate position records on job failure/recovery. | Liquidity position is a regulatory data point — duplicate or missing records are a compliance issue, not just an inconvenience. |
| **Incremental Checkpointing** | Every 30 seconds, only state deltas written to checkpoint store. Enables recovery to correct state in under 2 minutes. | Full state checkpoints would require writing entire RocksDB state every interval. Incremental checkpointing reduces checkpoint I/O by 90%. |

---

## Scalability & Latency Targets

| Metric | Target | How Achieved |
|--------|--------|-------------|
| **Payment Volume** | 50K payments/day, up to 500 TPS during settlement windows | 16 Kafka partitions keyed by currency. Flink parallelism=16. Linear scale-out by adding TaskManagers. |
| **End-to-End Latency** | Position update in Grafana within 5 seconds of payment event | Flink processing < 100ms. JDBC sink writes every 10 seconds. Grafana refreshes every 5 seconds. |
| **State Recovery** | Correct state restored in under 2 minutes after failure | RocksDB incremental checkpoints every 30 seconds. Kafka retains 7 days for replay if checkpoint lost. |
| **Threshold Propagation** | New threshold active within 10 seconds of treasury publishing | Broadcast State propagation within one checkpoint interval (30 seconds max). |

---

## Dashboard Design

**Tool:** Grafana — `localhost:3000` (5-second auto-refresh)  
**Provisioning:** Fully provisioned via `grafana_dashboard.json` — importable in one step, no manual configuration.

### Dashboard Panels

| Panel | Type | Description |
|-------|------|-------------|
| **Live Position Gauge** | Stat Panel | Current GBP liquidity as a large number. Background: green (above warning), amber (between warning and critical), red (below critical). Updates every 5 seconds. |
| **Intraday Timeline** | Time Series | Position from 00:00 to now. Reference lines: Opening Position (dotted blue), Warning Threshold (dashed amber), Critical Threshold (solid red). |
| **Net Flow by Currency** | Bar Chart | Net inflow minus outflow per currency (GBP, USD, EUR) for current 5-minute window. Sourced from Flink tumbling window output. |
| **Pending Payments Queue** | Table | 10 largest pending outbound payments in next 2 hours. Columns: Amount, Currency, Due Time, Counterparty. |
| **Feed Health Panel** | Status Indicator | Status per payment feed (RTGS, CHAPS, Internal). Turns amber/red when Flink FeedStaleAlert fires. |

### User Story

> At 10:47, a £85M RTGS payment leaves. The position gauge turns amber. The treasury dealer sees the timeline dipping toward the critical band. She initiates a repo transaction. By 11:05, position is green. Without the Flink-powered dashboard, the next visibility point would have been the 14:00 batch run — too late.

---

## Data Sources

| Source | Format | Volume | Partitioning |
|--------|--------|--------|-------------|
| RTGS Gateway | Kafka events (JSON) | ~20K payments/day | By currency |
| CHAPS Payments | Kafka events (JSON) | ~15K payments/day | By currency |
| Internal Transfers | Kafka events (JSON) | ~15K payments/day | By currency |
| Pending Payments File | CSV (morning load) | ~500 records/day | N/A |
| Threshold Updates | Kafka events (JSON) | Ad-hoc (treasury driven) | Single partition |

**Simulation:** All payment sources generated via Faker with realistic intraday volume patterns — heavy outflows 09:00–11:00, heavy inflows 14:00–16:00. Configurable failure injection for feed dropout simulation.

---

## Acceptance Criteria

| # | Criterion | Validation Method |
|---|-----------|-------------------|
| AC-1 | Payment producer running — events visible in `payments.all` Kafka topic | Kafka console consumer shows events from all 3 sources |
| AC-2 | Flink LiquidityPositionEngine updates `liquidity_positions` within 5 seconds of each payment event | Timestamp comparison: event timestamp vs database write timestamp |
| AC-3 | Inject large outbound payment breaching WARNING_THRESHOLD — alert appears in `liquidity.warnings` within 5 seconds | Kafka consumer on warnings topic, measure latency |
| AC-4 | Update threshold via `liquidity.thresholds` Kafka topic — Flink applies new threshold within 30 seconds, no job restart | Publish threshold, verify next breach uses new value |
| AC-5 | Simulate payment feed dropout — FeedStaleAlert fires within 60 seconds, Feed Health panel turns amber | Stop payment producer for one currency, observe alert |
| AC-6 | Kill Flink job, restart — balance resumes from correct value (RocksDB checkpoint restored) | Compare pre-kill balance with post-restart balance |

---

## Getting Started

```bash
# 1. Start infrastructure
docker-compose up -d kafka zookeeper postgresql grafana prometheus

# 2. Create Kafka topics
./scripts/create_topics.sh

# 3. Start Flink cluster
./scripts/start_flink_cluster.sh

# 4. Deploy ThresholdManager job
flink run -py jobs/threshold_manager.py

# 5. Deploy LiquidityPositionEngine job
flink run -py jobs/liquidity_position_engine.py

# 6. Start payment producer (simulated data)
python producers/payment_producer.py --tps 50

# 7. Load Grafana dashboard
curl -X POST http://localhost:3000/api/dashboards/db -d @grafana/grafana_dashboard.json

# 8. Start Airflow scheduler
airflow scheduler &
airflow webserver &
```

---

## Repository Structure

```
intraday-liquidity-monitoring/
├── README.md
├── docker-compose.yml
├── jobs/
│   ├── liquidity_position_engine.py      # Flink Job 1: Position engine
│   └── threshold_manager.py              # Flink Job 2: Threshold broadcast
├── producers/
│   ├── payment_producer.py               # Faker-based payment event generator
│   └── threshold_publisher.py            # Threshold update publisher
├── dags/
│   └── pending_payments_dag.py           # Airflow morning DAG
├── sql/
│   ├── schema.sql                        # PostgreSQL table definitions
│   └── seed_thresholds.sql               # Default threshold values
├── grafana/
│   ├── grafana_dashboard.json            # Full dashboard provisioning
│   └── datasources.yml                   # PostgreSQL + Prometheus config
├── config/
│   ├── flink-conf.yaml                   # Flink cluster configuration
│   └── kafka-topics.json                 # Topic definitions
├── scripts/
│   ├── create_topics.sh
│   └── start_flink_cluster.sh
└── tests/
    ├── test_position_engine.py
    ├── test_threshold_broadcast.py
    └── test_stale_feed_detection.py
```

---

> **Enterprise data engineering is not about writing more Flink code. It is about writing the right Flink code for the right reasons.**
