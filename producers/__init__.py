"""
producers package
==================
Kafka producers and PostgreSQL writers for the Intraday Liquidity Monitoring Pipeline.

Modules:
  payment_producer          — simulates real-time RTGS/CHAPS/Internal payment events
  pending_payments_generator — generates forward-looking scheduled payment obligations
  threshold_publisher        — treasury desk CLI tool for hot-reloading alert thresholds
"""

# Imports are intentionally omitted here so that confluent_kafka (only
# available inside Docker) is not loaded at package import time, allowing
# pytest to collect and run unit tests in a local Python environment.
# Individual modules are imported directly in test files and production code.
