"""
flink_jobs package
===================
Apache Flink streaming jobs for the Intraday Liquidity Monitoring Pipeline.

Modules:
  liquidity_position_engine — core Flink job: real-time balance tracking,
                               threshold checks, and alert emission (parallelism=16)
  threshold_manager         — Flink job that broadcasts live threshold updates
                               to all LiquidityPositionEngine instances (parallelism=1)
"""

# Imports are intentionally omitted here so that pyflink (only available
# inside Docker) is not loaded at package import time, allowing pytest to
# collect and run unit tests in a local Python environment.
# Individual modules are imported directly in test files and production code.
