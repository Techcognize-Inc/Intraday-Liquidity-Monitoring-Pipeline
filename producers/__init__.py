"""
producers package
==================
Kafka producers and PostgreSQL writers for the Intraday Liquidity Monitoring Pipeline.

Modules:
  payment_producer          — simulates real-time RTGS/CHAPS/Internal payment events
  pending_payments_generator — generates forward-looking scheduled payment obligations
  threshold_publisher        — treasury desk CLI tool for hot-reloading alert thresholds
"""

from producers.payment_producer import (
    build_payment,
    is_business_hours,
    run_rail_producer,
)
from producers.pending_payments_generator import (
    generate_pending_payments,
    load_to_postgres,
    run,
)
from producers.threshold_publisher import (
    publish_threshold,
    list_thresholds,
)

__all__ = [
    # payment_producer
    "build_payment",
    "is_business_hours",
    "run_rail_producer",
    # pending_payments_generator
    "generate_pending_payments",
    "load_to_postgres",
    "run",
    # threshold_publisher
    "publish_threshold",
    "list_thresholds",
]
