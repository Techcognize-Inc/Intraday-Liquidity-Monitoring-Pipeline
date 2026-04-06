"""
tests package
==============
Unit and integration tests for the Intraday Liquidity Monitoring Pipeline.

Test modules:
  test_payment_producer     — build_payment() shape, fan-out, business-hours rate logic
  test_position_engine      — balance updates, threshold breach detection, state recovery
  test_stale_feed_detection — EventTime timer fires FeedStaleAlert after 60 s silence
  test_threshold_broadcast  — BroadcastState hot-reload propagates to all parallel instances
"""
