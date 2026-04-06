"""
Tests: Stale Feed Detection (FeedStaleAlert)
=============================================
Covers acceptance criterion #5:
  "Simulate payment feed dropout → FeedStaleAlert fires within 60s,
   Feed Health panel turns amber"

Tests verify:
  - on_timer() emits a feed_stale event when called (simulates 60s timer firing)
  - feed_stale event contains the correct key and timestamps
  - No stale event is emitted if payments keep arriving (timer reset each time)
  - Timer is correctly cancelled and re-registered on each payment arrival
  - on_timer() does NOT emit a position or alert record — only feed_health
  - FeedStaleAlert includes stale_since_ts = detected_at_ts - 60_000ms

These are pure unit tests — no Flink runtime needed.
We call on_timer() directly on the LiquidityPositionFunction instance.
"""

import unittest
from unittest.mock import MagicMock

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flink_jobs.liquidity_position_engine import (
    LiquidityPositionFunction,
    STALE_FEED_TIMEOUT_MS,
)

# Reuse helpers from test_position_engine
from tests.test_position_engine import (
    MockRuntimeContext,
    MockContext,
    MockValueState,
    make_payment,
)


class MockOnTimerContext:
    """
    Mimics KeyedProcessFunction.OnTimerContext.
    Provides the key and current timestamp to on_timer().
    """
    def __init__(self, key="GBP:RTGS-GBP-001"):
        self._key = key

    def get_current_key(self):
        return self._key

    def window(self):
        return None


def make_function_with_payments(payments, thresholds=None):
    """Helper: open a LiquidityPositionFunction and drive it with payments."""
    func = LiquidityPositionFunction()
    ctx  = MockRuntimeContext(thresholds=thresholds or {})
    func.open(ctx)

    collected = []
    mock_ctx  = MockContext()
    for payment in payments:
        func.process_element(payment, mock_ctx, collected)

    return func, collected, mock_ctx


class TestOnTimerFiresStaleAlert(unittest.TestCase):

    def test_on_timer_emits_feed_health_event(self):
        """
        Calling on_timer() directly (as Flink would after 60s silence)
        must emit exactly one feed_health record.
        """
        func, _, _ = make_function_with_payments([make_payment()])

        collected_from_timer = []
        timer_ts = 1743498060000   # arbitrary timestamp
        timer_ctx = MockOnTimerContext("GBP:RTGS-GBP-001")

        func.on_timer(timer_ts, timer_ctx, collected_from_timer)

        feed_health_events = [r for t, r in collected_from_timer if t == "feed_health"]
        self.assertEqual(len(feed_health_events), 1,
                         msg="on_timer() must emit exactly one feed_health event")

    def test_stale_event_type_is_feed_stale(self):
        """The emitted record must have type='feed_stale'."""
        func, _, _ = make_function_with_payments([make_payment()])
        collected = []
        func.on_timer(1743498060000, MockOnTimerContext(), collected)

        feed_event = [r for t, r in collected if t == "feed_health"][0]
        self.assertEqual(feed_event["type"], "feed_stale")

    def test_stale_since_ts_is_60s_before_detected(self):
        """
        stale_since_ts must be detected_at_ts - STALE_FEED_TIMEOUT_MS (60,000ms).
        This tells the dashboard exactly when the feed went silent.
        """
        func, _, _ = make_function_with_payments([make_payment()])
        collected = []
        timer_ts  = 1743498060000
        func.on_timer(timer_ts, MockOnTimerContext(), collected)

        feed_event = [r for t, r in collected if t == "feed_health"][0]
        self.assertEqual(
            feed_event["detected_at_ts"],
            timer_ts,
        )
        self.assertEqual(
            feed_event["stale_since_ts"],
            timer_ts - STALE_FEED_TIMEOUT_MS,
        )

    def test_stale_event_contains_correct_key(self):
        """The key in the stale event must match the (currency:account) key."""
        key = "EUR:NOSTRO-EUR-001"
        func, _, _ = make_function_with_payments([
            make_payment(currency="EUR", account="NOSTRO-EUR-001")
        ])
        collected = []
        func.on_timer(1743498060000, MockOnTimerContext(key=key), collected)

        feed_event = [r for t, r in collected if t == "feed_health"][0]
        self.assertEqual(feed_event["key"], key)

    def test_on_timer_does_not_emit_position_or_alert(self):
        """on_timer() is for feed health only — must not emit positions or alerts."""
        func, _, _ = make_function_with_payments([make_payment()])
        collected = []
        func.on_timer(1743498060000, MockOnTimerContext(), collected)

        positions = [r for t, r in collected if t == "position"]
        alerts    = [r for t, r in collected if t == "alert"]
        self.assertEqual(len(positions), 0, "on_timer must not emit position records")
        self.assertEqual(len(alerts),    0, "on_timer must not emit alert records")


class TestTimerResetOnPaymentArrival(unittest.TestCase):
    """
    Verify that the stale-feed timer is cancelled and reset
    each time a new payment arrives.
    """

    def test_first_payment_registers_timer(self):
        """First payment must register one EventTimeTimer."""
        _, _, mock_ctx = make_function_with_payments([make_payment()])
        self.assertEqual(
            mock_ctx.timer_service.register_event_time_timer.call_count, 1
        )

    def test_second_payment_cancels_previous_timer(self):
        """
        Second payment must call delete_event_time_timer with the
        timestamp registered by the first payment.
        """
        payment1 = make_payment()
        payment2 = make_payment()

        func = LiquidityPositionFunction()
        ctx  = MockRuntimeContext()
        func.open(ctx)

        collected = []
        mock_ctx  = MockContext()

        func.process_element(payment1, mock_ctx, collected)
        first_timer_ts = mock_ctx.timer_service.register_event_time_timer.call_args_list[0][0][0]

        func.process_element(payment2, mock_ctx, collected)

        # The first timer should have been deleted when second payment arrived
        mock_ctx.timer_service.delete_event_time_timer.assert_called_with(first_timer_ts)

    def test_timer_extended_with_each_payment(self):
        """
        Each payment schedules a new timer 60s from its own event_time.
        This means as long as payments keep arriving, on_timer() never fires.
        """
        payment1 = make_payment()
        payment2 = {**make_payment(), "event_time_ms": payment1["event_time_ms"] + 10_000}  # 10s later

        func = LiquidityPositionFunction()
        ctx  = MockRuntimeContext()
        func.open(ctx)

        collected = []
        mock_ctx  = MockContext()

        func.process_element(payment1, mock_ctx, collected)
        func.process_element(payment2, mock_ctx, collected)

        # Second timer should be 60s after payment2's event_time
        all_timer_registrations = mock_ctx.timer_service.register_event_time_timer.call_args_list
        second_timer_ts = all_timer_registrations[1][0][0]
        expected_ts = payment2["event_time_ms"] + STALE_FEED_TIMEOUT_MS
        self.assertEqual(second_timer_ts, expected_ts)


class TestStaleThresholdConstant(unittest.TestCase):

    def test_stale_threshold_is_60_seconds(self):
        """
        Acceptance criterion: FeedStaleAlert fires within 60 seconds.
        STALE_FEED_TIMEOUT_MS must be exactly 60,000ms.
        """
        self.assertEqual(STALE_FEED_TIMEOUT_MS, 60_000,
                         "Stale feed timeout must be 60,000ms (60 seconds) per acceptance criteria")


if __name__ == "__main__":
    unittest.main()
