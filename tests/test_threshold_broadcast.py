"""
Tests: Threshold Broadcast (ThresholdManager + BroadcastState)
===============================================================
Covers acceptance criterion #4:
  "Update threshold via Kafka → Flink applies within 30 seconds,
   no job restart required"

Tests verify:
  - ThresholdBroadcastFunction.process_broadcast_element() writes to BroadcastState
  - Updated threshold is immediately visible to LiquidityPositionFunction
  - Compacted topic replay: replaying older then newer message keeps only latest
  - Invalid threshold updates (missing fields) are rejected gracefully
  - WARNING > CRITICAL constraint enforced by threshold_publisher.py
  - Threshold update changes which alert type fires (WARNING→CRITICAL)
  - Threshold key format matches between ThresholdManager and LiquidityPositionEngine
  - A new (currency, account) pair can be added at runtime without restart

No Kafka or Flink cluster needed — BroadcastState is mocked.
"""

import unittest
from unittest.mock import MagicMock

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flink_jobs.threshold_manager import ThresholdBroadcastFunction
from flink_jobs.liquidity_position_engine import LiquidityPositionFunction

from tests.test_position_engine import (
    MockRuntimeContext,
    MockBroadcastState,
    MockContext,
    make_payment,
)


# ─────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────

def make_threshold_update(currency="GBP", account="RTGS-GBP-001",
                           warning=2_000_000_000.0, critical=500_000_000.0,
                           updated_by="test"):
    return {
        "currency":           currency,
        "settlement_account": account,
        "warning_threshold":  warning,
        "critical_threshold": critical,
        "updated_by":         updated_by,
    }


class MockCollector:
    """Mimics Flink Collector — accumulates records from out.collect()."""
    def __init__(self):
        self.items = []

    def collect(self, item):
        self.items.append(item)


class MockBroadcastContext:
    """Mimics BroadcastProcessFunction.Context — wraps a MockBroadcastState."""
    def __init__(self, initial_state=None):
        self._broadcast_state = MockBroadcastState(initial_state or {})

    def get_broadcast_state(self, descriptor):
        return self._broadcast_state

    @property
    def broadcast_state(self):
        return self._broadcast_state


# ─────────────────────────────────────────────────────────────────
#  Tests: ThresholdBroadcastFunction
# ─────────────────────────────────────────────────────────────────

class TestThresholdBroadcastFunction(unittest.TestCase):

    def _run_update(self, update_dict, initial_state=None):
        """Run one threshold update through ThresholdBroadcastFunction."""
        func = ThresholdBroadcastFunction()
        ctx  = MockBroadcastContext(initial_state)
        out  = MockCollector()
        func.process_broadcast_element(update_dict, ctx, out)
        return ctx.broadcast_state, out.items

    def test_threshold_written_to_broadcast_state(self):
        """A valid update must be written into BroadcastState."""
        update = make_threshold_update()
        state, _ = self._run_update(update)

        key = "GBP:RTGS-GBP-001"
        stored = state.get(key)
        self.assertIsNotNone(stored, msg="Threshold not found in BroadcastState")
        self.assertAlmostEqual(stored["warning"],  2_000_000_000.0)
        self.assertAlmostEqual(stored["critical"],   500_000_000.0)

    def test_threshold_key_format_is_currency_colon_account(self):
        """Key in BroadcastState must be 'CURRENCY:account' to match LiquidityPositionFunction."""
        update = make_threshold_update(currency="USD", account="NOSTRO-USD-001")
        state, _ = self._run_update(update)

        self.assertIsNotNone(state.get("USD:NOSTRO-USD-001"))
        self.assertIsNone(state.get("USD-NOSTRO-USD-001"),
                          "Key separator must be ':', not '-'")

    def test_update_overwrites_previous_threshold(self):
        """
        Publishing a new threshold for the same key must overwrite the old one.
        This simulates the hot-reload: Kafka compaction → latest value wins.
        """
        initial = {"GBP:RTGS-GBP-001": {"warning": 2_000_000_000.0, "critical": 500_000_000.0}}
        new_update = make_threshold_update(warning=1_500_000_000.0, critical=300_000_000.0)

        state, _ = self._run_update(new_update, initial_state=initial)
        stored = state.get("GBP:RTGS-GBP-001")

        self.assertAlmostEqual(stored["warning"],  1_500_000_000.0)
        self.assertAlmostEqual(stored["critical"],   300_000_000.0)

    def test_replay_older_then_newer_keeps_latest(self):
        """
        Simulates compacted topic replay: old message processed first,
        then newer message. Final state must reflect the newest values.
        """
        func = ThresholdBroadcastFunction()
        ctx  = MockBroadcastContext()
        out  = MockCollector()

        old_update = make_threshold_update(warning=2_000_000_000.0, critical=500_000_000.0)
        new_update = make_threshold_update(warning=1_000_000_000.0, critical=200_000_000.0)

        func.process_broadcast_element(old_update, ctx, out)
        func.process_broadcast_element(new_update, ctx, out)

        stored = ctx.broadcast_state.get("GBP:RTGS-GBP-001")
        self.assertAlmostEqual(stored["warning"],  1_000_000_000.0,
                               msg="Newer threshold should overwrite older one")
        self.assertAlmostEqual(stored["critical"],   200_000_000.0)

    def test_update_emitted_downstream_for_audit(self):
        """process_broadcast_element must collect one confirmation record."""
        update = make_threshold_update()
        _, collected = self._run_update(update)
        self.assertEqual(len(collected), 1,
                         msg="One confirmation record must be emitted per update")
        self.assertIn("key",      collected[0])
        self.assertIn("warning",  collected[0])
        self.assertIn("critical", collected[0])

    def test_missing_currency_field_does_not_crash(self):
        """An update missing 'currency' must be silently discarded."""
        bad_update = {"settlement_account": "RTGS-GBP-001", "warning_threshold": 1e9, "critical_threshold": 5e8}
        state, collected = self._run_update(bad_update)
        # Nothing should be written to state
        self.assertIsNone(state.get(":RTGS-GBP-001"))
        self.assertEqual(len(collected), 0)

    def test_missing_account_field_does_not_crash(self):
        """An update missing 'settlement_account' must be silently discarded."""
        bad_update = {"currency": "GBP", "warning_threshold": 1e9, "critical_threshold": 5e8}
        state, collected = self._run_update(bad_update)
        self.assertIsNone(state.get("GBP:"))
        self.assertEqual(len(collected), 0)

    def test_new_account_added_at_runtime(self):
        """
        A threshold for a brand-new (currency, account) pair must be added
        to BroadcastState without requiring a Flink job restart.
        """
        update = make_threshold_update(currency="JPY", account="NOSTRO-JPY-001",
                                       warning=10_000_000_000.0, critical=2_000_000_000.0)
        state, _ = self._run_update(update)
        self.assertIsNotNone(state.get("JPY:NOSTRO-JPY-001"),
                             "New currency/account must be accepted at runtime")


# ─────────────────────────────────────────────────────────────────
#  Tests: Hot-reload — threshold change alters alert behaviour
# ─────────────────────────────────────────────────────────────────

class TestHotReloadChangesAlertBehaviour(unittest.TestCase):
    """
    Simulate the hot-reload acceptance criterion end-to-end:
    1. Process payment with OLD thresholds → get WARNING alert
    2. Update thresholds via BroadcastState (simulates ThresholdManager)
    3. Process same payment amount again → alert type changes
    """

    def _run_with_thresholds(self, thresholds, amount):
        func = LiquidityPositionFunction()
        runtime_ctx = MockRuntimeContext(thresholds=thresholds)
        func.open(runtime_ctx)

        mock_ctx = MockContext(broadcast_state=runtime_ctx._broadcast)
        collected = list(func.process_element(
            make_payment(amount=amount, direction="CREDIT"), mock_ctx
        ))
        return [r for t, r in collected if t == "alert"]

    def test_lowering_warning_threshold_suppresses_warning(self):
        """
        Balance = £1B.
        Old thresholds: warning=£2B → WARNING fires.
        New thresholds: warning=£500M → no warning (balance is above).
        """
        balance = 1_000_000_000.0  # £1B

        old_thresholds = {"GBP:RTGS-GBP-001": {"warning": 2_000_000_000.0, "critical": 500_000_000.0}}
        new_thresholds = {"GBP:RTGS-GBP-001": {"warning":   500_000_000.0, "critical": 100_000_000.0}}

        alerts_before = self._run_with_thresholds(old_thresholds, balance)
        alerts_after  = self._run_with_thresholds(new_thresholds, balance)

        self.assertEqual(alerts_before[0]["alert_type"], "WARNING",
                         "Should be WARNING with old high threshold")
        self.assertEqual(len(alerts_after), 0,
                         "Should be no alert after threshold lowered below balance")

    def test_raising_critical_threshold_escalates_alert(self):
        """
        Balance = £300M.
        Old thresholds: critical=£100M → WARNING fires.
        New thresholds: critical=£500M → CRITICAL fires.
        """
        balance = 300_000_000.0  # £300M

        old_thresholds = {"GBP:RTGS-GBP-001": {"warning": 2_000_000_000.0, "critical": 100_000_000.0}}
        new_thresholds = {"GBP:RTGS-GBP-001": {"warning": 2_000_000_000.0, "critical": 500_000_000.0}}

        alerts_before = self._run_with_thresholds(old_thresholds, balance)
        alerts_after  = self._run_with_thresholds(new_thresholds, balance)

        self.assertEqual(alerts_before[0]["alert_type"], "WARNING")
        self.assertEqual(alerts_after[0]["alert_type"],  "CRITICAL",
                         "Raising critical threshold above balance should escalate to CRITICAL")


# ─────────────────────────────────────────────────────────────────
#  Tests: threshold_publisher.py validation
# ─────────────────────────────────────────────────────────────────

class TestThresholdPublisherValidation(unittest.TestCase):
    """
    Tests for the CLI validation in threshold_publisher.py.
    WARNING must always be > CRITICAL (enforced before Kafka publish).
    """

    @unittest.mock.patch("producers.threshold_publisher.Producer")
    @unittest.mock.patch("producers.threshold_publisher.get_pg_conn")
    def test_warning_greater_than_critical_accepted(self, mock_pg, mock_kafka):
        """warning > critical → should not raise."""
        import unittest.mock
        mock_pg.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_pg.return_value.__exit__  = MagicMock(return_value=False)

        from producers.threshold_publisher import publish_threshold
        # Should not raise SystemExit
        try:
            publish_threshold(
                currency="GBP", account="RTGS-GBP-001",
                warning=2_000_000_000.0, critical=500_000_000.0,
                updated_by="test",
            )
        except SystemExit:
            self.fail("publish_threshold raised SystemExit for valid warning > critical")

    def test_warning_equal_to_critical_rejected(self):
        """warning == critical → should sys.exit(1)."""
        from producers.threshold_publisher import publish_threshold
        with self.assertRaises(SystemExit):
            publish_threshold(
                currency="GBP", account="RTGS-GBP-001",
                warning=500_000_000.0, critical=500_000_000.0,
                updated_by="test",
            )

    def test_warning_less_than_critical_rejected(self):
        """warning < critical → should sys.exit(1)."""
        from producers.threshold_publisher import publish_threshold
        with self.assertRaises(SystemExit):
            publish_threshold(
                currency="GBP", account="RTGS-GBP-001",
                warning=100_000_000.0, critical=500_000_000.0,
                updated_by="test",
            )


if __name__ == "__main__":
    unittest.main()
