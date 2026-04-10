"""
Tests: Liquidity Position Engine
==================================
Covers the core balance maths and alert logic inside
LiquidityPositionFunction (KeyedBroadcastProcessFunction).

These tests do NOT need a running Flink cluster.
We instantiate LiquidityPositionFunction directly and drive it
with a mock RuntimeContext and mock Context, then inspect what
was collected into the output list.

Key scenarios:
  - CREDIT payment increases balance
  - DEBIT payment decreases balance
  - Sequence of payments produces correct running total
  - Balance below WARNING threshold → WARNING alert emitted
  - Balance below CRITICAL threshold → CRITICAL alert emitted (not WARNING)
  - Balance above both thresholds → no alert emitted
  - Position record always emitted regardless of threshold state
  - parse_payment() handles valid and malformed JSON
  - parse_payment() converts event_time → epoch-ms correctly
"""

import json
import unittest
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flink_jobs.liquidity_position_engine import (
    LiquidityPositionFunction,
    parse_payment,
)


# ─────────────────────────────────────────────────────────────────
#  Helpers: build mock Flink state objects
# ─────────────────────────────────────────────────────────────────

class MockValueState:
    """Mimics Flink ValueState — stores a single value."""
    def __init__(self, initial=None):
        self._value = initial

    def value(self):
        return self._value

    def update(self, v):
        self._value = v


class MockMapState:
    """Mimics Flink MapState — dict of key→value, supports .contains() and .put()."""
    def __init__(self):
        self._data = {}

    def contains(self, key):
        return key in self._data

    def put(self, key, value):
        self._data[key] = value

    def get(self, key):
        return self._data.get(key)


class MockBroadcastState:
    """Mimics Flink BroadcastState — stores a dict keyed by string."""
    def __init__(self, data=None):
        self._data = data or {}

    def get(self, key):
        return self._data.get(key)

    def put(self, key, value):
        self._data[key] = value


class MockRuntimeContext:
    """Returns MockValueState/MockMapState for each descriptor name."""
    def __init__(self, thresholds=None):
        self._states = {}
        self._map_states = {}
        self._broadcast = MockBroadcastState(thresholds or {})

    def get_state(self, descriptor):
        name = descriptor.name
        if name not in self._states:
            self._states[name] = MockValueState()
        return self._states[name]

    def get_map_state(self, descriptor):
        """Required by engine for seen_payments duplicate-detection state."""
        name = descriptor.name
        if name not in self._map_states:
            self._map_states[name] = MockMapState()
        return self._map_states[name]

    def get_broadcast_state(self, descriptor):
        return self._broadcast


class MockContext:
    """
    Mimics KeyedBroadcastProcessFunction.ReadOnlyContext.
    Captures processing-time timer registrations and provides broadcast state.

    Note: engine calls ctx.timer_service() (as a callable), which returns
    timer_service.return_value — so mocks are set on .return_value.
    """
    def __init__(self, key="GBP:RTGS-GBP-001", broadcast_state=None):
        self._key = key
        self._timers = []
        self._broadcast = broadcast_state or MockBroadcastState()
        self.timer_service = MagicMock()
        # Engine calls: ctx.timer_service().register_processing_time_timer(ts)
        self.timer_service.return_value.register_processing_time_timer.side_effect = self._timers.append
        self.timer_service.return_value.delete_processing_time_timer = MagicMock()

    def get_current_key(self):
        return self._key

    def get_broadcast_state(self, descriptor):
        return self._broadcast


def make_payment(currency="GBP", account="RTGS-GBP-001",
                 amount=1_000_000.0, direction="DEBIT",
                 rail="RTGS", payment_id=None):
    """Build a minimal parsed payment dict (output of parse_payment).
    Uses a unique UUID by default so duplicate detection doesn't fire.
    """
    return {
        "payment_id":         payment_id or str(uuid.uuid4()),
        "currency":           currency,
        "settlement_account": account,
        "amount":             amount,
        "direction":          direction,
        "rail":               rail,
        "counterparty":       "BARCLAYS",
        "event_time":         "2026-04-01T09:00:00Z",
        "event_time_ms":      1743498000000,
    }


def drive_function(payments, thresholds=None):
    """
    Run a sequence of payments through LiquidityPositionFunction.
    Returns list of (type, record) tuples collected from the generator.
    """
    func = LiquidityPositionFunction()
    runtime_ctx = MockRuntimeContext(thresholds=thresholds)
    func.open(runtime_ctx)

    collected = []
    # Pass broadcast state to MockContext so process_element can read thresholds
    mock_ctx = MockContext(broadcast_state=runtime_ctx._broadcast)

    for payment in payments:
        # process_element uses yield — must consume the generator to execute it
        collected.extend(func.process_element(payment, mock_ctx))

    return collected, mock_ctx


# ─────────────────────────────────────────────────────────────────
#  Tests: parse_payment()
# ─────────────────────────────────────────────────────────────────

class TestParsePayment(unittest.TestCase):

    def _valid_raw(self, **overrides):
        """Build a valid RTGS payment JSON string.
        Amount £15M is within the RTGS range (£10M–£500M).
        """
        data = {
            "payment_id":         "abc-123",
            "currency":           "GBP",
            "settlement_account": "RTGS-GBP-001",
            "amount":             15_000_000.0,   # £15M — valid for RTGS
            "direction":          "DEBIT",
            "rail":               "RTGS",
            "counterparty":       "HSBC",
            "event_time":         "2026-04-01T09:15:00Z",
        }
        data.update(overrides)
        return json.dumps(data)

    def test_valid_payment_parsed_correctly(self):
        """A valid payment returns ("ok", record) with the correct fields."""
        tag, record = parse_payment(self._valid_raw())
        self.assertEqual(tag, "ok")
        self.assertEqual(record["currency"], "GBP")
        self.assertEqual(record["amount"], 15_000_000.0)

    def test_event_time_ms_added(self):
        """parse_payment must add event_time_ms (epoch-ms) to the dict."""
        tag, record = parse_payment(self._valid_raw())
        self.assertEqual(tag, "ok")
        self.assertIn("event_time_ms", record)
        self.assertIsInstance(record["event_time_ms"], int)
        # 2026-04-01T09:15:00Z → known epoch value
        expected_ms = int(datetime(2026, 4, 1, 9, 15, 0, tzinfo=timezone.utc).timestamp() * 1000)
        self.assertEqual(record["event_time_ms"], expected_ms)

    def test_malformed_json_routed_to_dlq(self):
        """Malformed JSON returns ("dlq", error_record)."""
        tag, error = parse_payment("not valid json {{")
        self.assertEqual(tag, "dlq")
        self.assertEqual(error["stage"], "json_parse")

    def test_missing_event_time_routed_to_dlq(self):
        """Payment missing event_time returns ("dlq", error_record)."""
        data = json.loads(self._valid_raw())
        del data["event_time"]
        tag, error = parse_payment(json.dumps(data))
        self.assertEqual(tag, "dlq")
        self.assertEqual(error["stage"], "schema_validation")

    def test_empty_string_routed_to_dlq(self):
        """Empty string returns ("dlq", error_record)."""
        tag, error = parse_payment("")
        self.assertEqual(tag, "dlq")
        self.assertEqual(error["stage"], "json_parse")


# ─────────────────────────────────────────────────────────────────
#  Tests: Balance arithmetic
# ─────────────────────────────────────────────────────────────────

class TestBalanceArithmetic(unittest.TestCase):

    def test_credit_increases_balance(self):
        """A CREDIT payment should increase available balance."""
        payments = [make_payment(amount=5_000_000.0, direction="CREDIT")]
        collected, _ = drive_function(payments)

        positions = [r for t, r in collected if t == "position"]
        self.assertEqual(len(positions), 1)
        self.assertAlmostEqual(positions[0]["available_balance"], 5_000_000.0)

    def test_debit_decreases_balance(self):
        """A DEBIT payment should decrease available balance."""
        payments = [
            make_payment(amount=10_000_000.0, direction="CREDIT"),
            make_payment(amount=3_000_000.0,  direction="DEBIT"),
        ]
        collected, _ = drive_function(payments)

        positions = [r for t, r in collected if t == "position"]
        final_balance = positions[-1]["available_balance"]
        self.assertAlmostEqual(final_balance, 7_000_000.0)

    def test_sequence_of_mixed_payments(self):
        """Running balance should accumulate correctly across a sequence."""
        payments = [
            make_payment(amount=100_000_000.0, direction="CREDIT"),  # +100M → 100M
            make_payment(amount=20_000_000.0,  direction="DEBIT"),   # -20M  → 80M
            make_payment(amount=5_000_000.0,   direction="DEBIT"),   # -5M   → 75M
            make_payment(amount=10_000_000.0,  direction="CREDIT"),  # +10M  → 85M
        ]
        collected, _ = drive_function(payments)

        positions = [r for t, r in collected if t == "position"]
        self.assertEqual(len(positions), 4)
        self.assertAlmostEqual(positions[-1]["available_balance"], 85_000_000.0)

    def test_balance_can_go_negative(self):
        """Balance going negative is valid — that's what triggers critical alerts."""
        payments = [make_payment(amount=5_000_000.0, direction="DEBIT")]
        collected, _ = drive_function(payments)

        positions = [r for t, r in collected if t == "position"]
        self.assertLess(positions[0]["available_balance"], 0)

    def test_total_inbound_accumulated(self):
        payments = [
            make_payment(amount=10_000_000.0, direction="CREDIT"),
            make_payment(amount=5_000_000.0,  direction="CREDIT"),
        ]
        collected, _ = drive_function(payments)
        positions = [r for t, r in collected if t == "position"]
        self.assertAlmostEqual(positions[-1]["total_inbound"], 15_000_000.0)

    def test_total_outbound_accumulated(self):
        payments = [
            make_payment(amount=7_000_000.0, direction="DEBIT"),
            make_payment(amount=3_000_000.0, direction="DEBIT"),
        ]
        collected, _ = drive_function(payments)
        positions = [r for t, r in collected if t == "position"]
        self.assertAlmostEqual(positions[-1]["total_outbound"], 10_000_000.0)

    def test_payment_count_increments(self):
        payments = [make_payment() for _ in range(5)]
        collected, _ = drive_function(payments)
        positions = [r for t, r in collected if t == "position"]
        self.assertEqual(positions[-1]["payment_count"], 5)

    def test_last_payment_id_updated(self):
        payments = [
            make_payment(payment_id="first-id"),
            make_payment(payment_id="second-id"),
        ]
        collected, _ = drive_function(payments)
        positions = [r for t, r in collected if t == "position"]
        self.assertEqual(positions[-1]["last_payment_id"], "second-id")


# ─────────────────────────────────────────────────────────────────
#  Tests: Threshold alert logic
# ─────────────────────────────────────────────────────────────────

class TestThresholdAlerts(unittest.TestCase):

    THRESHOLD_KEY = "GBP:RTGS-GBP-001"
    WARNING_LEVEL  = 2_000_000_000.0   # £2B
    CRITICAL_LEVEL =   500_000_000.0   # £500M

    def _thresholds(self):
        return {
            self.THRESHOLD_KEY: {
                "warning":  self.WARNING_LEVEL,
                "critical": self.CRITICAL_LEVEL,
            }
        }

    def test_no_alert_when_balance_above_warning(self):
        """Balance comfortably above warning threshold → no alerts."""
        payments = [
            make_payment(amount=5_000_000_000.0, direction="CREDIT"),
            make_payment(amount=100_000_000.0,   direction="DEBIT"),   # balance = £4.9B
        ]
        collected, _ = drive_function(payments, thresholds=self._thresholds())
        alerts = [r for t, r in collected if t == "alert"]
        self.assertEqual(len(alerts), 0, msg="Expected no alerts above warning threshold")

    def test_warning_alert_when_balance_below_warning_above_critical(self):
        """Balance between critical and warning → exactly one WARNING alert per payment."""
        # Balance will be £1B — below warning (£2B) but above critical (£500M)
        payments = [make_payment(amount=1_000_000_000.0, direction="CREDIT")]
        collected, _ = drive_function(payments, thresholds=self._thresholds())

        alerts = [r for t, r in collected if t == "alert"]
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["alert_type"], "WARNING")

    def test_critical_alert_when_balance_below_critical(self):
        """Balance below critical threshold → CRITICAL alert (not WARNING)."""
        # Balance = £200M — below critical (£500M)
        payments = [make_payment(amount=200_000_000.0, direction="CREDIT")]
        collected, _ = drive_function(payments, thresholds=self._thresholds())

        alerts = [r for t, r in collected if t == "alert"]
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["alert_type"], "CRITICAL")

    def test_critical_not_warning_when_below_critical(self):
        """When balance is below critical, CRITICAL is emitted, not WARNING."""
        payments = [make_payment(amount=100_000_000.0, direction="CREDIT")]
        collected, _ = drive_function(payments, thresholds=self._thresholds())

        alerts = [r for t, r in collected if t == "alert"]
        alert_types = {a["alert_type"] for a in alerts}
        self.assertNotIn("WARNING", alert_types,
                         "Should emit CRITICAL only, not WARNING, when below critical threshold")

    def test_alert_contains_correct_breach_amount(self):
        """breach_amount should be threshold - balance."""
        balance = 1_000_000_000.0  # £1B — below warning (£2B)
        payments = [make_payment(amount=balance, direction="CREDIT")]
        collected, _ = drive_function(payments, thresholds=self._thresholds())

        alerts = [r for t, r in collected if t == "alert"]
        self.assertEqual(len(alerts), 1)
        expected_breach = self.WARNING_LEVEL - balance
        self.assertAlmostEqual(alerts[0]["breach_amount"], expected_breach)

    def test_no_alert_when_no_threshold_configured(self):
        """If no threshold is set for a key, no alert should fire."""
        payments = [make_payment(amount=100_000.0, direction="CREDIT")]
        collected, _ = drive_function(payments, thresholds={})   # empty thresholds
        alerts = [r for t, r in collected if t == "alert"]
        self.assertEqual(len(alerts), 0)

    def test_position_always_emitted_regardless_of_threshold(self):
        """A position record must always be emitted, even without threshold config."""
        payments = [make_payment()]
        collected, _ = drive_function(payments, thresholds={})
        positions = [r for t, r in collected if t == "position"]
        self.assertEqual(len(positions), 1)

    def test_alert_currency_and_account_match_payment(self):
        """Alert fields must reflect the payment that triggered it."""
        payments = [make_payment(
            currency="USD",
            account="NOSTRO-USD-001",
            amount=100_000_000.0,
            direction="CREDIT",
        )]
        thresholds = {
            "USD:NOSTRO-USD-001": {"warning": 500_000_000.0, "critical": 100_000_000.0}
        }
        collected, _ = drive_function(payments, thresholds=thresholds)
        alerts = [r for t, r in collected if t == "alert"]
        self.assertTrue(len(alerts) > 0)
        self.assertEqual(alerts[0]["currency"], "USD")
        self.assertEqual(alerts[0]["settlement_account"], "NOSTRO-USD-001")


# ─────────────────────────────────────────────────────────────────
#  Tests: Timer registration
# ─────────────────────────────────────────────────────────────────

class TestTimerRegistration(unittest.TestCase):

    def test_processing_time_timer_registered_on_each_payment(self):
        """A stale-feed timer must be registered for every payment processed."""
        payments = [make_payment()]
        _, mock_ctx = drive_function(payments)
        mock_ctx.timer_service.return_value.register_processing_time_timer.assert_called_once()

    @patch('flink_jobs.liquidity_position_engine.time')
    def test_timer_set_60s_from_wall_clock(self, mock_time):
        """Timer should fire 60,000ms from current wall-clock time (not event_time)."""
        fake_now_s = 1743498000.0   # fake wall-clock in seconds
        mock_time.time.return_value = fake_now_s

        payment = make_payment()
        _, mock_ctx = drive_function([payment])

        registered_ts = mock_ctx.timer_service.return_value.register_processing_time_timer.call_args[0][0]
        expected_ts = int(fake_now_s * 1000) + 60_000
        self.assertEqual(registered_ts, expected_ts)

    def test_previous_timer_deleted_before_new_registration(self):
        """Each payment must cancel the previous timer before registering a new one."""
        payments = [make_payment(), make_payment()]
        _, mock_ctx = drive_function(payments)
        # delete should be called once (second payment cancels first timer)
        mock_ctx.timer_service.return_value.delete_processing_time_timer.assert_called_once()


if __name__ == "__main__":
    unittest.main()
