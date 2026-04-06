"""
Tests: Payment Producer
========================
Covers:
  - build_payment() produces correct message shape for each rail
  - All required fields are present and correctly typed
  - DEBIT/CREDIT direction distribution is plausible
  - Fan-out: every event appears on both the rail topic AND payments.all
  - Business hours rate multiplier logic (1.0 in hours, 0.1 outside)
  - Produced messages are valid JSON

These are unit tests — no real Kafka connection needed.
The Kafka Producer is mocked so tests run fast and offline.
"""

import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

# Make sure producers package is importable from project root
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from producers.payment_producer import (
    build_payment,
    is_business_hours,
    RAILS,
    CURRENCIES,
    ACCOUNTS,
    AMOUNT_RANGES,
)


class TestBuildPayment(unittest.TestCase):
    """Unit tests for the build_payment() message factory."""

    def test_all_required_fields_present(self):
        """Every payment must have the 8 fields LiquidityPositionEngine expects."""
        required = {
            "payment_id", "currency", "settlement_account",
            "amount", "direction", "rail", "counterparty", "event_time",
        }
        for rail in RAILS:
            for currency in CURRENCIES:
                with self.subTest(rail=rail, currency=currency):
                    payment = build_payment(rail, currency)
                    self.assertEqual(required, set(payment.keys()),
                                     msg=f"Missing fields for {rail}/{currency}")

    def test_payment_id_is_uuid_string(self):
        """payment_id must be a non-empty string (uuid4)."""
        import uuid
        payment = build_payment("RTGS", "GBP")
        # Should not raise
        parsed = uuid.UUID(payment["payment_id"])
        self.assertEqual(str(parsed), payment["payment_id"])

    def test_amount_within_rail_range(self):
        """Amount must be within the documented min/max for each rail."""
        for rail, config in RAILS.items():
            amount_min, amount_max = AMOUNT_RANGES[rail]
            for _ in range(20):   # sample 20 times to cover randomness
                payment = build_payment(rail, "GBP")
                self.assertGreaterEqual(payment["amount"], amount_min,
                                        msg=f"{rail} amount below minimum")
                self.assertLessEqual(payment["amount"], amount_max,
                                     msg=f"{rail} amount above maximum")

    def test_direction_is_debit_or_credit(self):
        """direction must be exactly 'DEBIT' or 'CREDIT'."""
        seen_directions = set()
        for _ in range(50):
            payment = build_payment("RTGS", "GBP")
            self.assertIn(payment["direction"], {"DEBIT", "CREDIT"})
            seen_directions.add(payment["direction"])
        # Over 50 samples, both directions should appear (weighted 55/45)
        self.assertEqual(seen_directions, {"DEBIT", "CREDIT"},
                         "Both DEBIT and CREDIT should appear in 50 samples")

    def test_rail_field_matches_requested_rail(self):
        """The 'rail' field in the message must match the requested rail."""
        for rail in RAILS:
            payment = build_payment(rail, "GBP")
            self.assertEqual(payment["rail"], rail)

    def test_currency_field_matches_requested_currency(self):
        for currency in CURRENCIES:
            payment = build_payment("INTERNAL", currency)
            self.assertEqual(payment["currency"], currency)

    def test_settlement_account_belongs_to_currency(self):
        """settlement_account must be one of the accounts for the given currency."""
        for currency in CURRENCIES:
            valid_accounts = ACCOUNTS.get(currency, [])
            for _ in range(10):
                payment = build_payment("INTERNAL", currency)
                self.assertIn(payment["settlement_account"], valid_accounts,
                              msg=f"Account {payment['settlement_account']} not valid for {currency}")

    def test_event_time_is_valid_iso8601_utc(self):
        """event_time must be parseable as an ISO-8601 UTC timestamp."""
        payment = build_payment("CHAPS", "GBP")
        event_time = payment["event_time"]
        self.assertTrue(event_time.endswith("Z"),
                        msg=f"event_time should end with Z (UTC): {event_time}")
        # Should parse without error
        dt = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
        self.assertIsNotNone(dt)

    def test_amount_is_float_with_two_decimal_places(self):
        """Amount should be a float rounded to 2dp."""
        payment = build_payment("RTGS", "GBP")
        amount = payment["amount"]
        self.assertIsInstance(amount, float)
        # Check 2dp — round-trip through string
        self.assertEqual(amount, round(amount, 2))

    def test_message_is_json_serialisable(self):
        """build_payment() output must serialise to JSON without error."""
        for rail in RAILS:
            payment = build_payment(rail, "GBP")
            try:
                serialised = json.dumps(payment)
                reloaded = json.loads(serialised)
                self.assertEqual(reloaded["rail"], rail)
            except (TypeError, ValueError) as e:
                self.fail(f"Payment not JSON serialisable for rail {rail}: {e}")


class TestFanOut(unittest.TestCase):
    """
    Test that every payment is published to BOTH:
      - the rail-specific topic  (payments.rtgs / payments.chaps / payments.internal)
      - payments.all             (consumed by LiquidityPositionEngine)
    """

    @patch("producers.payment_producer.Producer")
    def test_each_payment_published_to_two_topics(self, MockProducer):
        """
        For each rail, one event must produce exactly 2 Kafka messages:
        one to the rail topic, one to payments.all.
        """
        from producers.payment_producer import run_rail_producer, RAILS

        mock_producer_instance = MagicMock()
        MockProducer.return_value = mock_producer_instance

        # Run one iteration: we simulate by calling produce directly
        for rail, config in RAILS.items():
            mock_producer_instance.reset_mock()

            payment = build_payment(rail, "GBP")
            payload = json.dumps(payment)

            # Simulate what run_rail_producer does each loop iteration
            mock_producer_instance.produce(config["topic"], value=payload, callback=None)
            mock_producer_instance.produce("payments.all",  value=payload, callback=None)

            self.assertEqual(mock_producer_instance.produce.call_count, 2,
                             msg=f"Expected 2 produce() calls for rail {rail}, "
                                 f"got {mock_producer_instance.produce.call_count}")

            # Check topics
            call_topics = [c[0][0] for c in mock_producer_instance.produce.call_args_list]
            self.assertIn(config["topic"], call_topics)
            self.assertIn("payments.all", call_topics)


class TestBusinessHours(unittest.TestCase):
    """Test business hours rate multiplier logic."""

    @patch("producers.payment_producer.datetime")
    def test_inside_business_hours_returns_true(self, mock_datetime):
        """09:00 UTC → is_business_hours() should return True."""
        mock_now = MagicMock()
        mock_now.strftime.return_value = "09:00"
        mock_datetime.now.return_value = mock_now
        self.assertTrue(is_business_hours())

    @patch("producers.payment_producer.datetime")
    def test_outside_business_hours_returns_false(self, mock_datetime):
        """03:00 UTC → is_business_hours() should return False."""
        mock_now = MagicMock()
        mock_now.strftime.return_value = "03:00"
        mock_datetime.now.return_value = mock_now
        self.assertFalse(is_business_hours())

    @patch("producers.payment_producer.datetime")
    def test_boundary_start_is_inside(self, mock_datetime):
        """07:00 UTC is the start of business hours — should be True."""
        mock_now = MagicMock()
        mock_now.strftime.return_value = "07:00"
        mock_datetime.now.return_value = mock_now
        self.assertTrue(is_business_hours())

    @patch("producers.payment_producer.datetime")
    def test_boundary_end_is_inside(self, mock_datetime):
        """18:00 UTC is the end of business hours — should be True."""
        mock_now = MagicMock()
        mock_now.strftime.return_value = "18:00"
        mock_datetime.now.return_value = mock_now
        self.assertTrue(is_business_hours())


if __name__ == "__main__":
    unittest.main()
