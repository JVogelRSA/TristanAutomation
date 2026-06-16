"""
Unit tests for the pure logic in the weekly-automation bots.

No test here touches the network, Snowflake, IMAP, SMTP, or any LLM API —
HTTP calls are mocked and everything else operates on fixture data. Run with:

    venv/bin/python -m unittest discover tests -v
"""
import os
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from adapters import brex, mercury
from utils import history, email_sender
import spend_bot
import sales_bot
import inventory_bot


def _http_response(payload, status=200):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = payload
    return resp


class TestBrexAdapter(unittest.TestCase):
    def test_no_api_key_returns_empty(self):
        self.assertTrue(brex.fetch_brex_transactions(None).empty)

    @patch("adapters.brex.requests.get")
    def test_normalization_skips_bad_rows(self, mock_get):
        mock_get.return_value = _http_response({
            "items": [
                # Valid spend: 12345 cents → $123.45
                {"posted_at_date": "2026-06-01", "description": "AWS",
                 "amount": {"amount": 12345}, "merchant": {"mcc_description": "Cloud Services"}},
                # Payment (negative) — skipped
                {"posted_at_date": "2026-06-02", "description": "Card payment",
                 "amount": {"amount": -500000}},
                # Missing amount (None) — skipped, not zeroed
                {"posted_at_date": "2026-06-03", "description": "Pending txn",
                 "amount": {"amount": None}},
                # Missing amount object entirely — skipped
                {"posted_at_date": "2026-06-03", "description": "Weird txn"},
                # Missing date — skipped (would crash pd.to_datetime downstream)
                {"posted_at_date": None, "description": "Dateless",
                 "amount": {"amount": 1000}},
            ],
            "next_cursor": None,
        })
        df = brex.fetch_brex_transactions("fake-key")
        self.assertEqual(len(df), 1)
        row = df.iloc[0]
        self.assertEqual(row["Description"], "AWS")
        self.assertAlmostEqual(row["Amount"], 123.45)
        self.assertEqual(row["Category"], "Cloud Services")
        self.assertEqual(row["Source"], "Brex")
        # timeout must be set so a hung connection can't stall cron forever
        self.assertIn("timeout", mock_get.call_args.kwargs)

    @patch("adapters.brex.requests.get")
    def test_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _http_response({}, status=401)
        self.assertTrue(brex.fetch_brex_transactions("fake-key").empty)


class TestMercuryAdapter(unittest.TestCase):
    @patch("adapters.mercury.requests.get")
    def test_none_amount_is_skipped_not_crash(self, mock_get):
        """Regression: float(None) used to raise and kill the spend section."""
        mock_get.return_value = _http_response({
            "transactions": [
                {"postedAt": "2026-06-01T10:00:00Z", "bankDescription": "Vendor A",
                 "amount": -250.5, "kind": "externalTransfer"},
                {"postedAt": "2026-06-02T10:00:00Z", "bankDescription": "Pending",
                 "amount": None, "kind": "other"},
                # Deposit (positive) — skipped
                {"postedAt": "2026-06-03T10:00:00Z", "bankDescription": "Customer payment",
                 "amount": 9000.0, "kind": "incoming"},
            ],
        })
        df = mercury.fetch_mercury_transactions("fake-key")
        self.assertEqual(len(df), 1)
        row = df.iloc[0]
        self.assertEqual(row["Description"], "Vendor A")
        self.assertAlmostEqual(row["Amount"], 250.5)
        self.assertEqual(row["Date"], "2026-06-01")
        self.assertIn("timeout", mock_get.call_args.kwargs)


class TestSpendLogic(unittest.TestCase):
    def test_normalize_vendor(self):
        self.assertEqual(spend_bot._normalize_vendor("AWS *A1B2C3"), "AWS")
        self.assertEqual(spend_bot._normalize_vendor("Google Ads 884412"), "GOOGLE ADS")
        self.assertEqual(spend_bot._normalize_vendor("  stripe   payments  inc extra"), "STRIPE PAYMENTS INC")
        self.assertEqual(spend_bot._normalize_vendor(None), "")

    def test_compute_runway(self):
        hist = [{"total_spend": 10000.0} for _ in range(4)]
        runway = spend_bot.compute_runway(100000.0, hist, 10000.0)
        self.assertAlmostEqual(runway["weekly_burn"], 10000.0)
        self.assertAlmostEqual(runway["weeks_remaining"], 10.0)
        self.assertIn("runout_date", runway)

    def test_compute_runway_unconfigured(self):
        self.assertEqual(spend_bot.compute_runway(0, [], 5000.0), {})
        self.assertEqual(spend_bot.compute_runway(None, [], 5000.0), {})

    def test_detect_recurring_subscriptions(self):
        base = datetime(2026, 5, 1)
        rows = []
        for i in range(3):  # monthly, stable price → subscription
            rows.append({"Date": base + timedelta(days=30 * i), "Description": "NETFLIX.COM *X1",
                         "Amount": 15.99, "Category": "x", "Source": "Brex"})
        rows.append({"Date": base, "Description": "ONE OFF VENDOR",
                     "Amount": 500.0, "Category": "x", "Source": "Brex"})
        subs = spend_bot.detect_recurring_subscriptions(pd.DataFrame(rows))
        self.assertEqual(len(subs), 1)
        self.assertEqual(subs.iloc[0]["Cadence"], "monthly")
        self.assertAlmostEqual(subs.iloc[0]["Median Amount"], 15.99)


class TestInventoryLogic(unittest.TestCase):
    def test_reorder_flags(self):
        today = datetime(2026, 6, 1)
        # SKU '1' has a 12-week lead time; 100 units at 10/wk = 10 wks runway → OVERDUE
        _, runway, flag = inventory_bot._compute_stockout_and_reorder(100, 10, "1", today)
        self.assertAlmostEqual(runway, 10.0)
        self.assertEqual(flag, "OVERDUE")
        # 130 units at 10/wk = 13 wks → <1 wk buffer over 12-wk lead → THIS WEEK
        _, _, flag = inventory_bot._compute_stockout_and_reorder(125, 10, "1", today)
        self.assertEqual(flag, "THIS WEEK")
        # Plenty of runway → OK; zero burn → OK + infinite runway
        _, _, flag = inventory_bot._compute_stockout_and_reorder(10000, 10, "1", today)
        self.assertEqual(flag, "OK")
        _, runway, flag = inventory_bot._compute_stockout_and_reorder(100, 0, "1", today)
        self.assertEqual(flag, "OK")
        self.assertEqual(runway, float("inf"))

    def test_clean_csv_description(self):
        self.assertEqual(
            inventory_bot._clean_csv_description("Amber Sunday Bundle (2025) SKU: 1 + 34"),
            "Amber Sunday Bundle (2025)",
        )
        self.assertEqual(inventory_bot._clean_csv_description(""), "")

    def test_generate_llm_report_short_data_returns_three_tuple(self):
        """Regression: used to return a bare string, crashing 3-value unpacking."""
        result = inventory_bot.generate_llm_report([(datetime.now(), pd.DataFrame())])
        self.assertEqual(len(result), 3)
        html, summary_df, snapshot = result
        self.assertIn("Error", html)
        self.assertIsInstance(summary_df, pd.DataFrame)
        self.assertEqual(snapshot, {})


class TestHistory(unittest.TestCase):
    def test_roundtrip_and_same_week_exclusion(self):
        with TemporaryDirectory() as tmp:
            with patch.object(history, "DATA_DIR", Path(tmp)):
                mondays = [history.get_week_monday() - timedelta(weeks=i) for i in (2, 1, 0)]
                for i, monday in enumerate(mondays):
                    history.save_weekly_snapshot("spend", monday, {"total_spend": 1000.0 * (i + 1)})

                all_weeks = history.load_history("spend")
                self.assertEqual(len(all_weeks), 3)
                self.assertEqual(all_weeks[-1]["total_spend"], 3000.0)

                # Excluding the current week drops only its own snapshot —
                # this is what stops a re-run comparing the week to itself.
                current = history.get_week_monday()
                excluded = history.load_history("spend", exclude_week=current)
                self.assertEqual(len(excluded), 2)
                self.assertEqual(excluded[-1]["total_spend"], 2000.0)

                # String form works too
                excluded2 = history.load_history("spend", exclude_week=current.isoformat())
                self.assertEqual(len(excluded2), 2)

    def test_corrupt_snapshot_skipped(self):
        with TemporaryDirectory() as tmp:
            with patch.object(history, "DATA_DIR", Path(tmp)):
                monday = history.get_week_monday()
                history.save_weekly_snapshot("sales", monday, {"gross_sales_dc1": 5.0})
                bad = Path(tmp) / "sales" / "week_2020-01-06.json"
                bad.write_text("{not json")
                self.assertEqual(len(history.load_history("sales")), 1)


class TestSalesParsing(unittest.TestCase):
    def test_parse_metrics_from_results(self):
        df = pd.DataFrame(
            [
                ["Gross Sales DC-1", "$ 123,456", "$100,000", "23.5%"],
                ["=============== UNITS ===============", "", "", ""],
                ["Order Count", "42", "40", "5.0%"],
                ["Kids Revenue", "$9,876", "$8,000", "23.4%"],
            ],
            columns=["METRIC", "WEEK_1", "WEEK_2", "PCT_CHANGE"],
        )
        metrics = sales_bot.parse_metrics_from_results(df)
        self.assertEqual(metrics["gross_sales_dc1"], 123456.0)
        self.assertEqual(metrics["order_count"], 42.0)
        self.assertEqual(metrics["kids_rev"], 9876.0)
        self.assertNotIn("gross_sales_all", metrics)


class TestEmailSender(unittest.TestCase):
    def test_missing_recipient_returns_false(self):
        self.assertFalse(email_sender.send_report_email("s", "b", None))

    def test_missing_credentials_returns_false(self):
        with patch.object(email_sender, "SMTP_USERNAME", None):
            self.assertFalse(email_sender.send_report_email("s", "b", "x@y.com"))


if __name__ == "__main__":
    unittest.main()
