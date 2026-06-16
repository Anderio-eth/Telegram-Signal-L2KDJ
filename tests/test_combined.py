from __future__ import annotations

import unittest

from telegram_signal_k2.combined import (
    CombinedDirection,
    TimeframeSignal,
    evaluate_combined_signal,
)


class CombinedSignalTests(unittest.TestCase):
    def test_all_match_returns_partial_long_before_largest_timeframe(self) -> None:
        result = evaluate_combined_signal(
            symbol="BTCUSDT",
            required_timeframes=["1m", "3m", "5m", "15m"],
            rule="all_match",
            timeframe_signals={
                "1m": TimeframeSignal("1m", CombinedDirection.LONG),
                "3m": TimeframeSignal("3m", CombinedDirection.LONG),
                "5m": TimeframeSignal("5m", CombinedDirection.LONG),
                "15m": TimeframeSignal("15m", CombinedDirection.NEUTRAL),
            },
        )

        self.assertEqual(result.direction, CombinedDirection.LONG)
        self.assertEqual(result.stage, "partial")
        self.assertEqual(result.matched_timeframes, ["1m", "3m", "5m"])
        self.assertEqual(result.pending_timeframes, ["15m"])
        self.assertEqual(result.total_count, 4)
        self.assertIsNone(result.rejected_reason)

    def test_all_match_returns_full_short(self) -> None:
        result = evaluate_combined_signal(
            symbol="BTCUSDT",
            required_timeframes=["1m", "3m", "5m", "15m"],
            rule="all_match",
            timeframe_signals={
                "1m": TimeframeSignal("1m", CombinedDirection.SHORT),
                "3m": TimeframeSignal("3m", CombinedDirection.SHORT),
                "5m": TimeframeSignal("5m", CombinedDirection.SHORT),
                "15m": TimeframeSignal("15m", CombinedDirection.SHORT),
            },
        )

        self.assertEqual(result.direction, CombinedDirection.SHORT)
        self.assertEqual(result.stage, "full")
        self.assertEqual(result.matched_timeframes, ["1m", "3m", "5m", "15m"])

    def test_all_match_rejects_conflict(self) -> None:
        result = evaluate_combined_signal(
            symbol="BTCUSDT",
            required_timeframes=["1m", "3m", "5m", "15m"],
            rule="all_match",
            timeframe_signals={
                "1m": TimeframeSignal("1m", CombinedDirection.LONG),
                "3m": TimeframeSignal("3m", CombinedDirection.LONG),
                "5m": TimeframeSignal("5m", CombinedDirection.LONG),
                "15m": TimeframeSignal("15m", CombinedDirection.SHORT),
            },
        )

        self.assertEqual(result.direction, CombinedDirection.NEUTRAL)
        self.assertEqual(result.stage, "none")
        self.assertIn("all-but-last progress", result.rejected_reason or "")

    def test_majority_match_returns_partial_long_without_short_conflict(self) -> None:
        result = evaluate_combined_signal(
            symbol="BTCUSDT",
            required_timeframes=["5m", "15m", "1h"],
            rule="majority_match",
            timeframe_signals={
                "5m": TimeframeSignal("5m", CombinedDirection.LONG),
                "15m": TimeframeSignal("15m", CombinedDirection.LONG),
                "1h": TimeframeSignal("1h", CombinedDirection.NEUTRAL),
            },
        )

        self.assertEqual(result.direction, CombinedDirection.LONG)
        self.assertEqual(result.stage, "partial")
        self.assertEqual(result.matched_timeframes, ["5m", "15m"])
        self.assertEqual(result.pending_timeframes, ["1h"])

    def test_missing_timeframe_is_treated_as_neutral(self) -> None:
        result = evaluate_combined_signal(
            symbol="BTCUSDT",
            required_timeframes=["15m", "1h", "4h"],
            rule="all_match",
            timeframe_signals={
                "15m": TimeframeSignal("15m", CombinedDirection.LONG),
                "1h": TimeframeSignal("1h", CombinedDirection.LONG),
            },
        )

        self.assertEqual(result.direction, CombinedDirection.LONG)
        self.assertEqual(result.stage, "partial")
        self.assertEqual(result.pending_timeframes, ["4h"])


if __name__ == "__main__":
    unittest.main()
