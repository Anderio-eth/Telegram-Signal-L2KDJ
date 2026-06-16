from __future__ import annotations

import unittest

from telegram_signal_k2.combined import (
    CombinedDirection,
    TimeframeSignal,
    evaluate_combined_signal,
)


class CombinedSignalTests(unittest.TestCase):
    def test_all_match_returns_long(self) -> None:
        result = evaluate_combined_signal(
            symbol="BTCUSDT",
            required_timeframes=["15m", "1h"],
            rule="all_match",
            timeframe_signals={
                "15m": TimeframeSignal("15m", CombinedDirection.LONG, indicator_value=1.2),
                "1h": TimeframeSignal("1h", CombinedDirection.LONG, indicator_value=3.4),
            },
        )

        self.assertEqual(result.direction, CombinedDirection.LONG)
        self.assertEqual(result.matched_timeframes, ["15m", "1h"])
        self.assertIsNone(result.rejected_reason)

    def test_all_match_returns_short(self) -> None:
        result = evaluate_combined_signal(
            symbol="BTCUSDT",
            required_timeframes=["15m", "1h"],
            rule="all_match",
            timeframe_signals={
                "15m": TimeframeSignal("15m", CombinedDirection.SHORT),
                "1h": TimeframeSignal("1h", CombinedDirection.SHORT),
            },
        )

        self.assertEqual(result.direction, CombinedDirection.SHORT)
        self.assertEqual(result.matched_timeframes, ["15m", "1h"])

    def test_all_match_rejects_conflict(self) -> None:
        result = evaluate_combined_signal(
            symbol="BTCUSDT",
            required_timeframes=["15m", "1h"],
            rule="all_match",
            timeframe_signals={
                "15m": TimeframeSignal("15m", CombinedDirection.LONG),
                "1h": TimeframeSignal("1h", CombinedDirection.SHORT),
            },
        )

        self.assertEqual(result.direction, CombinedDirection.NEUTRAL)
        self.assertEqual(result.rejected_reason, "Required timeframes do not all match")

    def test_majority_match_returns_long_without_short_conflict(self) -> None:
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
        self.assertEqual(result.matched_timeframes, ["5m", "15m"])

    def test_missing_timeframe_rejected(self) -> None:
        result = evaluate_combined_signal(
            symbol="BTCUSDT",
            required_timeframes=["15m", "1h"],
            rule="all_match",
            timeframe_signals={
                "15m": TimeframeSignal("15m", CombinedDirection.LONG),
            },
        )

        self.assertEqual(result.direction, CombinedDirection.NEUTRAL)
        self.assertIn("Missing timeframe data", result.rejected_reason or "")


if __name__ == "__main__":
    unittest.main()
