from __future__ import annotations

import unittest

from telegram_signal_k2.combined import (
    CombinedDirection,
    TimeframeSignal,
    evaluate_combined_signal,
)
from telegram_signal_k2.state import BotState, CombinedConfig
from telegram_signal_k2.telegram_app import (
    combined_cycle_key,
    combined_cycle_payload,
    evaluate_combined_cycle,
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

    def test_cycle_requires_partial_before_full_and_resets_after_full(self) -> None:
        config = CombinedConfig(enabled=True, timeframes=["1m", "3m", "5m"], rule="all_match")
        state = BotState(
            chat_id="-1001",
            available_symbols=["ETHUSDT"],
            enabled_symbols=["ETHUSDT"],
            topic_threads={},
        )
        signals = {
            "1m": TimeframeSignal("1m", CombinedDirection.LONG, close_time=100),
            "3m": TimeframeSignal("3m", CombinedDirection.LONG, close_time=200),
            "5m": TimeframeSignal("5m", CombinedDirection.LONG, close_time=300),
        }

        partial = evaluate_combined_cycle(
            state=state,
            chat_id="-1001",
            config=config,
            symbol="ETHUSDT",
            timeframe_signals=signals,
        )

        self.assertEqual(partial.stage, "partial")
        self.assertEqual(partial.matched_timeframes, ["1m", "3m"])

        key = combined_cycle_key("-1001", config, "ETHUSDT")
        state.combined_cycles[key] = combined_cycle_payload(config, partial, {})

        same_final = evaluate_combined_cycle(
            state=state,
            chat_id="-1001",
            config=config,
            symbol="ETHUSDT",
            timeframe_signals=signals,
        )
        self.assertEqual(same_final.stage, "none")

        signals["5m"] = TimeframeSignal("5m", CombinedDirection.LONG, close_time=301)
        full = evaluate_combined_cycle(
            state=state,
            chat_id="-1001",
            config=config,
            symbol="ETHUSDT",
            timeframe_signals=signals,
        )
        self.assertEqual(full.stage, "full")
        self.assertEqual(full.matched_timeframes, ["1m", "3m", "5m"])

        state.combined_cycles[key] = combined_cycle_payload(config, full, state.combined_cycles[key])
        after_full = evaluate_combined_cycle(
            state=state,
            chat_id="-1001",
            config=config,
            symbol="ETHUSDT",
            timeframe_signals=signals,
        )
        self.assertEqual(after_full.stage, "none")

        next_cycle_signals = {
            "1m": TimeframeSignal("1m", CombinedDirection.LONG, close_time=110),
            "3m": TimeframeSignal("3m", CombinedDirection.LONG, close_time=210),
            "5m": TimeframeSignal("5m", CombinedDirection.LONG, close_time=301),
        }
        next_partial = evaluate_combined_cycle(
            state=state,
            chat_id="-1001",
            config=config,
            symbol="ETHUSDT",
            timeframe_signals=next_cycle_signals,
        )
        self.assertEqual(next_partial.stage, "partial")
        self.assertEqual(next_partial.matched_timeframes, ["1m", "3m"])


if __name__ == "__main__":
    unittest.main()
