from __future__ import annotations

import unittest
from decimal import Decimal

from telegram_signal_k2.binance import Kline
from telegram_signal_k2.charting import ChartOptions, SignalChartData, generate_signal_chart
from telegram_signal_k2.state import BotState, CombinedConfig


class StateTests(unittest.TestCase):
    def test_combined_config_roundtrip(self) -> None:
        state = BotState(
            chat_id=-1001,
            available_symbols=["BTCUSDT"],
            enabled_symbols=["BTCUSDT"],
            topic_threads={"15m": 123},
            latest_confirmed_signals={
                "BTCUSDT:15m": {
                    "symbol": "BTCUSDT",
                    "timeframe": "15m",
                    "direction": "LONG",
                    "close_time": 1_700_000_000_000,
                    "price": "100",
                    "j": 12.5,
                }
            },
            combined_configs={
                "-1001": CombinedConfig(
                    enabled=True,
                    timeframes=["15m", "1h"],
                    rule="all_match",
                    cooldown_seconds=900,
                    thread_id=777,
                )
            },
            combined_last_alerts={"key": 12345},
        )

        restored = BotState.from_payload(state.to_payload())

        self.assertTrue(restored.combined_configs["-1001"].enabled)
        self.assertEqual(restored.combined_configs["-1001"].timeframes, ["15m", "1h"])
        self.assertEqual(restored.combined_configs["-1001"].thread_id, 777)
        self.assertEqual(restored.combined_last_alerts["key"], 12345)
        self.assertEqual(restored.latest_confirmed_signals["BTCUSDT:15m"]["direction"], "LONG")


class ChartingTests(unittest.TestCase):
    def test_chart_disabled_returns_none(self) -> None:
        chart = generate_signal_chart(
            SignalChartData(
                symbol="BTCUSDT",
                timeframe="15m",
                direction="LONG",
                price=Decimal("100"),
                signal_time_ms=1_700_000_000_000,
                k=1,
                d=2,
                j=3,
                whale_pump=0,
                klines=[],
            ),
            ChartOptions(enabled=False),
        )

        self.assertIsNone(chart)

    def test_chart_smoke_renders_png_when_enabled(self) -> None:
        klines = [
            Kline(
                open_time=1_700_000_000_000 + index * 60_000,
                open=Decimal("100") + Decimal(index),
                high=Decimal("102") + Decimal(index),
                low=Decimal("99") + Decimal(index),
                close=Decimal("101") + Decimal(index),
                volume=Decimal("10"),
                close_time=1_700_000_000_000 + (index + 1) * 60_000 - 1,
                quote_volume=Decimal("1000") + Decimal(index * 10),
                trades=100,
                taker_buy_volume=Decimal("5"),
                taker_buy_quote_volume=Decimal("500"),
            )
            for index in range(20)
        ]
        chart = generate_signal_chart(
            SignalChartData(
                symbol="BTCUSDT",
                timeframe="15m",
                direction="LONG",
                price=klines[-1].close,
                signal_time_ms=klines[-1].close_time,
                k=10,
                d=9,
                j=12,
                whale_pump=0.1,
                klines=klines,
            ),
            ChartOptions(enabled=True, candles=20, width=4, height=3),
        )

        self.assertIsNotNone(chart)
        self.assertTrue(chart and chart.startswith(b"\x89PNG"))


if __name__ == "__main__":
    unittest.main()
