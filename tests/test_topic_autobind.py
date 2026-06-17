from __future__ import annotations

import unittest

from telegram_signal_k2.telegram_app import (
    is_combined_topic_title,
    timeframe_from_topic_title,
)


class TopicAutobindTests(unittest.TestCase):
    def test_timeframe_from_topic_title_accepts_localized_names(self) -> None:
        timeframes = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "3h", "4h"]

        self.assertEqual(timeframe_from_topic_title("1хв", timeframes), "1m")
        self.assertEqual(timeframe_from_topic_title("3 хв", timeframes), "3m")
        self.assertEqual(timeframe_from_topic_title("2h", timeframes), "2h")
        self.assertEqual(timeframe_from_topic_title("3г", timeframes), "3h")
        self.assertEqual(timeframe_from_topic_title("4 год", timeframes), "4h")

    def test_combined_topic_title(self) -> None:
        self.assertTrue(is_combined_topic_title("Combined Signals"))
        self.assertTrue(is_combined_topic_title("combined"))
        self.assertFalse(is_combined_topic_title("15хв"))


if __name__ == "__main__":
    unittest.main()
