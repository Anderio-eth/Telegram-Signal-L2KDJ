from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class CombinedDirection(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"


@dataclass(frozen=True)
class TimeframeSignal:
    timeframe: str
    direction: CombinedDirection
    indicator_value: float | None = None
    price: str | None = None
    close_time: int | None = None
    reason: str | None = None


@dataclass(frozen=True)
class CombinedEvaluation:
    symbol: str
    direction: CombinedDirection
    matched_timeframes: list[str]
    rejected_reason: str | None
    details: dict[str, TimeframeSignal] = field(default_factory=dict)

    @property
    def is_signal(self) -> bool:
        return self.direction in {CombinedDirection.LONG, CombinedDirection.SHORT}


def evaluate_combined_signal(
    *,
    symbol: str,
    timeframe_signals: dict[str, TimeframeSignal],
    rule: str,
    required_timeframes: list[str],
) -> CombinedEvaluation:
    normalized_rule = rule.strip().lower()
    if normalized_rule not in {"all_match", "majority_match"}:
        return CombinedEvaluation(
            symbol=symbol,
            direction=CombinedDirection.NEUTRAL,
            matched_timeframes=[],
            rejected_reason=f"Unknown combined rule: {rule}",
            details=timeframe_signals,
        )

    if not required_timeframes:
        return CombinedEvaluation(
            symbol=symbol,
            direction=CombinedDirection.NEUTRAL,
            matched_timeframes=[],
            rejected_reason="No required timeframes configured",
            details=timeframe_signals,
        )

    missing = [timeframe for timeframe in required_timeframes if timeframe not in timeframe_signals]
    if missing:
        return CombinedEvaluation(
            symbol=symbol,
            direction=CombinedDirection.NEUTRAL,
            matched_timeframes=[],
            rejected_reason=f"Missing timeframe data: {', '.join(missing)}",
            details=timeframe_signals,
        )

    long_matches = [
        timeframe
        for timeframe in required_timeframes
        if timeframe_signals[timeframe].direction == CombinedDirection.LONG
    ]
    short_matches = [
        timeframe
        for timeframe in required_timeframes
        if timeframe_signals[timeframe].direction == CombinedDirection.SHORT
    ]
    neutral_matches = [
        timeframe
        for timeframe in required_timeframes
        if timeframe_signals[timeframe].direction == CombinedDirection.NEUTRAL
    ]

    if normalized_rule == "all_match":
        if len(long_matches) == len(required_timeframes):
            return CombinedEvaluation(symbol, CombinedDirection.LONG, long_matches, None, timeframe_signals)
        if len(short_matches) == len(required_timeframes):
            return CombinedEvaluation(symbol, CombinedDirection.SHORT, short_matches, None, timeframe_signals)
        return CombinedEvaluation(
            symbol=symbol,
            direction=CombinedDirection.NEUTRAL,
            matched_timeframes=[],
            rejected_reason="Required timeframes do not all match",
            details=timeframe_signals,
        )

    required_count = len(required_timeframes) // 2 + 1
    if len(long_matches) >= required_count and not short_matches:
        return CombinedEvaluation(symbol, CombinedDirection.LONG, long_matches, None, timeframe_signals)
    if len(short_matches) >= required_count and not long_matches:
        return CombinedEvaluation(symbol, CombinedDirection.SHORT, short_matches, None, timeframe_signals)

    reason = "No majority match"
    if long_matches and short_matches:
        reason = "Conflicting LONG and SHORT confirmations"
    elif neutral_matches:
        reason = "Not enough non-neutral confirmations"

    return CombinedEvaluation(
        symbol=symbol,
        direction=CombinedDirection.NEUTRAL,
        matched_timeframes=[],
        rejected_reason=reason,
        details=timeframe_signals,
    )
