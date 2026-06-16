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
    stage: str = "none"
    total_count: int = 0
    pending_timeframes: list[str] = field(default_factory=list)

    @property
    def is_signal(self) -> bool:
        return (
            self.direction in {CombinedDirection.LONG, CombinedDirection.SHORT}
            and self.stage in {"partial", "full"}
        )


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
            total_count=len(required_timeframes),
        )

    if len(required_timeframes) < 2:
        return CombinedEvaluation(
            symbol=symbol,
            direction=CombinedDirection.NEUTRAL,
            matched_timeframes=[],
            rejected_reason="At least two timeframes are required",
            details=timeframe_signals,
            total_count=len(required_timeframes),
        )

    normalized_signals = {
        timeframe: timeframe_signals.get(
            timeframe,
            TimeframeSignal(timeframe, CombinedDirection.NEUTRAL, reason="missing"),
        )
        for timeframe in required_timeframes
    }

    full = _full_match(symbol, normalized_signals, required_timeframes, CombinedDirection.LONG)
    if full:
        return full
    full = _full_match(symbol, normalized_signals, required_timeframes, CombinedDirection.SHORT)
    if full:
        return full

    if normalized_rule == "all_match":
        partial = _all_match_progress(
            symbol,
            normalized_signals,
            required_timeframes,
            CombinedDirection.LONG,
        )
        if partial:
            return partial
        partial = _all_match_progress(
            symbol,
            normalized_signals,
            required_timeframes,
            CombinedDirection.SHORT,
        )
        if partial:
            return partial
        return CombinedEvaluation(
            symbol=symbol,
            direction=CombinedDirection.NEUTRAL,
            matched_timeframes=[],
            rejected_reason="Required timeframes do not have all-but-last progress",
            details=normalized_signals,
            total_count=len(required_timeframes),
        )

    partials = [
        _majority_progress(symbol, normalized_signals, required_timeframes, CombinedDirection.LONG),
        _majority_progress(symbol, normalized_signals, required_timeframes, CombinedDirection.SHORT),
    ]
    partials = [item for item in partials if item is not None]
    if partials:
        return max(partials, key=lambda item: len(item.matched_timeframes))

    reason = "No majority match"
    long_matches = _matched_timeframes(normalized_signals, required_timeframes, CombinedDirection.LONG)
    short_matches = _matched_timeframes(normalized_signals, required_timeframes, CombinedDirection.SHORT)
    neutral_matches = [
        timeframe
        for timeframe in required_timeframes
        if normalized_signals[timeframe].direction == CombinedDirection.NEUTRAL
    ]
    if long_matches and short_matches:
        reason = "Conflicting LONG and SHORT confirmations"
    elif neutral_matches:
        reason = "Not enough non-neutral confirmations"

    return CombinedEvaluation(
        symbol=symbol,
        direction=CombinedDirection.NEUTRAL,
        matched_timeframes=[],
        rejected_reason=reason,
        details=normalized_signals,
        total_count=len(required_timeframes),
    )


def _full_match(
    symbol: str,
    timeframe_signals: dict[str, TimeframeSignal],
    required_timeframes: list[str],
    direction: CombinedDirection,
) -> CombinedEvaluation | None:
    matches = _matched_timeframes(timeframe_signals, required_timeframes, direction)
    if len(matches) != len(required_timeframes):
        return None
    return CombinedEvaluation(
        symbol=symbol,
        direction=direction,
        matched_timeframes=matches,
        rejected_reason=None,
        details=timeframe_signals,
        stage="full",
        total_count=len(required_timeframes),
        pending_timeframes=[],
    )


def _all_match_progress(
    symbol: str,
    timeframe_signals: dict[str, TimeframeSignal],
    required_timeframes: list[str],
    direction: CombinedDirection,
) -> CombinedEvaluation | None:
    leading_timeframes = required_timeframes[:-1]
    final_timeframe = required_timeframes[-1]
    matches = _matched_timeframes(timeframe_signals, required_timeframes, direction)
    conflicts = _matched_timeframes(
        timeframe_signals,
        required_timeframes,
        _opposite_direction(direction),
    )

    if all(timeframe in matches for timeframe in leading_timeframes) and final_timeframe not in conflicts:
        return CombinedEvaluation(
            symbol=symbol,
            direction=direction,
            matched_timeframes=[timeframe for timeframe in required_timeframes if timeframe in matches],
            rejected_reason=None,
            details=timeframe_signals,
            stage="partial",
            total_count=len(required_timeframes),
            pending_timeframes=[final_timeframe],
        )
    return None


def _majority_progress(
    symbol: str,
    timeframe_signals: dict[str, TimeframeSignal],
    required_timeframes: list[str],
    direction: CombinedDirection,
) -> CombinedEvaluation | None:
    matches = _matched_timeframes(timeframe_signals, required_timeframes, direction)
    conflicts = _matched_timeframes(
        timeframe_signals,
        required_timeframes,
        _opposite_direction(direction),
    )
    required_count = len(required_timeframes) // 2 + 1
    if len(matches) < required_count or conflicts:
        return None

    pending = [timeframe for timeframe in required_timeframes if timeframe not in matches]
    if not pending:
        return None

    return CombinedEvaluation(
        symbol=symbol,
        direction=direction,
        matched_timeframes=matches,
        rejected_reason=None,
        details=timeframe_signals,
        stage="partial",
        total_count=len(required_timeframes),
        pending_timeframes=pending,
    )


def _matched_timeframes(
    timeframe_signals: dict[str, TimeframeSignal],
    required_timeframes: list[str],
    direction: CombinedDirection,
) -> list[str]:
    return [
        timeframe
        for timeframe in required_timeframes
        if timeframe_signals[timeframe].direction == direction
    ]


def _opposite_direction(direction: CombinedDirection) -> CombinedDirection:
    if direction == CombinedDirection.LONG:
        return CombinedDirection.SHORT
    if direction == CombinedDirection.SHORT:
        return CombinedDirection.LONG
    return CombinedDirection.NEUTRAL
