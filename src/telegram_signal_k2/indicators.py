from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from telegram_signal_k2.binance import Kline


class SignalKind(str, Enum):
    LONG = "long"
    SHORT = "short"


@dataclass(frozen=True)
class KdjPoint:
    close_time: int
    close: Decimal
    k: float
    d: float
    j: float
    whale_pump: float
    quote_volume: Decimal
    avg_quote_volume: Decimal
    volume_ratio: float


@dataclass(frozen=True)
class Signal:
    kind: SignalKind
    point: KdjPoint
    previous: KdjPoint
    volume_ok: bool
    strong_volume: bool
    reason: str

    @property
    def is_confirmed(self) -> bool:
        return True


@dataclass(frozen=True)
class SignalRules:
    # TradingView script defaults: n1=18, m1=4, m2=4.
    kdj_n1: int = 18
    kdj_m1: int = 4
    kdj_m2: int = 4
    buy_alert_limit: float = 0
    sell_alert_limit: float = 100
    indicator_scale_min: float = -10
    indicator_scale_max: float = 110
    volume_ma_period: int = 20
    min_volume_ratio: float = 0.8
    strong_volume_ratio: float = 1.3
    require_volume_for_confirmed: bool = False


def calculate_kdj(klines: list[Kline], rules: SignalRules) -> list[KdjPoint]:
    warmup = max(rules.kdj_n1, rules.volume_ma_period, 90) + 2
    if len(klines) < warmup:
        return []

    closes = [float(item.close) for item in klines]
    highs = [float(item.high) for item in klines]
    lows = [float(item.low) for item in klines]

    whalepump = calculate_whale_pump(lows, closes)

    rsv: list[float | None] = []
    for index, candle in enumerate(klines):
        if index + 1 < rules.kdj_n1:
            rsv.append(None)
            continue

        low = min(lows[index + 1 - rules.kdj_n1 : index + 1])
        high = max(highs[index + 1 - rules.kdj_n1 : index + 1])
        if high == low:
            rsv.append(None)
        else:
            rsv.append((float(candle.close) - low) / (high - low) * 100)

    k_series = xsa(rsv, rules.kdj_m1, 1)
    d_series = xsa(k_series, rules.kdj_m2, 1)

    points: list[KdjPoint] = []
    for index, candle in enumerate(klines):
        k_value = k_series[index]
        d_value = d_series[index]
        if k_value is None or d_value is None:
            continue

        j_value = 3 * k_value - 2 * d_value
        volume_window = klines[max(0, index - rules.volume_ma_period) : index]
        if not volume_window:
            volume_window = [candle]
        avg_quote_volume = sum((item.quote_volume for item in volume_window), Decimal("0")) / Decimal(
            len(volume_window)
        )
        volume_ratio = (
            float(candle.quote_volume / avg_quote_volume) if avg_quote_volume > Decimal("0") else 0.0
        )

        points.append(
            KdjPoint(
                close_time=candle.close_time,
                close=candle.close,
                k=k_value,
                d=d_value,
                j=j_value,
                whale_pump=whalepump[index] or 0.0,
                quote_volume=candle.quote_volume,
                avg_quote_volume=avg_quote_volume,
                volume_ratio=volume_ratio,
            )
        )

    return points


def calculate_whale_pump(lows: list[float], closes: list[float]) -> list[float | None]:
    previous_low: list[float | None] = [None]
    previous_low.extend(lows[:-1])

    abs_low_delta: list[float | None] = []
    positive_low_delta: list[float | None] = []
    for low, prev in zip(lows, previous_low, strict=False):
        if prev is None:
            abs_low_delta.append(None)
            positive_low_delta.append(None)
            continue
        delta = low - prev
        abs_low_delta.append(abs(delta))
        positive_low_delta.append(max(delta, 0))

    smoothed_abs = xsa(abs_low_delta, 3, 1)
    smoothed_positive = xsa(positive_low_delta, 3, 1)

    var2: list[float | None] = []
    for numerator, denominator in zip(smoothed_abs, smoothed_positive, strict=False):
        if numerator is None or denominator in (None, 0):
            var2.append(None)
        else:
            var2.append(numerator / denominator * 100)

    # The original Pine condition `iff(close*1.2, ...)` is effectively true for live crypto
    # prices, so this follows the visible TradingView behavior and uses var2 * 10.
    var3 = ema([value * 10 if value is not None else None for value in var2], 3)
    var7_input: list[float | None] = []

    for index, low in enumerate(lows):
        var4 = rolling_min(lows, index, 38)
        var5 = rolling_max(var3, index, 38)
        var6 = 1 if (rolling_min(lows, index, 90) or 0) != 0 else 0
        if var3[index] is None or var4 is None or var5 is None:
            var7_input.append(None)
        elif low <= var4:
            var7_input.append((var3[index] + var5 * 2) / 2 * var6)
        else:
            var7_input.append(0.0)

    return [value / 618 if value is not None else None for value in ema(var7_input, 3)]


def detect_signal(points: list[KdjPoint], rules: SignalRules) -> Signal | None:
    if len(points) < 2:
        return None

    previous = points[-2]
    current = points[-1]

    volume_ok = current.volume_ratio >= rules.min_volume_ratio
    strong_volume = current.volume_ratio >= rules.strong_volume_ratio

    crossed_buy = previous.j <= rules.buy_alert_limit and current.j > rules.buy_alert_limit
    crossed_sell = previous.j >= rules.sell_alert_limit and current.j < rules.sell_alert_limit
    if crossed_buy and (volume_ok or not rules.require_volume_for_confirmed):
        return Signal(SignalKind.LONG, current, previous, volume_ok, strong_volume, "J crossed above 0")

    if crossed_sell and (volume_ok or not rules.require_volume_for_confirmed):
        return Signal(SignalKind.SHORT, current, previous, volume_ok, strong_volume, "J crossed below 100")

    return None


def xsa(values: list[float | None], length: int, weight: int) -> list[float | None]:
    result: list[float | None] = []
    rolling_sum = 0.0

    for index, value in enumerate(values):
        rolling_sum += nz(value)
        if index - length >= 0:
            rolling_sum -= nz(values[index - length])

        ma = None if index - length < 0 or values[index - length] is None else rolling_sum / length
        previous = result[-1] if result else None
        if previous is None:
            result.append(ma)
        elif value is None:
            result.append(None)
        else:
            result.append((value * weight + previous * (length - weight)) / length)

    return result


def ema(values: list[float | None], length: int) -> list[float | None]:
    result: list[float | None] = []
    alpha = 2 / (length + 1)
    previous: float | None = None

    for value in values:
        if value is None:
            result.append(previous)
            continue
        previous = value if previous is None else value * alpha + previous * (1 - alpha)
        result.append(previous)

    return result


def rolling_min(values: list[float], index: int, length: int) -> float | None:
    if index + 1 < length:
        return None
    return min(values[index + 1 - length : index + 1])


def rolling_max(values: list[float | None], index: int, length: int) -> float | None:
    if index + 1 < length:
        return None
    window = [value for value in values[index + 1 - length : index + 1] if value is not None]
    return max(window) if window else None


def nz(value: float | None) -> float:
    return 0.0 if value is None else value
