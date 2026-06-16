from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from telegram_signal_k2.binance import Kline
from telegram_signal_k2.config import display_symbol
from telegram_signal_k2.indicators import Signal


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChartOptions:
    enabled: bool = True
    candles: int = 80
    width: float = 12
    height: float = 7


@dataclass(frozen=True)
class SignalChartData:
    symbol: str
    timeframe: str
    direction: str
    price: Decimal
    signal_time_ms: int
    k: float
    d: float
    j: float
    whale_pump: float
    klines: list[Kline]


def signal_chart_data(symbol: str, timeframe: str, signal: Signal, klines: list[Kline]) -> SignalChartData:
    direction = "LONG" if "long" in signal.kind.value else "SHORT"
    return SignalChartData(
        symbol=symbol,
        timeframe=timeframe,
        direction=direction,
        price=signal.point.close,
        signal_time_ms=signal.point.close_time,
        k=signal.point.k,
        d=signal.point.d,
        j=signal.point.j,
        whale_pump=signal.point.whale_pump,
        klines=klines,
    )


def generate_signal_chart(data: SignalChartData, options: ChartOptions) -> bytes | None:
    if not options.enabled:
        return None
    if not data.klines:
        logger.warning("Cannot render chart for %s %s: no klines", data.symbol, data.timeframe)
        return None

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
    except Exception:
        logger.exception("Chart rendering dependency is unavailable")
        return None

    candles = data.klines[-max(10, options.candles) :]
    x_values = list(range(len(candles)))
    closes = [float(item.close) for item in candles]
    highs = [float(item.high) for item in candles]
    lows = [float(item.low) for item in candles]
    volumes = [float(item.quote_volume) for item in candles]
    colors = ["#16a34a" if item.close >= item.open else "#dc2626" for item in candles]

    try:
        fig, (price_ax, volume_ax) = plt.subplots(
            2,
            1,
            figsize=(options.width, options.height),
            dpi=120,
            sharex=True,
            gridspec_kw={"height_ratios": [4, 1], "hspace": 0.04},
        )
        fig.patch.set_facecolor("#0f172a")
        price_ax.set_facecolor("#111827")
        volume_ax.set_facecolor("#111827")

        for index, candle in enumerate(candles):
            open_price = float(candle.open)
            close_price = float(candle.close)
            high = float(candle.high)
            low = float(candle.low)
            color = colors[index]
            body_bottom = min(open_price, close_price)
            body_height = max(abs(close_price - open_price), max(closes) * 0.00001)
            price_ax.vlines(index, low, high, color=color, linewidth=1.1, alpha=0.9)
            price_ax.add_patch(
                Rectangle(
                    (index - 0.32, body_bottom),
                    0.64,
                    body_height,
                    facecolor=color,
                    edgecolor=color,
                    linewidth=0.8,
                    alpha=0.95,
                )
            )

        marker_color = "#22c55e" if data.direction == "LONG" else "#ef4444"
        marker_symbol = "^" if data.direction == "LONG" else "v"
        signal_index = len(candles) - 1
        signal_price = float(data.price)
        price_ax.scatter(
            [signal_index],
            [signal_price],
            color=marker_color,
            marker=marker_symbol,
            s=180,
            zorder=5,
            edgecolor="#f8fafc",
            linewidth=1.0,
        )
        price_ax.axhline(signal_price, color=marker_color, linestyle="--", linewidth=1, alpha=0.75)
        price_ax.annotate(
            f"{data.direction} {format_price(data.price)}",
            xy=(signal_index, signal_price),
            xytext=(-95, 24 if data.direction == "LONG" else -34),
            textcoords="offset points",
            color="#f8fafc",
            bbox={"boxstyle": "round,pad=0.35", "fc": marker_color, "ec": "none", "alpha": 0.85},
            arrowprops={"arrowstyle": "->", "color": marker_color},
        )

        volume_ax.bar(x_values, volumes, color=colors, alpha=0.55, width=0.7)
        volume_ax.set_ylabel("Quote vol", color="#cbd5e1", fontsize=9)
        volume_ax.tick_params(colors="#94a3b8", labelsize=8)

        signal_time = datetime.fromtimestamp(data.signal_time_ms / 1000, tz=UTC).strftime(
            "%Y-%m-%d %H:%M UTC"
        )
        price_ax.set_title(
            f"{display_symbol(data.symbol)} {data.timeframe} | {data.direction} | {signal_time}",
            color="#f8fafc",
            fontsize=14,
            loc="left",
            pad=12,
        )
        price_ax.text(
            0.99,
            0.96,
            f"K {data.k:.2f}  D {data.d:.2f}  J {data.j:.2f}\nWhale {data.whale_pump:.4f}",
            transform=price_ax.transAxes,
            ha="right",
            va="top",
            color="#e2e8f0",
            fontsize=10,
            bbox={"boxstyle": "round,pad=0.35", "fc": "#1e293b", "ec": "#334155", "alpha": 0.9},
        )

        for ax in (price_ax, volume_ax):
            ax.grid(color="#334155", linestyle="-", linewidth=0.4, alpha=0.45)
            ax.spines["left"].set_color("#334155")
            ax.spines["right"].set_color("#334155")
            ax.spines["top"].set_color("#334155")
            ax.spines["bottom"].set_color("#334155")
            ax.tick_params(colors="#94a3b8")

        price_ax.set_xlim(-1, len(candles))
        price_ax.set_ylabel("Price", color="#cbd5e1")
        volume_ax.set_xlabel("Last candles", color="#cbd5e1")
        volume_ax.set_xticks([])

        buffer = io.BytesIO()
        fig.savefig(buffer, format="png", bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        return buffer.getvalue()
    except Exception:
        logger.exception("Failed to render chart for %s %s", data.symbol, data.timeframe)
        try:
            plt.close("all")
        except Exception:
            pass
        return None


def format_price(price: Decimal) -> str:
    text = format(price.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text
