"""The tradeable universe: assets listed on BOTH Entropy (io) and Lighter.

Symbol names differ per venue, so each pair carries both spellings plus a short human label. This is
the single source of truth for what the bot can hedge; add a row here when a new common asset
appears (verify it is the same underlying on both venues first — names can collide).

Verified 2026-09-17 by intersecting Hyperliquid `meta(dex="io")` with Lighter `orderBooks`. io-only
assets not on Lighter (IONQ, GPRO, SBE) are deliberately excluded — a hedge needs both legs.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Pair:
    key: str            # stable internal id (used in callbacks / storage)
    label: str          # what the user sees
    entropy: str        # Hyperliquid market name on the io builder, e.g. "io:ANTH"
    lighter: str        # Lighter order-book symbol, e.g. "ANTHROPIC"


PAIRS: tuple[Pair, ...] = (
    Pair("oai",  "OpenAI",    "io:OAI",  "OPENAI"),
    Pair("anth", "Anthropic", "io:ANTH", "ANTHROPIC"),
    Pair("sndk", "SanDisk",   "io:SNDK", "SNDK"),
    Pair("nbis", "Nebius",    "io:NBIS", "NBIS"),
    Pair("ewy",  "Korea ETF", "io:EWY",  "EWY"),
    Pair("tcnt", "Tencent",   "io:TCNT", "TENCENT"),
    Pair("dram", "DRAM",      "io:DRAM", "DRAM"),
)

BY_KEY: dict[str, Pair] = {p.key: p for p in PAIRS}


def get(key: str) -> Pair | None:
    return BY_KEY.get(key)
