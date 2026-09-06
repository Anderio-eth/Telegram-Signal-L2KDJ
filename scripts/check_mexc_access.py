"""Verify what a MEXC API key is actually allowed to do, before any code depends on it.

MEXC restricts futures order placement on some accounts (which is why "bypass" projects exist),
and there is no way to tell from outside — the endpoints answer identically to an unsigned probe.
This asks the account directly, and it is worth running before writing a line of copy-trading
logic rather than discovering the answer a week in.

It performs, in order:
  1. read balance                     — proves the key works at all
  2. read open positions              — proves futures read access
  3. read position mode               — hedge vs one-way must match across accounts
  4. OPTIONALLY place + close a tiny real order (only with --live), which is the only way to
     prove order placement is permitted

Usage (from the repo root, with the key in .env):

    python scripts/check_mexc_access.py
    python scripts/check_mexc_access.py --live      # places a real minimum-size order

Nothing here prints the secret.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

# Windows terminals default to a legacy code page (cp1251 here), which raises UnicodeEncodeError
# on any non-ASCII output — and this script prints its results, so a crash mid-run could leave a
# real position open. Force UTF-8 rather than stripping the characters.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.mexc.rest import (  # noqa: E402
    OPEN_TYPE_CROSS,
    SIDE_CLOSE_LONG,
    SIDE_OPEN_LONG,
    MexcError,
    MexcRestClient,
    get_contract_specs,
    get_ticker_price,
)

# ADA rather than BTC: one BTC contract is ~$8 of exposure, which on a small test account risks
# an "insufficient balance" that looks identical to "order placement is blocked" — the exact
# ambiguity this script exists to remove. One ADA contract is ~$0.22, and it is liquid enough
# that a market order fills instantly. Override with --symbol.
DEFAULT_SYMBOL = "ADA_USDT"


async def main(live: bool, symbol: str) -> int:
    load_dotenv()
    api_key = os.getenv("MEXC_TEST_API_KEY")
    secret = os.getenv("MEXC_TEST_SECRET")
    if not api_key or not secret:
        print("Missing MEXC_TEST_API_KEY / MEXC_TEST_SECRET in .env")
        return 1

    print(f"key ...{api_key[-4:]}\n")

    async with aiohttp.ClientSession() as session:
        client = MexcRestClient(api_key, secret, session=session)

        try:
            equity, available = await client.get_usdt_balance()
            print(f"[1/4] balance          OK   equity={equity:.2f} USDT, available={available:.2f} USDT")
        except MexcError as err:
            print(f"[1/4] balance          FAIL {err}")
            print("\n→ The key cannot even read the account. Check that it has futures permissions.")
            return 1

        try:
            positions = await client.get_open_positions()
            print(f"[2/4] open positions   OK   {len(positions)} open")
            for p in positions:
                side = "LONG" if p.is_long else "SHORT"
                print(f"                            {p.symbol} {side} vol={p.hold_vol} lev={p.leverage}x")
        except MexcError as err:
            print(f"[2/4] open positions   FAIL {err}")

        try:
            mode = await client.get_position_mode()
            print(f"[3/4] position mode    OK   {mode} ({'hedge' if mode == 1 else 'one-way'})")
        except MexcError as err:
            print(f"[3/4] position mode    FAIL {err}")

        if not live:
            print("\n[4/4] order placement  SKIPPED — rerun with --live to test it for real.")
            print("      This is the one thing that cannot be verified without placing an order.")
            return 0

        specs = await get_contract_specs(session, symbol)
        spec = specs.get(symbol)
        price = await get_ticker_price(session, symbol)
        if not spec or price <= 0:
            print("[4/4] order placement  FAIL could not read contract spec/price")
            return 1

        vol = max(spec.min_vol, 1)
        notional = spec.notional(vol, price)
        margin = notional / 20
        print(f"\n[4/4] placing a REAL order: {symbol} LONG {vol} contract(s)")
        print(f"      ≈ ${notional:.2f} notional, ≈ ${margin:.2f} margin at 20x — closed immediately after")

        try:
            result = await client.submit_order(
                symbol=symbol,
                side=SIDE_OPEN_LONG,
                vol=vol,
                leverage=20,
                open_type=OPEN_TYPE_CROSS,
                external_oid=f"accesscheck-{os.getpid()}",
            )
            print(f"      order placement  OK   order={result}")
        except MexcError as err:
            print(f"      order placement  FAIL {err}")
            print("\n→ Order placement is NOT available for this key. Copy trading cannot work on it.")
            print("  Check API-key permissions in MEXC, and that futures trading is enabled after KYC.")
            return 1

        await asyncio.sleep(2)
        try:
            open_now = [p for p in await client.get_open_positions(symbol) if p.hold_vol > 0]
            if open_now:
                p = open_now[0]
                await client.submit_order(
                    symbol=symbol,
                    side=SIDE_CLOSE_LONG,
                    vol=p.hold_vol,
                    open_type=p.open_type,
                    external_oid=f"accesscheck-close-{os.getpid()}",
                )
                print("      cleanup          OK   test position closed")
            else:
                print("      cleanup          nothing open to close")
        except MexcError as err:
            print(f"      cleanup          FAIL {err}  ← CLOSE THIS POSITION MANUALLY")
            return 1

        print("\n→ Everything works. Copy trading is viable on this account.")
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="place and immediately close a real minimum-size order")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help=f"contract to test with (default {DEFAULT_SYMBOL})")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main(args.live, args.symbol)))
