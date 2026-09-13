"""Settle, on a live HIBT key, everything the client could only take from the documentation.

hibt/rest.py marks each such assumption UNVERIFIED. This asks the venue directly, in order:

  1. public market data            — the host answers from this machine
  2. read balance                  — the SIGNATURE is accepted (220008 means auth.py is wrong)
  3. balance, raw beside parsed    — which figure `balance` really is; compare with the app
  4. open positions, raw + parsed  — field names and sides as the bot will read them
  5. finished orders               — whether `profit` already has the fee taken out
  6. OPTIONALLY (--rate) measure private-read pacing, which HIBT does not publish
  7. OPTIONALLY (--live) a real round trip at the MINIMUM size: open LONG, confirm it is a long,
     close; open SHORT, confirm it is a short, close. The side check is the one that matters —
     on MEXC the documented side numbers were wrong, and only a live order showed it.

Usage (from the repo root, with HIBT_API_KEY and HIBT_SECRET in .env):

    python scripts/check_hibt_access.py
    python scripts/check_hibt_access.py --rate
    python scripts/check_hibt_access.py --live --yes          # real orders, minimum size

Run it on YOUR OWN account only. Nothing here prints the secret.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.hibt import rest  # noqa: E402
from mexc_copy_bot.hibt.rest import HibtError, HibtRestClient  # noqa: E402
from mexc_copy_bot.mexc.rest import SIDE_OPEN_LONG, SIDE_OPEN_SHORT  # noqa: E402

# The cheapest liquid contract found on the venue: minimum order 0.01 at ~$20, so a round trip
# risks cents, and $1.9M of depth within 1% means a market order fills at once.
DEFAULT_SYMBOL = "POPMART_USDT"
# The --live test refuses any contract whose minimum order is worth more than this.
MAX_TEST_NOTIONAL_USD = 5.0

# What a failure code means for the person running this, in the order they are likely to meet them.
MEANING = {
    220003: "no key was sent — HIBT_API_KEY is empty",
    220004: "the venue does not know this key — check HIBT_API_KEY (or the IP whitelist)",
    220005: "no signature was sent",
    220008: "SIGNATURE REJECTED — the signing scheme in hibt/auth.py does not match the venue",
    220002: "timestamp expired — this machine's clock is off",
    220014: "the key has no trading permission — enable Trade on it in the app",
    1103: "this IP is not on the key's whitelist — add it in the app",
    2202: "this IP is disabled for the key",
}


def ok(text: str) -> None:
    print(f"  ok    {text}")


def bad(text: str) -> None:
    print(f" FAIL  {text}")


def explain(err: HibtError) -> str:
    hint = MEANING.get(err.code)
    return f"[{err.code}] {err.message}" + (f"\n        -> {hint}" if hint else "")


async def raw_get(client: HibtRestClient, path: str, params: dict | None = None):
    """The venue's own reply, before the client translates it — which is what this is here to see."""
    return await client._request("GET", path, params)


async def main(live: bool, yes: bool, rate: bool, symbol: str) -> int:
    load_dotenv()
    key, secret = os.getenv("HIBT_API_KEY", "").strip(), os.getenv("HIBT_SECRET", "").strip()
    if not key or not secret:
        print("Put HIBT_API_KEY and HIBT_SECRET in .env (from the HIBT mobile app → API management).")
        return 2
    print(f"HIBT key …{key[-4:]}  —  make sure this is YOUR account.\n")

    failures = 0
    async with aiohttp.ClientSession() as session:
        client = HibtRestClient(key, secret, session=session)

        print("1. public market data")
        try:
            rules = await rest.load_symbols(session)
            ok(f"{len(rules)} contracts")
        except (HibtError, aiohttp.ClientError, asyncio.TimeoutError) as err:
            bad(f"host did not answer: {err}")
            return 1

        print("\n2. signature (a private read)")
        try:
            balance_raw = await raw_get(client, "/v2/account/balance")
            ok("accepted — the signing scheme matches")
        except HibtError as err:
            bad(explain(err))
            return 1

        print("\n3. balance: the venue's fields, then what the bot makes of them")
        print("   raw:", json.dumps(balance_raw, ensure_ascii=False)[:600])
        snap = await client.get_usdt_snapshot()
        print(f"   bot: equity ${snap.equity:,.2f} · openable ${snap.openable:,.2f} · "
              f"in positions ${snap.position_margin:,.2f} · frozen ${snap.frozen:,.2f} · bonus ${snap.bonus:,.2f}")
        print("   -> compare 'openable' with the available balance the APP shows. If they differ while")
        print("      positions are open, `balance` is the total, not the free part (hibt/rest.py).")

        print("\n4. open positions")
        positions_raw = await raw_get(client, "/v2/account/position")
        rows = rest._rows(positions_raw)
        if not rows:
            ok("none open (open one by hand in the app and run again to check the fields)")
        for row in rows[:5]:
            print("   raw:", json.dumps(row, ensure_ascii=False)[:400])
            parsed = rest.position_row(row)
            side = {1: "LONG", 2: "SHORT"}.get(parsed["positionType"], "?")
            print(f"   bot: {parsed['symbol']} {side} {parsed['holdVol']:g} @ {parsed['leverage']}x "
                  f"id …{str(parsed['positionId'])[-6:]}")

        print("\n5. finished orders (how PnL will be read)")
        try:
            finished = rest._rows(await raw_get(client, "/v2/order/finished"))
            ok(f"{len(finished)} row(s) on the default page")
            for row in finished[:3]:
                print("   raw:", {k: row.get(k) for k in ("symbol", "side", "action", "state", "amount",
                                                         "filledAmount", "profit", "fee", "positionID")})
            closed = rest.closed_rows(finished)
            if closed:
                c = closed[0]
                print(f"   bot: last closed {c['symbol']} gross {c['realisedGross']:+.4f} fees {c['fee']:.4f} "
                      f"-> reported {c['realised']:+.4f}")
                print("   -> compare 'reported' with the realised PnL the APP shows for that position.")
                print("      Off by exactly the fee means `profit` is already net (hibt/rest.py closed_rows).")
        except HibtError as err:
            bad(f"could not read finished orders: {explain(err)}")
            failures += 1

        if rate:
            failures += await measure_rate(client)

        if live:
            failures += await live_round_trip(client, session, symbol, yes)

    print("\nall checks passed" if failures == 0 else f"\n{failures} problem(s)")
    return 1 if failures else 0


async def measure_rate(client: HibtRestClient) -> int:
    """How fast private reads may go before the venue refuses. HIBT publishes no figure."""
    print("\n6. private read pacing (40 balance reads per interval)")
    original = rest.RATE_LIMIT_BACKOFF
    rest.RATE_LIMIT_BACKOFF = ()  # count refusals instead of quietly retrying them
    try:
        for interval in (0.25, 0.14, 0.08, 0.04):
            refused = 0
            started = time.monotonic()
            for _ in range(40):
                try:
                    await client._request_once("GET", "/v2/account/balance", None)
                except HibtError as err:
                    if err.is_rate_limited:
                        refused += 1
                await asyncio.sleep(interval)
            rate_per_s = 40 / (time.monotonic() - started)
            print(f"   every {interval:.2f}s ({rate_per_s:4.1f}/s)  ->  {refused} refused")
            if refused:
                await asyncio.sleep(5)
    finally:
        rest.RATE_LIMIT_BACKOFF = original
    print("   -> PRIVATE_INTERVAL in hibt/rest.py should sit below the fastest clean row.")
    return 0


async def live_round_trip(client: HibtRestClient, session: aiohttp.ClientSession, symbol: str, yes: bool) -> int:
    print(f"\n7. LIVE round trip on {symbol}")
    rules = (await rest.load_symbols(session)).get(symbol.upper())
    if not rules:
        bad(f"{symbol} is not listed")
        return 1
    minimum = float(rules.get("marketMiniAmount") or 0)
    price = await rest.get_ticker_price(session, symbol)
    notional = minimum * price
    if not minimum or not price or notional > MAX_TEST_NOTIONAL_USD:
        bad(f"minimum order on {symbol} is ~${notional:,.2f}; refusing anything above ${MAX_TEST_NOTIONAL_USD:g}")
        return 1

    # close_all closes the WHOLE symbol. On an account already holding it, this test would close a
    # real position along with its own — so it does not start.
    held = [p for p in await client.get_open_positions(symbol) if p.hold_vol > 0]
    if held:
        bad(f"this account already holds {symbol}; the test would close that too. Pick another --symbol.")
        return 1

    print(f"   will open and close {minimum:g} {symbol} (~${notional:,.2f}) twice: LONG, then SHORT, at 1x")
    if not yes:
        print("   add --yes to actually place these orders")
        return 0

    failures = 0
    for side, expected, name in ((SIDE_OPEN_LONG, 1, "LONG"), (SIDE_OPEN_SHORT, 2, "SHORT")):
        try:
            result = await client.submit_order(symbol=symbol, side=side, vol=minimum, leverage=1,
                                               external_oid=f"chk{name[0]}{int(time.time())}")
            ok(f"{name} order accepted: {result}")
        except HibtError as err:
            bad(f"{name} order refused: {explain(err)}")
            return failures + 1

        await asyncio.sleep(1.5)
        mine = [p for p in await client.get_open_positions(symbol) if p.hold_vol > 0]
        types = sorted({p.position_type for p in mine})
        if types == [expected]:
            ok(f"the position is a {name}, as intended — side mapping confirmed")
        else:
            bad(f"expected a {name}, the venue shows position types {types} — SIDE MAPPING IS WRONG")
            failures += 1

        try:
            await client.close_all(symbol)
        except HibtError as err:
            bad(f"close_all refused: {explain(err)} — CLOSE {symbol} BY HAND IN THE APP")
            return failures + 1

        await asyncio.sleep(1.5)
        left = [p for p in await client.get_open_positions(symbol) if p.hold_vol > 0]
        if left:
            bad(f"still holding {symbol} after close_all — CLOSE IT BY HAND IN THE APP")
            return failures + 1
        ok(f"{name} closed")

        closed = await client.get_closed_positions(symbol)
        if closed:
            ok(f"settled: realised {closed[0].realised:+.4f} (compare with the app)")
        else:
            print("   ??    no settled row yet — the PnL lookup may need more time or other parameters")
    return failures


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--live", action="store_true", help="place real minimum-size orders")
    parser.add_argument("--yes", action="store_true", help="confirm --live")
    parser.add_argument("--rate", action="store_true", help="measure private read pacing")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.live, args.yes, args.rate, args.symbol)))
