"""Run a scheduled hedged laddered entry on two HIBT accounts, from a JSON config.

    python scripts/run_ladder.py config.json                 # plan only, sends nothing
    python scripts/run_ladder.py config.json --arm           # actually fires at the set time

Config (JSON):
    {
      "long_account": 35, "short_account": 36,     account ids in the bot's database
      "symbol": "XRP_USDT",
      "leverage": 5,
      "margin_usd": 1.0,                             collateral per account; size = margin × leverage
      "parts": 4,
      "step_seconds": 1.0,
      "target": "2026-09-15 21:30:00",              local time the position must be FULLY open,
      "target_in_seconds": 60                        OR this many seconds from now (overrides target)
    }

Without --arm it prints the plan — slice sizes, the exact send times, the margin each account needs
— and stops. With --arm it presets leverage, waits, fires, and while it runs it watches both
accounts over the websocket and reports what actually opened.

Only run it on YOUR OWN accounts. It reads their keys from the bot's database, never from the CLI.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime

import aiohttp
import asyncpg
from dotenv import load_dotenv

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.core.ladder import LadderConfig, LadderExecutor, fmt_time, plan_ladder  # noqa: E402
from mexc_copy_bot.exchange import Credentials  # noqa: E402
from mexc_copy_bot.hibt import rest  # noqa: E402
from mexc_copy_bot.hibt.rest import HibtRestClient  # noqa: E402
from mexc_copy_bot.hibt.websocket import HibtUserStream  # noqa: E402
from mexc_copy_bot.security.encryption import CredentialCipher  # noqa: E402


async def load_account(conn, cipher, account_id: int) -> tuple[Credentials, str]:
    row = await conn.fetchrow(
        "SELECT a.api_key_enc, a.api_secret_enc, a.api_key_hint, a.label, f.exchange"
        " FROM copy_accounts a JOIN copy_folders f ON f.id = a.folder_id WHERE a.id = $1", account_id)
    if not row:
        raise SystemExit(f"account {account_id} not found")
    if row["exchange"] != "hibt":
        raise SystemExit(f"account {account_id} is {row['exchange']}, not HIBT")
    creds = Credentials(cipher.decrypt(row["api_key_enc"]), cipher.decrypt(row["api_secret_enc"]), "hibt")
    return creds, f"{row['label']} …{row['api_key_hint']}"


async def measure_latency(client) -> float:
    best = 1.0
    for _ in range(5):
        t = time.perf_counter()
        await client._request("GET", "/v2/account/position", {"symbol": "xrp_usdt"})
        best = min(best, time.perf_counter() - t)
    return best


async def held(client, symbol) -> float:
    return sum(p.hold_vol for p in await client.get_open_positions(symbol) if p.hold_vol > 0)


async def main(config_path: str, arm: bool) -> int:
    load_dotenv("d:/Telegram-Signal-L2KDJ/.env")
    cfg = json.loads(open(config_path, encoding="utf-8").read())
    cipher = CredentialCipher(os.environ["COPY_BOT_ENCRYPTION_KEY"])
    conn = await asyncpg.connect(os.environ["COPY_BOT_DATABASE_URL"])
    try:
        long_creds, long_label = await load_account(conn, cipher, cfg["long_account"])
        short_creds, short_label = await load_account(conn, cipher, cfg["short_account"])
    finally:
        await conn.close()

    symbol = cfg["symbol"].upper()
    if "target_in_seconds" in cfg:
        target_epoch = time.time() + float(cfg["target_in_seconds"])
    else:
        target_epoch = datetime.strptime(cfg["target"], "%Y-%m-%d %H:%M:%S").timestamp()

    async with aiohttp.ClientSession() as session:
        long_client = HibtRestClient(*long_creds[:2], session=session)
        short_client = HibtRestClient(*short_creds[:2], session=session)

        rules = (await rest.load_symbols(session)).get(symbol)
        if not rules:
            raise SystemExit(f"{symbol} is not listed on HIBT")
        price = await rest.get_ticker_price(session, symbol)
        latency, long_snap, short_snap = await asyncio.gather(
            measure_latency(long_client), long_client.get_usdt_snapshot(), short_client.get_usdt_snapshot(),
        )

        config = LadderConfig(
            symbol=symbol, leverage=int(cfg["leverage"]), margin_usd=float(cfg["margin_usd"]),
            parts=int(cfg["parts"]), step_seconds=float(cfg["step_seconds"]), target_epoch=target_epoch,
        )
        plan = plan_ladder(
            config, price=price, size_precision=rest.size_precision(rules),
            min_order=float(rules.get("marketMiniAmount") or 0), latency_seconds=latency,
            available=[long_snap.openable, short_snap.openable], now=time.time(),
        )

        print(f"LONG  {long_label}: free ${long_snap.openable:.2f}")
        print(f"SHORT {short_label}: free ${short_snap.openable:.2f}")
        print(f"{symbol} price {price}, measured latency {latency*1000:.0f}ms")
        print(f"size ${plan.target_notional:,.0f}/account at {config.leverage}x = {plan.total_amount} "
              f"{symbol} each, in {config.parts} slices")
        print(f"position must be full by T = {fmt_time(target_epoch)}")
        for s in plan.slices:
            print(f"   slice {s.index}: {s.amount} at {fmt_time(s.send_epoch)} "
                  f"(T−{target_epoch - s.send_epoch:.1f}s)")
        for w in plan.warnings:
            print(f"  ! {w}")
        for e in plan.errors:
            print(f"  ✗ {e}")
        if not plan.ok:
            return 1
        if not arm:
            print("\nplan only. Re-run with --arm to fire at T.")
            return 0

        for label, client in ((long_label, long_client), (short_label, short_client)):
            if await held(client, symbol) > 0:
                raise SystemExit(f"{label} already holds {symbol}; refusing to add to it")

        # Watch both accounts over the websocket while the ladder runs.
        # Each user.position frame is a full snapshot; key by positionID so a transient pre-merge
        # snapshot of two positions is not summed as double the real size.
        latest = {"long": {}, "short": {}}
        def watcher(bucket):
            def on_frame(topic, frame):
                if topic == "user.position":
                    latest[bucket] = {d["positionID"]: float(d.get("amount", 0))
                                      for d in frame.get("data", [])
                                      if str(d.get("symbol", "")).upper() == symbol}
            return on_frame
        long_stream = HibtUserStream(*long_creds[:2], session=session, on_frame=watcher("long"))
        short_stream = HibtUserStream(*short_creds[:2], session=session, on_frame=watcher("short"))
        long_stream.start(); short_stream.start()
        await asyncio.gather(long_stream.wait_connected(8), short_stream.wait_connected(8))
        print("\nwebsocket watching both accounts; arming.")

        executor = LadderExecutor([(cfg['long_account'], long_client)], [(cfg['short_account'], short_client)],
                                  symbol=symbol, leverage=config.leverage)
        await executor.prepare()
        report = await executor.run(plan)

        await asyncio.sleep(1.5)  # let the last fills settle and the socket report them
        report.final_long, report.final_short = f"{await held(long_client, symbol):g}", f"{await held(short_client, symbol):g}"
        await long_stream.stop(); await short_stream.stop()

        print("\n" + report.summary())
        ws_long = sum(latest["long"].values())
        ws_short = sum(latest["short"].values())
        print(f"websocket saw: long {ws_long:g}, short {ws_short:g}")
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config")
    parser.add_argument("--arm", action="store_true", help="actually fire at the target time")
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.config, args.arm)))
