"""How opening actually works for HIBT accounts — the bot's real copy path, end to end.

Account A opens a position the way a person would, by hand. The bot reads A's positions over REST,
turns the change into an event with the same tracker the group poller uses, and hands it to the
same CopyEngine that runs in production, which opens B. Nothing on the way is a stand-in except,
in --dry mode, the exchange itself.

    python scripts/test_hibt_open.py                 # --dry: no keys, no orders; prints what
                                                     #   would be sent to HIBT for account B
    python scripts/test_hibt_open.py --live          # shows the plan and stops
    python scripts/test_hibt_open.py --live --yes    # real orders at the minimum size, then closes

--live needs two accounts in .env, BOTH YOUR OWN:

    HIBT_A_API_KEY / HIBT_A_SECRET     the account opened by hand (the trigger)
    HIBT_B_API_KEY / HIBT_B_SECRET     the account the bot copies onto

It refuses to run if either account already holds the test symbol (close_all closes the whole
symbol, so it would close a real position with it), or if the minimum order is worth more than a
few dollars. A is put in group 1 and B in group 2, as in REVERSE mode, so B should come out on the
OPPOSITE side to A at the same size. --same-group puts both in group 1 instead.

Nothing here prints a secret.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.core.copy_engine import CopyEngine  # noqa: E402
from mexc_copy_bot.core.events import MasterPositionTracker, parse_position  # noqa: E402
from mexc_copy_bot.db.store import DIRECTION_COPY, DIRECTION_REVERSE, FOLLOWER, MASTER, Account  # noqa: E402
from mexc_copy_bot.exchange import EXCHANGE_HIBT, Credentials  # noqa: E402
from mexc_copy_bot.hibt import rest  # noqa: E402
from mexc_copy_bot.hibt.rest import HibtError, HibtRestClient  # noqa: E402
from mexc_copy_bot.mexc.rest import SIDE_OPEN_LONG, SIDE_OPEN_SHORT  # noqa: E402

SYMBOL = "POPMART_USDT"
LEVERAGE = 5
MAX_TEST_NOTIONAL_USD = 5.0
SIDE_NAME = {1: "LONG", 2: "SHORT"}


class Store:
    """Only what CopyEngine asks of the database, kept in memory. Records the bookkeeping the engine
    would write, so the run can show it."""

    def __init__(self, credentials: dict[int, Credentials]) -> None:
        self.credentials = credentials
        self.tasks: dict[int, dict] = {}
        self.positions: dict[tuple[int, str, int], float] = {}
        self.errors: dict[int, str | None] = {}

    async def create_task(self, **kw):
        task_id = len(self.tasks) + 1
        self.tasks[task_id] = kw
        return task_id

    async def finish_task(self, task_id, **kw):
        self.tasks[task_id].update(kw)

    async def get_credentials(self, account_id, owner_id):
        return self.credentials.get(account_id)

    async def set_account_error(self, account_id, error):
        self.errors[account_id] = error

    async def get_positions(self, account_id):
        return {}

    async def upsert_position(self, row):
        self.positions[(row.account_id, row.symbol, row.position_type)] = row.hold_vol

    async def delete_position(self, account_id, symbol, position_type):
        self.positions.pop((account_id, symbol, position_type), None)


def account(account_id: int, label: str, kind: str, group_two: bool) -> Account:
    return Account(
        id=account_id, owner_id=1, label=label, kind=kind, api_key_hint="test",
        size_multiplier=1.0, active=True, position_mode=1, last_error=None,
        direction=DIRECTION_REVERSE if group_two else DIRECTION_COPY,
    )


# ── --dry: the real path against a recorded exchange ─────────────────────────────────────────────
class Recorder:
    """HIBT's API as far as the copy path touches it: accepts, records, and answers like the venue."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict]] = []

    def request(self, method, url, **kwargs):
        body = kwargs.get("json") or kwargs.get("params") or {}
        self.calls.append((method, url.replace(rest.BASE_URL, ""), dict(body)))
        data = {"orderID": "24081233332184101100143203708"} if url.endswith("/order/open") else None
        return _Reply({"code": 0, "msg": "success", "data": data})


class _Reply:
    status = 200

    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def text(self):
        return json.dumps(self._payload)

    async def json(self, content_type=None):
        return self._payload


async def dry(same_group: bool) -> int:
    print("DRY RUN — the bot's real copy path, with HIBT recorded instead of called\n")

    async with aiohttp.ClientSession() as public:
        rules = await rest.load_symbols(public)  # public market data is real
    rule = rules[SYMBOL]
    minimum = float(rule["marketMiniAmount"])
    print(f"{SYMBOL}: minimum {minimum:g}, precision {rule['volumePrecision']} — from the live venue")

    # 1. Account A opens by hand. This is the row HIBT's /v2/account/position returns for it.
    a_row = {"positionID": "900000000000000000000000000001", "symbol": "popmart_usdt", "side": 1,
             "leverage": LEVERAGE, "price": "19.70", "amount": f"{minimum:g}", "margin": "0.04"}
    tracker = MasterPositionTracker()
    tracker.resync([])  # A held nothing before
    event = tracker.apply(parse_position(rest.position_row(a_row)))
    print(f"\n1. A's position as the bot reads it -> event {event.action.value} "
          f"{event.symbol} {SIDE_NAME[event.position_type]} {event.delta_vol:g} @ {event.leverage}x")

    # 2. The engine copies onto B.
    recorder = Recorder()
    credentials = {2: Credentials("key-b", "secret-b", EXCHANGE_HIBT)}
    store = Store(credentials)
    engine = CopyEngine(store, recorder, retry_attempts=1, exchange=EXCHANGE_HIBT)
    a = account(1, "A", MASTER, group_two=False)
    b = account(2, "B", FOLLOWER, group_two=not same_group)

    async def no_block(_event):
        return None
    engine._blocked_contract_reason = no_block  # the check reads public data; not what is under test

    loaded = rules

    async def cached_rules(_session, *, force=False):
        return loaded
    rest.load_symbols = cached_rules

    [result] = await engine.execute_groups(event, event_id=1, accounts=[b], trigger_group=a.group)
    print(f"\n2. engine result for B: ok={result.ok} side={SIDE_NAME.get(result.position_type)} "
          f"vol={result.vol:g} error={result.error}")

    print("\n3. what would reach HIBT for B, in order:")
    for method, path, body in recorder.calls:
        shown = {k: v for k, v in body.items() if k != "timestamp"}
        print(f"   {method} {path}  {shown}")

    expected_side = 1 if same_group else 2
    opens = [body for _, path, body in recorder.calls if path == "/v2/order/open"]
    checks = [
        ("exactly one order was opened", len(opens) == 1),
        (f"B goes {SIDE_NAME[expected_side]} ({'same' if same_group else 'opposite'} group)",
         bool(opens) and opens[0]["side"] == (rest.VENUE_SIDE_BUY if expected_side == 1 else rest.VENUE_SIDE_SELL)),
        ("same size as A", bool(opens) and float(opens[0]["amount"]) == minimum),
        ("A's leverage carried over", bool(opens) and opens[0]["leverage"] == LEVERAGE),
        ("market order", bool(opens) and opens[0]["type"] == rest.VENUE_TYPE_MARKET),
        ("the engine recorded success", result.ok),
    ]
    print()
    for name, passed in checks:
        print(f"  {'ok  ' if passed else 'FAIL'}  {name}")
    return 0 if all(p for _, p in checks) else 1


# ── --live: real orders, minimum size ────────────────────────────────────────────────────────────
async def live(yes: bool, same_group: bool) -> int:
    load_dotenv()
    keys = {name: (os.getenv(f"HIBT_{name}_API_KEY", "").strip(), os.getenv(f"HIBT_{name}_SECRET", "").strip())
            for name in ("A", "B")}
    missing = [name for name, (k, s) in keys.items() if not k or not s]
    if missing:
        print(f"Missing in .env: {', '.join(f'HIBT_{n}_API_KEY / HIBT_{n}_SECRET' for n in missing)}")
        return 2
    if keys["A"][0] == keys["B"][0]:
        print("A and B are the same key. The test needs two different accounts.")
        return 2

    async with aiohttp.ClientSession() as session:
        a_client = HibtRestClient(*keys["A"], session=session)
        b_client = HibtRestClient(*keys["B"], session=session)
        print(f"A = key …{keys['A'][0][-4:]}   B = key …{keys['B'][0][-4:]}   — both must be YOUR accounts\n")

        for name, client in (("A", a_client), ("B", b_client)):
            try:
                snapshot = await client.get_usdt_snapshot()
            except HibtError as err:
                print(f"{name}: could not read the account — [{err.code}] {err.message}")
                return 1
            held = [p for p in await client.get_open_positions(SYMBOL) if p.hold_vol > 0]
            print(f"{name}: openable ${snapshot.openable:,.2f}, holds {SYMBOL}: {bool(held)}")
            if held:
                print(f"   {name} already holds {SYMBOL}; close_all would close it too. Stopping.")
                return 1

        rule = (await rest.load_symbols(session))[SYMBOL]
        minimum = float(rule["marketMiniAmount"])
        price = await rest.get_ticker_price(session, SYMBOL)
        if minimum * price > MAX_TEST_NOTIONAL_USD:
            print(f"minimum order is ~${minimum * price:,.2f}; refusing above ${MAX_TEST_NOTIONAL_USD:g}")
            return 1

        expected = 1 if same_group else 2
        print(f"\nplan: A opens LONG {minimum:g} {SYMBOL} (~${minimum * price:,.2f}) at {LEVERAGE}x by hand;")
        print(f"      the bot copies onto B, which should go {SIDE_NAME[expected]}; then both are closed.")
        if not yes:
            print("add --yes to place these orders")
            return 0

        failures = 0
        try:
            # 1. A opens, as a person would in the app.
            await a_client.submit_order(symbol=SYMBOL, side=SIDE_OPEN_LONG, vol=minimum, leverage=LEVERAGE,
                                        external_oid=f"testA{os.getpid()}")
            await asyncio.sleep(2.0)

            # 2. The bot reads A exactly as the group poller does.
            tracker = MasterPositionTracker()
            tracker.resync([])
            events = [tracker.apply(s) for s in (parse_position(r) for r in await a_client.get_open_positions_raw())
                      if s and s.symbol == SYMBOL and s.hold_vol > 0]
            events = [e for e in events if e]
            if not events:
                print("FAIL  A's position did not show up as an event")
                return 1
            event = events[0]
            print(f"\n1. read A: {event.action.value} {SIDE_NAME[event.position_type]} {event.delta_vol:g} @ {event.leverage}x")
            if event.position_type != 1:
                print("FAIL  A opened with side=1 but the venue shows it as a SHORT — side mapping is wrong")
                failures += 1

            # 3. The production engine copies onto B.
            store = Store({2: Credentials(*keys["B"], EXCHANGE_HIBT)})
            engine = CopyEngine(store, session, retry_attempts=3, exchange=EXCHANGE_HIBT)
            a = account(1, "A", MASTER, group_two=False)
            b = account(2, "B", FOLLOWER, group_two=not same_group)
            started = asyncio.get_running_loop().time()
            [result] = await engine.execute_groups(event, event_id=1, accounts=[b], trigger_group=a.group)
            took = asyncio.get_running_loop().time() - started
            print(f"2. engine -> B: ok={result.ok} in {took:.2f}s  error={result.error}")
            if not result.ok:
                failures += 1

            # 4. What B really holds now.
            await asyncio.sleep(2.0)
            b_positions = [p for p in await b_client.get_open_positions(SYMBOL) if p.hold_vol > 0]
            shown = [(SIDE_NAME.get(p.position_type), p.hold_vol, p.leverage) for p in b_positions]
            print(f"3. B holds: {shown}")
            good = (len(b_positions) == 1 and b_positions[0].position_type == expected
                    and abs(b_positions[0].hold_vol - event.delta_vol) < 1e-9)
            print(f"   {'ok  ' if good else 'FAIL'}  B is {SIDE_NAME[expected]} {event.delta_vol:g}, as the group rule says")
            failures += 0 if good else 1
        finally:
            # Always close, whatever went wrong above.
            print("\n4. closing both")
            for name, client in (("A", a_client), ("B", b_client)):
                try:
                    if [p for p in await client.get_open_positions(SYMBOL) if p.hold_vol > 0]:
                        await client.close_all(SYMBOL)
                    await asyncio.sleep(1.5)
                    left = [p for p in await client.get_open_positions(SYMBOL) if p.hold_vol > 0]
                    print(f"   {name}: {'CLOSED' if not left else 'STILL OPEN — CLOSE IT IN THE APP'}")
                    failures += 1 if left else 0
                except HibtError as err:
                    print(f"   {name}: close failed [{err.code}] {err.message} — CLOSE IT IN THE APP")
                    failures += 1

    print("\nall good" if failures == 0 else f"\n{failures} problem(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--live", action="store_true", help="place real minimum-size orders")
    parser.add_argument("--yes", action="store_true", help="confirm --live")
    parser.add_argument("--same-group", action="store_true", help="put B in A's group (same side)")
    args = parser.parse_args()
    runner = live(args.yes, args.same_group) if args.live else dry(args.same_group)
    sys.exit(asyncio.run(runner))
