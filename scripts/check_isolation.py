"""Verify that one owner cannot see or touch another owner's accounts.

This is the check that matters most after making the bot multi-tenant: two people share one bot
process and one database, and the only thing standing between them is that every query carries an
owner id. A regression here would be silent — the UI would look right while one brother's
Emergency Stop closed the other's positions.

Runs against the real database using COPY_BOT_DATABASE_URL, creates two throwaway owners with
obviously fake credentials, asserts the isolation properties, and deletes everything it made.

    python scripts/check_isolation.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.stdout.reconfigure(encoding="utf-8")

from mexc_copy_bot.config import Settings  # noqa: E402
from mexc_copy_bot.db.store import FOLLOWER, MASTER, Store  # noqa: E402
from mexc_copy_bot.security.encryption import CredentialCipher  # noqa: E402

# Ids far outside Telegram's real range, so a bug in the cleanup cannot touch a live account.
OWNER_A = -9001
OWNER_B = -9002

checks = 0
failures: list[str] = []


def check(label: str, condition: bool) -> None:
    global checks
    checks += 1
    print(f"  {'OK  ' if condition else 'FAIL'}  {label}")
    if not condition:
        failures.append(label)


async def main() -> int:
    settings = Settings.from_env()
    store = await Store.connect(settings.database_url, CredentialCipher(settings.encryption_key))

    try:
        await cleanup(store)

        print("\nsetup: each owner adds their own master + follower")
        a_master = await store.add_account(
            owner_id=OWNER_A, label="Master", kind=MASTER,
            api_key="AAAA-key-1111", api_secret="AAAA-secret", position_mode=1,
        )
        a_follower = await store.add_account(
            owner_id=OWNER_A, label="Follower #1", kind=FOLLOWER,
            api_key="AAAA-key-2222", api_secret="AAAA-secret", position_mode=1,
        )
        b_master = await store.add_account(
            owner_id=OWNER_B, label="Master", kind=MASTER,
            api_key="BBBB-key-3333", api_secret="BBBB-secret", position_mode=1,
        )
        print(f"  owner A: master={a_master} follower={a_follower}")
        print(f"  owner B: master={b_master}")

        print("\ntwo masters can coexist (the old schema allowed only one, globally)")
        check("A has a master", (await store.get_master(OWNER_A)) is not None)
        check("B has a master", (await store.get_master(OWNER_B)) is not None)
        check("they are different rows", a_master != b_master)

        print("\none master per owner is still enforced")
        try:
            await store.add_account(
                owner_id=OWNER_A, label="Master 2", kind=MASTER,
                api_key="AAAA-key-4444", api_secret="AAAA-secret", position_mode=1,
            )
            check("A cannot add a second master", False)
        except Exception:
            check("A cannot add a second master", True)

        print("\nlisting shows only your own accounts")
        a_all = await store.list_accounts(OWNER_A)
        b_all = await store.list_accounts(OWNER_B)
        check("A sees exactly 2", len(a_all) == 2)
        check("A does not see B's master", b_master not in {a.id for a in a_all})
        check("B sees exactly 1", len(b_all) == 1)
        check("B does not see A's accounts", not ({a_master, a_follower} & {a.id for a in b_all}))

        print("\ncredentials cannot be read across owners")
        check("A reads its own key", (await store.get_credentials(a_master, OWNER_A)) is not None)
        check("B cannot read A's key", (await store.get_credentials(a_master, OWNER_B)) is None)

        print("\nmutations cannot cross owners")
        check("B cannot delete A's follower", (await store.remove_account(a_follower, OWNER_B)) is False)
        check("A's follower still exists", len(await store.list_accounts(OWNER_A)) == 2)

        await store.set_size_multiplier(a_follower, OWNER_B, 0.5)
        untouched = [a for a in await store.list_accounts(OWNER_A) if a.id == a_follower][0]
        check("B cannot resize A's follower", untouched.size_multiplier == 1.0)

        await store.set_account_active(a_follower, OWNER_B, False)
        untouched = [a for a in await store.list_accounts(OWNER_A) if a.id == a_follower][0]
        check("B cannot deactivate A's follower", untouched.active is True)

        print("\nrun state is per owner")
        await store.set_running(OWNER_A, True)
        check("A is running", await store.is_running(OWNER_A) is True)
        check("B is not", await store.is_running(OWNER_B) is False)
        check("only A is resumed on boot", OWNER_A in await store.running_owners()
              and OWNER_B not in await store.running_owners())
        await store.set_running(OWNER_A, False)
        check("A can stop without touching B", await store.is_running(OWNER_A) is False)

        print("\nevent history is per owner")
        common = dict(
            symbol="ADA_USDT", position_type=1, action="OPEN",
            master_vol=1.0, delta_vol=1.0, leverage=20, open_type=2, raw=None,
        )
        # Same dedupe key for both: it used to be globally unique, so B's event would have been
        # silently swallowed as a duplicate of A's.
        ev_a = await store.record_event(owner_id=OWNER_A, dedupe_key="shared-key", **common)
        ev_b = await store.record_event(owner_id=OWNER_B, dedupe_key="shared-key", **common)
        check("A's event recorded", ev_a is not None)
        check("B's identical event is NOT swallowed", ev_b is not None)
        dup = await store.record_event(owner_id=OWNER_A, dedupe_key="shared-key", **common)
        check("A's own replay is still deduped", dup is None)
        check("A's history has 1 entry", len(await store.recent_events(OWNER_A)) == 1)
        check("B's history has 1 entry", len(await store.recent_events(OWNER_B)) == 1)

        print("\nowners are discoverable for the service registry")
        owners = await store.list_owners()
        check("both owners listed", OWNER_A in owners and OWNER_B in owners)

    finally:
        await cleanup(store)
        await store.close()

    print(f"\n{checks - len(failures)}/{checks} checks passed")
    for f in failures:
        print(f"  FAILED: {f}")
    return 1 if failures else 0


async def cleanup(store: Store) -> None:
    async with store._pool.acquire() as conn:  # noqa: SLF001 — a test script, not production code
        await conn.execute("DELETE FROM copy_master_events WHERE owner_id = ANY($1)", [OWNER_A, OWNER_B])
        await conn.execute("DELETE FROM copy_accounts WHERE owner_id = ANY($1)", [OWNER_A, OWNER_B])
        await conn.execute("DELETE FROM copy_state WHERE owner_id = ANY($1)", [OWNER_A, OWNER_B])


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
