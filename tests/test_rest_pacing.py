"""How outbound calls are paced, and when they are signed.

Both facts here were learned by measuring a live account, not from the documentation:

  · The allowance belongs to the API KEY, not the address. Measured on ten live accounts:
    one account sending 40 requests at 17/s had 17 refused, while ten accounts sending 7/s each —
    47/s between them — had none. A single queue for the whole process therefore divided one
    account's budget among all of them, and with ten followers each got 0.7 requests a second.

  · A signature carries a timestamp the venue checks. Slowing the pacing down made the queue long
    enough that requests signed on arrival went out already stale — 120 queued calls produced 48
    "Confirming signature failed". The wait has to come first, and the clock read after it.

The second only appeared because of the first fix, which is the reason it is pinned here: nothing
about the code makes the order look load-bearing.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.mexc import rest  # noqa: E402


def test_private_pacing_stays_under_what_one_account_allows():
    """A single account started refusing above ~13/s. Anything at or above that is a regression."""
    rate = 1.0 / rest.PRIVATE_INTERVAL
    assert rate <= 9.0, f"pacing {rate:.1f}/s leaves no room under the ~13/s one account allows"


def test_every_account_gets_its_own_queue():
    """The bug this replaced: one queue for the process meant ten accounts shared one account's
    allowance, and an order that should take half a second took six."""
    a, b = rest.throttle_for("KEY-A"), rest.throttle_for("KEY-B")
    assert a is not b
    assert rest.throttle_for("KEY-A") is a, "the same account must keep the same queue"


def test_the_queues_are_not_keyed_by_the_key_itself():
    """No process-wide dictionary of live API keys."""
    rest.throttle_for("SECRET-LOOKING-KEY")
    assert "SECRET-LOOKING-KEY" not in rest._ACCOUNT_THROTTLES


def test_public_data_is_not_held_to_the_private_limit():
    """Market data carries no key to attribute it to, so it really is per process — and it is on a
    far looser allowance, 30/s having gone through untouched."""
    assert rest.PUBLIC_THROTTLE._min_interval < rest.PRIVATE_INTERVAL


def test_the_signature_is_taken_after_the_wait_not_before():
    """The regression this guards: sign, then queue, and the stamp is old by the time it is sent."""
    order: list[str] = []

    class SlowSlot:
        _min_interval = 0.05

        def slot(self):
            import contextlib

            @contextlib.asynccontextmanager
            async def cm():
                await asyncio.sleep(0.05)
                order.append("waited")
                yield

            return cm()

    class Response:
        status = 200

        async def text(self):
            return "{}"

        async def json(self, content_type=None):
            return {"success": True, "data": []}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    class Session:
        def request(self, *args, **kwargs):
            return Response()

    def spy_sign(*args, **kwargs):
        order.append("signed")
        return {}

    original_for, original_sign = rest.throttle_for, rest.sign_rest
    rest.throttle_for, rest.sign_rest = (lambda _key: SlowSlot()), spy_sign
    try:
        client = rest.MexcRestClient("k", "s", session=Session())
        asyncio.run(client._request_once("GET", "/private/position/open_positions"))
    finally:
        rest.throttle_for, rest.sign_rest = original_for, original_sign

    assert order == ["waited", "signed"], f"signed before waiting: {order}"


def test_a_refused_request_is_retried_rather_than_reported_as_a_failure():
    """A rate limit is transient. Surfacing it as an error meant a read returned "nothing", which
    downstream is indistinguishable from an account that genuinely holds nothing."""
    attempts = {"n": 0}

    async def flaky(self, method, path, *, params=None, body=None):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise rest.MexcError(510, "Requests are too frequent, please try again later", endpoint=path)
        return []

    original_once, original_backoff = rest.MexcRestClient._request_once, rest.RATE_LIMIT_BACKOFF
    rest.MexcRestClient._request_once = flaky
    rest.RATE_LIMIT_BACKOFF = (0.0, 0.0, 0.0)
    try:
        client = rest.MexcRestClient("k", "s", session=object())
        assert asyncio.run(client._request("GET", "/x")) == []
        assert attempts["n"] == 3
    finally:
        rest.MexcRestClient._request_once = original_once
        rest.RATE_LIMIT_BACKOFF = original_backoff


def test_retrying_gives_up_rather_than_looping_forever():
    async def always_refused(self, method, path, *, params=None, body=None):
        raise rest.MexcError(510, "Requests are too frequent", endpoint=path)

    original_once, original_backoff = rest.MexcRestClient._request_once, rest.RATE_LIMIT_BACKOFF
    rest.MexcRestClient._request_once = always_refused
    rest.RATE_LIMIT_BACKOFF = (0.0, 0.0)
    try:
        client = rest.MexcRestClient("k", "s", session=object())
        try:
            asyncio.run(client._request("GET", "/x"))
        except rest.MexcError as err:
            assert err.is_rate_limited
        else:
            raise AssertionError("a permanently refused request must surface, not hang")
    finally:
        rest.MexcRestClient._request_once = original_once
        rest.RATE_LIMIT_BACKOFF = original_backoff
