"""How outbound calls are paced, and when they are signed.

Both facts here were learned by measuring a live account, not from the documentation:

  · Private endpoints refuse above roughly 13 requests a second from one IP, whatever mix of
    accounts they belong to. The pacing had been set to 16.6/s, so a lone follower on an
    otherwise quiet account could still be told its requests were too frequent.

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


def test_private_pacing_stays_under_the_measured_limit():
    """13/s is where the live account began refusing. Anything at or above it is a regression."""
    rate = 1.0 / rest.PRIVATE_THROTTLE._min_interval
    assert rate <= 9.0, f"pacing {rate:.1f}/s leaves no room under the ~13/s the venue allows"


def test_public_data_is_not_held_to_the_private_limit():
    """Market data is on a far looser allowance — 30/s went through untouched — and must not be
    made to queue behind the trading path."""
    assert rest.PUBLIC_THROTTLE._min_interval < rest.PRIVATE_THROTTLE._min_interval


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

    original_throttle, original_sign = rest.PRIVATE_THROTTLE, rest.sign_rest
    rest.PRIVATE_THROTTLE, rest.sign_rest = SlowSlot(), spy_sign
    try:
        client = rest.MexcRestClient("k", "s", session=Session())
        asyncio.run(client._request_once("GET", "/private/position/open_positions"))
    finally:
        rest.PRIVATE_THROTTLE, rest.sign_rest = original_throttle, original_sign

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
