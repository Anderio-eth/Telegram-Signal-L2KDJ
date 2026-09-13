"""HIBT as a second venue: the translation, the guards, and the rules that keep venues apart.

Everything here runs without a network and without a key. What it pins down is the part that can be
wrong on our side — how HIBT rows become the MEXC shapes the rest of the bot reads, what an order is
refused for before it is ever sent, and that no MEXC client can be built for a HIBT account. What
only a live key can settle (the signature being accepted, balance semantics, private rate limits)
is marked UNVERIFIED in hibt/rest.py instead.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import re
from pathlib import Path

import pytest

from mexc_copy_bot.core.events import Action, MasterEvent, MasterPositionTracker, parse_position
from mexc_copy_bot.exchange import (
    EXCHANGE_HIBT,
    EXCHANGE_MEXC,
    Credentials,
    exchange_of,
    make_rest_client,
    normalize,
)
from mexc_copy_bot.hibt import auth, rest
from mexc_copy_bot.hibt.rest import HibtError, HibtRestClient
from mexc_copy_bot.mexc.rest import (
    ORDER_TYPE_MARKET,
    SIDE_CLOSE_LONG,
    SIDE_CLOSE_SHORT,
    SIDE_OPEN_LONG,
    SIDE_OPEN_SHORT,
    MexcError,
    MexcRestClient,
)

# A real-shaped HIBT id: 29 digits, more than a BIGINT holds.
LONG_ID = "24081233332184101100143203708"


# ── signing ──────────────────────────────────────────────────────────────────────────────────────
def test_the_signed_string_is_sorted_and_skips_unset_values():
    params = {"symbol": "xag_usdt", "side": 1, "amount": "5", "price": None, "timestamp": 1724916869475}
    assert auth.signing_string(params) == "amount=5&side=1&symbol=xag_usdt&timestamp=1724916869475"


def test_booleans_are_signed_the_way_json_sends_them():
    # Python would write True; the JSON body carries true. A mismatch is a "bad signature".
    assert auth.signing_string({"isSetSl": True, "isSetSp": False, "timestamp": 1}) == (
        "isSetSl=true&isSetSp=false&timestamp=1"
    )


def test_the_signature_is_hmac_sha256_hex_of_that_string():
    params = {"symbol": "btc_usdt", "timestamp": 1724916869475}
    expected = hmac.new(b"secret", b"symbol=btc_usdt&timestamp=1724916869475", hashlib.sha256).hexdigest()
    assert auth.sign("secret", params) == expected
    headers = auth.signed_headers("key", "secret", params)
    assert headers == {"X-ACCESS-KEY": "key", "X-SIGNATURE": expected, "X-TIMESTAMP": "1724916869475"}


# ── positions: HIBT rows become MEXC rows ────────────────────────────────────────────────────────
def hibt_position(side=1, amount="0.36", symbol="eth_usdt", position_id=LONG_ID, leverage=10):
    return {
        "positionID": position_id, "symbol": symbol, "side": side, "leverage": leverage,
        "price": "2490.03", "amount": amount, "margin": "89.64", "openProfit": "-1.2",
        "slPrice": "", "spPrice": "2600", "updatedAt": 1724916869475,
    }


def test_a_buy_position_is_a_long_and_a_sell_position_is_a_short():
    assert rest.position_row(hibt_position(side=1))["positionType"] == 1
    assert rest.position_row(hibt_position(side=2))["positionType"] == 2


def test_the_row_reads_like_mexc():
    row = rest.position_row(hibt_position())
    assert row["symbol"] == "ETH_USDT"
    assert row["holdVol"] == 0.36
    assert row["leverage"] == 10
    assert row["state"] == 1
    assert row["im"] == 89.64
    assert row["stopLossPrice"] is None  # "" is unset, never a price of zero
    assert row["takeProfitPrice"] == 2600.0


def test_a_29_digit_position_id_survives_exactly():
    row = rest.position_row(hibt_position())
    assert row["positionId"] == int(LONG_ID)
    snapshot = parse_position(row)
    assert snapshot is not None and snapshot.position_id == int(LONG_ID)


def test_hibt_rows_drive_the_tracker_exactly_like_mexc_rows():
    """The point of translating at the boundary: open and add are told apart by the same code."""
    tracker = MasterPositionTracker()
    opened = tracker.apply(parse_position(rest.position_row(hibt_position(amount="0.36"))))
    added = tracker.apply(parse_position(rest.position_row(hibt_position(amount="0.54"))))

    assert opened.action is Action.OPEN and opened.delta_vol == pytest.approx(0.36)
    assert added.action is Action.INCREASE and added.delta_vol == pytest.approx(0.18)
    assert f"#{LONG_ID}" in opened.dedupe_key
    assert opened.dedupe_key != added.dedupe_key


# ── orders and settlement ────────────────────────────────────────────────────────────────────────
def test_order_states_mean_the_same_as_mexcs():
    state = lambda s: rest.order_row({"id": "1", "symbol": "eth_usdt", "side": 1, "type": 1, "state": s})["state"]  # noqa: E731
    assert state(1) == 2  # active    -> resting
    assert state(4) == 2  # partial   -> still resting
    assert state(2) == 3  # filled
    assert state(3) == 4  # cancelled
    assert state(5) == 4  # partial, then cancelled


def test_a_closed_position_is_rebuilt_from_its_orders_net_of_fees():
    orders = [
        {"positionID": LONG_ID, "symbol": "eth_usdt", "side": 1, "profit": "0", "fee": "0.45",
         "filledAmount": "0.36", "updatedAt": 1},
        {"positionID": LONG_ID, "symbol": "eth_usdt", "side": 2, "profit": "12.5", "fee": "0.46",
         "filledAmount": "0.36", "updatedAt": 2},
        {"positionID": "999", "symbol": "sol_usdt", "side": 2, "profit": "-3", "fee": "0.1",
         "filledAmount": "5", "updatedAt": 3},
    ]
    rows = {row["positionId"]: row for row in rest.closed_rows(orders)}

    eth = rows[int(LONG_ID)]
    assert eth["holdVol"] == 0.0 and eth["state"] == 3
    assert eth["realised"] == pytest.approx(12.5 - 0.45 - 0.46)
    assert eth["closeVol"] == pytest.approx(0.36)  # only the order that realised anything
    assert rows[999]["realised"] == pytest.approx(-3.1)


def test_sizes_round_down_never_up():
    assert str(rest.floor_amount(0.2394, 2)) == "0.23"
    assert str(rest.floor_amount(0.18000000000000002, 2)) == "0.18"
    assert str(rest.floor_amount(5.999, 0)) == "5"


# ── the client, against a fake session ───────────────────────────────────────────────────────────
class Response:
    def __init__(self, payload, status=200):
        self._payload, self.status = payload, status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def text(self):
        return str(self._payload)

    async def json(self, content_type=None):
        if isinstance(self._payload, str):
            raise ValueError("not json")
        return self._payload


class Session:
    """Records every request and answers from a queue."""

    def __init__(self, *payloads):
        self.payloads = list(payloads)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return Response(self.payloads.pop(0))


RULES = {"ETH_USDT": {"symbol": "eth_usdt", "supportTrade": True, "volumePrecision": 2,
                      "marketMiniAmount": "0.18", "limitMiniAmount": "0.18"}}


@pytest.fixture
def rules(monkeypatch):
    async def load(session, *, force=False):
        return RULES
    monkeypatch.setattr(rest, "load_symbols", load)


def submit(session, **kwargs):
    client = HibtRestClient("key", "secret", session=session)
    base = dict(symbol="ETH_USDT", side=SIDE_OPEN_LONG, vol=0.36, leverage=10, order_type=ORDER_TYPE_MARKET)
    return asyncio.run(client.submit_order(**(base | kwargs)))


@pytest.mark.parametrize("side", [SIDE_CLOSE_LONG, SIDE_CLOSE_SHORT])
def test_a_closing_side_is_refused_before_anything_is_sent(rules, side):
    # On HIBT an order can only open. A closing side sent as one would open the opposite way.
    session = Session()
    with pytest.raises(HibtError, match="close_all"):
        submit(session, side=side)
    assert session.calls == []


def test_an_order_without_leverage_is_refused_rather_than_guessed(rules):
    session = Session()
    with pytest.raises(HibtError, match="leverage"):
        submit(session, leverage=None)
    assert session.calls == []


def test_a_size_that_rounds_below_the_minimum_is_refused(rules):
    session = Session()
    with pytest.raises(HibtError, match="minimum"):
        submit(session, vol=0.1799)
    assert session.calls == []


def test_an_opening_short_goes_out_as_a_sell_with_the_size_floored(rules):
    session = Session({"code": 0, "msg": "success", "data": {"orderID": LONG_ID}})
    result = submit(session, side=SIDE_OPEN_SHORT, vol=0.2394 * 2, external_oid="cp1-2-abc")

    method, url, kwargs = session.calls[0]
    body = kwargs["json"]
    assert (method, url) == ("POST", rest.BASE_URL + "/v2/order/open")
    assert body["side"] == rest.VENUE_SIDE_SELL
    assert body["type"] == rest.VENUE_TYPE_MARKET
    assert body["amount"] == "0.47"  # 0.4788 floored to two places
    assert body["leverage"] == 10 and body["symbol"] == "eth_usdt" and body["customID"] == "cp1-2-abc"
    assert "price" not in body
    # The signature covers exactly what was sent.
    assert kwargs["headers"]["X-SIGNATURE"] == auth.sign("secret", body)
    assert kwargs["headers"]["User-Agent"] == rest.USER_AGENT
    # Handed back in MEXC's shape, so the engine reads the id without knowing the venue.
    assert result == {"orderId": LONG_ID}


def test_a_refusal_under_http_200_is_still_an_error_and_a_mexc_error():
    session = Session({"code": 220014, "msg": "No trading permissions", "data": None})
    client = HibtRestClient("key", "secret", session=session)
    with pytest.raises(MexcError) as caught:
        asyncio.run(client.get_open_positions())
    assert isinstance(caught.value, HibtError) and caught.value.code == 220014


def test_an_edge_page_instead_of_json_is_an_error_not_an_empty_account():
    session = Session("error code: 1010")
    client = HibtRestClient("key", "secret", session=session)
    with pytest.raises(HibtError, match="not JSON"):
        asyncio.run(client.get_open_positions())


def test_too_many_requests_is_retryable():
    assert HibtError(220017, "Too many requests", endpoint="x").is_rate_limited
    assert not HibtError(220008, "Signature verification failed", endpoint="x").is_rate_limited


def test_close_all_uses_the_documented_call():
    session = Session({"code": 0, "msg": "success", "data": {"listOrderID": ["1"]}})
    asyncio.run(HibtRestClient("key", "secret", session=session).close_all("ETH_USDT"))
    method, url, kwargs = session.calls[0]
    assert (method, url) == ("POST", rest.BASE_URL + "/v2/order/closeAll")
    assert kwargs["json"]["symbol"] == "eth_usdt"


def test_positions_come_back_translated():
    session = Session({"code": 0, "msg": "success", "data": [hibt_position(side=2, amount="5", symbol="xag_usdt")]})
    positions = asyncio.run(HibtRestClient("key", "secret", session=session).get_open_positions("XAG_USDT"))
    assert session.calls[0][2]["params"]["symbol"] == "xag_usdt"
    assert [(p.symbol, p.position_type, p.hold_vol) for p in positions] == [("XAG_USDT", 2, 5.0)]


# ── keeping venues apart ─────────────────────────────────────────────────────────────────────────
def test_credentials_still_unpack_as_a_pair():
    api_key, secret = Credentials("k", "s", EXCHANGE_HIBT)
    assert (api_key, secret) == ("k", "s")


def test_plain_pairs_and_unknown_values_mean_mexc():
    assert exchange_of(("k", "s")) == EXCHANGE_MEXC
    assert normalize(None) == normalize("") == normalize("binance") == EXCHANGE_MEXC


def test_the_factory_builds_the_client_for_the_keys_venue():
    assert isinstance(make_rest_client(Credentials("k", "s", EXCHANGE_HIBT), session=object()), HibtRestClient)
    assert isinstance(make_rest_client(Credentials("k", "s", EXCHANGE_MEXC), session=object()), MexcRestClient)
    assert isinstance(make_rest_client(("k", "s"), session=object()), MexcRestClient)


def test_credentials_never_print_the_secret():
    text = repr(Credentials("abcdefgh1234", "TOPSECRET", EXCHANGE_HIBT))
    assert "TOPSECRET" not in text and "abcdefgh" not in text


def test_no_mexc_client_is_built_by_hand_anywhere():
    """A MexcRestClient built outside the factory would send a HIBT account's keys to MEXC."""
    src = Path(__file__).resolve().parents[1] / "src" / "mexc_copy_bot"
    offenders = [
        f"{path.relative_to(src)}:{number}"
        for path in src.rglob("*.py")
        if path.name != "exchange.py"
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if re.search(r"\bMexcRestClient\(", line) and not line.lstrip().startswith(("class ", "#"))
        and "mexc/rest.py" not in str(path).replace("\\", "/")
    ]
    assert offenders == []


# ── echo suppression, per venue ──────────────────────────────────────────────────────────────────
class _NoStore:
    async def get_folder(self, folder_id, owner_id):
        return None


def _event(delta):
    return MasterEvent(
        action=Action.INCREASE, symbol="ETH_USDT", position_type=1, master_vol=delta, delta_vol=delta,
        leverage=10, open_type=2, dedupe_key=f"k{delta}",
    )


@pytest.mark.parametrize(
    ("exchange", "manual_extra", "swallowed"),
    [
        # MEXC rounds to whole contracts, so one contract of slack is right there.
        (EXCHANGE_MEXC, 0.5, True),
        # On HIBT the same slack would be half an ETH of somebody's real trade read as the bot's own.
        (EXCHANGE_HIBT, 0.5, False),
        (EXCHANGE_HIBT, 0.0, True),
    ],
)
def test_a_trade_made_by_hand_is_not_mistaken_for_the_bots_copy(exchange, manual_extra, swallowed):
    from mexc_copy_bot.core.service import CopyService

    async def run():
        service = CopyService(_NoStore(), 1, 1)
        service._exchange = exchange
        service._expect(account_id=5, symbol="ETH_USDT", side=1, vol=0.18)
        return service._was_expected(5, _event(0.18 + manual_extra))

    assert asyncio.run(run()) is swallowed
