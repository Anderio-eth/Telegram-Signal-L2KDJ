"""The HIBT user stream: the login handshake, subscriptions, and frame delivery.

Driven against a fake websocket that plays back the exact frames the live venue sent (recorded
2026-09-14), so the protocol is pinned without a network: hello, auth-ok, sub acks, then a position
push. What is asserted is that the client signs the timestamp, subscribes to each topic, and hands
the handler the data frames and nothing else.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac

import aiohttp
import pytest

from mexc_copy_bot.hibt import websocket as ws_mod
from mexc_copy_bot.hibt.websocket import HibtUserStream, _sign


class FakeWS:
    def __init__(self, incoming):
        self._incoming = list(incoming)
        self.sent = []
        self.closed = False

    async def send_json(self, payload):
        self.sent.append(payload)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._incoming:
            raise StopAsyncIteration
        item = self._incoming.pop(0)
        return FakeMsg(item)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True
        return False


class FakeMsg:
    def __init__(self, data):
        self.type = aiohttp.WSMsgType.TEXT
        self._data = data

    def json(self):
        import json
        return json.loads(self._data)


class FakeSession:
    def __init__(self, ws):
        self._ws = ws

    def ws_connect(self, url, **kw):
        self._ws.url = url
        return self._ws


LIVE_FRAMES = [
    '{"data":"success","ts":1789418871135,"type":"hello"}',
    '{"type":"auth","ts":1789418871357,"data":"ok"}',
    '{"type":"sub","ts":1,"data":{"topic":"user.position","status":"ok"}}',
    '{"type":"user.balance","ts":2,"data":{"balance":"4.79","coin":"usdt"}}',
    '{"type":"user.position","ts":3,"data":[{"positionID":"260914204754698031055","symbol":"xrp_usdt",'
    '"side":1,"leverage":5,"price":"1.4749","amount":"10"}]}',
    '{"type":"user.position","ts":4,"data":[]}',
]


def test_it_signs_the_timestamp_and_subscribes_to_every_topic():
    received = []
    ws = FakeWS(LIVE_FRAMES)
    stream = HibtUserStream("KEY", "SEC", session=FakeSession(ws),
                            on_frame=lambda topic, frame: received.append((topic, frame)))

    async def go():
        # one session, then stop before it reconnects
        await stream._session_once()

    asyncio.run(go())

    auth = ws.sent[0]
    assert auth["event"] == "auth" and auth["accessKey"] == "KEY"
    assert auth["signature"] == _sign("SEC", auth["timestamp"]) == hmac.new(
        b"SEC", auth["timestamp"].encode(), hashlib.sha256).hexdigest()
    subs = [m for m in ws.sent if m.get("event") == "sub"]
    assert {m["topic"] for m in subs} == set(ws_mod.TOPICS)


def test_only_data_frames_reach_the_handler():
    received = []
    ws = FakeWS(LIVE_FRAMES)
    stream = HibtUserStream("KEY", "SEC", session=FakeSession(ws),
                            on_frame=lambda topic, frame: received.append((topic, frame)))
    asyncio.run(stream._session_once())

    topics = [t for t, _ in received]
    # hello and the sub acks are not data; balance and both position frames are
    assert topics == ["user.balance", "user.position", "user.position"]
    first_position = received[1][1]["data"][0]
    assert first_position["symbol"] == "xrp_usdt" and first_position["amount"] == "10"
    assert received[2][1]["data"] == []  # flat


def test_a_refused_auth_raises_so_the_loop_reconnects():
    ws = FakeWS(['{"type":"auth","data":"fail"}'])
    stream = HibtUserStream("KEY", "SEC", session=FakeSession(ws), on_frame=lambda *_: None)
    with pytest.raises(RuntimeError, match="auth refused"):
        asyncio.run(stream._session_once())


def test_a_handler_that_throws_does_not_kill_the_stream():
    def boom(topic, frame):
        raise ValueError("handler bug")
    ws = FakeWS(LIVE_FRAMES)
    stream = HibtUserStream("KEY", "SEC", session=FakeSession(ws), on_frame=boom)
    asyncio.run(stream._session_once())  # must not raise
    assert stream.connected
