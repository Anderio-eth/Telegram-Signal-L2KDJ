"""MEXC futures request signing.

Signature scheme, per MEXC's futures API docs:

    REST:  signature = HMAC_SHA256(secret, apiKey + reqTime + payload)
           payload is the sorted query string for GET, or the raw JSON body for POST.
           Sent as headers: ApiKey, Request-Time, Signature.

    WS:    signature = HMAC_SHA256(secret, apiKey + reqTime)
           i.e. the same construction with an empty payload, sent in the login frame.

The secret never leaves this module in any readable form: it is passed in, used to sign, and
never logged, echoed, or attached to an exception. Anything that formats an error must take the
signature or the api key only, never the secret.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any
from urllib.parse import urlencode


def now_ms() -> int:
    return int(time.time() * 1000)


def _sign(secret: str, message: str) -> str:
    return hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()


def sign_rest(api_key: str, secret: str, *, params: dict[str, Any] | None = None, body: Any = None) -> dict[str, str]:
    """Headers for one authenticated REST call.

    Exactly one of params (GET) or body (POST) carries the payload. MEXC signs the *sorted*
    query string for GET and the *exact* serialized body for POST — the body string returned
    here must therefore be the one actually sent, byte for byte, or the signature won't match.
    """
    req_time = str(now_ms())
    if body is not None:
        payload = json.dumps(body, separators=(",", ":"), sort_keys=True)
    elif params:
        payload = urlencode(sorted((k, v) for k, v in params.items() if v is not None))
    else:
        payload = ""

    return {
        "ApiKey": api_key,
        "Request-Time": req_time,
        "Signature": _sign(secret, f"{api_key}{req_time}{payload}"),
        "Content-Type": "application/json",
    }


def rest_body_string(body: Any) -> str:
    """The exact JSON string that sign_rest() signed for this body."""
    return json.dumps(body, separators=(",", ":"), sort_keys=True)


def sign_ws_login(api_key: str, secret: str) -> dict[str, Any]:
    """The login frame for the private websocket (wss://contract.mexc.com/edge)."""
    req_time = str(now_ms())
    return {
        "method": "login",
        "param": {
            "apiKey": api_key,
            "reqTime": req_time,
            "signature": _sign(secret, f"{api_key}{req_time}"),
        },
    }
