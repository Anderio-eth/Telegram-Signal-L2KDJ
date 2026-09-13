"""HIBT futures request signing.

Signature scheme, per HIBT's OpenAPI docs (apidoc.hibt.co, "Preparation for Access"):

    every request parameter, `timestamp` included, sorted by key in ASCII order
    joined as  key=value&key=value
    signature = hex(HMAC_SHA256(secret, that string))
    sent as headers: X-ACCESS-KEY, X-SIGNATURE, X-TIMESTAMP

The same construction serves GET (parameters in the query) and POST (parameters in the JSON body):
what is signed is the parameter set, not the bytes on the wire. A signature is valid for five
minutes, which is why it is computed inside the request slot rather than before it — the same
lesson MEXC taught with "Confirming signature failed" on calls that waited in the queue.

NOT YET VERIFIED against a live key. The docs are the only source so far, and on MEXC the docs
were wrong about something this basic. How a boolean or a float is rendered into the signed string
is the most likely place for them to be wrong here too; if the first private call answers 220008
("signature verification failed"), look at `_render` first.

The secret never leaves this module in readable form: it is used to sign and nothing else, and is
never logged or attached to an exception.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from typing import Any


def now_ms() -> int:
    return int(time.time() * 1000)


def _render(value: Any) -> str:
    """One parameter value as it appears in the signed string.

    Booleans are lowercase because that is how they arrive in the JSON body; a Python "True" in the
    signature against a JSON "true" on the wire is a mismatch the venue can only report as a bad
    signature.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def signing_string(params: dict[str, Any]) -> str:
    """The exact string that gets signed. Separate so a test can pin it down."""
    return "&".join(f"{key}={_render(params[key])}" for key in sorted(params) if params[key] is not None)


def sign(secret: str, params: dict[str, Any]) -> str:
    return hmac.new(secret.encode("utf-8"), signing_string(params).encode("utf-8"), hashlib.sha256).hexdigest()


def signed_headers(api_key: str, secret: str, params: dict[str, Any]) -> dict[str, str]:
    """Headers for one authenticated call. `params` must already carry its `timestamp`."""
    return {
        "X-ACCESS-KEY": api_key,
        "X-SIGNATURE": sign(secret, params),
        "X-TIMESTAMP": str(params["timestamp"]),
    }
