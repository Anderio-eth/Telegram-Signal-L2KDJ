"""AES-256-GCM encryption for stored exchange secrets.

Private keys never touch the database in the clear. The key comes from ENCRYPTION_KEY (32 bytes, as
base64 or hex); losing it makes stored secrets permanently unreadable, so it lives only in the
environment. Each token is nonce||ciphertext, base64-encoded.
"""

from __future__ import annotations

import base64
import binascii
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def _load_key(raw: str) -> bytes:
    raw = raw.strip()
    for decode in (base64.b64decode, lambda s: binascii.unhexlify(s)):
        try:
            key = decode(raw)
            if len(key) == 32:
                return key
        except Exception:  # noqa: BLE001 — try the next encoding
            pass
    raise RuntimeError("ENCRYPTION_KEY must be 32 bytes as base64 or hex")


class CredentialCipher:
    def __init__(self, key_material: str) -> None:
        self._aes = AESGCM(_load_key(key_material))

    def encrypt(self, plaintext: str) -> str:
        nonce = os.urandom(12)
        ct = self._aes.encrypt(nonce, plaintext.encode(), None)
        return base64.b64encode(nonce + ct).decode()

    def decrypt(self, token: str) -> str:
        blob = base64.b64decode(token)
        nonce, ct = blob[:12], blob[12:]
        return self._aes.decrypt(nonce, ct, None).decode()
