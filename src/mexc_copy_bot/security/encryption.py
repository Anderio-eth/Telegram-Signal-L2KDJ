"""Encryption for stored exchange credentials.

AES-256-GCM: authenticated, so a tampered ciphertext fails to decrypt instead of silently
producing garbage that would then be sent to MEXC as someone's API key.

The key lives in the environment (COPY_BOT_ENCRYPTION_KEY), never in the database — otherwise
encrypting at rest would protect nothing, since anyone reading the table would also read the key.
Losing it makes every stored credential permanently unreadable; there is no recovery path, by
design.

Stored format: base64( nonce[12] || ciphertext || tag[16] ). The nonce is random per encryption,
which is what makes it safe to encrypt the same secret twice.
"""

from __future__ import annotations

import base64
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

NONCE_BYTES = 12
KEY_BYTES = 32


class EncryptionError(RuntimeError):
    """Raised when a value cannot be decrypted — wrong key, or the data was altered."""


class CredentialCipher:
    def __init__(self, key_hex: str) -> None:
        try:
            key = bytes.fromhex(key_hex.strip())
        except ValueError as err:
            raise RuntimeError("COPY_BOT_ENCRYPTION_KEY must be hex") from err
        if len(key) != KEY_BYTES:
            raise RuntimeError(
                f"COPY_BOT_ENCRYPTION_KEY must be {KEY_BYTES} bytes ({KEY_BYTES * 2} hex chars), got {len(key)}"
            )
        self._aead = AESGCM(key)

    def encrypt(self, plaintext: str) -> str:
        nonce = os.urandom(NONCE_BYTES)
        blob = nonce + self._aead.encrypt(nonce, plaintext.encode("utf-8"), None)
        return base64.b64encode(blob).decode("ascii")

    def decrypt(self, stored: str) -> str:
        try:
            blob = base64.b64decode(stored)
            nonce, payload = blob[:NONCE_BYTES], blob[NONCE_BYTES:]
            return self._aead.decrypt(nonce, payload, None).decode("utf-8")
        except Exception as err:  # noqa: BLE001 — the cause must not leak into logs
            # Deliberately opaque: the original exception can carry fragments of key material
            # into a stack trace, and this is called from paths that log failures.
            raise EncryptionError("Could not decrypt stored credential") from None


def generate_key_hex() -> str:
    """A fresh 32-byte key, for putting in the environment."""
    return os.urandom(KEY_BYTES).hex()


def mask(value: str, keep: int = 4) -> str:
    """Safe rendering of a credential for logs and Telegram — never the whole thing."""
    if len(value) <= keep:
        return "*" * len(value)
    return f"{'*' * (len(value) - keep)}{value[-keep:]}"
