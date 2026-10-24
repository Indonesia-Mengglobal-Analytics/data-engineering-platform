"""Encrypt published JSON with a key derived from the site PIN.

The site is static and public, so the PIN login only protects anything because the
data files themselves are encrypted: the browser derives the same key from the PIN
(PBKDF2-SHA256) and decrypts with AES-256-GCM via WebCrypto. A wrong PIN fails the
GCM tag check. See site/assets/app.js `unseal`.
"""

from __future__ import annotations

import base64
import json
import os
import re

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

ITERATIONS = 600_000
PIN_PATTERN = re.compile(r"^\d{12}$")


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def derive_key(pin: str, salt: bytes) -> bytes:
    return PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=ITERATIONS).derive(pin.encode())


class Sealer:
    def __init__(self, pin: str):
        if not PIN_PATTERN.match(pin):
            raise ValueError("SITE_PIN must be exactly 12 digits")
        # One salt per build: the browser derives the key once per login.
        self.salt = os.urandom(16)
        self.key = derive_key(pin, self.salt)

    def seal(self, payload: object) -> dict:
        iv = os.urandom(12)
        plaintext = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        ciphertext = AESGCM(self.key).encrypt(iv, plaintext, None)
        return {
            "sealed": 1,
            "kdf": {"name": "PBKDF2", "hash": "SHA-256", "iterations": ITERATIONS, "salt": _b64(self.salt)},
            "iv": _b64(iv),
            "data": _b64(ciphertext),
        }


def unseal(envelope: dict, pin: str) -> object:
    salt = base64.b64decode(envelope["kdf"]["salt"])
    key = derive_key(pin, salt)
    plaintext = AESGCM(key).decrypt(base64.b64decode(envelope["iv"]), base64.b64decode(envelope["data"]), None)
    return json.loads(plaintext)
