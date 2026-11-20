"""Encrypt short secrets where Windows DPAPI is not available.

The key is 32 random bytes in ``$XDG_CONFIG_HOME/multi-stream-recorder/secret.key``
(or ``MSR_SECRET_DIR``), mode 0600, directory 0700. It is not kept in the data
folder, so a copied ``config.ini`` does not decrypt on its own. The ciphertext
is a version byte, a random nonce, an HMAC-SHA256, and a SHA-256 keystream.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os

KEY_NAME = "secret.key"
_DIRNAME = "multi-stream-recorder"
_ENV = "MSR_SECRET_DIR"


class CryptoError(Exception):
    """The secret could not be encrypted or did not decrypt."""


def key_dir() -> str:
    override = (os.environ.get(_ENV) or "").strip()
    if override:
        return os.path.abspath(os.path.expanduser(override))
    base = (os.environ.get("XDG_CONFIG_HOME") or "").strip()
    if not base:
        base = os.path.join(os.path.expanduser("~"), ".config")
    return os.path.join(os.path.abspath(os.path.expanduser(base)), _DIRNAME)


def _key_path() -> str:
    return os.path.join(key_dir(), KEY_NAME)


def _tighten(path: str, mode: int) -> None:
    if os.name == "nt":
        return
    try:
        os.chmod(path, mode)
    except OSError:
        pass


def load_key(*, create: bool = True) -> bytes:
    """The user key. Owner-only.

    Created on first encrypt. A decrypt never creates one: a missing key stays
    missing, so a copied config can be tried again after the key file is restored.
    """
    folder = key_dir()
    path = _key_path()
    if os.path.isfile(path):
        _tighten(folder, 0o700)
        _tighten(path, 0o600)
        with open(path, "rb") as fh:
            key = fh.read()
        if len(key) != 32:
            raise CryptoError("the secret key file is not usable")
        return key
    if not create:
        raise CryptoError("the secret key file is missing")
    os.makedirs(folder, mode=0o700, exist_ok=True)
    _tighten(folder, 0o700)
    key = os.urandom(32)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        fd = os.open(path, flags, 0o600)
    except FileExistsError:
        return load_key()
    try:
        os.write(fd, key)
    finally:
        os.close(fd)
    _tighten(path, 0o600)
    return key


def _stream(key: bytes, nonce: bytes, size: int) -> bytes:
    enc_key = hmac.new(key, b"msr-filecrypt-enc", hashlib.sha256).digest()
    out = bytearray()
    counter = 0
    while len(out) < size:
        block = hmac.new(enc_key, nonce + counter.to_bytes(4, "big"), hashlib.sha256).digest()
        out.extend(block)
        counter += 1
    return bytes(out[:size])


def encrypt(data: bytes) -> bytes:
    """``version | nonce | mac | ciphertext``. Empty input is refused."""
    if not isinstance(data, (bytes, bytearray)) or not data:
        raise CryptoError("nothing to encrypt")
    key = load_key()
    nonce = os.urandom(16)
    stream = _stream(key, nonce, len(data))
    cipher = bytes(a ^ b for a, b in zip(data, stream))
    mac_key = hmac.new(key, b"msr-filecrypt-mac", hashlib.sha256).digest()
    mac = hmac.new(mac_key, b"\x01" + nonce + cipher, hashlib.sha256).digest()
    return b"\x01" + nonce + mac + cipher


def decrypt(blob: bytes) -> bytes:
    if not blob or blob[0:1] != b"\x01" or len(blob) < 1 + 16 + 32 + 1:
        raise CryptoError("not an encrypted secret")
    key = load_key(create=False)
    nonce, mac, cipher = blob[1:17], blob[17:49], blob[49:]
    mac_key = hmac.new(key, b"msr-filecrypt-mac", hashlib.sha256).digest()
    expect = hmac.new(mac_key, b"\x01" + nonce + cipher, hashlib.sha256).digest()
    if not hmac.compare_digest(mac, expect):
        raise CryptoError("the secret could not be decrypted")
    stream = _stream(key, nonce, len(cipher))
    return bytes(a ^ b for a, b in zip(cipher, stream))


def encrypt_text(text: str) -> str:
    return base64.b64encode(encrypt(text.encode("utf-8"))).decode("ascii")


def decrypt_text(b64: str) -> str:
    try:
        raw = base64.b64decode(b64.strip(), validate=True)
    except Exception as exc:
        raise CryptoError("the secret could not be decrypted") from exc
    return decrypt(raw).decode("utf-8")
