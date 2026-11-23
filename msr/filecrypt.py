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


def _write_new(path: str, data: bytes) -> None:
    """Create ``path`` containing exactly ``data``.

    On Windows ``os.open`` is text mode unless ``O_BINARY`` is set, so a key
    byte of ``\\n`` or Ctrl+Z would be rewritten and the file would no longer
    be 32 bytes. Mode 0o600 is also skipped there: Python 3.14 turns it into
    an ACL that later opens cannot read. DPAPI is what Windows uses instead.
    """
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    mode = 0o600
    if os.name == "nt":
        flags |= getattr(os, "O_BINARY", 0)
        mode = 0o666
    fd = os.open(path, flags, mode)
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("could not write the secret key file")
            view = view[written:]
    finally:
        os.close(fd)


def load_key(*, create: bool = True) -> bytes:
    """The user key. Owner-only.

    Created on first encrypt. A decrypt never creates one: a missing key stays
    missing, so a copied config can be tried again after the key file is restored.
    """
    folder = key_dir()
    path = _key_path()
    if os.path.lexists(path):
        if os.path.islink(path):
            raise CryptoError("the secret key file must not be a symbolic link")
        if os.name != "nt":
            try:
                if os.lstat(path).st_uid != os.getuid():
                    raise CryptoError("the secret key file is not owned by this user")
            except CryptoError:
                raise
            except OSError as exc:
                raise CryptoError("the secret key file is not usable") from exc
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
    # 0o700 on Windows (Python 3.14) becomes an ACL that denies later reads.
    os.makedirs(folder, mode=0o777 if os.name == "nt" else 0o700, exist_ok=True)
    _tighten(folder, 0o700)
    key = os.urandom(32)
    try:
        _write_new(path, key)
    except FileExistsError:
        return load_key()
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


def _mac_input(version: bytes, nonce: bytes, cipher: bytes, aad: bytes) -> bytes:
    if version == b"\x02":
        return version + len(aad).to_bytes(4, "big") + aad + nonce + cipher
    return version + nonce + cipher


def encrypt(data: bytes, *, aad: bytes = b"") -> bytes:
    """``version | nonce | mac | ciphertext``. Empty input is refused.

    When *aad* is given, version ``0x02`` binds the ciphertext to that context
    so a ciphertext cannot be transplanted into another config field or file.
    """
    if not isinstance(data, (bytes, bytearray)) or not data:
        raise CryptoError("nothing to encrypt")
    if isinstance(aad, str):
        aad = aad.encode("utf-8")
    key = load_key()
    nonce = os.urandom(16)
    stream = _stream(key, nonce, len(data))
    cipher = bytes(a ^ b for a, b in zip(data, stream))
    version = b"\x02" if aad else b"\x01"
    mac_key = hmac.new(key, b"msr-filecrypt-mac", hashlib.sha256).digest()
    mac = hmac.new(mac_key, _mac_input(version, nonce, cipher, aad), hashlib.sha256).digest()
    return version + nonce + mac + cipher


def decrypt(blob: bytes, *, aad: bytes = b"") -> bytes:
    if not blob or blob[0:1] not in (b"\x01", b"\x02") or len(blob) < 1 + 16 + 32 + 1:
        raise CryptoError("not an encrypted secret")
    if isinstance(aad, str):
        aad = aad.encode("utf-8")
    version = blob[0:1]
    key = load_key(create=False)
    nonce, mac, cipher = blob[1:17], blob[17:49], blob[49:]
    mac_key = hmac.new(key, b"msr-filecrypt-mac", hashlib.sha256).digest()
    # Version 0x02 strictly binds to *aad*; legacy 0x01 blobs decrypt without AAD for migration.
    expect = hmac.new(mac_key, _mac_input(version, nonce, cipher, aad), hashlib.sha256).digest()
    if not hmac.compare_digest(mac, expect):
        raise CryptoError("the secret could not be decrypted")
    stream = _stream(key, nonce, len(cipher))
    return bytes(a ^ b for a, b in zip(cipher, stream))


def encrypt_text(text: str, *, aad: bytes = b"") -> str:
    return base64.b64encode(encrypt(text.encode("utf-8"), aad=aad)).decode("ascii")


def decrypt_text(b64: str, *, aad: bytes = b"") -> str:
    try:
        raw = base64.b64decode(b64.strip(), validate=True)
    except Exception as exc:
        raise CryptoError("the secret could not be decrypted") from exc
    return decrypt(raw, aad=aad).decode("utf-8")
