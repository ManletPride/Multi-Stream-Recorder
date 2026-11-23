"""Windows DPAPI (current user) for secrets at rest.

Other systems encrypt the same values with ``msr.filecrypt`` (a user key
outside the data folder). This module is the Windows half only.
"""
import base64
import sys


def available():
    return sys.platform == "win32"


def crypt(data, protect, entropy: bytes = b""):
    import ctypes
    from ctypes import wintypes

    class BLOB(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]
    crypt32, kernel32 = ctypes.windll.crypt32, ctypes.windll.kernel32
    buf = ctypes.create_string_buffer(data, len(data))
    src = BLOB(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))
    ent_ptr = None
    if entropy:
        if isinstance(entropy, str):
            entropy = entropy.encode("utf-8")
        ent_buf = ctypes.create_string_buffer(entropy, len(entropy))
        ent_blob = BLOB(len(entropy), ctypes.cast(ent_buf, ctypes.POINTER(ctypes.c_char)))
        ent_ptr = ctypes.byref(ent_blob)
    dst = BLOB()
    fn = crypt32.CryptProtectData if protect else crypt32.CryptUnprotectData
    # 0x1 = CRYPTPROTECT_UI_FORBIDDEN (never prompt)
    if not fn(ctypes.byref(src), None, ent_ptr, None, None, 0x1, ctypes.byref(dst)):
        raise OSError("DPAPI call failed")
    try:
        return ctypes.string_at(dst.pbData, dst.cbData)
    finally:
        kernel32.LocalFree(dst.pbData)


def encrypt_text(text, *, entropy: bytes = b""):
    b64 = base64.b64encode(crypt(text.encode("utf-8"), True, entropy=entropy)).decode("ascii")
    return ("v2:" + b64) if entropy else b64


def decrypt_text(b64, *, entropy: bytes = b""):
    raw_text = b64.strip()
    if raw_text.startswith("v2:"):
        raw = base64.b64decode(raw_text[3:], validate=True)
        return crypt(raw, False, entropy=entropy).decode("utf-8")
    # Legacy unprefixed DPAPI blob (saved before optional-entropy context binding)
    return crypt(base64.b64decode(raw_text, validate=True), False).decode("utf-8")
