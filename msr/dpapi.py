"""Windows DPAPI (current user) for secrets at rest; callers keep plaintext elsewhere."""
import base64
import sys


def available():
    return sys.platform == "win32"


def crypt(data, protect):
    import ctypes
    from ctypes import wintypes

    class BLOB(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]
    crypt32, kernel32 = ctypes.windll.crypt32, ctypes.windll.kernel32
    buf = ctypes.create_string_buffer(data, len(data))
    src = BLOB(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))
    dst = BLOB()
    fn = crypt32.CryptProtectData if protect else crypt32.CryptUnprotectData
    # 0x1 = CRYPTPROTECT_UI_FORBIDDEN (never prompt)
    if not fn(ctypes.byref(src), None, None, None, None, 0x1, ctypes.byref(dst)):
        raise OSError("DPAPI call failed")
    try:
        return ctypes.string_at(dst.pbData, dst.cbData)
    finally:
        kernel32.LocalFree(dst.pbData)


def encrypt_text(text):
    return base64.b64encode(crypt(text.encode("utf-8"), True)).decode("ascii")


def decrypt_text(b64):
    return crypt(base64.b64decode(b64.strip(), validate=True), False).decode("utf-8")
