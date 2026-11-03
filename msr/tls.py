"""HTTPS context for MSR's own requests (GitHub update checks and downloads).

Python's default context trusts only the roots already in the Windows store. Windows adds
missing roots on demand for its own apps, but not for Python, so a site on a newer root
(api.github.com: Sectigo Public Server Authentication Root E46) can fail with
CERTIFICATE_VERIFY_FAILED. The certifi bundle (installed with MSR's packages) is added on top;
nothing is ever trusted less."""
import ssl

_CTX = None


def https_context():
    global _CTX
    if _CTX is None:
        ctx = ssl.create_default_context()
        try:
            import certifi
            ctx.load_verify_locations(certifi.where())
        except Exception:
            pass   # no certifi: the system store alone, as before
        _CTX = ctx
    return _CTX


def is_cert_error(e) -> bool:
    r = getattr(e, "reason", e)
    return isinstance(r, ssl.SSLCertVerificationError) or isinstance(e, ssl.SSLCertVerificationError)
