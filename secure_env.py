"""
DPAPI-backed secret store for Study With Me tooling.

Secrets are encrypted per Windows user using CryptProtectData so the plaintext
never lives on disk. Use scripts/secure_env_tool.py to manage values.
"""

from __future__ import annotations

import base64
import json
import os
import sys
from pathlib import Path
from typing import Dict

if os.name != "nt":  # pragma: no cover - this project currently targets Windows hosts
    raise RuntimeError("secure_env.py requires Windows (DPAPI).")

import ctypes
import ctypes.wintypes as wintypes

_CRYPTPROTECT_UI_FORBIDDEN = 0x01
_CryptProtectData = ctypes.windll.crypt32.CryptProtectData
_CryptUnprotectData = ctypes.windll.crypt32.CryptUnprotectData
_LocalFree = ctypes.windll.kernel32.LocalFree


class _DATA_BLOB(ctypes.Structure):
    _fields_ = [
        ("cbData", wintypes.DWORD),
        ("pbData", ctypes.POINTER(ctypes.c_char)),
    ]


def _blob_from_bytes(data: bytes) -> tuple[_DATA_BLOB, ctypes.Array[ctypes.c_char]]:
    """Create a DATA_BLOB + backing buffer for the given bytes."""
    if not data:
        data = b"\x00"
    buf = ctypes.create_string_buffer(data, len(data))
    blob = _DATA_BLOB(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))
    return blob, buf


def _bytes_from_blob(blob: _DATA_BLOB) -> bytes:
    """Copy bytes out of a DATA_BLOB that CryptProtect/Unprotect returned."""
    if not blob.cbData or not blob.pbData:
        return b""
    pointer = ctypes.cast(blob.pbData, ctypes.POINTER(ctypes.c_char))
    result = ctypes.string_at(pointer, blob.cbData)
    _LocalFree(blob.pbData)
    return result


def _dpapi_protect(data: bytes) -> bytes:
    blob_in, _buf = _blob_from_bytes(data)
    blob_out = _DATA_BLOB()
    if not _CryptProtectData(
        ctypes.byref(blob_in),
        None,
        None,
        None,
        None,
        _CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(blob_out),
    ):
        raise ctypes.WinError()
    return _bytes_from_blob(blob_out)


def _dpapi_unprotect(data: bytes) -> bytes:
    blob_in, _buf = _blob_from_bytes(data)
    blob_out = _DATA_BLOB()
    if not _CryptUnprotectData(
        ctypes.byref(blob_in),
        None,
        None,
        None,
        None,
        _CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(blob_out),
    ):
        raise ctypes.WinError()
    return _bytes_from_blob(blob_out)


class SecureEnvStore:
    """Small helper that persists key/value secrets using DPAPI."""

    def __init__(self, path: Path | None = None) -> None:
        base_dir = Path(__file__).resolve().parent
        self.path = path or (base_dir / "var" / "secure_env.dat")
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> Dict[str, str]:
        if not self.path.exists():
            return {}
        encoded = self.path.read_text(encoding="utf-8").strip()
        if not encoded:
            return {}
        payload = base64.b64decode(encoded)
        plain = _dpapi_unprotect(payload)
        try:
            data = json.loads(plain.decode("utf-8"))
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive
            raise ValueError("Secure env store corrupted.") from exc
        if not isinstance(data, dict):
            raise ValueError("Secure env store invalid structure.")
        return {str(k): str(v) for k, v in data.items()}

    def save(self, values: Dict[str, str]) -> None:
        payload = json.dumps(values, sort_keys=True).encode("utf-8")
        protected = _dpapi_protect(payload)
        self.path.write_text(base64.b64encode(protected).decode("ascii"), encoding="utf-8")

    def load_into_environ(self, overwrite: bool = False) -> Dict[str, str]:
        values = self.load()
        for key, value in values.items():
            if overwrite or os.getenv(key) is None:
                os.environ[key] = value
        return values

    def set(self, key: str, value: str | None) -> None:
        data = self.load()
        if value is None:
            data.pop(key, None)
        else:
            data[str(key)] = str(value)
        self.save(data)


DEFAULT_STORE = SecureEnvStore()


def load_secure_env(overwrite: bool = False) -> Dict[str, str]:
    """
    Convenience wrapper to load DPAPI secrets into os.environ.

    Returns the mapping that was loaded for logging/debugging.
    """
    return DEFAULT_STORE.load_into_environ(overwrite=overwrite)


if __name__ == "__main__":
    store = SecureEnvStore()
    data = store.load()
    print(f"{len(data)} secure env entries found.")
