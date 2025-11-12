"""Shared helpers for loading local/secure environment variables."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict

from secure_env import load_secure_env, SecureEnvStore

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env.local"
SECURE_STORE = SecureEnvStore()


def _parse_env_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    try:
        data = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return values
    except OSError:
        return values

    for raw_line in data.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        values[key] = value
    return values


def load_project_env(overwrite: bool = False) -> Dict[str, str]:
    """
    Load secrets from the DPAPI store and fall back to .env.local for public
    defaults. Values already set in os.environ win unless overwrite=True.
    """
    secure_values = load_secure_env(overwrite=overwrite)
    file_values = _parse_env_file(ENV_FILE)
    for key, value in file_values.items():
        if overwrite or key not in os.environ:
            os.environ[key] = value
    merged = dict(file_values)
    merged.update(secure_values)
    return merged
