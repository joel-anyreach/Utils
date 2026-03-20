"""
API key management.

Two modes (auto-detected):
  LOCAL  — reads/writes keys.json  (running on your machine)
  CLOUD  — reads from st.secrets   (running on Streamlit Community Cloud)

Cloud secrets format in Streamlit Cloud UI (TOML):
  [Reoon]
  "Production Key" = "your-key-here"

  [ZeroBounce]
  "Main" = "your-key-here"
"""

import json
from pathlib import Path
from typing import Dict

KEYS_FILE = Path(__file__).parent / "keys.json"
PROVIDERS  = ["Reoon", "ZeroBounce", "NeverBounce", "Hunter", "Gemini"]


# ── Mode detection ────────────────────────────────────────────────────────────

def is_cloud() -> bool:
    """True when running on Streamlit Cloud (no writable local filesystem)."""
    return not KEYS_FILE.parent.stat().st_mode & 0o200 or _secrets_available()


def _secrets_available() -> bool:
    try:
        import streamlit as st
        # st.secrets raises if no secrets configured
        _ = dict(st.secrets)
        return True
    except Exception:
        return False


# ── Cloud: read from st.secrets ───────────────────────────────────────────────

def _load_from_secrets() -> Dict[str, Dict[str, str]]:
    """Read keys from Streamlit secrets (cloud mode)."""
    try:
        import streamlit as st
        result = {}
        for p in PROVIDERS:
            if p in st.secrets:
                result[p] = dict(st.secrets[p])
            else:
                result[p] = {}
        return result
    except Exception:
        return {p: {} for p in PROVIDERS}


# ── Local: read/write keys.json ───────────────────────────────────────────────

def _load_local() -> Dict[str, Dict[str, str]]:
    if KEYS_FILE.exists():
        try:
            return json.loads(KEYS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {p: {} for p in PROVIDERS}


def _save_local(data: dict):
    KEYS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ── Public API ────────────────────────────────────────────────────────────────

def get_all_keys() -> Dict[str, Dict[str, str]]:
    """Return {provider: {label: key}} for all providers."""
    if _secrets_available():
        data = _load_from_secrets()
    else:
        data = _load_local()
    for p in PROVIDERS:
        data.setdefault(p, {})
    return data


def get_keys_for_provider(provider: str) -> Dict[str, str]:
    """Return {label: key} for one provider."""
    return get_all_keys().get(provider, {})


def get_key(provider: str, label: str) -> str:
    return get_keys_for_provider(provider).get(label, "")


def save_key(provider: str, label: str, key: str):
    """Save a key. Only works in local mode."""
    data = _load_local()
    data.setdefault(provider, {})[label] = key
    _save_local(data)


def delete_key(provider: str, label: str):
    """Delete a key. Only works in local mode."""
    data = _load_local()
    data.get(provider, {}).pop(label, None)
    _save_local(data)
