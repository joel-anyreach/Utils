"""
Checkpoint module — saves Phase 2 and Phase 3 progress to disk so runs
can resume after a crash, browser refresh, or interruption.

Checkpoint files live in:
  Email_Validation/checkpoints/<run_id>_phase2.json
  Email_Validation/checkpoints/<run_id>_phase3.json

Run ID = 12-char MD5 of the sorted, lowercased input email list.
Same input always produces the same run ID.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Dict, List, Optional

CHECKPOINT_DIR = Path(__file__).parent / "checkpoints"


# ── Run ID ────────────────────────────────────────────────────────────────────

def run_id(emails: List[str]) -> str:
    """Stable 12-char ID derived from the input email list."""
    key = "|".join(sorted(e.lower().strip() for e in emails))
    return hashlib.md5(key.encode()).hexdigest()[:12]


# ── File paths ────────────────────────────────────────────────────────────────

def _path(emails: List[str], phase: int) -> Path:
    CHECKPOINT_DIR.mkdir(exist_ok=True)
    return CHECKPOINT_DIR / f"{run_id(emails)}_phase{phase}.json"


# ── Load ──────────────────────────────────────────────────────────────────────

def load(emails: List[str], phase: int) -> Optional[Dict]:
    """
    Load an existing checkpoint.
    Returns {"results": {key: result, ...}, "provider": str} or None.
    """
    p = _path(emails, phase)
    if p.exists():
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


# ── Save (incremental) ────────────────────────────────────────────────────────

def save(emails: List[str], phase: int, results: Dict, meta: Dict = None):
    """
    Overwrite the checkpoint with the latest results dict.
    Call this after every completed item for incremental saves.
    """
    p = _path(emails, phase)
    data = {"results": results, "meta": meta or {}}
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f)


# ── Clear ─────────────────────────────────────────────────────────────────────

def clear(emails: List[str], phase: int):
    """Delete the checkpoint file on successful completion."""
    p = _path(emails, phase)
    if p.exists():
        p.unlink()


# ── Helpers ───────────────────────────────────────────────────────────────────

def count(emails: List[str], phase: int) -> int:
    """Number of items already saved in the checkpoint (0 if none)."""
    cp = load(emails, phase)
    if not cp:
        return 0
    return len(cp.get("results", {}))
