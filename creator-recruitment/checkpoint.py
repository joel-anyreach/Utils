"""
Checkpoint module — saves Stage 1 (enrichment) and Stage 2.5 (Reoon) progress
to disk so pipeline runs can resume after a crash, Ctrl+C, or interruption.

Checkpoint files live in:
  creator-recruitment/checkpoints/<run_id>_stage1.json   — email enrichment
  creator-recruitment/checkpoints/<run_id>_stage25.json  — Reoon verification

Run ID = 12-char MD5 of the sorted, lowercased input item list
  (channel_urls for Stage 1, emails for Stage 2.5).
Same inputs always produce the same run ID.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Dict, List, Optional

CHECKPOINT_DIR = Path(__file__).parent / "checkpoints"


# ── Run ID ────────────────────────────────────────────────────────────────────

def run_id(items: List[str]) -> str:
    """Stable 12-char ID derived from a list of strings (channel_urls or emails)."""
    key = "|".join(sorted(s.lower().strip() for s in items))
    return hashlib.md5(key.encode()).hexdigest()[:12]


# ── File paths ────────────────────────────────────────────────────────────────

def _path(items: List[str], stage: int) -> Path:
    CHECKPOINT_DIR.mkdir(exist_ok=True)
    return CHECKPOINT_DIR / f"{run_id(items)}_stage{stage}.json"


# ── Load ──────────────────────────────────────────────────────────────────────

def load(items: List[str], stage: int) -> Optional[Dict]:
    """
    Load an existing checkpoint.
    Returns {"results": {key: value, ...}, "meta": {...}} or None.
    """
    p = _path(items, stage)
    if p.exists():
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


# ── Save (incremental) ────────────────────────────────────────────────────────

def save(items: List[str], stage: int, results: Dict, meta: Dict = None):
    """
    Overwrite the checkpoint with the latest results dict.
    Call this after every completed item for incremental saves.
    """
    p = _path(items, stage)
    data = {"results": results, "meta": meta or {}}
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f)


# ── Clear ─────────────────────────────────────────────────────────────────────

def clear(items: List[str], stage: int):
    """Delete the checkpoint file on successful completion."""
    p = _path(items, stage)
    if p.exists():
        p.unlink()


# ── Helpers ───────────────────────────────────────────────────────────────────

def count(items: List[str], stage: int) -> int:
    """Number of items already saved in the checkpoint (0 if none)."""
    cp = load(items, stage)
    if not cp:
        return 0
    return len(cp.get("results", {}))
