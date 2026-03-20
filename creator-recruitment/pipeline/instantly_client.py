"""
Stage 3: Push new leads to an Instantly campaign via the Instantly v2 API.

API reference: https://api.instantly.ai (v2)
Endpoint: POST /api/v2/leads/add
Auth: Authorization: Bearer {INSTANTLY_API_KEY}
"""
from __future__ import annotations

import time
from typing import TYPE_CHECKING

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from pipeline.models import ChannelRecord

if TYPE_CHECKING:
    from config import Config


BASE_URL = "https://api.instantly.ai"
BATCH_SIZE = 100  # well within the 1000-lead API max


class _RateLimitError(Exception):
    pass


def _build_lead(record: ChannelRecord, campaign_id: str) -> dict:
    """Map a ChannelRecord to an Instantly lead object."""
    # Split channel name into first/last for Instantly's name fields
    parts = (record.channel_name or "").split(None, 1)
    first_name = parts[0] if parts else record.channel_name or ""
    last_name = parts[1] if len(parts) > 1 else ""

    return {
        "email": record.email,
        "first_name": first_name,
        "last_name": last_name,
        "website": record.channel_url,
        "campaign": campaign_id,
        # NOTE: skip_if_in_workspace is set at the request body level in _post_batch,
        # not here — the Instantly v2 API expects it on the outer payload, not per-lead.
        "custom_variables": {
            "channel_url": record.channel_url,
            "subscriber_count": record.subscriber_count,
            "niche": record.niche or "",
            "country": record.country or "",
            "email_source": record.email_source,
        },
    }


@retry(
    retry=retry_if_exception_type(_RateLimitError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    reraise=True,
)
def _post_batch(leads: list[dict], api_key: str) -> dict:
    resp = requests.post(
        f"{BASE_URL}/api/v2/leads/add",
        json={
            "leads": leads,
            "skip_if_in_workspace": True,   # request-body level — this is where the v2 API reads it
        },
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        timeout=30,
    )

    if resp.status_code == 429:
        raise _RateLimitError("Instantly rate limit hit")
    if resp.status_code == 401:
        raise RuntimeError(
            "Invalid Instantly API key. Check INSTANTLY_API_KEY in .env"
        )
    if resp.status_code >= 500:
        raise RuntimeError(f"Instantly server error: {resp.status_code} — {resp.text[:200]}")

    # 400 on a batch = bad payload; log and continue (don't abort entire run)
    if resp.status_code == 400:
        print(f"  Warning: Instantly rejected a batch (400): {resp.text[:200]}")
        return {"leads_uploaded": 0, "duplicated_leads": 0, "error": resp.text[:200]}

    try:
        return resp.json()
    except Exception:
        # Non-JSON 2xx body (empty response, HTML, etc.) — treat as success with no stats
        print(f"  Warning: Instantly returned non-JSON response ({resp.status_code}): {resp.text[:100]}")
        return {"leads_uploaded": len(leads), "duplicated_leads": 0}


def upload_leads_to_campaign(records: list[ChannelRecord], config: "Config") -> dict:
    """
    Upload leads to the configured Instantly campaign in batches.
    Returns aggregate stats: leads_uploaded, duplicated_leads, errors.
    """
    if not records:
        return {"leads_uploaded": 0, "duplicated_leads": 0, "errors": 0}

    total_uploaded = 0
    total_dupes = 0
    total_errors = 0

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        leads = [_build_lead(r, config.instantly_campaign_id) for r in batch]

        try:
            result = _post_batch(leads, config.instantly_api_key)
            total_uploaded += result.get("leads_uploaded", 0)
            total_dupes += result.get("duplicated_leads", 0)
            if "error" in result:
                total_errors += 1
        except RuntimeError as e:
            # Fatal errors (401, 5xx after retries) — re-raise
            raise
        except Exception as e:
            print(f"  Warning: unexpected error for batch {i // BATCH_SIZE + 1}: {e}")
            total_errors += 1

        time.sleep(0.5)  # ~2 batches/sec, well within 100 req/sec limit

    return {
        "leads_uploaded": total_uploaded,
        "duplicated_leads": total_dupes,
        "errors": total_errors,
    }
