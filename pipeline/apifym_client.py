"""
Stage 1: Email enrichment via Apify youtube-instant-email-scraper actor.

Calls the actor once per channel (it processes one at a time).
Extracts the @handle from the channel URL and passes it as input.
Uses the same APIFY_API_TOKEN — no separate service needed.
"""
from __future__ import annotations

import re
from typing import TYPE_CHECKING

from apify_client import ApifyClient

from pipeline.apify_client import ApifyCreditsExhaustedError, _is_credit_error
from pipeline.models import ChannelRecord

if TYPE_CHECKING:
    from config import Config


def _extract_handle(channel_url: str) -> str | None:
    """
    Extract @handle from a YouTube channel URL.
    e.g. https://youtube.com/@willprowse  →  @willprowse
    """
    match = re.search(r"(@[\w\-]+)", channel_url)
    return match.group(1) if match else None


def enrich_channels(records: list[ChannelRecord], config: "Config") -> list[ChannelRecord]:
    """
    Run email enrichment on channels with no email from Stage 0.
    Calls the Apify actor once per channel, extracts handle from URL.
    """
    to_enrich = [r for r in records if r.email is None]

    if not to_enrich:
        print("  No channels need email enrichment (all have public emails).")
        return records

    print(f"  Running email enrichment on {len(to_enrich)} channels (1 actor call each)...")

    client = ApifyClient(token=config.apify_api_token)
    found = 0

    for i, record in enumerate(to_enrich, 1):
        handle = _extract_handle(record.channel_url)
        if not handle:
            print(f"  [{i}/{len(to_enrich)}] Skipped {record.channel_url} — no @handle in URL")
            record.enrichment_status = "not_found"
            continue

        print(f"  [{i}/{len(to_enrich)}] Looking up {handle}...")

        try:
            run = client.actor(config.apify_email_actor_id).call(
                run_input={"channelHandle": handle}
            )

            status = run.get("status", "UNKNOWN")
            if status != "SUCCEEDED":
                status_msg = run.get("statusMessage", "") or run.get("errorMessage", "") or ""
                if _is_credit_error(status_msg):
                    print(f"\n  [!] Apify credits exhausted at channel {i}/{len(to_enrich)}.")
                    print(f"      {found} email(s) found so far — continuing with partial results.")
                    break
                record.enrichment_status = "error"
                print(f"    Actor finished with status '{status}' — skipping")
                continue

            items = client.dataset(run["defaultDatasetId"]).list_items().items

            if items and items[0].get("found"):
                email = items[0].get("email", "").strip().lower()
                if email:
                    record.email = email
                    record.email_source = "apify_email"
                    record.enrichment_status = "found"
                    found += 1
                    print(f"    Found: {email}")
                else:
                    record.enrichment_status = "not_found"
                    print(f"    No email found")
            else:
                record.enrichment_status = "not_found"
                print(f"    No email found")

        except ApifyCreditsExhaustedError:
            print(f"\n  [!] Apify credits exhausted at channel {i}/{len(to_enrich)}.")
            print(f"      {found} email(s) found so far — continuing with partial results.")
            break
        except Exception as e:
            err_str = str(e)
            if _is_credit_error(err_str):
                print(f"\n  [!] Apify credits exhausted at channel {i}/{len(to_enrich)}.")
                print(f"      {found} email(s) found so far — continuing with partial results.")
                break
            record.enrichment_status = "error"
            print(f"    Error: {e}")

    print(f"  Email enrichment complete: {found}/{len(to_enrich)} emails found")
    return records
