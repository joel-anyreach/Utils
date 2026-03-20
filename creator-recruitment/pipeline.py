"""
Anyreach Creator Recruitment Pipeline
======================================
Fully automated: YouTube discovery → email enrichment
                 → Google Sheets dedup + write → Instantly campaign upload

Discovery sources (pick exactly one):
  --query         Keyword/niche search via Apify (uses Apify credits)
  --playlist-url  Extract all channel creators from a YouTube playlist (YouTube API, no Apify)
  --channel-url   Enrich one or more specific YouTube channel URLs directly (YouTube API, no Apify)

Common options:
  --max-results   Max channels to fetch from Apify (keyword mode only; default: from .env)
  --dry-run       Run all read/enrichment stages but skip Sheet + Instantly writes
  --export-csv    Write enriched results to CSV after enrichment (before Sheet dedup)
  --language      Language code for Apify search, e.g. 'en', 'es' (keyword mode only)
  --strict-match  Strict keyword match (keyword mode only)
  --min-subs      Minimum subscriber count (all modes)
  --max-subs      Maximum subscriber count (all modes)
"""
import argparse
import csv
import sys
from pathlib import Path

from config import load_config
from pipeline.apify_client import ApifyCreditsExhaustedError, discover_channels
from pipeline.apifym_client import enrich_channels
from pipeline.email_utils import is_role_email
from pipeline.instantly_client import upload_leads_to_campaign
from pipeline.models import ChannelRecord
from pipeline.reoon_client import BLOCKED_STATUSES, ReoonCreditsExhaustedError, verify_emails
from pipeline.sheets_client import append_records, filter_new_records, load_existing_leads
from pipeline.youtube_client import discover_from_channel_urls, discover_from_playlist


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Anyreach creator recruitment pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # ── Discovery source (mutually exclusive) ─────────────────────────────────
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--query",        default=None, help="YouTube keyword/niche (Apify discovery)")
    src.add_argument("--playlist-url", default=None, metavar="URL",
                     help="YouTube playlist URL — extract all channel creators from it")
    src.add_argument("--channel-url",  dest="channel_urls", action="append", metavar="URL",
                     help="YouTube channel URL to enrich directly (can be repeated)")

    # ── Common options ─────────────────────────────────────────────────────────
    parser.add_argument("--max-results", type=int, default=None,
                        help="Max Apify results (keyword mode only; overrides .env)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip all writes (Sheet + Instantly). Enrichment still runs.")
    parser.add_argument("--export-csv", default=None, metavar="PATH",
                        help="Write enriched results to CSV after enrichment.")

    # ── Filters (not all apply to every mode) ─────────────────────────────────
    parser.add_argument("--language",     default=None,
                        help="Language code for Apify, e.g. 'en', 'es' (keyword mode only)")
    parser.add_argument("--strict-match", action="store_true",
                        help="Strict keyword match (keyword mode only)")
    parser.add_argument("--min-subs",     type=int, default=None,
                        help="Minimum subscriber count (all modes)")
    parser.add_argument("--max-subs",     type=int, default=None,
                        help="Maximum subscriber count (all modes)")
    parser.add_argument("--verify-emails", action="store_true",
                        help="Verify emails via Reoon API before Instantly upload (requires REOON_API_KEY in .env)")
    return parser.parse_args()


def _export_csv(records: list[ChannelRecord], path_str: str, label: str) -> None:
    export_path = Path(path_str)
    export_path.parent.mkdir(parents=True, exist_ok=True)
    enriched = [r for r in records if r.email]
    with open(export_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "query", "channel_id", "channel_name", "channel_handle",
            "email", "status", "subscriber_count", "country",
            "total_views", "total_videos_count",
            "niche", "channel_url", "email_source",
            "is_role_email", "reoon_status",
        ])
        writer.writeheader()
        for r in enriched:
            status = "EMAIL_AVAILABLE" if r.email else (
                "ERROR" if r.enrichment_status == "error" else "EMAIL_NOT_FOUND"
            )
            writer.writerow({
                "query":              r.query or label,
                "channel_id":         r.channel_id or "",
                "channel_name":       r.channel_name,
                "channel_handle":     r.channel_handle or "",
                "email":              r.email or "",
                "status":             status,
                "subscriber_count":   r.subscriber_count if r.subscriber_count is not None else "",
                "country":            r.country or "",
                "total_views":        r.total_views if r.total_views is not None else "",
                "total_videos_count": r.total_videos_count if r.total_videos_count is not None else "",
                "niche":              r.niche or "",
                "channel_url":        r.channel_url,
                "email_source":       r.email_source,
                "is_role_email":      "TRUE" if r.is_role_email else "FALSE",
                "reoon_status":       r.reoon_status or "",
            })
    print(f"  Exported {len(enriched)} enriched record(s) to: {export_path}\n")


def main() -> int:
    args = parse_args()

    # ── Config ────────────────────────────────────────────────────────────────
    try:
        config = load_config()
    except EnvironmentError as e:
        print(f"[ERROR] Configuration failed:\n{e}", file=sys.stderr)
        return 1

    if args.max_results is not None:
        config = config.__class__(**{**config.__dict__, "apify_max_results": args.max_results})

    if args.dry_run:
        print("*** DRY RUN — no data will be written to Sheet or Instantly ***\n")

    credits_exhausted = False   # set True when Apify credits ran out mid-run

    # ── Stage 0: Channel discovery ────────────────────────────────────────────
    records: list[ChannelRecord] = []

    if args.query:
        # ── A: Apify keyword search ───────────────────────────────────────────
        source_label = args.query
        print(f"[0/3] Discovering YouTube channels via Apify (keyword: '{args.query}')...")
        try:
            records = discover_channels(
                args.query, config,
                language=args.language,
                strict_match=args.strict_match,
                min_subs=args.min_subs,
                max_subs=args.max_subs,
            )
        except ApifyCreditsExhaustedError as e:
            print(f"\n[!] APIFY CREDITS EXHAUSTED", file=sys.stderr)
            print(f"    {e}", file=sys.stderr)
            if e.partial_records:
                records = e.partial_records
                print(
                    f"\n  Salvaged {len(records)} partial result(s) — "
                    "continuing pipeline with what was found before credits ran out.\n"
                )
                credits_exhausted = True
            else:
                print(
                    "\n  No results were salvaged. "
                    "Top up your credits at https://console.apify.com/billing and re-run.",
                    file=sys.stderr,
                )
                return 2
        except Exception as e:
            print(f"[ERROR] Apify stage failed: {e}", file=sys.stderr)
            return 1

    elif args.playlist_url:
        # ── B: YouTube playlist scraping ──────────────────────────────────────
        source_label = args.playlist_url
        print(f"[0/3] Discovering channels from YouTube playlist...")
        try:
            records = discover_from_playlist(
                args.playlist_url, config,
                min_subs=args.min_subs,
                max_subs=args.max_subs,
            )
        except Exception as e:
            print(f"[ERROR] Playlist discovery failed: {e}", file=sys.stderr)
            return 1

    else:
        # ── C: Direct channel URL lookup ──────────────────────────────────────
        source_label = args.channel_urls[0] if args.channel_urls else "direct"
        print(f"[0/3] Looking up {len(args.channel_urls)} channel URL(s) directly...")
        try:
            records = discover_from_channel_urls(
                args.channel_urls, config,
                min_subs=args.min_subs,
                max_subs=args.max_subs,
            )
        except Exception as e:
            print(f"[ERROR] Channel URL lookup failed: {e}", file=sys.stderr)
            return 1

    apify_with_email = sum(1 for r in records if r.email_source == "apify")
    print(f"  Found {len(records)} channel(s) | {apify_with_email} already have a public email\n")

    if not records:
        print("  Nothing to process. Exiting.")
        return 0

    # ── Stage 1: Email enrichment ─────────────────────────────────────────────
    print("[1/3] Enriching channels via Apify email actor...")
    try:
        records = enrich_channels(records, config)
    except Exception as e:
        print(f"[ERROR] Email enrichment failed: {e}", file=sys.stderr)
        return 1

    apifym_found   = sum(1 for r in records if r.email_source == "apify_email")
    total_with_email = sum(1 for r in records if r.email)
    print(f"  Emails found: {apifym_found} new | Total with email: {total_with_email}/{len(records)}\n")

    # ── Role email flagging ────────────────────────────────────────────────────
    for r in records:
        if r.email:
            r.is_role_email = is_role_email(r.email)
    role_email_count = sum(1 for r in records if r.is_role_email)
    print(f"  Role emails flagged: {role_email_count}\n")

    # ── CSV Export (after enrichment, before Sheet dedup) ─────────────────────
    if args.export_csv:
        _export_csv(records, args.export_csv, source_label)

    # ── Stage 2: Google Sheets dedup + write ──────────────────────────────────
    print("[2/3] Deduplicating against Google Sheet...")
    try:
        existing_emails, existing_handles, ws = load_existing_leads(config)
        new_records = filter_new_records(records, existing_emails, existing_handles)
    except Exception as e:
        print(f"[ERROR] Google Sheets read failed: {e}", file=sys.stderr)
        return 1

    print(f"  New leads (not in sheet): {len(new_records)} of {total_with_email} enriched")

    written = 0
    if not args.dry_run:
        try:
            written = append_records(new_records, config, ws=ws)
            print(f"  Written to Google Sheet: {written} row(s)")
        except Exception as e:
            print(f"[ERROR] Google Sheets write failed: {e}", file=sys.stderr)
            return 1
    else:
        print(f"  [DRY RUN] Would write {len(new_records)} row(s) to sheet")
    print()

    # ── Stage 2.5: Reoon email verification (optional) ────────────────────────
    upload_records = new_records
    reoon_blocked = 0
    reoon_verified = 0
    reoon_credits_exhausted = False

    if args.verify_emails:
        if not config.reoon_api_key:
            print("  [WARNING] --verify-emails set but REOON_API_KEY is missing in .env. Skipping verification.\n")
        else:
            print("[2.5/3] Verifying emails via Reoon...")
            try:
                verify_emails(new_records, config.reoon_api_key)
            except ReoonCreditsExhaustedError as e:
                reoon_credits_exhausted = True
                print(f"\n  [!] Reoon credits exhausted after {e.partial_count} verification(s).")
                print("      Unverified emails will be uploaded as-is.\n")
            reoon_blocked  = sum(1 for r in new_records if r.reoon_status in BLOCKED_STATUSES)
            reoon_verified = sum(1 for r in new_records if r.reoon_status is not None)
            upload_records = [r for r in new_records if r.reoon_status not in BLOCKED_STATUSES]
            unverified     = len(new_records) - reoon_verified
            print(
                f"  Verified: {reoon_verified} | Blocked (invalid/disposable): {reoon_blocked}"
                + (f" | Unverified (pass-through): {unverified}" if unverified else "")
                + "\n"
            )

    # ── Stage 3: Instantly campaign upload ────────────────────────────────────
    print("[3/3] Uploading leads to Instantly campaign...")
    uploaded = 0
    dupes    = 0
    if not args.dry_run:
        try:
            stats    = upload_leads_to_campaign(upload_records, config)
            uploaded = stats["leads_uploaded"]
            dupes    = stats["duplicated_leads"]
            print(f"  Uploaded: {uploaded} | Workspace duplicates skipped: {dupes}")
        except Exception as e:
            print(f"[ERROR] Instantly upload failed: {e}", file=sys.stderr)
            return 1
    else:
        print(f"  [DRY RUN] Would upload {len(upload_records)} leads to campaign {config.instantly_campaign_id}")
    print()

    # ── Summary ───────────────────────────────────────────────────────────────
    print("=" * 45)
    print("PIPELINE SUMMARY")
    print("=" * 45)
    print(f"  Channels discovered:            {len(records)}")
    print(f"  Emails from Apify (public):     {apify_with_email}")
    print(f"  Emails from enrichment:         {apifym_found}")
    print(f"  Total with email:               {total_with_email}")
    print(f"  Role emails flagged:            {role_email_count}")
    print(f"  New (not in sheet):             {len(new_records)}")
    if args.verify_emails and config.reoon_api_key:
        print(f"  Reoon verified:                 {reoon_verified}")
        print(f"  Reoon blocked (inv/disp):       {reoon_blocked}")
    if args.dry_run:
        print(f"  Written to Sheet:               [DRY RUN]")
        print(f"  Pushed to Instantly:            [DRY RUN]")
    else:
        print(f"  Written to Sheet:               {written}")
        print(f"  Pushed to Instantly:            {uploaded}")
    if credits_exhausted:
        print()
        print("  ⚡ NOTE: Apify credits ran out mid-run. Results above are partial.")
        print("     Top up at https://console.apify.com/billing, then re-run.")
    if reoon_credits_exhausted:
        print()
        print("  ⚡ NOTE: Reoon credits ran out mid-verification. Unverified emails were uploaded as-is.")
        print("     Top up at https://reoon.com to verify the remaining emails.")
    print("=" * 45)

    return 2 if credits_exhausted else 0


if __name__ == "__main__":
    sys.exit(main())
