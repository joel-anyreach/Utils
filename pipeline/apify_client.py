"""
Stage 0: YouTube channel discovery via Apify.

Uses the Apify Python SDK to run a YouTube niche channel finder actor,
then maps results to ChannelRecord objects.
"""
from __future__ import annotations

import re
from typing import TYPE_CHECKING

from apify_client import ApifyClient

from pipeline.models import ChannelRecord


class ApifyCreditsExhaustedError(RuntimeError):
    """
    Raised when an Apify actor run fails due to insufficient platform credits.
    partial_records holds any channels that were successfully discovered before
    credits ran out so the rest of the pipeline can still run on them.
    """
    def __init__(self, message: str, partial_records: list | None = None):
        super().__init__(message)
        self.partial_records: list = partial_records or []


_CREDIT_KEYWORDS = ("credit", "quota", "payment", "insufficient funds", "billing", "limit exceeded")


def _is_credit_error(message: str) -> bool:
    msg = message.lower()
    return any(kw in msg for kw in _CREDIT_KEYWORDS)

if TYPE_CHECKING:
    from config import Config


# Possible field name variants across different Apify YouTube actors
_URL_KEYS      = ("customUrl", "channelUrl", "channel_url", "url", "channelLink")
_NAME_KEYS     = ("channelName", "channel_name", "name", "title")
_SUBS_KEYS     = ("subscriberCount", "subscribers", "subscriber_count", "subscribersCount")
_EMAIL_KEYS    = ("email", "businessEmail", "business_email", "contactEmail")
_NICHE_KEYS    = ("searchedKeyword", "niche", "category", "topics", "tags", "keywords")
_COUNTRY_KEYS  = ("country", "countryCode", "country_code")
_ID_KEYS       = ("channelId", "channel_id", "ucId", "id")
_HANDLE_KEYS   = ("channelHandle", "handle", "customUrl")
_VIEWS_KEYS    = ("totalViews", "viewCount", "total_views", "views")
_VIDEOS_KEYS   = ("totalVideosCount", "videoCount", "total_videos_count", "videosCount", "numberOfVideos")


def _get(item: dict, keys: tuple) -> str | None:
    for k in keys:
        val = item.get(k)
        if val and str(val).strip():
            return str(val).strip()
    return None


def _parse_subs(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    s = str(value).strip().upper().replace(",", "")
    try:
        if s.endswith("K"):
            return int(float(s[:-1]) * 1_000)
        if s.endswith("M"):
            return int(float(s[:-1]) * 1_000_000)
        return int(float(s))
    except (ValueError, TypeError):
        return None


def _normalize_url(url: str | None) -> str | None:
    if not url:
        return None
    return url.strip().rstrip("/").lower()


def _extract_handle(url: str | None) -> str | None:
    """Pull the @handle out of a YouTube URL, e.g. https://youtube.com/@willprowse → @willprowse."""
    if not url:
        return None
    m = re.search(r"(@[\w\-]+)", url)
    return m.group(1) if m else None


def _parse_int(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _map_item(item: dict) -> ChannelRecord | None:
    channel_url = _normalize_url(_get(item, _URL_KEYS))
    if not channel_url:
        return None

    channel_name = _get(item, _NAME_KEYS) or "Unknown"

    raw_subs = None
    for k in _SUBS_KEYS:
        if k in item:
            raw_subs = item[k]
            break
    subscriber_count = _parse_subs(raw_subs)

    email_raw = _get(item, _EMAIL_KEYS)
    email = email_raw.lower() if email_raw else None

    niche_raw = _get(item, _NICHE_KEYS)
    # niche can sometimes be a list; take the first element
    if isinstance(niche_raw, list):
        niche = niche_raw[0] if niche_raw else None
    else:
        niche = niche_raw

    country = _get(item, _COUNTRY_KEYS)

    # Extended fields
    channel_id = _get(item, _ID_KEYS)

    # Handle: prefer dedicated field, fall back to extracting from URL
    handle_raw = _get(item, _HANDLE_KEYS)
    if handle_raw and handle_raw.startswith("@"):
        channel_handle = handle_raw
    else:
        channel_handle = _extract_handle(channel_url)

    # Numeric counters
    raw_views = None
    for k in _VIEWS_KEYS:
        if k in item:
            raw_views = item[k]
            break
    total_views = _parse_int(raw_views)

    raw_videos = None
    for k in _VIDEOS_KEYS:
        if k in item:
            raw_videos = item[k]
            break
    total_videos_count = _parse_int(raw_videos)

    return ChannelRecord(
        channel_url=channel_url,
        channel_name=channel_name,
        subscriber_count=subscriber_count,
        email=email,
        niche=niche,
        country=country,
        email_source="apify" if email else "none",
        enrichment_status="found" if email else "pending",
        channel_id=channel_id,
        channel_handle=channel_handle,
        total_views=total_views,
        total_videos_count=total_videos_count,
    )


def discover_channels(
    query: str,
    config: "Config",
    language: str | None = None,
    strict_match: bool = False,
    min_subs: int | None = None,
    max_subs: int | None = None,
) -> list[ChannelRecord]:
    """
    Run the Apify YouTube niche channel finder actor and return ChannelRecord list.

    Filters:
      language    — ISO 639-1 code passed to YouTube API (e.g. "en")
      strict_match — only return channels whose name/description contains the exact keyword
      min_subs / max_subs — applied locally after fetching (YouTube API has no sub filter)
    """
    filter_desc = []
    if language:       filter_desc.append(f"lang={language}")
    if strict_match:   filter_desc.append("strict")
    if min_subs:       filter_desc.append(f"subs>={min_subs:,}")
    if max_subs:       filter_desc.append(f"subs<={max_subs:,}")
    suffix = f" [{', '.join(filter_desc)}]" if filter_desc else ""
    print(f"  Starting Apify actor '{config.apify_actor_id}' | query: '{query}'{suffix}")

    client = ApifyClient(token=config.apify_api_token)

    run_input = {
        "keyword":          query,
        "maxResults":       config.apify_max_results,
        "youtubeApiKey":    config.youtube_api_key,
        "strictKeywordMatch": strict_match,
    }
    if language:
        run_input["language"] = language   # passed through to YouTube relevanceLanguage

    run = client.actor(config.apify_actor_id).call(run_input=run_input)

    status     = run.get("status", "UNKNOWN")
    dataset_id = run.get("defaultDatasetId")

    # Always try to fetch the dataset — failed/aborted runs may still have partial data.
    raw_items: list[dict] = []
    if dataset_id:
        try:
            raw_items = client.dataset(dataset_id).list_items(
                limit=config.apify_max_results
            ).items
        except Exception as fetch_err:
            print(f"  Warning: Could not read dataset {dataset_id}: {fetch_err}")

    if status != "SUCCEEDED":
        status_msg = run.get("statusMessage", "") or run.get("errorMessage", "") or ""
        # Map whatever partial items we got so they can be salvaged by the caller.
        partial: list[ChannelRecord] = []
        for item in raw_items:
            r = _map_item(item)
            if r:
                r.query = query
                partial.append(r)
        if partial:
            print(f"  Salvaged {len(partial)} partial result(s) from the interrupted run.")
        if _is_credit_error(status_msg):
            raise ApifyCreditsExhaustedError(
                f"Apify credits exhausted (status={status}): {status_msg}\n"
                "Top up your credits at https://console.apify.com/billing and re-run.",
                partial_records=partial,
            )
        raise RuntimeError(
            f"Apify actor run finished with status '{status}': {status_msg or 'no details'}. "
            "Check your Apify console for details."
        )

    print(f"  Actor run succeeded. Fetching results from dataset {dataset_id}...")
    items = raw_items  # already fetched above

    records: list[ChannelRecord] = []
    skipped = 0
    for item in items:
        record = _map_item(item)
        if record:
            record.query = query   # stamp the search keyword on every record
            records.append(record)
        else:
            skipped += 1

    if skipped:
        print(f"  Skipped {skipped} items with no channel URL.")

    # ── Subscriber count filtering (post-fetch) ───────────────────────────────
    before = len(records)
    if min_subs is not None:
        records = [r for r in records if r.subscriber_count is not None and r.subscriber_count >= min_subs]
    if max_subs is not None:
        records = [r for r in records if r.subscriber_count is not None and r.subscriber_count <= max_subs]
    filtered = before - len(records)
    if filtered:
        print(f"  Filtered out {filtered} channels outside subscriber range. Remaining: {len(records)}")

    return records
