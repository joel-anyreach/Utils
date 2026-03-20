"""
YouTube Data API v3 — direct playlist and channel URL discovery.

Used as an alternative to Apify keyword search when the user already knows:
  • a specific playlist URL  → extract all unique channel creators from it
  • specific channel URL(s)  → look up those channels directly for enrichment

Requires only the YOUTUBE_API_KEY already in .env — no Apify credits consumed.
"""
from __future__ import annotations

import re
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlparse

import requests

from pipeline.models import ChannelRecord

if TYPE_CHECKING:
    from config import Config

YT_API_BASE = "https://www.googleapis.com/youtube/v3"


# ── Low-level API helper ───────────────────────────────────────────────────────

def _yt_get(endpoint: str, params: dict) -> dict:
    """Single GET to the YouTube Data API v3. Raises RuntimeError on quota/auth issues."""
    resp = requests.get(f"{YT_API_BASE}/{endpoint}", params=params, timeout=15)
    if resp.status_code == 403:
        try:
            reason = resp.json().get("error", {}).get("errors", [{}])[0].get("reason", "")
        except Exception:
            reason = ""
        if reason in ("quotaExceeded", "rateLimitExceeded"):
            raise RuntimeError(
                "YouTube Data API v3 daily quota exceeded. "
                "Check https://console.cloud.google.com/ → APIs → YouTube Data API v3."
            )
        raise RuntimeError(f"YouTube API 403 Forbidden: {resp.text[:300]}")
    if resp.status_code == 400:
        raise RuntimeError(f"YouTube API 400 Bad Request: {resp.text[:300]}")
    resp.raise_for_status()
    return resp.json()


# ── URL parsers ────────────────────────────────────────────────────────────────

def extract_playlist_id(url: str) -> str | None:
    """
    Pull the playlist ID out of any YouTube URL containing ?list=...
    e.g. https://youtube.com/playlist?list=PLxxxxxx
         https://youtube.com/watch?v=abc&list=PLxxxxxx
    """
    qs = parse_qs(urlparse(url).query)
    lst = qs.get("list", [])
    return lst[0] if lst else None


def extract_channel_identifier(url: str) -> tuple[str, str] | None:
    """
    Parse a YouTube channel URL and return (lookup_type, value) or None.

    Supported formats:
      https://youtube.com/@handle           → ("handle",  "@handle")
      https://youtube.com/channel/UCxxxx    → ("id",      "UCxxxx")
      https://youtube.com/c/Name            → ("username", "Name")   (legacy)
      https://youtube.com/user/Name         → ("username", "Name")   (legacy)
    """
    url = url.strip().rstrip("/")

    m = re.search(r"youtube\.com/@([\w\-]+)", url)
    if m:
        return ("handle", f"@{m.group(1)}")

    m = re.search(r"youtube\.com/channel/(UC[\w\-]+)", url)
    if m:
        return ("id", m.group(1))

    m = re.search(r"youtube\.com/(?:c|user)/([\w\-]+)", url)
    if m:
        return ("username", m.group(1))

    return None


def is_playlist_url(url: str) -> bool:
    return extract_playlist_id(url) is not None


def is_channel_url(url: str) -> bool:
    return extract_channel_identifier(url) is not None


# ── YouTube → ChannelRecord mapper ────────────────────────────────────────────

def _item_to_record(item: dict, query_label: str = "") -> ChannelRecord:
    """Convert a YouTube channels.list response item to a ChannelRecord."""
    snippet = item.get("snippet", {})
    stats   = item.get("statistics", {})

    channel_id   = item.get("id", "")
    channel_name = snippet.get("title", "Unknown")
    custom_url   = snippet.get("customUrl", "")   # YouTube returns @handle here
    country      = snippet.get("country") or None

    if custom_url:
        handle      = custom_url if custom_url.startswith("@") else f"@{custom_url}"
        channel_url = f"https://youtube.com/{handle}".lower()
    elif channel_id:
        handle      = None
        channel_url = f"https://youtube.com/channel/{channel_id}".lower()
    else:
        handle      = None
        channel_url = ""

    def _safe_int(val) -> int | None:
        try:
            return int(val)
        except (TypeError, ValueError):
            return None

    return ChannelRecord(
        channel_url=channel_url,
        channel_name=channel_name,
        subscriber_count=_safe_int(stats.get("subscriberCount")),
        country=country,
        query=query_label,
        channel_id=channel_id,
        channel_handle=handle,
        total_views=_safe_int(stats.get("viewCount")),
        total_videos_count=_safe_int(stats.get("videoCount")),
        email_source="none",
        enrichment_status="pending",
    )


# ── Batch channel metadata fetch ──────────────────────────────────────────────

def _fetch_by_ids(channel_ids: list[str], api_key: str) -> list[dict]:
    """Batch-fetch snippet + statistics for up to 50 channel IDs per request."""
    out = []
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i : i + 50]
        data  = _yt_get("channels", {
            "part":       "snippet,statistics",
            "id":         ",".join(batch),
            "maxResults": 50,
            "key":        api_key,
        })
        out.extend(data.get("items", []))
    return out


def _apply_sub_filter(
    records: list[ChannelRecord],
    min_subs: int | None,
    max_subs: int | None,
) -> list[ChannelRecord]:
    before = len(records)
    if min_subs is not None:
        records = [r for r in records if r.subscriber_count is not None and r.subscriber_count >= min_subs]
    if max_subs is not None:
        records = [r for r in records if r.subscriber_count is not None and r.subscriber_count <= max_subs]
    removed = before - len(records)
    if removed:
        print(f"  Filtered out {removed} channel(s) outside subscriber range. Remaining: {len(records)}")
    return records


# ── Public discovery functions ────────────────────────────────────────────────

def discover_from_playlist(
    playlist_url: str,
    config: "Config",
    min_subs: int | None = None,
    max_subs: int | None = None,
) -> list[ChannelRecord]:
    """
    Extract all unique channel creators from a YouTube playlist.

    Steps:
      1. Paginate through playlistItems.list to collect unique channel IDs.
      2. Batch-fetch channel snippet + statistics via channels.list.
      3. Apply subscriber range filter.
    """
    playlist_id = extract_playlist_id(playlist_url)
    if not playlist_id:
        raise ValueError(f"Could not extract playlist ID from: {playlist_url!r}")

    api_key = config.youtube_api_key
    print(f"  Playlist ID: {playlist_id}")

    # 1. Collect unique channel IDs (paginated)
    seen_ids: dict[str, str] = {}   # id → title (for logging)
    page_token = None
    pages = 0

    while True:
        params: dict = {
            "part":       "snippet",
            "playlistId": playlist_id,
            "maxResults": 50,
            "key":        api_key,
        }
        if page_token:
            params["pageToken"] = page_token

        data = _yt_get("playlistItems", params)
        pages += 1

        for item in data.get("items", []):
            sn  = item.get("snippet", {})
            cid = sn.get("videoOwnerChannelId")
            if cid and cid not in seen_ids:
                seen_ids[cid] = sn.get("videoOwnerChannelTitle", "")

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    print(f"  Found {len(seen_ids)} unique channel(s) across {pages} page(s)")
    if not seen_ids:
        return []

    # 2. Fetch full channel metadata in batches
    query_label = f"playlist:{playlist_id}"
    items       = _fetch_by_ids(list(seen_ids.keys()), api_key)
    records     = [_item_to_record(it, query_label) for it in items]
    print(f"  Channel metadata fetched: {len(records)}")

    # 3. Subscriber filter
    return _apply_sub_filter(records, min_subs, max_subs)


def discover_from_channel_urls(
    channel_urls: list[str],
    config: "Config",
    min_subs: int | None = None,
    max_subs: int | None = None,
) -> list[ChannelRecord]:
    """
    Look up channel metadata for a list of YouTube channel URLs directly.

    Supports @handle, /channel/UCxxxx, /c/Name, /user/Name formats.
    Results go through normal email enrichment, sheet dedup, and Instantly upload.
    """
    api_key = config.youtube_api_key

    id_list:       list[str] = []
    handle_list:   list[str] = []
    username_list: list[str] = []

    for raw_url in channel_urls:
        parsed = extract_channel_identifier(raw_url.strip())
        if not parsed:
            print(f"  Warning: unrecognised channel URL: {raw_url!r} — skipping")
            continue
        kind, value = parsed
        if kind == "id":
            id_list.append(value)
        elif kind == "handle":
            handle_list.append(value)
        elif kind == "username":
            username_list.append(value)

    all_items: list[dict] = []

    # Batch by ID (up to 50 per call)
    if id_list:
        all_items.extend(_fetch_by_ids(id_list, api_key))

    # By handle — YouTube API only accepts one forHandle at a time
    for handle in handle_list:
        data = _yt_get("channels", {
            "part":      "snippet,statistics",
            "forHandle": handle,
            "key":       api_key,
        })
        all_items.extend(data.get("items", []))

    # Legacy /c/ or /user/ — forUsername
    for username in username_list:
        data = _yt_get("channels", {
            "part":        "snippet,statistics",
            "forUsername": username,
            "key":         api_key,
        })
        all_items.extend(data.get("items", []))

    records = [_item_to_record(it, "direct") for it in all_items]
    print(f"  Channel metadata fetched: {len(records)} of {len(channel_urls)} URL(s)")
    return _apply_sub_filter(records, min_subs, max_subs)
