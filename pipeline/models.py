from dataclasses import dataclass
from typing import Optional


@dataclass
class ChannelRecord:
    channel_url: str           # canonical dedup key (lowercased, stripped)
    channel_name: str
    subscriber_count: Optional[int] = None
    email: Optional[str] = None       # populated by Apify or email actor
    niche: Optional[str] = None
    country: Optional[str] = None
    email_source: str = "none"          # "apify" | "apify_email" | "none"
    enrichment_status: str = "pending"  # pending | found | not_found | error
    # Extended fields (aligned with Google Sheet schema)
    query: Optional[str] = None               # search keyword that produced this channel
    channel_id: Optional[str] = None          # YouTube channel ID (UCxxxxxxx)
    channel_handle: Optional[str] = None      # @handle extracted from URL
    total_views: Optional[int] = None         # lifetime channel view count
    total_videos_count: Optional[int] = None  # total published videos
    # Email quality flags
    is_role_email: bool = False               # True if local-part matches a known role prefix (info@, contact@, etc.)
    reoon_status: Optional[str] = None        # "safe"|"valid"|"invalid"|"disposable"|"accept_all"|"unknown"|"error"|None
