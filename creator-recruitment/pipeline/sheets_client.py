"""
Stage 2: Google Sheets deduplication and write.

Reads the entire leads sheet once to build in-memory dedup sets,
filters new records, then appends them in a single batch write.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import gspread
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound

from pipeline.models import ChannelRecord

if TYPE_CHECKING:
    from config import Config


# Exact column names as they appear in the existing Google Sheet header row.
# These 10 columns MUST stay in this order — other automations depend on this structure.
SHEET_HEADERS = [
    "Query",
    "ChannelId",
    "ChannelName",
    "ChannelHandle",
    "Email",
    "Status",
    "SubscriberCount",
    "Country",
    "TotalViews",
    "TotalVideosCount",
]


def _email_status(r: "ChannelRecord") -> str:
    if r.email:
        return "EMAIL_AVAILABLE"
    if r.enrichment_status == "error":
        return "ERROR"
    return "EMAIL_NOT_FOUND"


def _open_worksheet(config: "Config") -> gspread.Worksheet:
    # Authenticate via OAuth as demo@anyreach.ai.
    # First run: opens a browser window to log in — after that the token is
    # cached at ~/.config/gspread/authorized_user.json and reused automatically.
    gc = gspread.oauth(
        credentials_filename=config.google_oauth_credentials_file,
        authorized_user_filename=config.google_oauth_token_file,
    )
    try:
        sh = gc.open_by_key(config.google_sheet_id)
    except SpreadsheetNotFound:
        raise RuntimeError(
            f"Google Sheet not found: {config.google_sheet_id}\n"
            "Check GOOGLE_SHEET_ID in .env and confirm demo@anyreach.ai has Editor access."
        )

    try:
        ws = sh.worksheet(config.sheet_tab_name)
    except WorksheetNotFound:
        print(f"  Tab '{config.sheet_tab_name}' not found. Creating it with headers...")
        ws = sh.add_worksheet(title=config.sheet_tab_name, rows=1, cols=len(SHEET_HEADERS))
        ws.append_row(SHEET_HEADERS, value_input_option="USER_ENTERED")

    # Auto-create headers if the sheet is empty
    existing = ws.get_all_values()
    if not existing:
        ws.append_row(SHEET_HEADERS, value_input_option="USER_ENTERED")

    return ws


def load_existing_leads(config: "Config") -> tuple[set[str], set[str], gspread.Worksheet]:
    """
    Open the sheet once and read all existing leads.
    Returns (existing_emails, existing_channel_handles, worksheet).
    Passing the worksheet back avoids a second OAuth round-trip in append_records.
    """
    ws = _open_worksheet(config)
    records = ws.get_all_records()  # uses header row as keys

    existing_emails: set[str] = set()
    existing_handles: set[str] = set()

    for row in records:
        email  = str(row.get("Email")  or row.get("email")         or "").strip().lower()
        handle = str(row.get("ChannelHandle") or row.get("channel_handle") or "").strip().lower()
        if email:
            existing_emails.add(email)
        if handle:
            existing_handles.add(handle)

    return existing_emails, existing_handles, ws


def filter_new_records(
    records: list[ChannelRecord],
    existing_emails: set[str],
    existing_handles: set[str],
) -> list[ChannelRecord]:
    """
    Return only records not already in the sheet AND not duplicated within this batch.
    Dedup: email first, ChannelHandle fallback.
    Only records with an email are eligible (required by Instantly).
    """
    new_records = []
    batch_emails: set[str] = set()    # dedup within the current run
    batch_handles: set[str] = set()

    for r in records:
        if not r.email:
            continue  # can't add to Instantly without an email

        email_lower = r.email.lower()
        handle_lower = r.channel_handle.lower() if r.channel_handle else None

        if email_lower in existing_emails or email_lower in batch_emails:
            continue
        if handle_lower and (handle_lower in existing_handles or handle_lower in batch_handles):
            continue

        new_records.append(r)
        batch_emails.add(email_lower)
        if handle_lower:
            batch_handles.add(handle_lower)

    return new_records


def append_records(
    records: list[ChannelRecord],
    config: "Config",
    ws: gspread.Worksheet | None = None,
) -> int:
    """
    Append new records to the sheet in a single batch call.
    Pass the worksheet returned by load_existing_leads to reuse the open connection.
    Returns the number of rows written.
    """
    if not records:
        return 0

    if ws is None:
        ws = _open_worksheet(config)

    rows = []
    for r in records:
        # Exactly 10 values — must match the sheet's column order
        rows.append([
            r.query or "",
            r.channel_id or "",
            r.channel_name,
            r.channel_handle or "",
            r.email or "",
            _email_status(r),
            r.subscriber_count if r.subscriber_count is not None else "",
            r.country or "",
            r.total_views if r.total_views is not None else "",
            r.total_videos_count if r.total_videos_count is not None else "",
        ])

    try:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    except APIError as e:
        raise RuntimeError(f"Google Sheets API error while appending rows: {e}")

    return len(rows)
