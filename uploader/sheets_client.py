"""
Google Sheets client for the uploader app.
Handles authentication, writing tabs, and appending the upload log.
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.constants import SHEET_TABS

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CREDENTIALS_PATH = os.path.join(
    os.path.dirname(__file__), "..", "credentials", "service_account.json"
)


def _load_credentials():
    """
    Load Google service account credentials.
    Priority:
      1. st.secrets["gcp_service_account"] (Streamlit Cloud)
      2. credentials/service_account.json (local)
    Returns google.oauth2.service_account.Credentials or None.
    """
    try:
        import streamlit as st
        if "gcp_service_account" in st.secrets:
            from google.oauth2.service_account import Credentials
            return Credentials.from_service_account_info(
                dict(st.secrets["gcp_service_account"]), scopes=SCOPES
            )
    except Exception:
        pass

    cred_path = os.path.abspath(CREDENTIALS_PATH)
    if os.path.exists(cred_path):
        from google.oauth2.service_account import Credentials
        return Credentials.from_service_account_file(cred_path, scopes=SCOPES)

    return None


def get_client():
    """Return an authenticated gspread Client, or raise RuntimeError if no credentials."""
    import gspread
    creds = _load_credentials()
    if creds is None:
        raise RuntimeError("no_credentials")
    return gspread.authorize(creds)


def get_service_account_email() -> str:
    """Return the service account email for display in setup instructions."""
    try:
        import streamlit as st
        if "gcp_service_account" in st.secrets:
            return st.secrets["gcp_service_account"].get("client_email", "")
    except Exception:
        pass
    cred_path = os.path.abspath(CREDENTIALS_PATH)
    if os.path.exists(cred_path):
        with open(cred_path) as f:
            data = json.load(f)
        return data.get("client_email", "")
    return ""


def open_or_create_spreadsheet(client, sheet_name_or_url: str):
    """
    Open an existing spreadsheet by URL.
    The sheet must already exist in the user's Google Drive and be shared
    with the service account as Editor.
    Returns (spreadsheet, was_created).
    Raises ValueError with a clear message if URL is not provided.
    """
    import gspread

    url = sheet_name_or_url.strip()
    if url.startswith("http"):
        return client.open_by_url(url), False

    raise ValueError(
        "Please paste the full Google Sheet URL (starting with https://docs.google.com/spreadsheets/...).\n\n"
        "The sheet must be created in your own Google Drive first, then shared with the service account as Editor. "
        "Service accounts cannot create sheets in your Drive — they can only write to sheets you share with them."
    )


def _ensure_tab(spreadsheet, tab_name: str):
    """Return a worksheet, creating it if it doesn't exist."""
    import gspread
    try:
        return spreadsheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=tab_name, rows=50000, cols=50)


def write_tab(spreadsheet, tab_key: str, df: pd.DataFrame, progress_cb=None) -> int:
    """
    Overwrite a tab completely with the DataFrame.
    tab_key is a key in SHEET_TABS (e.g., "raw_students").
    Returns rows written.
    """
    from gspread_dataframe import set_with_dataframe

    tab_name = SHEET_TABS.get(tab_key, tab_key)
    ws = _ensure_tab(spreadsheet, tab_name)
    ws.clear()

    df_clean = df.fillna("").astype(str)
    set_with_dataframe(ws, df_clean, include_index=False, resize=True)

    if progress_cb:
        progress_cb(tab_name, len(df_clean))

    return len(df_clean)


def append_upload_log(spreadsheet, log_row: dict):
    """Append one row to the upload_log tab (never overwrites)."""
    tab_name = SHEET_TABS["upload_log"]
    ws = _ensure_tab(spreadsheet, tab_name)

    existing = ws.get_all_values()
    headers = [
        "upload_timestamp", "students_rows", "reenrollments_rows",
        "schools_rows", "terms_rows", "summary_enrollment_rows",
        "summary_funnel_rows", "sm_apps_rows", "sm_regs_rows",
        "sm_recruitment_rows", "hs_contacts_rows", "hs_funnel_summary_rows",
        "warnings_count", "notes",
    ]
    if not existing:
        ws.append_row(headers)

    row = [log_row.get(h, "") for h in headers]
    ws.append_row(row)


def read_tab(sheet_url: str, tab_name: str) -> pd.DataFrame:
    """
    Read an existing tab from Google Sheets and return as DataFrame.
    Returns an empty DataFrame if the tab is not found or any error occurs.
    """
    try:
        from gspread_dataframe import get_as_dataframe
        client = get_client()
        ss = client.open_by_url(sheet_url)
        ws = ss.worksheet(tab_name)
        df = get_as_dataframe(ws, evaluate_formulas=False, dtype=str)
        return df.dropna(how="all").dropna(axis=1, how="all")
    except Exception:
        return pd.DataFrame()


def push_all_data(
    normalized: dict,
    sheet_name_or_url: str,
    progress_cb=None,
) -> dict:
    """
    Push all normalized tables to Google Sheets.
    Returns dict with sheet_url and row counts.
    """
    client = get_client()
    ss, created = open_or_create_spreadsheet(client, sheet_name_or_url)

    results = {"sheet_url": ss.url, "tabs_written": {}}

    tab_map = [
        ("raw_students",         normalized["students"]),
        ("raw_reenrollments",    normalized["reenrollments"]),
        ("raw_schools",          normalized["schools"]),
        ("raw_terms",            normalized["terms"]),
        ("summary_enrollment_by_sy", normalized["summary_enrollment"]),
        ("summary_funnel_current",   normalized["summary_funnel"]),
    ]

    # Conditionally add SchoolMint tabs if data was provided
    if not normalized.get("sm_applications", pd.DataFrame()).empty:
        tab_map.append(("raw_sm_applications", normalized["sm_applications"]))
    if not normalized.get("sm_registrations", pd.DataFrame()).empty:
        tab_map.append(("raw_sm_registrations", normalized["sm_registrations"]))
    if not normalized.get("sm_recruitment", pd.DataFrame()).empty:
        tab_map.append(("summary_sm_recruitment", normalized["sm_recruitment"]))

    # Conditionally add HubSpot tabs if data was provided
    if not normalized.get("hs_contacts", pd.DataFrame()).empty:
        tab_map.append(("enrollment_funnel", normalized["hs_contacts"]))
    if not normalized.get("hs_funnel_summary", pd.DataFrame()).empty:
        tab_map.append(("funnel_summary", normalized["hs_funnel_summary"]))

    for tab_key, df in tab_map:
        n = write_tab(ss, tab_key, df, progress_cb=progress_cb)
        results["tabs_written"][SHEET_TABS[tab_key]] = n

    # Append to upload log
    append_upload_log(ss, {
        "upload_timestamp":         normalized["upload_timestamp"],
        "students_rows":            len(normalized["students"]),
        "reenrollments_rows":       len(normalized["reenrollments"]),
        "schools_rows":             len(normalized["schools"]),
        "terms_rows":               len(normalized["terms"]),
        "summary_enrollment_rows":  len(normalized["summary_enrollment"]),
        "summary_funnel_rows":      len(normalized["summary_funnel"]),
        "sm_apps_rows":             len(normalized.get("sm_applications", pd.DataFrame())),
        "sm_regs_rows":             len(normalized.get("sm_registrations", pd.DataFrame())),
        "sm_recruitment_rows":      len(normalized.get("sm_recruitment", pd.DataFrame())),
        "hs_contacts_rows":         len(normalized.get("hs_contacts", pd.DataFrame())),
        "hs_funnel_summary_rows":   len(normalized.get("hs_funnel_summary", pd.DataFrame())),
        "warnings_count":           len(normalized["all_warnings"]),
        "notes":                    "",
    })
    results["tabs_written"][SHEET_TABS["upload_log"]] = "appended"

    return results
