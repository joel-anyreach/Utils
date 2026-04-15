"""
Google Sheets reader for the dashboard.
Uses st.cache_data for performance.
"""

import os
import sys
import json
import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.constants import SHEET_TABS

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

CREDENTIALS_PATH = os.path.join(
    os.path.dirname(__file__), "..", "credentials", "service_account.json"
)


def _load_credentials():
    try:
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


def get_service_account_email() -> str:
    try:
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


def _get_gspread_client():
    import gspread
    creds = _load_credentials()
    if creds is None:
        raise RuntimeError("no_credentials")
    return gspread.authorize(creds)


@st.cache_data(ttl=300, show_spinner=False)
def read_tab(sheet_url: str, tab_name: str) -> pd.DataFrame:
    """Read a worksheet and return as DataFrame. Cached for 5 minutes."""
    from gspread_dataframe import get_as_dataframe
    client = _get_gspread_client()
    ss = client.open_by_url(sheet_url)
    ws = ss.worksheet(tab_name)
    df = get_as_dataframe(ws, evaluate_formulas=False, dtype=str)
    # Drop fully empty rows/columns
    df = df.dropna(how="all").dropna(axis=1, how="all")
    return df


def load_all_data(sheet_url: str) -> dict:
    """
    Load all dashboard data from the Google Sheet.
    Returns dict of DataFrames keyed by logical name.
    Raises RuntimeError("no_credentials") if not authenticated.
    """
    tabs = {
        "students":           SHEET_TABS["raw_students"],
        "reenrollments":      SHEET_TABS["raw_reenrollments"],
        "schools":            SHEET_TABS["raw_schools"],
        "terms":              SHEET_TABS["raw_terms"],
        "summary_enrollment": SHEET_TABS["summary_enrollment_by_sy"],
        "summary_funnel":     SHEET_TABS["summary_funnel_current"],
        "upload_log":         SHEET_TABS["upload_log"],
        # SchoolMint recruitment pipeline (optional — empty DataFrame if tab absent)
        "sm_applications":    SHEET_TABS["raw_sm_applications"],
        "sm_registrations":   SHEET_TABS["raw_sm_registrations"],
        "sm_recruitment":     SHEET_TABS["summary_sm_recruitment"],
    }

    result = {}
    for key, tab_name in tabs.items():
        try:
            result[key] = read_tab(sheet_url, tab_name)
        except Exception as e:
            result[key] = pd.DataFrame()

    return result


def get_last_upload(sheet_url: str) -> str:
    """Return the most recent upload timestamp from the upload_log tab."""
    try:
        df = read_tab(sheet_url, SHEET_TABS["upload_log"])
        if not df.empty and "upload_timestamp" in df.columns:
            ts = df["upload_timestamp"].dropna()
            if not ts.empty:
                return ts.iloc[-1]
    except Exception:
        pass
    return "Unknown"


def clear_cache():
    """Force reload of data on next access."""
    read_tab.clear()
