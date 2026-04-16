"""
App 1: Ingenium Re-enrollment Data Uploader
Upload 4 PowerSchool CSV exports → normalize → push to Google Sheets
"""

import sys
import os
from datetime import datetime
import streamlit as st
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from normalizer import normalize_all
import sheets_client as sc
from shared.constants import SHEET_TABS

st.set_page_config(
    page_title="Ingenium — Data Uploader",
    page_icon="📤",
    layout="wide",
)

# ── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("📤 Data Uploader")
    st.markdown("---")

    # Connection status
    email = sc.get_service_account_email()
    if email:
        st.success(f"**Credentials loaded**\n\n{email}")
    else:
        st.error("**No credentials found**")
        with st.expander("Setup instructions", expanded=True):
            st.markdown("""
**How to set up Google credentials:**

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Create or select a project (e.g. `IngeniumReenroll`)
3. Enable **Google Sheets API** and **Google Drive API**
4. Navigate to **IAM & Admin → Service Accounts**
5. Click **Create Service Account**, fill in a name, click Done
6. Click the service account → **Keys** tab → **Add Key → JSON**
7. Download the JSON file
8. Place it at:
   ```
   credentials/service_account.json
   ```
9. Share your Google Sheet (or Drive folder) with the
   `client_email` value from the JSON, as **Editor**
10. Reload this page
""")

    st.markdown("---")
    st.markdown("**Google Sheet URL**")
    st.caption(
        "Paste the URL of a Sheet you already created in your Google Drive. "
        "The sheet must be shared with the service account above as **Editor**."
    )
    sheet_input = st.text_input(
        "Google Sheet URL",
        placeholder="https://docs.google.com/spreadsheets/d/...",
        label_visibility="collapsed",
        help="Create the Sheet in your Google Drive first, share it with the service account as Editor, then paste the URL here.",
    )
    with st.expander("How to set this up"):
        if email:
            st.markdown(f"""
1. Go to [Google Sheets](https://sheets.google.com) and create a new blank spreadsheet
2. Name it **Ingenium_Reenrollment_Data**
3. Click **Share** → paste this email → set to **Editor** → Send:
   ```
   {email}
   ```
4. Copy the URL from your browser and paste it above
""")
        else:
            st.markdown("Add credentials first (see instructions above), then return here.")

# ── Main content ──────────────────────────────────────────────────────────────

st.title("Ingenium Re-enrollment — Data Uploader")
st.markdown(
    "Upload the four PowerSchool CSV exports. The app will normalize them, "
    "combine them, and push the results to Google Sheets."
)

# ─── Step 1: Upload files ────────────────────────────────────────────────────

st.markdown("## Step 1 — Upload CSV Files")
st.markdown("**PowerSchool exports** (Students and ReEnrollments are required):")

col1, col2 = st.columns(2)
with col1:
    students_file = st.file_uploader(
        "Students export (`Students_export_*.csv`)",
        type=["csv", "xlsx"],
        key="students",
    )
    reenroll_file = st.file_uploader(
        "ReEnrollments export (`ReEnrollments_export_*.csv`)",
        type=["csv", "xlsx"],
        key="reenroll",
    )
with col2:
    schools_file = st.file_uploader(
        "Schools export — optional (`Schools_export*.csv`)",
        type=["csv", "xlsx"],
        key="schools",
    )
    terms_file = st.file_uploader(
        "Terms export — optional (`Terms_export*.csv`)",
        type=["csv", "xlsx"],
        key="terms",
    )

st.markdown("**SchoolMint exports** (optional — enables Recruitment Pipeline tab in dashboard):")

col_sm1, col_sm2 = st.columns(2)
with col_sm1:
    sm_apps_file = st.file_uploader(
        "SchoolMint Applications export (`schoolmint applications.csv`) — optional",
        type=["csv", "xlsx"],
        key="sm_apps",
    )
with col_sm2:
    sm_regs_file = st.file_uploader(
        "SchoolMint Registrations export (`schoolmint registrations.csv`) — optional",
        type=["csv", "xlsx"],
        key="sm_regs",
    )

_today = datetime.today()
_cur_sy = _today.year if _today.month >= 8 else _today.year - 1
_sy_options = [f"{y}-{y+1}" for y in range(_cur_sy - 2, _cur_sy + 3)]
_default_sy = f"{_cur_sy + 1}-{_cur_sy + 2}"

sm_school_year = st.selectbox(
    "School year for this SchoolMint upload",
    options=_sy_options,
    index=_sy_options.index(_default_sy),
    key="sm_school_year",
    help="Stamps this school year on every row in the SM raw tabs and recruitment summary. "
         "Overrides the school_year column in the CSV if present.",
)

sm_uploaded = sm_apps_file is not None or sm_regs_file is not None
if not sm_uploaded:
    st.caption(
        "SchoolMint files not uploaded — the Recruitment Pipeline tab will be unavailable in the dashboard."
    )

st.markdown("**HubSpot Contacts export** (required for Enrollment Funnel — needs SM + PS files above):")

hs_file = st.file_uploader(
    "HubSpot Contacts export (`contacts_export*.csv` / `.xlsx`)",
    type=["csv", "xlsx"],
    key="hs_contacts",
)
hs_uploaded = hs_file is not None
funnel_uploaded = all([hs_file, sm_apps_file, sm_regs_file])
if hs_uploaded and not funnel_uploaded:
    missing_funnel = [n for n, f in [("SM Applications", sm_apps_file), ("SM Registrations", sm_regs_file)] if f is None]
    st.caption(
        f"Enrollment Funnel requires: {', '.join(missing_funnel)}. "
        "Upload them above to enable funnel matching."
    )
elif not hs_uploaded:
    st.caption(
        "HubSpot file not uploaded — if the Enrollment Funnel tab already exists in your Sheet, "
        "match columns will be refreshed automatically using the latest PS/SM data."
    )

st.markdown("---")

# Only Students + ReEnrollments are required; Schools + Terms fall back to Google Sheet
all_uploaded = all([students_file, reenroll_file])

if not all_uploaded:
    missing = [
        name for name, f in [
            ("Students", students_file), ("ReEnrollments", reenroll_file),
        ] if f is None
    ]
    st.info(f"Waiting for: {', '.join(missing)}")
    opt_missing = [n for n, f in [("Schools", schools_file), ("Terms", terms_file)] if f is None]
    if opt_missing:
        st.caption(
            f"Optional not uploaded: {', '.join(opt_missing)}. "
            "Existing data from the Google Sheet will be used when available."
        )
    st.stop()

# ─── Step 2: Normalize & Validate ────────────────────────────────────────────

st.markdown("---")
st.markdown("## Step 2 — Validate & Preview")

# Fetch existing schools/terms/funnel from Google Sheet when the files weren't uploaded
existing_schools_df = pd.DataFrame()
existing_terms_df = pd.DataFrame()
existing_funnel_df = pd.DataFrame()
if sheet_input.strip():
    with st.spinner("Fetching existing data from Google Sheet…"):
        if schools_file is None:
            existing_schools_df = sc.read_tab(sheet_input.strip(), SHEET_TABS["raw_schools"])
            if existing_schools_df.empty:
                st.caption("ℹ️ No existing schools data found in Sheet — school names will use built-in constants.")
        if terms_file is None:
            existing_terms_df = sc.read_tab(sheet_input.strip(), SHEET_TABS["raw_terms"])
        if hs_file is None:
            existing_funnel_df = sc.read_tab(sheet_input.strip(), SHEET_TABS["enrollment_funnel"])

with st.spinner("Parsing and normalizing data..."):
    try:
        normalized = normalize_all(
            students_file, reenroll_file,
            schools_file=schools_file,
            terms_file=terms_file,
            existing_schools_df=existing_schools_df,
            existing_terms_df=existing_terms_df,
            sm_applications_file=sm_apps_file,
            sm_registrations_file=sm_regs_file,
            sm_school_year=sm_school_year,
            hs_contacts_file=hs_file,
            existing_funnel_df=existing_funnel_df,
        )
    except Exception as e:
        st.error(f"**Error during normalization:** {e}")
        st.stop()

# Row count summary
c1, c2, c3, c4 = st.columns(4)
c1.metric("Students", f"{len(normalized['students']):,}")
c2.metric("ReEnrollments", f"{len(normalized['reenrollments']):,}")
c3.metric("Schools", f"{len(normalized['schools']):,}")
c4.metric("Terms", f"{len(normalized['terms']):,}")

c5, c6 = st.columns(2)
c5.metric("Enrollment Summary rows", f"{len(normalized['summary_enrollment']):,}")
c6.metric("Funnel Summary rows", f"{len(normalized['summary_funnel']):,}")

if sm_uploaded:
    cs1, cs2, cs3 = st.columns(3)
    cs1.metric("SM Applications", f"{len(normalized['sm_applications']):,}")
    cs2.metric("SM Registrations", f"{len(normalized['sm_registrations']):,}")
    cs3.metric("SM Recruitment Summary rows", f"{len(normalized['sm_recruitment']):,}")

# Warnings
if normalized["all_warnings"]:
    with st.expander(f"⚠️ {len(normalized['all_warnings'])} validation notice(s)", expanded=False):
        for w in normalized["all_warnings"]:
            st.warning(w)
else:
    st.success("No data quality issues found.")

# Known edge-case info
active_count = (normalized["students"]["enroll_status"].astype(str) == "0").sum()
st.info(
    f"**{active_count:,} currently active students** across all schools. "
    "Schools with 0 active students (e.g., BOCS, ICCMS) are expected — "
    "all students at those campuses may have transferred out."
)

# Data previews
with st.expander("Preview: Students (first 10 rows)"):
    st.dataframe(normalized["students"].head(10), use_container_width=True)

with st.expander("Preview: ReEnrollments (first 10 rows)"):
    st.dataframe(normalized["reenrollments"].head(10), use_container_width=True)

with st.expander("Preview: Enrollment Summary"):
    st.dataframe(normalized["summary_enrollment"].tail(20), use_container_width=True)

with st.expander("Preview: Funnel Summary"):
    st.dataframe(normalized["summary_funnel"], use_container_width=True)

if sm_uploaded and not normalized["sm_applications"].empty:
    with st.expander("Preview: SchoolMint Applications (first 10 rows)"):
        st.dataframe(normalized["sm_applications"].head(10), use_container_width=True)

if sm_uploaded and not normalized["sm_registrations"].empty:
    with st.expander("Preview: SchoolMint Registrations (first 10 rows)"):
        st.dataframe(normalized["sm_registrations"].head(10), use_container_width=True)

if sm_uploaded and not normalized["sm_recruitment"].empty:
    with st.expander("Preview: Recruitment Pipeline Summary"):
        st.dataframe(normalized["sm_recruitment"], use_container_width=True)

if not normalized["hs_contacts"].empty:
    ch1, ch2, ch3, ch4 = st.columns(4)
    hs_df = normalized["hs_contacts"]
    ch1.metric("HubSpot Contacts", f"{len(hs_df):,}")
    ch2.metric("In SchoolMint", f"{int(hs_df['Is_App'].sum()):,}")
    ch3.metric("In PowerSchool", f"{int((hs_df['PS_Match'] == 'Yes').sum()):,}")
    ch4.metric("Currently Enrolled", f"{int(hs_df['Is_Enrolled'].sum()):,}")

    with st.expander("Preview: HubSpot Contacts (first 10 rows)"):
        st.dataframe(hs_df.head(10), use_container_width=True)

    with st.expander("Preview: Enrollment Funnel Summary"):
        st.dataframe(normalized["hs_funnel_summary"], use_container_width=True)

# ─── Step 3: Push to Sheets ──────────────────────────────────────────────────

st.markdown("---")
st.markdown("## Step 3 — Push to Google Sheets")

if not email:
    st.error("Cannot push — credentials not configured. See sidebar for setup instructions.")
    st.stop()

if not sheet_input.strip():
    st.warning("Enter a Google Sheet name or URL in the sidebar.")
    st.stop()

if st.button("Push to Google Sheets", type="primary", use_container_width=True):
    progress_bar = st.progress(0, text="Starting...")
    status_area = st.empty()
    steps = [
        "raw_students", "raw_reenrollments", "raw_schools", "raw_terms",
        "summary_enrollment_by_school_year", "summary_funnel_current", "upload_log",
    ]
    if sm_uploaded:
        steps += ["raw_sm_applications", "raw_sm_registrations", "summary_sm_recruitment"]
    if hs_uploaded or not existing_funnel_df.empty:
        steps += ["enrollment_funnel", "enrollment_funnel_summary"]
    completed_steps = []

    def progress_cb(tab_name, n_rows):
        completed_steps.append(tab_name)
        pct = len(completed_steps) / len(steps)
        progress_bar.progress(pct, text=f"Writing {tab_name} ({n_rows:,} rows)...")
        status_area.markdown(
            "\n".join([f"✅ `{s}`" for s in completed_steps])
        )

    try:
        results = sc.push_all_data(
            normalized,
            sheet_name_or_url=sheet_input.strip(),
            progress_cb=progress_cb,
        )
        progress_bar.progress(1.0, text="Done!")

        # ─── Step 4: Confirmation ─────────────────────────────────────────
        st.markdown("---")
        st.markdown("## Step 4 — Complete")
        st.success("**Data pushed successfully!**")

        ts = normalized["upload_timestamp"]
        st.markdown(f"**Upload timestamp:** `{ts}`")

        st.markdown("**Rows written per tab:**")
        for tab, n in results["tabs_written"].items():
            st.markdown(f"- `{tab}`: {n}")

        sheet_url = results.get("sheet_url", "")
        if sheet_url:
            st.markdown(f"[Open Google Sheet]({sheet_url})")

        st.balloons()

    except RuntimeError as e:
        if "no_credentials" in str(e):
            st.error(
                "**Credentials not found.** Place your service account JSON at "
                "`credentials/service_account.json` and reload. See sidebar for details."
            )
        else:
            st.error(f"**Error:** {e}")

    except ValueError as e:
        st.error(f"**Setup required:** {e}")

    except Exception as e:
        err_str = str(e)
        st.error(f"**Error pushing to Sheets:** {err_str}")

        sa_email = sc.get_service_account_email()
        if "quota" in err_str.lower() or "storage" in err_str.lower():
            st.warning(
                "**Drive storage quota exceeded.** The service account's own Drive is full. "
                "**Fix:** Create the Sheet in your Google Drive, share it with "
                f"**{sa_email}** as Editor, then paste the URL in the sidebar."
            )
        elif "403" in err_str or "Forbidden" in err_str:
            st.warning(
                f"**Access denied.** Make sure the Google Sheet is shared with "
                f"**{sa_email}** as Editor."
            )
        elif "404" in err_str or "not found" in err_str.lower():
            st.warning(
                "Sheet not found. Make sure the URL is correct and the sheet is shared with the service account."
            )
        else:
            st.warning("Check your network connection and try again.")
