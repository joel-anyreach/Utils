"""
app.py — Streamlit UI for the Anyreach Creator Recruitment Pipeline
Run with:  streamlit run app.py
"""
import csv
import io
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

# ── Constants ─────────────────────────────────────────────────────────────────
PIPELINE_DIR    = Path(__file__).parent
PIPELINE_SCRIPT = PIPELINE_DIR / "pipeline.py"
EXPORTS_DIR     = PIPELINE_DIR / "exports"
ANSI_ESCAPE     = re.compile(r"\x1b\[[0-9;]*m")
SUMMARY_LINE_RE = re.compile(r"^\s{2}(.+?):\s{2,}(.+)$")


# ── Helpers ───────────────────────────────────────────────────────────────────
def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE.sub("", text)


def make_csv_path(keyword: str) -> Path:
    """Generate a unique CSV export path for a keyword run."""
    EXPORTS_DIR.mkdir(exist_ok=True)
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_kw  = re.sub(r"[^\w]", "_", keyword)[:40]
    return EXPORTS_DIR / f"{safe_kw}_{ts}.csv"


def read_csv(path: Path) -> tuple[list[dict], str]:
    """Read a CSV file. Returns (rows as list-of-dicts, raw CSV string)."""
    try:
        raw  = path.read_text(encoding="utf-8")
        rows = list(csv.DictReader(io.StringIO(raw)))
        return rows, raw
    except Exception:
        return [], ""


def run_pipeline(
    source_mode: str,        # "keyword" | "playlist" | "channel"
    inputs: list[str],       # keywords, or [playlist_url], or [channel_url, ...]
    max_results: int,
    dry_run: bool,
    log_placeholder,
    language: str | None = None,
    strict_match: bool = False,
    min_subs: int | None = None,
    max_subs: int | None = None,
    verify_emails: bool = False,
) -> tuple[str, int, Path]:
    """
    Build and run a pipeline.py command for one job, streaming output live.
    Returns (full_log_text, exit_code, csv_export_path).
    """
    label    = inputs[0] if inputs else "run"
    csv_path = make_csv_path(label)

    cmd = [
        sys.executable,
        str(PIPELINE_SCRIPT),
        "--export-csv", str(csv_path),
    ]

    if source_mode == "keyword":
        cmd += ["--query", inputs[0], "--max-results", str(max_results)]
        if language:
            cmd += ["--language", language]
        if strict_match:
            cmd.append("--strict-match")
    elif source_mode == "playlist":
        cmd += ["--playlist-url", inputs[0]]
    elif source_mode == "channel":
        for url in inputs:
            cmd += ["--channel-url", url]

    if dry_run:
        cmd.append("--dry-run")
    if verify_emails:
        cmd.append("--verify-emails")
    if min_subs is not None:
        cmd += ["--min-subs", str(min_subs)]
    if max_subs is not None:
        cmd += ["--max-subs", str(max_subs)]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=str(PIPELINE_DIR),
    )

    log_lines: list[str] = []
    for raw_line in iter(proc.stdout.readline, b""):
        line = strip_ansi(raw_line.decode("utf-8", errors="replace")).rstrip("\r\n")
        log_lines.append(line)
        log_placeholder.code("\n".join(log_lines), language=None)

    proc.stdout.close()
    proc.wait()

    # Exit code 2 = Apify credits exhausted (distinct from general error)
    return "\n".join(log_lines), proc.returncode, csv_path


def parse_summary(log_text: str) -> dict[str, str]:
    metrics: dict[str, str] = {}
    sep_count  = 0
    in_summary = False

    for line in log_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("==="):
            sep_count += 1
            if sep_count == 3:
                break
            continue
        if sep_count == 1 and stripped == "PIPELINE SUMMARY":
            in_summary = True
            continue
        if in_summary:
            m = SUMMARY_LINE_RE.match(line)
            if m:
                metrics[m.group(1).strip()] = m.group(2).strip()

    return metrics


def render_summary_cards(metrics: dict[str, str], keyword: str, exit_code: int, log_text: str = "") -> None:
    if exit_code == 0:
        st.success(f"Pipeline completed successfully for **{keyword}**")
    elif exit_code == 2:
        st.warning(
            f"⚡ **Apify credits exhausted** while processing **{keyword}**. "
            "Any channels discovered before credits ran out have been uploaded. "
            "Top up your credits at [console.apify.com/billing](https://console.apify.com/billing) and re-run."
        )
    else:
        st.error(f"Pipeline finished with errors for **{keyword}**")

    if "Reoon credits ran out mid-verification" in log_text:
        st.warning(
            "⚡ **Reoon credits exhausted** mid-verification. Unverified emails were uploaded as-is. "
            "Top up at [reoon.com](https://reoon.com) to verify the remaining emails."
        )

    if not metrics:
        st.caption("No summary data parsed from output.")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Channels Found",     metrics.get("Channels discovered", "—"))
    col2.metric("Emails (Apify)",     metrics.get("Emails from Apify (public)", "—"))
    col3.metric("Emails (Enriched)",  metrics.get("Emails from enrichment", "—"))
    col4.metric("Total With Email",   metrics.get("Total with email", "—"))

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("New Leads",          metrics.get("New (not in sheet)", "—"))
    col6.metric("Role Emails",        metrics.get("Role emails flagged", "—"))
    col7.metric("Written to Sheet",   metrics.get("Written to Sheet", "—"))
    col8.metric("Pushed to Instantly",metrics.get("Pushed to Instantly", "—"))

    reoon_verified = metrics.get("Reoon verified")
    reoon_blocked  = metrics.get("Reoon blocked (inv/disp)")
    if reoon_verified or reoon_blocked:
        col9, col10, _ = st.columns(3)
        col9.metric("Reoon Verified",  reoon_verified or "—")
        col10.metric("Reoon Blocked",  reoon_blocked  or "—")


def render_csv_section(csv_path: Path, keyword: str) -> None:
    """Show results table and CSV download button."""
    if not csv_path.exists():
        return

    rows, raw_csv = read_csv(csv_path)
    if not rows:
        return

    st.divider()
    st.subheader(f"Enriched Results — {len(rows)} leads")

    # ── Column config: make URLs clickable ────────────────────────────────────
    st.dataframe(
        rows,
        use_container_width=True,
        column_config={
            "query":              st.column_config.TextColumn("Query"),
            "channel_id":         st.column_config.TextColumn("Channel ID"),
            "channel_name":       st.column_config.TextColumn("Channel Name"),
            "channel_handle":     st.column_config.TextColumn("Handle"),
            "email":              st.column_config.TextColumn("Email"),
            "status":             st.column_config.TextColumn("Status"),
            "subscriber_count":   st.column_config.NumberColumn("Subscribers", format="%d"),
            "country":            st.column_config.TextColumn("Country"),
            "total_views":        st.column_config.NumberColumn("Total Views", format="%d"),
            "total_videos_count": st.column_config.NumberColumn("Videos", format="%d"),
            "niche":              st.column_config.TextColumn("Niche"),
            "channel_url":        st.column_config.LinkColumn("Channel URL"),
            "email_source":       st.column_config.TextColumn("Email Source"),
            "is_role_email":      st.column_config.TextColumn("Role Email?"),
            "reoon_status":       st.column_config.TextColumn("Reoon Status"),
        },
    )

    # ── Download button ───────────────────────────────────────────────────────
    safe_kw   = re.sub(r"[^\w]", "_", keyword)[:40]
    file_name = f"{safe_kw}_results.csv"

    st.download_button(
        label="Download CSV",
        data=raw_csv,
        file_name=file_name,
        mime="text/csv",
        type="primary",
        use_container_width=False,
    )
    st.caption(f"Saved locally: `{csv_path}`")


# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Anyreach Creator Recruitment",
    page_icon="🎬",
    layout="wide",
)

st.title("🎬 Anyreach Creator Recruitment")
st.caption("YouTube discovery → email enrichment → Google Sheets → Instantly")
st.divider()

# ── Subscriber preset maps ────────────────────────────────────────────────────
MIN_SUBS_OPTIONS = {
    "No minimum": None,
    "1K+":   1_000,
    "5K+":   5_000,
    "10K+":  10_000,
    "50K+":  50_000,
    "100K+": 100_000,
    "500K+": 500_000,
    "1M+":   1_000_000,
}
MAX_SUBS_OPTIONS = {
    "No maximum": None,
    "1K":    1_000,
    "5K":    5_000,
    "10K":   10_000,
    "50K":   50_000,
    "100K":  100_000,
    "500K":  500_000,
    "1M":    1_000_000,
}
LANGUAGES = {
    "Any":        None,
    "English":    "en",
    "Spanish":    "es",
    "Portuguese": "pt",
    "French":     "fr",
    "German":     "de",
    "Italian":    "it",
    "Dutch":      "nl",
    "Arabic":     "ar",
    "Hindi":      "hi",
    "Japanese":   "ja",
    "Korean":     "ko",
    "Chinese":    "zh",
    "Russian":    "ru",
    "Turkish":    "tr",
}

# ── Session state init ────────────────────────────────────────────────────────
for key, default in [
    ("running",       False),
    ("pending_jobs",  []),    # list of dicts: {mode, inputs, label}
    ("max_results",   50),
    ("dry_run",       False),
    ("verify_emails", False),
    ("language",      None),
    ("strict_match",  False),
    ("min_subs",      None),
    ("max_subs",      None),
    ("results",       []),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── Input form ────────────────────────────────────────────────────────────────
with st.form("pipeline_form"):
    st.subheader("Configure Pipeline Run")

    # ── Source mode selector ──────────────────────────────────────────────────
    source_mode = st.radio(
        "Discovery source",
        options=["🔍 Keyword Search (Apify)", "📋 Playlist URL", "📺 Channel URL(s)"],
        horizontal=True,
        help=(
            "**Keyword Search** uses Apify credits to find channels by niche.  \n"
            "**Playlist URL** extracts every unique channel creator from a YouTube playlist (no Apify).  \n"
            "**Channel URLs** looks up specific channels you already know (no Apify)."
        ),
    )

    # ── Mode-specific input ───────────────────────────────────────────────────
    if source_mode == "🔍 Keyword Search (Apify)":
        raw_input = st.text_area(
            "YouTube keywords / niches",
            placeholder="Enter one keyword per line, e.g.:\nsolar energy\npersonal finance\nhome automation",
            height=130,
            help="The pipeline runs once per keyword in sequence.",
        )
    elif source_mode == "📋 Playlist URL":
        raw_input = st.text_input(
            "YouTube playlist URL",
            placeholder="https://www.youtube.com/playlist?list=PLxxxxxxxxxxxxxx",
            help="All unique channel creators in this playlist will be discovered and enriched.",
        )
    else:  # Channel URLs
        raw_input = st.text_area(
            "YouTube channel URLs",
            placeholder=(
                "One channel URL per line, e.g.:\n"
                "https://youtube.com/@mkbhd\n"
                "https://youtube.com/channel/UCBcRF18a7Qf58cCRy5xuWwQ\n"
                "https://youtube.com/@linus"
            ),
            height=130,
            help="These channels are looked up directly and sent through email enrichment.",
        )

    # ── Max results + dry run + verify emails ────────────────────────────────
    col_slider, col_dry, col_verify = st.columns([5, 1, 1])
    with col_slider:
        max_results = st.slider(
            "Max results per keyword",
            min_value=10, max_value=500, value=50, step=10,
            disabled=(source_mode != "🔍 Keyword Search (Apify)"),
            help="Only applies to Keyword Search mode.",
        )
    with col_dry:
        st.write("")
        st.write("")
        dry_run = st.toggle(
            "Dry run",
            value=False,
            help="Runs all stages but skips writing to Google Sheet and Instantly.",
        )
    with col_verify:
        st.write("")
        st.write("")
        verify_emails = st.toggle(
            "Verify (Reoon)",
            value=False,
            help="Verify emails via Reoon API before uploading to Instantly. Requires REOON_API_KEY in .env.",
        )

    # ── Advanced Filters ──────────────────────────────────────────────────────
    with st.expander("Advanced Filters", expanded=False):
        st.caption("Language and strict match apply to Keyword Search only. Subscriber range applies to all modes.")

        col_lang, col_strict = st.columns([3, 1])
        with col_lang:
            language_label = st.selectbox(
                "Language",
                options=list(LANGUAGES.keys()),
                index=0,
                disabled=(source_mode != "🔍 Keyword Search (Apify)"),
                help="Only applied in Keyword Search (Apify) mode.",
            )
        with col_strict:
            st.write("")
            st.write("")
            strict_match = st.toggle(
                "Strict keyword match",
                value=False,
                disabled=(source_mode != "🔍 Keyword Search (Apify)"),
                help="Only applied in Keyword Search (Apify) mode.",
            )

        st.markdown("**Subscriber count range**")
        col_min, col_max = st.columns(2)
        with col_min:
            min_subs_label = st.selectbox(
                "Minimum subscribers",
                options=list(MIN_SUBS_OPTIONS.keys()),
                index=0,
            )
        with col_max:
            max_subs_label = st.selectbox(
                "Maximum subscribers",
                options=list(MAX_SUBS_OPTIONS.keys()),
                index=0,
            )

    submitted = st.form_submit_button(
        label="Run Pipeline" if not st.session_state.running else "Running...",
        disabled=st.session_state.running,
        type="primary",
        use_container_width=True,
    )

if submitted:
    # Build job list based on source mode
    jobs = []
    mode_key = (
        "keyword"  if source_mode == "🔍 Keyword Search (Apify)"
        else "playlist" if source_mode == "📋 Playlist URL"
        else "channel"
    )

    if mode_key == "keyword":
        lines = [l.strip() for l in raw_input.splitlines() if l.strip()]
        if not lines:
            st.warning("Please enter at least one keyword before running.")
        else:
            jobs = [{"mode": "keyword", "inputs": [kw], "label": kw} for kw in lines]

    elif mode_key == "playlist":
        url = raw_input.strip()
        if not url:
            st.warning("Please enter a playlist URL.")
        else:
            jobs = [{"mode": "playlist", "inputs": [url], "label": url}]

    else:  # channel
        urls = [l.strip() for l in raw_input.splitlines() if l.strip()]
        if not urls:
            st.warning("Please enter at least one channel URL.")
        else:
            # Run all channel URLs as a single job (one pipeline.py call)
            jobs = [{"mode": "channel", "inputs": urls, "label": f"{len(urls)} channel URL(s)"}]

    if jobs:
        st.session_state.pending_jobs  = jobs
        st.session_state.max_results   = max_results
        st.session_state.dry_run       = dry_run
        st.session_state.verify_emails = verify_emails
        st.session_state.language      = LANGUAGES[language_label]
        st.session_state.strict_match  = strict_match
        st.session_state.min_subs      = MIN_SUBS_OPTIONS[min_subs_label]
        st.session_state.max_subs      = MAX_SUBS_OPTIONS[max_subs_label]
        st.session_state.running       = True
        st.session_state.results       = []

# ── Pipeline execution ────────────────────────────────────────────────────────
if st.session_state.running and st.session_state.pending_jobs:
    st.divider()
    st.subheader("Live Output")

    jobs          = st.session_state.pending_jobs
    max_r         = st.session_state.max_results
    dry           = st.session_state.dry_run
    verify_em     = st.session_state.verify_emails
    language      = st.session_state.language
    strict_match  = st.session_state.strict_match
    min_subs      = st.session_state.min_subs
    max_subs      = st.session_state.max_subs

    all_results = []

    with st.spinner(f"Running pipeline for {len(jobs)} job(s)..."):
        for idx, job in enumerate(jobs):
            st.markdown(f"**[{idx + 1}/{len(jobs)}] {job['label']}**")
            log_placeholder = st.empty()

            log_text, exit_code, csv_path = run_pipeline(
                source_mode=job["mode"],
                inputs=job["inputs"],
                max_results=max_r,
                dry_run=dry,
                log_placeholder=log_placeholder,
                language=language,
                strict_match=strict_match,
                min_subs=min_subs,
                max_subs=max_subs,
                verify_emails=verify_em,
            )
            metrics = parse_summary(log_text)

            all_results.append({
                "keyword":   job["label"],
                "log":       log_text,
                "metrics":   metrics,
                "exit_code": exit_code,
                "csv_path":  csv_path,
            })

    st.session_state.results      = all_results
    st.session_state.running      = False
    st.session_state.pending_jobs = []
    st.rerun()

# ── Results display ───────────────────────────────────────────────────────────
if st.session_state.results and not st.session_state.running:
    st.divider()
    st.subheader("Results")

    for entry in st.session_state.results:
        ec = entry["exit_code"]
        status_icon = "✅" if ec == 0 else ("⚡" if ec == 2 else "❌")
        with st.expander(f"{status_icon} {entry['keyword']}", expanded=True):

            # Summary metrics
            render_summary_cards(entry["metrics"], entry["keyword"], ec, log_text=entry["log"])

            # CSV preview table + download button
            render_csv_section(entry["csv_path"], entry["keyword"])

            # Full log (collapsed by default)
            with st.expander("Full log", expanded=False):
                st.code(entry["log"], language=None)
