"""
Email Validation Web App — Streamlit
Two-phase: Phase 1 (local) → Phase 2 (API)
"""

import asyncio
import io
import time
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

import key_manager as km
import validators as val
import providers as prov
import enricher as enr

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Email Validator",
    page_icon="✉️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE DEFAULTS
# ─────────────────────────────────────────────────────────────────────────────
for key, default in {
    "phase1_results": None,
    "phase2_results": None,
    "phase3_results": None,
    "raw_emails": [],
    "active_provider": "Reoon",
    "active_key_label": "",
    "processing": False,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
STATUS_COLORS = {
    "Valid":       "#28a745",
    "Invalid":     "#dc3545",
    "Risky":       "#fd7e14",
    "Catch-all":   "#ffc107",
    "Role-based":  "#ffc107",
    "Disposable":  "#e83e8c",
    "Spam Trap":   "#dc3545",
    "Duplicate":   "#6c757d",
    "Unknown":     "#6c757d",
}

STATUS_EMOJI = {
    "Valid": "✅", "Invalid": "❌", "Risky": "⚠️", "Catch-all": "🔄",
    "Role-based": "👤", "Disposable": "🗑️", "Spam Trap": "🚨",
    "Duplicate": "🔁", "Unknown": "❓",
}


def results_to_df(results: List[Dict]) -> pd.DataFrame:
    cols = ["email", "status", "failure_reason", "phase", "provider",
            "mailbox_exists", "is_role_based", "is_disposable", "is_duplicate",
            "mx_found", "confidence_score"]
    return pd.DataFrame(results, columns=cols)


def df_to_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def summary_stats(df: pd.DataFrame) -> Dict[str, int]:
    counts = df["status"].value_counts().to_dict()
    return {
        "Total": len(df),
        "✅ Valid":      counts.get("Valid", 0),
        "❌ Invalid":    counts.get("Invalid", 0),
        "⚠️ Risky":      counts.get("Risky", 0),
        "🔄 Catch-all":  counts.get("Catch-all", 0),
        "👤 Role-based": counts.get("Role-based", 0),
        "🗑️ Disposable": counts.get("Disposable", 0),
        "🔁 Duplicate":  counts.get("Duplicate", 0),
        "❓ Unknown":    counts.get("Unknown", 0),
    }


def color_status(val):
    color = STATUS_COLORS.get(val, "#000")
    return f"color: {color}; font-weight: bold;"


def run_async(coro):
    """Run an async coroutine from sync Streamlit context (Python 3.10+ safe)."""
    try:
        asyncio.get_running_loop()
        # A loop is already running (Streamlit's internal loop).
        # Offload to a fresh thread where asyncio.run() can create its own loop.
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result()
    except RuntimeError:
        # No running loop — safe to run directly.
        return asyncio.run(coro)


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — API KEY MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ API Key Management")
    st.divider()

    cloud_mode = km._secrets_available()
    if cloud_mode:
        st.info(
            "☁️ **Cloud mode** — keys are managed via "
            "Streamlit Cloud Secrets. "
            "To add/remove keys, go to your app's **Settings → Secrets** in "
            "the Streamlit Cloud dashboard.",
            icon="🔒",
        )

    provider_tabs = st.tabs(km.PROVIDERS)

    for tab, provider in zip(provider_tabs, km.PROVIDERS):
        with tab:
            keys = km.get_keys_for_provider(provider)
            key_labels = list(keys.keys())

            st.markdown(f"**{provider}**")

            # ── Saved key selector ───────────────────────────────────────────
            if key_labels:
                sel_label = st.selectbox(
                    "Saved keys", key_labels,
                    key=f"sel_{provider}",
                )
                masked_key = keys[sel_label]
                show = st.checkbox("Show key", key=f"show_{provider}")
                display_val = masked_key if show else ("•" * min(len(masked_key), 32))
                st.code(display_val, language=None)

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("🧪 Test Key", key=f"test_{provider}"):
                        with st.spinner("Testing..."):
                            ok, credits = run_async(
                                prov.check_credits(provider, masked_key)
                            )
                        if ok:
                            st.success(f"✅ Valid — {credits:,} credits" if isinstance(credits, int) else f"✅ Valid — {credits}")
                        else:
                            st.error(f"❌ {credits}")
                with col2:
                    if st.button("🗑️ Delete", key=f"del_{provider}"):
                        km.delete_key(provider, sel_label)
                        st.rerun()

                if "test" in sel_label.lower() or "free" in sel_label.lower():
                    st.warning("⚠️ Test/limited key — check quota before large batches.")

                if provider == "Gemini":
                    # Gemini is Phase 3 only — not a Phase 2 email verification provider
                    st.info("🏢 Used for **Phase 3 Company Enrichment** only.")
                else:
                    # Make active provider (Phase 2 only)
                    if st.button(f"Set as Active Provider", key=f"active_{provider}"):
                        st.session_state.active_provider = provider
                        st.session_state.active_key_label = sel_label
                        st.rerun()

                    # Status indicator
                    if (st.session_state.active_provider == provider and
                            st.session_state.active_key_label == sel_label):
                        st.success("🟢 Currently Active")
            else:
                st.info("No saved keys yet.")

            # ── Add / Update Key ──────────────────────────────────────────────
            st.markdown("**Add / Update Key**")
            if cloud_mode:
                st.caption("⚠️ Cloud mode — keys saved for this session only (lost on refresh).")
            new_label = st.text_input("Label", placeholder='e.g. "Production Key"',
                                      key=f"lbl_{provider}")
            new_key   = st.text_input("API Key", type="password",
                                      key=f"key_{provider}")
            if st.button("💾 Save Key", key=f"save_{provider}"):
                if new_label and new_key:
                    km.save_key(provider, new_label, new_key)
                    st.success(f"Saved '{new_label}'")
                    st.rerun()
                else:
                    st.warning("Enter both a label and a key.")

    st.divider()

    # ── Active provider badge ────────────────────────────────────────────────
    st.markdown("### 🏷️ Active Provider")
    if st.session_state.active_key_label:
        st.success(
            f"**{st.session_state.active_provider}**  \n"
            f"Key: *{st.session_state.active_key_label}*"
        )
        # Show credit balance for active key
        if st.button("🔄 Check Active Credits"):
            key = km.get_key(
                st.session_state.active_provider,
                st.session_state.active_key_label
            )
            ok, credits = run_async(
                prov.check_credits(st.session_state.active_provider, key)
            )
            if ok:
                st.success(f"Credits remaining: **{credits:,}**" if isinstance(credits, int) else f"Credits: **{credits}**")
            else:
                st.error(f"Error: {credits}")
    else:
        st.warning("No active provider set.\nGo to a provider tab → Set as Active.")

    st.divider()

    # ── Provider switcher (Phase 2 only — Gemini excluded) ───────────────────
    st.markdown("### 🔀 Provider Switcher")
    all_keys = km.get_all_keys()
    for p in km.PROVIDERS:
        if p == "Gemini":
            continue  # Gemini is Phase 3 only, managed separately
        p_keys = all_keys.get(p, {})
        if p_keys:
            labels = list(p_keys.keys())
            sel = st.selectbox(p, labels, key=f"switch_{p}")
            is_active = (st.session_state.active_provider == p and
                         st.session_state.active_key_label == sel)
            badge = "🟢 Active" if is_active else ""
            if badge:
                st.caption(badge)
            if not is_active and st.button(f"Use {p}", key=f"use_{p}"):
                st.session_state.active_provider = p
                st.session_state.active_key_label = sel
                st.rerun()
        else:
            st.caption(f"⚫ {p} — no key saved")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN AREA
# ─────────────────────────────────────────────────────────────────────────────
st.title("✉️ Email Validator")
st.markdown(
    "Two-phase validation: **Phase 1** runs free local checks instantly. "
    "**Phase 2** uses your API provider to verify deliverability."
)
st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# INPUT SECTION
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("## 📥 Input Emails")
input_tab1, input_tab2 = st.tabs(["📁 Upload CSV", "📋 Paste Emails"])

raw_emails: List[str] = []

with input_tab1:
    uploaded = st.file_uploader(
        "Upload a CSV file with an 'email' column (or one email per row)",
        type=["csv"],
    )
    if uploaded:
        try:
            df_up = pd.read_csv(uploaded)
            # Try to find email column
            email_col = None
            for col in df_up.columns:
                if "email" in col.lower():
                    email_col = col
                    break
            if email_col:
                raw_emails = df_up[email_col].dropna().astype(str).tolist()
            else:
                raw_emails = df_up.iloc[:, 0].dropna().astype(str).tolist()
            st.success(f"Loaded **{len(raw_emails):,}** emails from CSV.")
        except Exception as e:
            st.error(f"Error reading CSV: {e}")

with input_tab2:
    pasted = st.text_area(
        "Paste emails (one per line, or comma/semicolon separated)",
        height=160,
        placeholder="john@example.com\njane@company.org\n...",
    )
    if pasted.strip():
        raw_emails = val.split_emails(pasted)
        st.caption(f"Detected **{len(raw_emails):,}** email addresses.")

if raw_emails:
    st.session_state.raw_emails = raw_emails

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1
# ─────────────────────────────────────────────────────────────────────────────
st.divider()
st.markdown("## 🔍 Phase 1 — Local Validation")
st.caption("Format check · MX records · Disposable domains · Role-based · Duplicates")

if st.session_state.raw_emails:
    if st.button("▶️ Run Phase 1", type="primary", use_container_width=True):
        with st.spinner("Running Phase 1 checks..."):
            t0 = time.time()
            results = val.validate_phase1(st.session_state.raw_emails)
            elapsed = time.time() - t0
        st.session_state.phase1_results = results
        st.session_state.phase2_results = None
        st.session_state.phase3_results = None
        speed = len(results) / elapsed if elapsed > 0 else len(results)
        st.success(
            f"Phase 1 complete — **{len(results):,}** emails in "
            f"**{elapsed:.2f}s** ({speed:,.0f} emails/sec)"
        )
else:
    st.info("Upload a CSV or paste emails above to begin.")

# Display Phase 1 results
if st.session_state.phase1_results:
    df1 = results_to_df(st.session_state.phase1_results)
    stats = summary_stats(df1)

    # Summary stats
    st.markdown("### 📊 Phase 1 Summary")
    stat_cols = st.columns(len(stats))
    for col, (label, count) in zip(stat_cols, stats.items()):
        col.metric(label, f"{count:,}")

    # Export + Proceed buttons
    st.markdown("### ⬇️ Actions")
    act_col1, act_col2 = st.columns(2)
    with act_col1:
        csv1 = df_to_csv(df1)
        st.download_button(
            "📥 Export Phase 1 Results",
            data=csv1,
            file_name="phase1_results.csv",
            mime="text/csv",
            use_container_width=True,
        )

    # Results table
    st.markdown("### 📋 Phase 1 Results")
    pd.set_option("styler.render.max_elements", df1.size)
    st.dataframe(
        df1.style.map(color_status, subset=["status"]),
        use_container_width=True,
        height=400,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 2 SECTION
    # ─────────────────────────────────────────────────────────────────────────
    st.divider()
    st.markdown("## 🌐 Phase 2 — API Verification")

    # Emails to verify = those that passed Phase 1 (Valid or Role-based/Catch-all)
    passable = {"Valid", "Role-based", "Risky"}
    to_verify = [
        r["email"] for r in st.session_state.phase1_results
        if r["status"] in passable
    ]

    if not to_verify:
        st.warning("No emails passed Phase 1 — nothing to send to Phase 2.")
    else:
        # ── Inline provider + key selector ────────────────────────────────────
        p2_col1, p2_col2, p2_col3 = st.columns([1, 2, 1])

        P2_PROVIDERS = ["Reoon", "ZeroBounce", "NeverBounce", "Hunter"]

        with p2_col1:
            p2_provider = st.selectbox(
                "Provider",
                P2_PROVIDERS,
                index=0,
                key="p2_provider_sel",
                help="Select which email verification API to use.",
            )

        with p2_col2:
            # Auto-fill from saved key (first saved key for the chosen provider)
            saved_keys = km.get_keys_for_provider(p2_provider)
            saved_key_value = list(saved_keys.values())[0] if saved_keys else ""
            p2_key_input = st.text_input(
                "API Key",
                value=saved_key_value,
                type="password",
                placeholder="Paste your API key here…",
                key="p2_key_input",
                help="Key is pre-filled from your saved keys if available.",
            )
            if saved_key_value and p2_key_input == saved_key_value:
                st.caption(f"💾 Loaded from saved key: *{list(saved_keys.keys())[0]}*")
            elif p2_key_input:
                st.caption("🔑 Using manually entered key")
            else:
                st.caption("⚠️ Enter an API key to proceed")

        with p2_col3:
            st.metric("Emails to Verify", f"{len(to_verify):,}")
            st.caption(f"({len(df1) - len(to_verify):,} filtered by Phase 1)")

        active_key = p2_key_input.strip()

        # ── Optional: check credits for selected provider ──────────────────────
        st.markdown("#### 💳 Credit Balances (all providers)")
        with st.expander("Check all provider credits", expanded=False):
            cred_cols = st.columns(len(P2_PROVIDERS))
            for col, p in zip(cred_cols, P2_PROVIDERS):
                p_keys = km.get_keys_for_provider(p)
                if p_keys:
                    first_label = list(p_keys.keys())[0]
                    first_key   = p_keys[first_label]
                    ok, credits = run_async(prov.check_credits(p, first_key))
                    if ok:
                        col.metric(p, f"{credits:,}" if isinstance(credits, int) else credits)
                        col.caption(first_label)
                    else:
                        col.metric(p, "Error")
                        col.caption(str(credits)[:40])
                else:
                    col.metric(p, "—")
                    col.caption("No key saved")

        # ── Live credit check for the currently entered key ────────────────────
        if active_key:
            ok, credits = run_async(prov.check_credits(p2_provider, active_key))
            if ok and isinstance(credits, int):
                st.markdown(f"**Remaining credits:** `{credits:,}`  |  "
                            f"**Emails to verify:** `{len(to_verify):,}`")
                if credits < len(to_verify):
                    st.warning(
                        f"⚠️ You have **{credits:,}** credits but need **{len(to_verify):,}**. "
                        f"Consider switching providers or splitting the batch."
                    )

        # ── Concurrency + confirmation + run ──────────────────────────────────
        concurrency = st.slider(
            "Concurrent API requests (higher = faster, watch rate limits)",
            min_value=1, max_value=20, value=5,
        )

        key_hint = f"•••{active_key[-4:]}" if len(active_key) >= 4 else "****"
        confirmed = st.checkbox(
            f"I confirm sending **{len(to_verify):,}** emails to "
            f"**{p2_provider}** (key: *{key_hint}*)"
        )

        with act_col2:
            proceed_btn = st.button(
                "🚀 Proceed to Phase 2 — API Verification",
                type="primary",
                use_container_width=True,
                disabled=not confirmed or not active_key,
            )

        if proceed_btn and confirmed and active_key:
            progress_bar  = st.progress(0)
            status_text   = st.empty()
            speed_display = st.empty()
            t_start = time.time()
            done_so_far = [0]

            def on_progress(done, total):
                done_so_far[0] = done
                pct = done / total
                progress_bar.progress(pct)
                elapsed = time.time() - t_start
                speed   = done / elapsed if elapsed > 0 else done
                status_text.markdown(
                    f"Verifying... **{done:,} / {total:,}** "
                    f"({pct*100:.1f}%)"
                )
                speed_display.caption(f"⚡ {speed:.1f} emails/sec")

            with st.spinner("Running Phase 2 API verification..."):
                api_results = run_async(
                    prov.verify_batch(
                        to_verify,
                        p2_provider,
                        active_key,
                        concurrency=concurrency,
                        progress_callback=on_progress,
                    )
                )

            elapsed_total = time.time() - t_start
            progress_bar.progress(1.0)
            status_text.empty()
            speed_display.empty()

            # Merge Phase 2 results back into full result list
            phase2_map = {
                email: api_res
                for email, api_res in zip(to_verify, api_results)
                if api_res
            }

            merged = []
            for row in st.session_state.phase1_results:
                r = dict(row)
                if r["email"] in phase2_map:
                    api = phase2_map[r["email"]]
                    r["status"]           = api["status"]
                    r["failure_reason"]   = api["failure_reason"]
                    r["phase"]            = 2
                    r["provider"]         = api["provider"]
                    r["mailbox_exists"]   = api["mailbox_exists"]
                    r["is_role_based"]    = api.get("is_role_based", r["is_role_based"])
                    r["is_disposable"]    = api.get("is_disposable", r["is_disposable"])
                    r["mx_found"]         = api.get("mx_found", r["mx_found"])
                    r["confidence_score"] = api.get("confidence_score", "")
                merged.append(r)

            st.session_state.phase2_results = merged
            st.session_state.phase3_results = None
            speed2 = len(to_verify) / elapsed_total if elapsed_total > 0 else len(to_verify)
            st.success(
                f"Phase 2 complete — **{len(to_verify):,}** emails in "
                f"**{elapsed_total:.1f}s** ({speed2:.1f} emails/sec)"
            )

# Display Phase 2 results
if st.session_state.phase2_results:
    df2 = results_to_df(st.session_state.phase2_results)
    stats2 = summary_stats(df2)

    st.markdown("### 📊 Phase 2 Summary")
    stat_cols2 = st.columns(len(stats2))
    for col, (label, count) in zip(stat_cols2, stats2.items()):
        col.metric(label, f"{count:,}")

    st.download_button(
        "📥 Export Phase 2 Results",
        data=df_to_csv(df2),
        file_name="phase2_results.csv",
        mime="text/csv",
        use_container_width=True,
    )

    st.markdown("### 📋 Phase 2 Results")
    pd.set_option("styler.render.max_elements", df2.size)
    st.dataframe(
        df2.style.map(color_status, subset=["status"]),
        use_container_width=True,
        height=500,
    )

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 3 — COMPANY ENRICHMENT
# ─────────────────────────────────────────────────────────────────────────────
base_results = st.session_state.phase2_results or st.session_state.phase1_results

if base_results:
    st.divider()
    st.markdown("## 🏢 Phase 3 — Company Enrichment")
    st.caption(
        "Fetches company homepages (free) + uses **Gemini 2.5 Flash Lite** to generate "
        "company descriptions, pain point hints, and industry classifications. "
        "Role emails are skipped automatically.  \n"
        "**Free tier:** ~1,500 domains/day &nbsp;|&nbsp; **Paid tier:** unlimited"
    )

    # ── Gemini key status ─────────────────────────────────────────────────────
    gemini_keys = km.get_keys_for_provider("Gemini")
    gemini_key_label = ""
    gemini_key_value = ""

    if gemini_keys:
        gemini_key_label = list(gemini_keys.keys())[0]
        gemini_key_value = gemini_keys[gemini_key_label]
        # Prefer the active provider's key if it's Gemini
        if st.session_state.active_provider == "Gemini" and st.session_state.active_key_label:
            ak = km.get_key("Gemini", st.session_state.active_key_label)
            if ak:
                gemini_key_label = st.session_state.active_key_label
                gemini_key_value = ak

    # ── Controls row ──────────────────────────────────────────────────────────
    enr_col1, enr_col2, enr_col3 = st.columns([2, 1, 1])

    with enr_col1:
        if gemini_key_value:
            st.info(f"🔑 Gemini key: **{gemini_key_label}**")
        else:
            st.warning("⚠️ No Gemini API key saved. Add one in the sidebar under **Gemini**.")

    with enr_col2:
        tier_choice = st.radio(
            "API tier",
            options=["Free (15 RPM)", "Paid (fast)"],
            index=0,
            horizontal=False,
            help=(
                "**Free:** 15 requests/min — add a delay between calls to stay within quota.  \n"
                "**Paid:** Full speed, no delay."
            ),
        )
        tier_mode = "free" if tier_choice.startswith("Free") else "paid"

    with enr_col3:
        # Count unique non-role domains
        non_role_emails = [
            r["email"] for r in base_results
            if r["email"].split("@")[0].lower().strip() not in enr.ROLE_PREFIXES
        ]
        unique_domains = list(dict.fromkeys(
            e.split("@")[1].lower() for e in non_role_emails if "@" in e
        ))
        role_skip_count = len(base_results) - len(non_role_emails)
        st.metric("Unique domains", len(unique_domains))
        if role_skip_count:
            st.caption(f"({role_skip_count} role email(s) skipped)")

    # ── Run button ────────────────────────────────────────────────────────────
    run_enrich = st.button(
        "▶️ Run Enrichment",
        type="primary",
        use_container_width=True,
        disabled=not gemini_key_value,
    )

    if run_enrich and gemini_key_value:
        all_emails = [r["email"] for r in base_results]
        progress_bar3  = st.progress(0)
        status_text3   = st.empty()
        speed_display3 = st.empty()
        t_start3 = time.time()

        def on_enrich_progress(done: int, total: int):
            pct = done / total if total else 1.0
            progress_bar3.progress(pct)
            elapsed = time.time() - t_start3
            rate = (done / elapsed * 60) if elapsed > 0 else 0
            status_text3.markdown(
                f"Enriching domain **{done} / {total}** ({pct*100:.0f}%)"
            )
            speed_display3.caption(f"⚡ {rate:.1f} domains/min")

        with st.spinner("Running company enrichment…"):
            enrich_rows = run_async(
                enr.enrich_batch(
                    all_emails,
                    gemini_key_value,
                    tier=tier_mode,
                    progress_callback=on_enrich_progress,
                )
            )

        elapsed3 = time.time() - t_start3
        progress_bar3.progress(1.0)
        status_text3.empty()
        speed_display3.empty()

        st.session_state.phase3_results = enrich_rows
        st.success(
            f"Enrichment complete — **{len(unique_domains):,}** domain(s) in "
            f"**{elapsed3:.1f}s**"
        )

    # ── Display Phase 3 results ───────────────────────────────────────────────
    if st.session_state.phase3_results:
        enrich_map = {row["email"]: row for row in st.session_state.phase3_results}

        # Merge enrichment columns into base results
        merged3 = []
        for r in base_results:
            row = dict(r)
            e = enrich_map.get(r["email"], {})
            row["first_name"]          = e.get("first_name", "")
            row["last_name"]           = e.get("last_name", "")
            row["job_title"]           = e.get("job_title", "")
            row["industry"]            = e.get("industry", "")
            row["company_description"] = e.get("company_description", "")
            row["pain_point_hint"]     = e.get("pain_point_hint", "")
            merged3.append(row)

        df3 = pd.DataFrame(merged3)

        st.markdown("### 📋 Enriched Results")
        enr_cols = ["email", "first_name", "last_name", "job_title",
                    "industry", "company_description", "pain_point_hint", "status"]
        display_cols = [c for c in enr_cols if c in df3.columns]

        pd.set_option("styler.render.max_elements", df3[display_cols].size)
        st.dataframe(
            df3[display_cols].style.map(color_status, subset=["status"]),
            use_container_width=True,
            height=500,
            column_config={
                "email":               st.column_config.TextColumn("Email"),
                "first_name":          st.column_config.TextColumn("First Name"),
                "last_name":           st.column_config.TextColumn("Last Name"),
                "job_title":           st.column_config.TextColumn("Job Title"),
                "industry":            st.column_config.TextColumn("Industry"),
                "company_description": st.column_config.TextColumn(
                    "Company Description", width="large"
                ),
                "pain_point_hint":     st.column_config.TextColumn(
                    "Pain Point Hint", width="large"
                ),
                "status":              st.column_config.TextColumn("Status"),
            },
        )

        st.download_button(
            "📥 Export Enriched Results",
            data=df_to_csv(df3),
            file_name="enriched_results.csv",
            mime="text/csv",
            type="primary",
            use_container_width=True,
        )
