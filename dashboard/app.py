"""
App 2: Ingenium Re-enrollment Dashboard
5-tab reporting dashboard reading from Google Sheets
"""

import sys
import os
import streamlit as st
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import sheets_client as sc
import metrics as m
import charts as c
from shared.constants import (
    ACTIVE_SCHOOL_IDS, SCHOOL_MAP, GRADE_LABEL_MAP, GRADE_SORT_ORDER
)

st.set_page_config(
    page_title="Ingenium Re-enrollment Dashboard",
    page_icon="📊",
    layout="wide",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("📊 Re-enrollment Dashboard")
    st.markdown("---")

    # Sheet URL input
    sheet_url = st.text_input(
        "Google Sheet URL",
        placeholder="https://docs.google.com/spreadsheets/d/...",
        help="Paste the full URL of your Ingenium_Reenrollment_Data Google Sheet.",
    )

    if not sheet_url:
        st.info("Paste your Google Sheet URL above to load data.")
        st.stop()

    # Try loading data
    with st.spinner("Loading data..."):
        try:
            data = sc.load_all_data(sheet_url)
        except RuntimeError as e:
            if "no_credentials" in str(e):
                st.error("No credentials found. See the Uploader app setup instructions.")
                st.stop()
            else:
                st.error(f"Error: {e}")
                st.stop()
        except Exception as e:
            st.error(f"Could not load data: {e}")
            st.stop()

    funnel_df       = data.get("summary_funnel", pd.DataFrame())
    summary_df      = data.get("summary_enrollment", pd.DataFrame())
    students_df     = data.get("students", pd.DataFrame())
    reenroll_df     = data.get("reenrollments", pd.DataFrame())
    sm_recruitment_df = data.get("sm_recruitment", pd.DataFrame())
    sm_apps_df      = data.get("sm_applications", pd.DataFrame())

    # Last upload timestamp
    last_upload = sc.get_last_upload(sheet_url)
    st.caption(f"Data as of: **{last_upload}**")

    if st.button("Refresh Data"):
        sc.clear_cache()
        st.rerun()

    st.markdown("---")
    st.markdown("**Filters**")

    # School filter — abbreviations from PS Schools export (canonical); SCHOOL_MAP as fallback
    _schools_ref = data.get("schools", pd.DataFrame())
    if not _schools_ref.empty and "school_id" in _schools_ref.columns and "school_abbr" in _schools_ref.columns:
        _sref = _schools_ref.copy()
        _sref["school_id"] = pd.to_numeric(_sref["school_id"], errors="coerce").fillna(0).astype(int)
        _sref = _sref[_sref["school_id"].isin(ACTIVE_SCHOOL_IDS)].drop_duplicates("school_id")
        _abbr_map = dict(zip(_sref["school_id"], _sref["school_abbr"]))
    else:
        _abbr_map = {}
    school_options = {
        _abbr_map.get(sid, SCHOOL_MAP[sid]["abbr"]): sid
        for sid in ACTIVE_SCHOOL_IDS
        if sid in SCHOOL_MAP
    }
    # ICS (121137), ICMS (127985), BOCS (118760) visible by default; others available but unchecked
    # Resolved by school_id so the correct option is pre-selected regardless of PS abbreviation
    _DEFAULT_IDS = {118760, 121137, 127985}
    _sid_to_abbr = {v: k for k, v in school_options.items()}  # reverse: school_id → abbr key
    selected_school_abbrs = st.multiselect(
        "School",
        options=list(school_options.keys()),
        default=[_sid_to_abbr[sid] for sid in _DEFAULT_IDS if sid in _sid_to_abbr],
    )
    selected_school_ids = [school_options[a] for a in selected_school_abbrs] if selected_school_abbrs else None

    # Grade filter
    grade_options = {GRADE_LABEL_MAP[g]: g for g in GRADE_SORT_ORDER if g in GRADE_LABEL_MAP and g != 99}
    selected_grade_labels = st.multiselect(
        "Grade",
        options=list(grade_options.keys()),
        default=[],
        placeholder="All grades",
    )
    selected_grades = [grade_options[l] for l in selected_grade_labels] if selected_grade_labels else None

    # Year range filter
    try:
        sy_df = summary_df.copy()
        sy_df["school_year_start"] = pd.to_numeric(sy_df["school_year_start"], errors="coerce")
        min_year = int(sy_df["school_year_start"].min())
        max_year = int(sy_df["school_year_start"].max())
    except Exception:
        min_year, max_year = 2010, 2025

    year_range = st.slider(
        "School Year Range",
        min_value=min_year, max_value=max_year,
        value=(max(min_year, max_year - 9), max_year),
        format="%d",
        help="Affects trend and historical charts",
    )

    if st.button("Reset Filters"):
        st.rerun()

    # ── SchoolMint Recruitment filters (only shown when SM data is loaded) ──
    sm_data_available = not sm_recruitment_df.empty
    if sm_data_available:
        st.markdown("---")
        st.markdown("**Recruitment Filters**")
        _sm_school_opts = sorted(
            sm_recruitment_df["school_abbr"].dropna().unique().tolist()
        )
        selected_sm_schools = st.multiselect(
            "SM School",
            options=_sm_school_opts,
            default=_sm_school_opts,
            key="sm_school_filter",
        )
        selected_sm_grade_labels = st.multiselect(
            "SM Grade",
            options=[GRADE_LABEL_MAP[g] for g in GRADE_SORT_ORDER if g in GRADE_LABEL_MAP and g != 99],
            default=[],
            placeholder="All grades",
            key="sm_grade_filter",
        )
        _sm_grade_options = {GRADE_LABEL_MAP[g]: g for g in GRADE_SORT_ORDER if g in GRADE_LABEL_MAP and g != 99}
        selected_sm_grades = (
            [_sm_grade_options[l] for l in selected_sm_grade_labels]
            if selected_sm_grade_labels else None
        )
    else:
        selected_sm_schools = []
        selected_sm_grades = None

# ── Shared: available school years (used across tabs) ─────────────────────────

try:
    _sy_df = summary_df.copy()
    _sy_df["school_year_start"] = pd.to_numeric(_sy_df["school_year_start"], errors="coerce")
    avail_years = sorted(_sy_df["school_year_start"].dropna().astype(int).unique(), reverse=True)
    avail_years = [y for y in avail_years if y >= 2015]
except Exception:
    avail_years = list(range(2020, 2026))

# ── Main Tabs ─────────────────────────────────────────────────────────────────

st.title("Ingenium Schools — Re-enrollment Dashboard")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Funnel Overview",
    "Enrollment Trends",
    "Grade Distribution",
    "Retention Analysis",
    "Historical Deep Dive",
    "Recruitment Pipeline",
])

# ═══════════════════════════════════════════════════════════════════
# TAB 1: FUNNEL OVERVIEW
# ═══════════════════════════════════════════════════════════════════

with tab1:
    st.markdown("### Re-enrollment Funnel")

    if funnel_df.empty:
        st.warning("Funnel data not available. Run the uploader to push data.")
    else:
        kpis = m.funnel_kpis(funnel_df, selected_school_ids, selected_grades)

        # KPI cards
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Currently Enrolled", f"{kpis['enrolled']:,}")
        k2.metric("Next School Assigned", f"{kpis['assigned']:,}",
                  delta=f"{kpis['rate_enrolled_to_assigned']}% of enrolled")
        k3.metric("Re-enrollment Records", f"{kpis['reenrolled']:,}",
                  delta=f"{kpis['rate_enrolled_to_reenrolled']}% of enrolled")
        k4.metric("Retention Rate", f"{kpis['retention_rate']}%",
                  delta=f"{kpis['retained']:,} retained")

        st.markdown("---")

        # Conversion rates row
        r1, r2, r3 = st.columns(3)
        r1.metric("Enrolled → Assigned", f"{kpis['rate_enrolled_to_assigned']}%")
        r2.metric("Assigned → Re-enrolled", f"{kpis['rate_assigned_to_reenrolled']}%")
        r3.metric("Enrolled → Re-enrolled", f"{kpis['rate_enrolled_to_reenrolled']}%")

        st.markdown("---")

        # Funnel chart + retention breakdown
        left, right = st.columns([1, 2])
        with left:
            st.plotly_chart(c.funnel_chart(kpis), use_container_width=True)
        with right:
            df_ret = m.retention_breakdown(funnel_df, selected_school_ids, selected_grades)
            st.plotly_chart(c.retention_stacked_bar(df_ret), use_container_width=True)

        # Not Decided alert
        nd_count = kpis.get("enrolled", 0) - kpis.get("assigned", 0)
        if nd_count > 0:
            st.warning(
                f"**{nd_count:,} students** have no next school assigned yet. "
                "See the 'Not Decided' risk table in the Retention Analysis tab."
            )

        st.markdown("---")
        st.markdown("#### Monthly Re-enrollment Pace")

        if not reenroll_df.empty and avail_years:
            pace_year = st.selectbox(
                "School Year",
                options=avail_years,
                format_func=lambda y: f"{y}-{y+1}",
                key="pace_year",
            )
            pace_df = m.monthly_reenroll_pace(reenroll_df, selected_school_ids, pace_year)
            if not pace_df.empty:
                st.plotly_chart(c.monthly_reenroll_pace_chart(pace_df), use_container_width=True)
            else:
                st.info("No re-enrollment records found for this school year.")
        else:
            st.info("ReEnrollments data needed for monthly pace chart.")


# ═══════════════════════════════════════════════════════════════════
# TAB 2: ENROLLMENT TRENDS
# ═══════════════════════════════════════════════════════════════════

with tab2:
    st.markdown("### Enrollment Trends")

    if summary_df.empty:
        st.warning("Enrollment summary data not available. Run the uploader.")
    else:
        enroll_kpis = m.enrollment_summary_kpis(summary_df, selected_school_ids)

        e1, e2, e3 = st.columns(3)
        e1.metric(
            f"Network Enrollment ({enroll_kpis['current_year_label']})",
            f"{enroll_kpis['current_total']:,}"
        )
        if enroll_kpis["yoy_delta"] is not None:
            e2.metric("YoY Change", f"{enroll_kpis['yoy_delta']:+,}",
                      delta=f"{enroll_kpis['yoy_pct']:+.1f}%")
        else:
            e2.metric("YoY Change", "N/A")

        # Projected
        if not funnel_df.empty:
            proj_df = m.projected_enrollment(summary_df, funnel_df, selected_school_ids)
            total_proj = proj_df["projected_enrollment"].sum() if not proj_df.empty else 0
            e3.metric("Projected Next Year", f"~{int(total_proj):,}")

        st.markdown("---")

        # Line chart
        trend_data = m.enrollment_by_sy(summary_df, selected_school_ids, year_range)
        st.plotly_chart(c.enrollment_trend_line(trend_data), use_container_width=True, key="enroll_trend_tab2")

        col_a, col_b = st.columns(2)
        with col_a:
            yoy_data = m.yoy_change(summary_df, selected_school_ids)
            yoy_filtered = yoy_data[yoy_data["school_year_start"].between(year_range[0], year_range[1])]
            st.plotly_chart(c.yoy_delta_bar(yoy_filtered), use_container_width=True)
        with col_b:
            share_data = m.network_share_by_sy(summary_df, selected_school_ids, year_range)
            st.plotly_chart(c.network_share_area(share_data), use_container_width=True)

        st.markdown("---")
        st.markdown("#### Monthly Enrollment Snapshot")

        if avail_years:
            snap_year = st.selectbox(
                "School Year",
                options=avail_years,
                format_func=lambda y: f"{y}-{y+1}",
                key="snap_year",
            )
            snap_df = m.monthly_enrollment_snapshot(
                reenroll_df, students_df, selected_school_ids, snap_year
            )
            if not snap_df.empty:
                st.plotly_chart(c.monthly_enrollment_line(snap_df), use_container_width=True, key="monthly_snap_chart")
                st.plotly_chart(c.monthly_breakdown_bar(snap_df), use_container_width=True, key="monthly_snap_breakdown")
            else:
                st.info("No enrollment data available for this school year.")
        else:
            st.info("No school year data available.")


# ═══════════════════════════════════════════════════════════════════
# TAB 3: GRADE DISTRIBUTION
# ═══════════════════════════════════════════════════════════════════

with tab3:
    st.markdown("### Grade Distribution — Current Enrollment")

    if funnel_df.empty:
        st.warning("Funnel data not available.")
    else:
        grade_data = m.grade_distribution(funnel_df, selected_school_ids)
        st.plotly_chart(c.grade_bar(grade_data), use_container_width=True)

        col_a, col_b = st.columns(2)
        with col_a:
            heatmap_data = m.school_grade_heatmap_data(funnel_df, selected_school_ids)
            st.plotly_chart(
                c.school_grade_heatmap(heatmap_data, "Enrollment by School × Grade"),
                use_container_width=True
            )

        with col_b:
            st.markdown("#### Grade Cohort Progression (Sankey)")
            school_options_sankey = {
                _abbr_map.get(sid, SCHOOL_MAP[sid]["abbr"]): sid
                for sid in (selected_school_ids or ACTIVE_SCHOOL_IDS)
                if sid in SCHOOL_MAP
            }
            sankey_school = st.selectbox(
                "School", options=list(school_options_sankey.keys()), key="sankey_school"
            )
            if avail_years:
                sankey_year = st.selectbox(
                    "From Year",
                    options=avail_years,
                    format_func=lambda y: f"{y}-{y+1}",
                    key="sankey_year"
                )
                sankey_sid = school_options_sankey.get(sankey_school)
                if not reenroll_df.empty and sankey_sid:
                    prog_df = m.cohort_progression(reenroll_df, sankey_sid, sankey_year)
                    st.plotly_chart(
                        c.cohort_sankey(prog_df, sankey_school, f"{sankey_year}-{sankey_year+1}"),
                        use_container_width=True
                    )
                else:
                    st.info("ReEnrollments data needed for Sankey.")


# ═══════════════════════════════════════════════════════════════════
# TAB 4: RETENTION ANALYSIS
# ═══════════════════════════════════════════════════════════════════

with tab4:
    st.markdown("### Retention Analysis")

    if funnel_df.empty:
        st.warning("Funnel data not available.")
    else:
        kpis = m.funnel_kpis(funnel_df, selected_school_ids, selected_grades)
        enrolled = kpis["enrolled"]

        # Top KPIs
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Retention Rate", f"{kpis['retention_rate']}%",
                  help="Retained same school / Total enrolled")
        transfer_df = m.filter_funnel(funnel_df, selected_school_ids, selected_grades)
        net_xfer   = int(pd.to_numeric(transfer_df["network_transfer"],  errors="coerce").fillna(0).sum()) if "network_transfer"  in transfer_df.columns else 0
        ext_xfer   = int(pd.to_numeric(transfer_df["external_transfer"], errors="coerce").fillna(0).sum()) if "external_transfer" in transfer_df.columns else 0
        graduating = int(pd.to_numeric(transfer_df["graduating"],        errors="coerce").fillna(0).sum()) if "graduating"        in transfer_df.columns else 0

        total_transfer = net_xfer + ext_xfer
        transfer_rate = round(total_transfer / enrolled * 100, 1) if enrolled else 0
        r2.metric("Transfer Rate", f"{transfer_rate}%")
        r3.metric("Not Decided", f"{kpis['enrolled'] - kpis['assigned']:,}",
                  help="Active students with no next school assigned")
        r4.metric("Graduating", f"{graduating:,}")

        st.markdown("---")

        # Grouped bar by school and by grade
        col_a, col_b = st.columns(2)
        with col_a:
            df_ret = m.retention_breakdown(funnel_df, selected_school_ids, selected_grades)
            st.plotly_chart(c.retention_grouped_bar(df_ret, by="school"), use_container_width=True)
        with col_b:
            st.plotly_chart(c.retention_grouped_bar(df_ret, by="grade"), use_container_width=True)

        # Retention heatmap
        ret_pivot = m.retention_heatmap_data(funnel_df, selected_school_ids)
        st.plotly_chart(c.retention_rate_heatmap(ret_pivot), use_container_width=True)

        # Historical retention trend
        st.markdown("#### Historical Retention Trend (2015–Present)")
        if not reenroll_df.empty:
            hist_ret = m.historical_retention(reenroll_df, selected_school_ids, students_df)
            hist_ret_filtered = hist_ret[
                hist_ret["school_year_start"].between(year_range[0], year_range[1])
            ]
            st.plotly_chart(c.historical_retention_line(hist_ret_filtered), use_container_width=True)
        else:
            st.info("ReEnrollments data needed for historical retention chart.")

        # Not Decided risk table
        st.markdown("#### Students With No Next School Assigned")
        if not students_df.empty:
            nd_df = m.not_decided_students(students_df, selected_school_ids, selected_grades)
            if nd_df.empty:
                st.success("All active students have a next school assigned.")
            else:
                st.markdown(f"**{len(nd_df):,} students** without a next school assignment:")
                st.dataframe(nd_df, use_container_width=True, height=300)
                csv = nd_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download Not-Decided List (CSV)",
                    data=csv,
                    file_name="not_decided_students.csv",
                    mime="text/csv",
                )


# ═══════════════════════════════════════════════════════════════════
# TAB 5: HISTORICAL DEEP DIVE
# ═══════════════════════════════════════════════════════════════════

with tab5:
    st.markdown("### Historical Deep Dive")

    if reenroll_df.empty:
        st.warning("ReEnrollments data not available.")
    else:
        re_filtered = m.filter_reenroll(reenroll_df, selected_school_ids, selected_grades, year_range)
        hist_enroll = m.enrollment_history(reenroll_df, students_df, selected_school_ids, year_range)
        st.plotly_chart(
            c.enrollment_trend_line(hist_enroll),
            use_container_width=True,
            key="enroll_trend_tab5"
        )

        st.markdown("#### Monthly Drill-Down")
        if avail_years:
            drill_year = st.selectbox(
                "School Year",
                options=avail_years,
                format_func=lambda y: f"{y}-{y+1}",
                key="drill_year",
            )
            drill_df = m.monthly_enrollment_snapshot(
                reenroll_df, students_df, selected_school_ids, drill_year
            )
            if not drill_df.empty:
                st.plotly_chart(c.monthly_enrollment_line(drill_df), use_container_width=True, key="monthly_drill_chart")
                st.plotly_chart(c.monthly_breakdown_bar(drill_df), use_container_width=True, key="monthly_drill_breakdown")
            else:
                st.info("No monthly data available for this school year.")

        st.markdown("---")
        st.markdown("#### Student-Level Data")

        # Build filterable student table
        if not students_df.empty:
            view_df = students_df.copy()
            if selected_school_ids:
                view_df["school_id"] = pd.to_numeric(view_df["school_id"], errors="coerce").fillna(0).astype(int)
                view_df = view_df[view_df["school_id"].isin(selected_school_ids)]
            if selected_grades:
                view_df["grade_level"] = pd.to_numeric(view_df["grade_level"], errors="coerce").fillna(-99).astype(int)
                view_df = view_df[view_df["grade_level"].isin(selected_grades)]

            display_cols = [c for c in [
                "student_number", "last_first", "school_abbr", "grade_label",
                "enroll_status_label", "next_school_abbr", "retention_status",
                "entry_date", "exit_date",
            ] if c in view_df.columns]

            st.dataframe(
                view_df[display_cols],
                use_container_width=True,
                height=400,
                column_config={
                    "student_number": st.column_config.TextColumn("ID"),
                    "last_first": st.column_config.TextColumn("Name"),
                    "school_abbr": st.column_config.TextColumn("School"),
                    "grade_label": st.column_config.TextColumn("Grade"),
                    "enroll_status_label": st.column_config.TextColumn("Status"),
                    "next_school_abbr": st.column_config.TextColumn("Next School"),
                    "retention_status": st.column_config.TextColumn("Retention"),
                    "entry_date": st.column_config.TextColumn("Entry"),
                    "exit_date": st.column_config.TextColumn("Exit"),
                },
            )

            csv = view_df[display_cols].to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download Filtered Students (CSV)",
                data=csv,
                file_name="students_filtered.csv",
                mime="text/csv",
            )

        # Historical ReEnrollments table
        with st.expander("View ReEnrollment Records (filtered)"):
            re_cols = [col for col in [
                "reenroll_id", "student_id", "school_abbr", "grade_label",
                "school_year_label", "entry_date", "exit_date", "enrollment_code",
            ] if col in re_filtered.columns]
            st.dataframe(re_filtered[re_cols], use_container_width=True, height=300)
            re_csv = re_filtered[re_cols].to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download ReEnrollment Records (CSV)",
                data=re_csv,
                file_name="reenrollments_filtered.csv",
                mime="text/csv",
            )

# ═══════════════════════════════════════════════════════════════════
# TAB 6: RECRUITMENT PIPELINE (SchoolMint)
# ═══════════════════════════════════════════════════════════════════

with tab6:
    st.markdown("### SchoolMint Recruitment Pipeline")

    if sm_recruitment_df.empty:
        st.info(
            "No SchoolMint data available. "
            "Upload `schoolmint applications.csv` and `schoolmint registrations.csv` "
            "in the **Uploader app** (Step 1B) to enable this tab."
        )
    else:
        sm_kpis = m.sm_funnel_kpis(
            sm_recruitment_df,
            school_abbrs=selected_sm_schools or None,
            grades=selected_sm_grades,
        )

        # ── Stage counts ────────────────────────────────────────────
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Leads", f"{sm_kpis['leads']:,}")
        k2.metric(
            "Apps Submitted",
            f"{sm_kpis['apps_submitted']:,}",
            delta=f"{sm_kpis['rate_leads_to_apps']}% of leads",
        )
        k3.metric(
            "Reg Complete",
            f"{sm_kpis['reg_submitted']:,}",
            delta=f"{sm_kpis['rate_leads_to_rc']}% of leads",
        )
        k4.metric(
            "Reg Approved",
            f"{sm_kpis['reg_approved']:,}",
            delta=f"{sm_kpis['rate_leads_to_ra']}% of leads",
        )

        st.markdown("---")

        # ── Conversion rates (matching the Looker Studio layout) ────
        st.markdown("**Current Conversion Rates**")
        r1, r2, r3 = st.columns(3)
        r1.metric("Rate Leads to Apps",   f"{sm_kpis['rate_leads_to_apps']}%")
        r2.metric("Rate Apps to RC",      f"{sm_kpis['rate_apps_to_rc']}%")
        r3.metric("Rate RC to RA",        f"{sm_kpis['rate_rc_to_ra']}%")

        r4, r5, r6 = st.columns(3)
        r4.metric("Rate Apps to RA",      f"{sm_kpis['rate_apps_to_ra']}%")
        r5.metric("Rate Leads to RC",     f"{sm_kpis['rate_leads_to_rc']}%")
        r6.metric("Leads to RA",          f"{sm_kpis['rate_leads_to_ra']}%")

        st.markdown("---")

        # ── Funnel chart + per-school breakdown ─────────────────────
        col_left, col_right = st.columns([1, 2])
        with col_left:
            st.plotly_chart(
                c.sm_pipeline_funnel_chart(sm_kpis),
                use_container_width=True,
            )
        with col_right:
            st.markdown("**By School**")
            _sm_filtered = m.sm_filter(
                sm_recruitment_df,
                school_abbrs=selected_sm_schools or None,
                grades=selected_sm_grades,
            )
            school_summary = (
                _sm_filtered
                .groupby("school_abbr", as_index=False)[["leads", "apps_submitted", "reg_submitted", "reg_approved"]]
                .sum()
                .sort_values("school_abbr")
            )
            st.dataframe(
                school_summary,
                use_container_width=True,
                column_config={
                    "school_abbr":    st.column_config.TextColumn("School"),
                    "leads":          st.column_config.NumberColumn("Leads"),
                    "apps_submitted": st.column_config.NumberColumn("Apps Submitted"),
                    "reg_submitted":  st.column_config.NumberColumn("Reg Complete"),
                    "reg_approved":   st.column_config.NumberColumn("Reg Approved"),
                },
                hide_index=True,
            )

        st.markdown("---")

        # ── Monthly applications pace ────────────────────────────────
        if not sm_apps_df.empty:
            pace_df = m.sm_monthly_pace(
                sm_apps_df,
                school_abbrs=selected_sm_schools or None,
            )
            if not pace_df.empty:
                st.plotly_chart(
                    c.sm_monthly_apps_chart(pace_df),
                    use_container_width=True,
                )
            else:
                st.info("No submitted applications with timestamps found for the selected filters.")

        # ── Grade-level breakdown ────────────────────────────────────
        with st.expander("Grade-level breakdown"):
            grade_table = m.sm_filter(
                sm_recruitment_df,
                school_abbrs=selected_sm_schools or None,
                grades=selected_sm_grades,
            ).sort_values(["school_abbr", "grade_level"])
            _rec_cols = ["school_abbr", "grade_label", "leads",
                         "apps_submitted", "reg_submitted", "reg_approved"]
            disp = grade_table[[col for col in _rec_cols if col in grade_table.columns]]
            st.dataframe(
                disp,
                use_container_width=True,
                column_config={
                    "school_abbr":    st.column_config.TextColumn("School"),
                    "grade_label":    st.column_config.TextColumn("Grade"),
                    "leads":          st.column_config.NumberColumn("Leads"),
                    "apps_submitted": st.column_config.NumberColumn("Apps Submitted"),
                    "reg_submitted":  st.column_config.NumberColumn("Reg Complete"),
                    "reg_approved":   st.column_config.NumberColumn("Reg Approved"),
                },
                hide_index=True,
            )
            csv_rec = disp.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download Recruitment Data (CSV)",
                data=csv_rec,
                file_name="recruitment_pipeline.csv",
                mime="text/csv",
            )
