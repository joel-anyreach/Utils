"""
Plotly chart factory functions for the dashboard.
Each function takes a DataFrame and returns a plotly Figure.
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.constants import SCHOOL_COLORS, RETENTION_COLORS, GRADE_LABEL_MAP


def _school_color(abbr: str) -> str:
    return SCHOOL_COLORS.get(abbr, "#636efa")


# ── Funnel Charts ─────────────────────────────────────────────────────────────

def funnel_chart(kpis: dict) -> go.Figure:
    """Horizontal funnel: Enrolled → Assigned → Re-enrolled."""
    labels = ["Currently Enrolled", "Next School Assigned", "Re-enrollment Record"]
    values = [kpis["enrolled"], kpis["assigned"], kpis["reenrolled"]]
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]

    fig = go.Figure(go.Funnel(
        y=labels,
        x=values,
        textinfo="value+percent initial",
        marker=dict(color=colors),
        connector=dict(line=dict(color="lightgray", width=1)),
    ))
    fig.update_layout(
        title="Re-enrollment Funnel",
        margin=dict(l=10, r=10, t=40, b=10),
        height=300,
    )
    return fig


def retention_stacked_bar(funnel_df: pd.DataFrame) -> go.Figure:
    """Stacked bar: retention status breakdown by school."""
    if funnel_df.empty:
        return go.Figure()

    df = funnel_df.copy()
    for col in ["retained_same_school", "network_transfer", "graduating",
                "not_decided", "external_transfer"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    grp = df.groupby("school_abbr", as_index=False).agg(
        Retained=("retained_same_school", "sum"),
        Network_Transfer=("network_transfer", "sum"),
        Graduating=("graduating", "sum"),
        Not_Decided=("not_decided", "sum"),
        External_Transfer=("external_transfer", "sum"),
    )

    fig = go.Figure()
    status_map = [
        ("Retained", "Retained"),
        ("Network_Transfer", "Network Transfer"),
        ("Graduating", "Graduating"),
        ("Not_Decided", "Not Decided"),
        ("External_Transfer", "External Transfer"),
    ]
    for col, label in status_map:
        fig.add_trace(go.Bar(
            name=label,
            x=grp["school_abbr"],
            y=grp[col],
            marker_color=RETENTION_COLORS.get(label, "#636efa"),
        ))

    fig.update_layout(
        barmode="stack",
        title="Retention Status by School",
        xaxis_title="School",
        yaxis_title="Students",
        legend_title="Status",
        margin=dict(l=10, r=10, t=40, b=10),
        height=350,
    )
    return fig


# ── Enrollment Trend Charts ───────────────────────────────────────────────────

def enrollment_trend_line(summary_df: pd.DataFrame) -> go.Figure:
    """Line chart: unique students per school per year."""
    if summary_df.empty:
        return go.Figure()

    df = summary_df.copy()
    df["unique_students"] = pd.to_numeric(df["unique_students"], errors="coerce").fillna(0).astype(int)

    fig = go.Figure()
    for abbr, grp in df.groupby("school_abbr"):
        grp = grp.sort_values("school_year_start")
        fig.add_trace(go.Scatter(
            x=grp["school_year_label"],
            y=grp["unique_students"],
            mode="lines+markers",
            name=abbr,
            line=dict(color=_school_color(abbr), width=2),
            marker=dict(size=6),
            hovertemplate=f"<b>{abbr}</b><br>%{{x}}<br>%{{y:,}} students<extra></extra>",
        ))

    fig.update_layout(
        title="Enrollment by School — Historical Trend",
        xaxis_title="School Year",
        yaxis_title="Unique Students",
        legend_title="School",
        hovermode="x unified",
        margin=dict(l=10, r=10, t=40, b=10),
        height=400,
    )
    return fig


def yoy_delta_bar(yoy_df: pd.DataFrame) -> go.Figure:
    """Grouped bar chart of YoY enrollment change per school."""
    if yoy_df.empty:
        return go.Figure()

    df = yoy_df.copy()
    df["yoy_delta"] = pd.to_numeric(df["yoy_delta"], errors="coerce").fillna(0)

    fig = go.Figure()
    for abbr, grp in df.groupby("school_abbr"):
        grp = grp.sort_values("school_year_start")
        colors = ["#2ca02c" if v >= 0 else "#d62728" for v in grp["yoy_delta"]]
        fig.add_trace(go.Bar(
            x=grp["school_year_label"],
            y=grp["yoy_delta"],
            name=abbr,
            marker_color=_school_color(abbr),
            hovertemplate=f"<b>{abbr}</b><br>%{{x}}<br>Change: %{{y:+,}}<extra></extra>",
        ))

    fig.update_layout(
        barmode="group",
        title="Year-over-Year Enrollment Change",
        xaxis_title="School Year",
        yaxis_title="Change in Students",
        legend_title="School",
        margin=dict(l=10, r=10, t=40, b=10),
        height=350,
    )
    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    return fig


def network_share_area(share_df: pd.DataFrame) -> go.Figure:
    """100% stacked area showing each school's % of total network enrollment."""
    if share_df.empty:
        return go.Figure()

    df = share_df.copy()
    df["pct_of_network"] = pd.to_numeric(df["pct_of_network"], errors="coerce").fillna(0)

    fig = go.Figure()
    for abbr, grp in df.groupby("school_abbr"):
        grp = grp.sort_values("school_year_start")
        fig.add_trace(go.Scatter(
            x=grp["school_year_label"],
            y=grp["pct_of_network"],
            mode="lines",
            name=abbr,
            stackgroup="one",
            line=dict(color=_school_color(abbr)),
            hovertemplate=f"<b>{abbr}</b><br>%{{x}}<br>%{{y:.1f}}% of network<extra></extra>",
        ))

    fig.update_layout(
        title="Network Enrollment Share Over Time",
        xaxis_title="School Year",
        yaxis_title="% of Network Total",
        yaxis=dict(ticksuffix="%", range=[0, 100]),
        legend_title="School",
        hovermode="x unified",
        margin=dict(l=10, r=10, t=40, b=10),
        height=350,
    )
    return fig


# ── Monthly Charts ────────────────────────────────────────────────────────────

def monthly_enrollment_line(snapshot_df: pd.DataFrame) -> go.Figure:
    """Line chart: active students per school per month for a selected school year."""
    if snapshot_df.empty:
        return go.Figure()

    df = snapshot_df.copy()
    df["active_students"] = pd.to_numeric(df["active_students"], errors="coerce").fillna(0).astype(int)
    df = df.sort_values("month_date")

    # Derive year label from the data
    if not df.empty:
        first_month = df["month_date"].min()
        sy_start = first_month.year if first_month.month >= 8 else first_month.year - 1
        year_label = f"{sy_start}-{sy_start + 1}"
    else:
        year_label = ""

    fig = go.Figure()
    for abbr, grp in df.groupby("school_abbr"):
        grp = grp.sort_values("month_date")
        fig.add_trace(go.Scatter(
            x=grp["month_label"],
            y=grp["active_students"],
            mode="lines+markers",
            name=abbr,
            line=dict(color=_school_color(abbr), width=2),
            marker=dict(size=6),
            hovertemplate=f"<b>{abbr}</b><br>%{{x}}<br>%{{y:,}} students<extra></extra>",
        ))

    fig.update_layout(
        title=f"Monthly Enrollment Snapshot — {year_label}",
        xaxis_title="Month",
        yaxis_title="Active Students",
        legend_title="School",
        hovermode="x unified",
        margin=dict(l=10, r=10, t=40, b=10),
        height=380,
    )
    return fig


def monthly_breakdown_bar(snapshot_df: pd.DataFrame) -> go.Figure:
    """
    Stacked bar: returning vs new enrollees per month (network total across all schools).
    Overlay dotted line = total active students.
    """
    if snapshot_df.empty or "returning_students" not in snapshot_df.columns:
        return go.Figure()

    df = snapshot_df.copy()
    for col in ("returning_students", "new_students", "active_students"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    agg = (
        df.groupby(["month_date", "month_label"], as_index=False)
        .agg(returning=("returning_students", "sum"),
             new=("new_students", "sum"),
             total=("active_students", "sum"))
        .sort_values("month_date")
    )

    month_order = agg["month_label"].tolist()

    # Derive year label
    sy_start = agg["month_date"].min().year
    if agg["month_date"].min().month < 8:
        sy_start -= 1
    year_label = f"{sy_start}-{sy_start + 1}"

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Returning",
        x=agg["month_label"],
        y=agg["returning"],
        marker_color="#1f77b4",
        hovertemplate="Returning: %{y:,}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="New",
        x=agg["month_label"],
        y=agg["new"],
        marker_color="#ff7f0e",
        hovertemplate="New: %{y:,}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=agg["month_label"],
        y=agg["total"],
        mode="lines+markers",
        name="Total",
        line=dict(color="black", width=2, dash="dot"),
        marker=dict(size=6, symbol="diamond"),
        hovertemplate="Total: %{y:,}<extra></extra>",
    ))

    fig.update_layout(
        barmode="stack",
        title=f"Monthly Enrollment — Returning vs New ({year_label})",
        xaxis_title="Month",
        yaxis_title="Students",
        legend_title="Type",
        xaxis=dict(categoryorder="array", categoryarray=month_order),
        hovermode="x unified",
        margin=dict(l=10, r=10, t=40, b=10),
        height=380,
    )
    return fig


def monthly_reenroll_pace_chart(pace_df: pd.DataFrame) -> go.Figure:
    """
    Grouped bar chart of new re-enrollment records per school per month,
    with a cumulative-total line overlay.
    """
    if pace_df.empty:
        return go.Figure()

    df = pace_df.copy()
    df["new_enrollments"] = pd.to_numeric(df["new_enrollments"], errors="coerce").fillna(0).astype(int)
    df["cumulative"] = pd.to_numeric(df["cumulative"], errors="coerce").fillna(0).astype(int)
    df = df.sort_values("month_date")

    month_order = df[["month_date", "month_label"]].drop_duplicates().sort_values("month_date")["month_label"].tolist()

    fig = go.Figure()

    # Bars: new enrollments per school
    for abbr, grp in df.groupby("school_abbr"):
        grp = grp.sort_values("month_date")
        fig.add_trace(go.Bar(
            name=abbr,
            x=grp["month_label"],
            y=grp["new_enrollments"],
            marker_color=_school_color(abbr),
            hovertemplate=f"<b>{abbr}</b><br>%{{x}}<br>New: %{{y:,}}<extra></extra>",
        ))

    # Line: cumulative total across all schools
    cum_total = df.groupby(["month_date", "month_label"], as_index=False)["new_enrollments"].sum()
    cum_total = cum_total.sort_values("month_date")
    cum_total["cumulative_total"] = cum_total["new_enrollments"].cumsum()

    fig.add_trace(go.Scatter(
        x=cum_total["month_label"],
        y=cum_total["cumulative_total"],
        mode="lines+markers",
        name="Cumulative Total",
        line=dict(color="black", width=2, dash="dot"),
        marker=dict(size=6, symbol="diamond"),
        hovertemplate="Cumulative: %{y:,}<extra></extra>",
    ))

    fig.update_layout(
        barmode="group",
        title="Monthly Re-enrollment Pace",
        xaxis_title="Month",
        yaxis_title="Re-enrollment Records",
        legend_title="School",
        xaxis=dict(categoryorder="array", categoryarray=month_order),
        hovermode="x unified",
        margin=dict(l=10, r=10, t=40, b=10),
        height=380,
    )
    return fig


# ── Grade Distribution Charts ─────────────────────────────────────────────────

def grade_bar(grade_df: pd.DataFrame) -> go.Figure:
    """Grouped bar: enrollment by grade, colored by school."""
    if grade_df.empty:
        return go.Figure()

    df = grade_df.copy()
    df["total_enrolled"] = pd.to_numeric(df["total_enrolled"], errors="coerce").fillna(0).astype(int)

    fig = go.Figure()
    for abbr, grp in df.groupby("school_abbr"):
        grp = grp.sort_values("grade_sort")
        fig.add_trace(go.Bar(
            x=grp["grade_label"],
            y=grp["total_enrolled"],
            name=abbr,
            marker_color=_school_color(abbr),
            hovertemplate=f"<b>{abbr}</b><br>Grade %{{x}}<br>%{{y}} students<extra></extra>",
        ))

    fig.update_layout(
        barmode="group",
        title="Current Enrollment by Grade",
        xaxis_title="Grade",
        yaxis_title="Students",
        legend_title="School",
        margin=dict(l=10, r=10, t=40, b=10),
        height=350,
    )
    return fig


def school_grade_heatmap(pivot_df: pd.DataFrame, title="Enrollment Heatmap") -> go.Figure:
    """Heatmap: school × grade, color = enrollment count."""
    if pivot_df.empty:
        return go.Figure()

    fig = go.Figure(go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns.tolist(),
        y=pivot_df.index.tolist(),
        colorscale="Blues",
        text=pivot_df.values,
        texttemplate="%{text}",
        showscale=True,
        hovertemplate="School: %{y}<br>Grade: %{x}<br>Students: %{z}<extra></extra>",
    ))
    fig.update_layout(
        title=title,
        xaxis_title="Grade",
        yaxis_title="School",
        margin=dict(l=10, r=10, t=40, b=10),
        height=300,
    )
    return fig


def cohort_sankey(progression_df: pd.DataFrame, school_abbr: str, year_label: str) -> go.Figure:
    """Sankey diagram: from_grade → outcome for a given school/year."""
    if progression_df.empty:
        return go.Figure()

    df = progression_df.copy()
    df["students"] = pd.to_numeric(df["students"], errors="coerce").fillna(0).astype(int)
    df = df[df["students"] > 0]

    from_labels = sorted(df["from_grade_label"].unique().tolist())
    to_labels = sorted(df["outcome"].unique().tolist())
    all_labels = from_labels + [l for l in to_labels if l not in from_labels]
    label_idx = {l: i for i, l in enumerate(all_labels)}

    sources = [label_idx[r["from_grade_label"]] for _, r in df.iterrows()]
    targets = [label_idx[r["outcome"]] for _, r in df.iterrows()]
    values = [r["students"] for _, r in df.iterrows()]

    fig = go.Figure(go.Sankey(
        node=dict(
            pad=15, thickness=20,
            label=all_labels,
            color=["#1f77b4"] * len(from_labels) + ["#aec7e8"] * (len(all_labels) - len(from_labels)),
        ),
        link=dict(source=sources, target=targets, value=values),
    ))
    fig.update_layout(
        title=f"Grade Cohort Flow — {school_abbr} ({year_label})",
        margin=dict(l=10, r=10, t=40, b=10),
        height=400,
    )
    return fig


# ── Retention Analysis Charts ─────────────────────────────────────────────────

def retention_rate_heatmap(pivot_df: pd.DataFrame) -> go.Figure:
    """Heatmap: school × grade, color = retention rate %."""
    if pivot_df.empty:
        return go.Figure()

    fig = go.Figure(go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns.tolist(),
        y=pivot_df.index.tolist(),
        colorscale="RdYlGn",
        zmin=0, zmax=100,
        text=[[f"{v:.0f}%" if not np.isnan(v) else "—" for v in row] for row in pivot_df.values],
        texttemplate="%{text}",
        showscale=True,
        colorbar=dict(ticksuffix="%"),
        hovertemplate="School: %{y}<br>Grade: %{x}<br>Retention: %{z:.1f}%<extra></extra>",
    ))
    fig.update_layout(
        title="Retention Rate by School and Grade",
        xaxis_title="Grade",
        yaxis_title="School",
        margin=dict(l=10, r=10, t=40, b=10),
        height=300,
    )
    return fig


def historical_retention_line(hist_df: pd.DataFrame) -> go.Figure:
    """Line chart: historical retention rate per school over time."""
    if hist_df.empty:
        return go.Figure()

    df = hist_df.copy()
    df["historical_retention_rate"] = pd.to_numeric(
        df["historical_retention_rate"], errors="coerce"
    ).fillna(0)

    fig = go.Figure()
    for abbr, grp in df.groupby("school_abbr"):
        grp = grp.sort_values("school_year_start")
        fig.add_trace(go.Scatter(
            x=grp["school_year_label"],
            y=grp["historical_retention_rate"],
            mode="lines+markers",
            name=abbr,
            line=dict(color=_school_color(abbr), width=2),
            marker=dict(size=6),
            hovertemplate=f"<b>{abbr}</b><br>%{{x}}<br>Retention: %{{y:.1f}}%<extra></extra>",
        ))

    fig.update_layout(
        title="Historical Retention Rate (Year-over-Year)",
        xaxis_title="School Year",
        yaxis_title="Retention Rate",
        yaxis=dict(ticksuffix="%", range=[0, 105]),
        legend_title="School",
        hovermode="x unified",
        margin=dict(l=10, r=10, t=40, b=10),
        height=380,
    )
    fig.add_hline(y=80, line_dash="dot", line_color="orange",
                  annotation_text="80% target", annotation_position="top right")
    return fig


def retention_grouped_bar(funnel_df: pd.DataFrame, by: str = "school") -> go.Figure:
    """Grouped bar: retention breakdown by school or grade."""
    if funnel_df.empty:
        return go.Figure()

    df = funnel_df.copy()
    for col in ["retained_same_school", "network_transfer", "graduating", "not_decided", "external_transfer"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    group_col = "school_abbr" if by == "school" else "grade_label"
    grp = df.groupby(group_col, as_index=False).agg(
        Retained=("retained_same_school", "sum"),
        Network_Transfer=("network_transfer", "sum"),
        Graduating=("graduating", "sum"),
        Not_Decided=("not_decided", "sum"),
        External_Transfer=("external_transfer", "sum"),
    )

    fig = go.Figure()
    for col, label in [("Retained", "Retained"), ("Network_Transfer", "Network Transfer"),
                       ("Graduating", "Graduating"), ("Not_Decided", "Not Decided"),
                       ("External_Transfer", "External Transfer")]:
        fig.add_trace(go.Bar(
            name=label,
            x=grp[group_col],
            y=grp[col],
            marker_color=RETENTION_COLORS.get(label, "#636efa"),
        ))

    fig.update_layout(
        barmode="group",
        title=f"Retention Breakdown by {by.title()}",
        xaxis_title=by.title(),
        yaxis_title="Students",
        legend_title="Status",
        margin=dict(l=10, r=10, t=40, b=10),
        height=350,
    )
    return fig


# ── Historical Deep Dive ──────────────────────────────────────────────────────

def historical_enrollment_lines(reenroll_df: pd.DataFrame, school_ids=None) -> go.Figure:
    """Multi-line time-series of enrollment records by school × year."""
    df = reenroll_df.copy()
    df["school_id"] = pd.to_numeric(df["school_id"], errors="coerce").fillna(0).astype(int)
    df["school_year_start"] = pd.to_numeric(df["school_year_start"], errors="coerce").fillna(0).astype(int)
    df["student_id"] = pd.to_numeric(df["student_id"], errors="coerce").fillna(0).astype(int)

    from shared.constants import ACTIVE_SCHOOL_IDS
    active = school_ids or ACTIVE_SCHOOL_IDS
    df = df[df["school_id"].isin(active)]

    grp = df.groupby(["school_year_start", "school_year_label", "school_id", "school_abbr"],
                     as_index=False).agg(unique_students=("student_id", "nunique"))

    fig = go.Figure()
    for abbr, g in grp.groupby("school_abbr"):
        g = g.sort_values("school_year_start")
        fig.add_trace(go.Scatter(
            x=g["school_year_label"],
            y=g["unique_students"],
            mode="lines+markers",
            name=abbr,
            line=dict(color=_school_color(abbr), width=2),
            marker=dict(size=5),
            hovertemplate=f"<b>{abbr}</b><br>%{{x}}<br>%{{y:,}} students<extra></extra>",
        ))

    fig.update_layout(
        title="Historical Enrollment Records by School",
        xaxis_title="School Year",
        yaxis_title="Unique Students",
        legend_title="School",
        hovermode="x unified",
        margin=dict(l=10, r=10, t=40, b=10),
        height=400,
    )
    return fig


# ── SchoolMint Recruitment Pipeline charts ────────────────────────────────────

def sm_pipeline_funnel_chart(kpis: dict) -> go.Figure:
    """Four-stage horizontal funnel: Leads → Apps Submitted → Reg Complete → Reg Approved."""
    labels = ["Leads", "Apps Submitted", "Reg Complete", "Reg Approved"]
    values = [
        kpis.get("leads", 0),
        kpis.get("apps_submitted", 0),
        kpis.get("reg_submitted", 0),
        kpis.get("reg_approved", 0),
    ]
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#9467bd"]

    fig = go.Figure(go.Funnel(
        y=labels,
        x=values,
        textinfo="value+percent initial",
        marker=dict(color=colors),
        connector=dict(line=dict(color="lightgray", width=1)),
    ))
    fig.update_layout(
        title="Recruitment Pipeline",
        margin=dict(l=10, r=10, t=40, b=10),
        height=350,
    )
    return fig


def sm_monthly_apps_chart(pace_df: pd.DataFrame) -> go.Figure:
    """Grouped bar (apps per school per month) + cumulative network line."""
    if pace_df.empty:
        return go.Figure()

    fig = go.Figure()

    # Bars per school
    for abbr, grp in pace_df.groupby("school_abbr"):
        grp = grp.sort_values("month_date")
        fig.add_trace(go.Bar(
            name=abbr,
            x=grp["month_label"],
            y=grp["apps_count"],
            marker_color=_school_color(abbr),
            hovertemplate=f"<b>{abbr}</b><br>%{{x}}<br>%{{y:,}} applications<extra></extra>",
        ))

    # Cumulative network total line
    network = (
        pace_df.groupby(["month_date", "month_label"], as_index=False)["apps_count"]
        .sum()
        .sort_values("month_date")
    )
    network["cumulative"] = network["apps_count"].cumsum()
    fig.add_trace(go.Scatter(
        name="Cumulative Total",
        x=network["month_label"],
        y=network["cumulative"],
        mode="lines+markers",
        line=dict(color="#333333", width=2, dash="dot"),
        marker=dict(size=5),
        yaxis="y2",
        hovertemplate="Cumulative: %{y:,}<extra></extra>",
    ))

    fig.update_layout(
        title="Monthly Applications Submitted",
        barmode="stack",
        xaxis_title="Month",
        yaxis_title="Applications",
        yaxis2=dict(
            title="Cumulative",
            overlaying="y",
            side="right",
            showgrid=False,
        ),
        legend_title="School",
        hovermode="x unified",
        margin=dict(l=10, r=10, t=40, b=10),
        height=400,
    )
    return fig
