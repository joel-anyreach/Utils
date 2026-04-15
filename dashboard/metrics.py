"""
Pure metric computation functions for the dashboard.
No Streamlit imports — all functions take DataFrames and return numbers/DataFrames.
"""

import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.constants import ACTIVE_SCHOOL_IDS, GRADE_SORT_ORDER, SCHOOL_MAP, GRADE_LABEL_MAP


# ── Helpers ───────────────────────────────────────────────────────────────────

def _int(val):
    try:
        return int(val)
    except Exception:
        return 0


_FUNNEL_NUM_COLS = [
    "total_enrolled", "next_school_assigned", "has_reenroll_record",
    "not_decided", "retained_same_school", "network_transfer",
    "graduating", "external_transfer",
]

def _cast_numeric(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Cast a list of columns to numeric int, filling NaN with 0."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df


def filter_funnel(funnel_df: pd.DataFrame, school_ids=None, grades=None) -> pd.DataFrame:
    df = funnel_df.copy()
    df["school_id"] = pd.to_numeric(df["school_id"], errors="coerce").fillna(0).astype(int)
    df["grade_level"] = pd.to_numeric(df["grade_level"], errors="coerce").fillna(-99).astype(int)
    df = _cast_numeric(df, _FUNNEL_NUM_COLS)
    if school_ids:
        df = df[df["school_id"].isin(school_ids)]
    if grades is not None and len(grades) > 0:
        df = df[df["grade_level"].isin(grades)]
    return df


def filter_students(students_df: pd.DataFrame, school_ids=None, grades=None,
                    active_only=True) -> pd.DataFrame:
    df = students_df.copy()
    df["school_id"] = pd.to_numeric(df["school_id"], errors="coerce").fillna(0).astype(int)
    df["grade_level"] = pd.to_numeric(df["grade_level"], errors="coerce").fillna(-99).astype(int)
    df["enroll_status"] = pd.to_numeric(df["enroll_status"], errors="coerce").fillna(-99).astype(int)
    if active_only:
        df = df[df["enroll_status"] == 0]
    if school_ids:
        df = df[df["school_id"].isin(school_ids)]
    if grades is not None and len(grades) > 0:
        df = df[df["grade_level"].isin(grades)]
    return df


def filter_reenroll(reenroll_df: pd.DataFrame, school_ids=None, grades=None,
                    year_range=None) -> pd.DataFrame:
    df = reenroll_df.copy()
    df["school_id"] = pd.to_numeric(df["school_id"], errors="coerce").fillna(0).astype(int)
    df["grade_level"] = pd.to_numeric(df["grade_level"], errors="coerce").fillna(-99).astype(int)
    df["school_year_start"] = pd.to_numeric(df["school_year_start"], errors="coerce").fillna(0).astype(int)
    if school_ids:
        df = df[df["school_id"].isin(school_ids)]
    if grades is not None and len(grades) > 0:
        df = df[df["grade_level"].isin(grades)]
    if year_range:
        df = df[df["school_year_start"].between(year_range[0], year_range[1])]
    return df


# ── Funnel KPIs ───────────────────────────────────────────────────────────────

def funnel_kpis(funnel_df: pd.DataFrame, school_ids=None, grades=None) -> dict:
    """Return the four top-level funnel metrics."""
    df = filter_funnel(funnel_df, school_ids, grades)
    if df.empty:
        return {"enrolled": 0, "assigned": 0, "reenrolled": 0, "retention_rate": 0.0,
                "rate_enrolled_to_assigned": 0.0, "rate_assigned_to_reenrolled": 0.0,
                "rate_enrolled_to_reenrolled": 0.0}

    enrolled = _int(df["total_enrolled"].sum())
    assigned = _int(df["next_school_assigned"].sum())
    reenrolled = _int(df["has_reenroll_record"].sum())
    retained = _int(df["retained_same_school"].sum())

    return {
        "enrolled": enrolled,
        "assigned": assigned,
        "reenrolled": reenrolled,
        "retained": retained,
        "retention_rate": round(retained / enrolled * 100, 1) if enrolled else 0.0,
        "rate_enrolled_to_assigned": round(assigned / enrolled * 100, 1) if enrolled else 0.0,
        "rate_assigned_to_reenrolled": round(reenrolled / assigned * 100, 1) if assigned else 0.0,
        "rate_enrolled_to_reenrolled": round(reenrolled / enrolled * 100, 1) if enrolled else 0.0,
    }


def retention_breakdown(funnel_df: pd.DataFrame, school_ids=None, grades=None) -> pd.DataFrame:
    """Retention status breakdown by school and grade."""
    df = filter_funnel(funnel_df, school_ids, grades)
    cols = ["school_abbr", "grade_label", "grade_level",
            "retained_same_school", "network_transfer", "graduating",
            "not_decided", "external_transfer", "total_enrolled"]
    available = [c for c in cols if c in df.columns]
    return df[available].copy()


# ── Enrollment Trends ─────────────────────────────────────────────────────────

def enrollment_by_sy(summary_df: pd.DataFrame, school_ids=None, year_range=None) -> pd.DataFrame:
    df = summary_df.copy()
    df["school_id"]         = pd.to_numeric(df["school_id"],         errors="coerce").fillna(0).astype(int)
    df["school_year_start"] = pd.to_numeric(df["school_year_start"], errors="coerce").fillna(0).astype(int)
    df["unique_students"]   = pd.to_numeric(df["unique_students"],   errors="coerce").fillna(0).astype(int)
    df["reenroll_records"]  = pd.to_numeric(df.get("reenroll_records", 0), errors="coerce").fillna(0).astype(int)

    # Aggregate per school × year: SUM counts so duplicate rows (e.g. from multiple uploads)
    # never silently drop data the way drop_duplicates would. Label columns use first non-null.
    agg_spec = {"unique_students": ("unique_students", "sum"),
                "reenroll_records": ("reenroll_records", "sum")}
    for lbl in ("school_year_label", "school_name", "school_abbr"):
        if lbl in df.columns:
            agg_spec[lbl] = (lbl, "first")
    df = df.groupby(["school_id", "school_year_start"], as_index=False).agg(**agg_spec)

    if school_ids:
        df = df[df["school_id"].isin(school_ids)]
    if year_range:
        df = df[df["school_year_start"].between(year_range[0], year_range[1])]
    df = df[df["school_id"].isin(ACTIVE_SCHOOL_IDS)]
    return df.sort_values(["school_year_start", "school_id"])


def yoy_change(summary_df: pd.DataFrame, school_ids=None) -> pd.DataFrame:
    """Compute year-over-year enrollment change per school."""
    df = enrollment_by_sy(summary_df, school_ids)
    df = df.sort_values(["school_id", "school_year_start"])
    df["prev_students"] = df.groupby("school_id")["unique_students"].shift(1)
    df["yoy_delta"] = df["unique_students"] - df["prev_students"]
    df["yoy_pct"] = (df["yoy_delta"] / df["prev_students"] * 100).round(1)
    return df.dropna(subset=["prev_students"])


def network_share_by_sy(summary_df: pd.DataFrame, school_ids=None, year_range=None) -> pd.DataFrame:
    """Each selected school's % of total network enrollment per year."""
    df = enrollment_by_sy(summary_df, school_ids=school_ids, year_range=year_range)
    totals = df.groupby("school_year_start")["unique_students"].sum().rename("network_total")
    df = df.merge(totals, on="school_year_start")
    df["pct_of_network"] = (df["unique_students"] / df["network_total"] * 100).round(1)
    return df


def enrollment_summary_kpis(summary_df: pd.DataFrame, school_ids=None) -> dict:
    """Current year enrollment total and YoY delta."""
    df = enrollment_by_sy(summary_df, school_ids)
    if df.empty:
        return {"current_year": 0, "current_total": 0, "yoy_delta": None, "yoy_pct": None}

    latest_year = df["school_year_start"].max()
    prev_year = latest_year - 1

    current = df[df["school_year_start"] == latest_year]["unique_students"].sum()
    prev = df[df["school_year_start"] == prev_year]["unique_students"].sum()

    delta = int(current - prev) if prev > 0 else None
    pct = round(delta / prev * 100, 1) if (prev > 0 and delta is not None) else None

    return {
        "current_year": latest_year,
        "current_year_label": f"{latest_year}-{latest_year+1}",
        "current_total": int(current),
        "yoy_delta": delta,
        "yoy_pct": pct,
    }


# ── Monthly Helpers ───────────────────────────────────────────────────────────

def _school_year_months(school_year_start: int) -> list:
    """
    Return ordered list of month dicts for a school year (Aug → Jul).
    Each dict: {month_start, month_end, month_label, month_date}
    Caps at today so future months are excluded for the current year.
    """
    import calendar
    from datetime import date, datetime
    today = date.today()
    months = []
    for i in range(12):
        # Month 0 = August of school_year_start, Month 11 = July of school_year_start+1
        month_num = (7 + i) % 12 + 1  # Aug=8 … Dec=12, Jan=1 … Jul=7
        year = school_year_start if month_num >= 8 else school_year_start + 1
        first_day = date(year, month_num, 1)
        last_day = date(year, month_num, calendar.monthrange(year, month_num)[1])
        if first_day > today:
            break
        months.append({
            "month_start": first_day,
            "month_end": last_day,
            "month_label": first_day.strftime("%b %Y"),
            "month_date": first_day,
        })
    return months


def monthly_enrollment_snapshot(
    reenroll_df: pd.DataFrame,
    students_df: pd.DataFrame,
    school_ids=None,
    school_year_start: int = None,
) -> pd.DataFrame:
    """
    Return active student headcount per school per month for a school year,
    split into returning_students (in network prior year) and new_students.
    - Historical years: uses reenroll_df entry/exit dates.
    - Current year: uses students_df (active students, no exit_date).
    """
    from datetime import date
    today_year = date.today().year
    today_month = date.today().month
    current_sy = today_year if today_month >= 8 else today_year - 1

    if school_year_start is None:
        school_year_start = current_sy

    months = _school_year_months(school_year_start)
    if not months:
        return pd.DataFrame()

    # Build network-wide prior-year student ID set (unfiltered by school so transfers count as returning)
    re = reenroll_df.copy()
    re["school_year_start"] = pd.to_numeric(re["school_year_start"], errors="coerce").fillna(0).astype(int)
    re["student_id"] = pd.to_numeric(re["student_id"], errors="coerce").fillna(0).astype(int)
    prior_year_sids = set(re[re["school_year_start"] == school_year_start - 1]["student_id"])

    rows = []

    if school_year_start == current_sy:
        # Use active students from students_df
        df = students_df.copy()
        df["school_id"] = pd.to_numeric(df["school_id"], errors="coerce").fillna(0).astype(int)
        df["student_id"] = pd.to_numeric(df["student_id"], errors="coerce").fillna(0).astype(int)
        df["enroll_status"] = pd.to_numeric(df["enroll_status"], errors="coerce").fillna(-99).astype(int)
        df = df[df["enroll_status"] == 0]
        if school_ids:
            df = df[df["school_id"].isin(school_ids)]
        df = df[df["school_id"].isin(ACTIVE_SCHOOL_IDS)]
        df["entry_date"] = pd.to_datetime(df["entry_date"], errors="coerce")

        for m in months:
            for sid, grp in df.groupby("school_id"):
                eligible = grp[grp["entry_date"] <= pd.Timestamp(m["month_end"])]
                count = len(eligible)
                returning = int(eligible["student_id"].isin(prior_year_sids).sum())
                abbr = grp["school_abbr"].iloc[0] if "school_abbr" in grp.columns and not grp.empty else SCHOOL_MAP.get(sid, {}).get("abbr", str(sid))
                rows.append({
                    "month_label": m["month_label"],
                    "month_date": m["month_date"],
                    "school_id": sid,
                    "school_abbr": abbr,
                    "active_students": count,
                    "returning_students": returning,
                    "new_students": count - returning,
                })
    else:
        # Use reenrollments for historical year
        df = reenroll_df.copy()
        df["school_id"] = pd.to_numeric(df["school_id"], errors="coerce").fillna(0).astype(int)
        df["student_id"] = pd.to_numeric(df["student_id"], errors="coerce").fillna(0).astype(int)
        df["school_year_start"] = pd.to_numeric(df["school_year_start"], errors="coerce").fillna(0).astype(int)
        df = df[df["school_year_start"] == school_year_start]
        if school_ids:
            df = df[df["school_id"].isin(school_ids)]
        df = df[df["school_id"].isin(ACTIVE_SCHOOL_IDS)]
        df["entry_date"] = pd.to_datetime(df["entry_date"], errors="coerce")
        df["exit_date"] = pd.to_datetime(df["exit_date"], errors="coerce")

        for m in months:
            ms = pd.Timestamp(m["month_start"])
            me = pd.Timestamp(m["month_end"])
            in_month = df[
                (df["entry_date"] <= me) &
                (df["exit_date"].isna() | (df["exit_date"] >= ms))
            ]
            for sid, grp in in_month.groupby("school_id"):
                abbr = grp["school_abbr"].iloc[0] if "school_abbr" in grp.columns and not grp.empty else SCHOOL_MAP.get(sid, {}).get("abbr", str(sid))
                count = len(grp)
                returning = int(grp["student_id"].isin(prior_year_sids).sum())
                rows.append({
                    "month_label": m["month_label"],
                    "month_date": m["month_date"],
                    "school_id": sid,
                    "school_abbr": abbr,
                    "active_students": count,
                    "returning_students": returning,
                    "new_students": count - returning,
                })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def monthly_reenroll_pace(
    reenroll_df: pd.DataFrame,
    school_ids=None,
    school_year_start: int = None,
) -> pd.DataFrame:
    """
    Count new re-enrollment records by month within a school year.
    Groups by school and month of entry_date.
    Returns new_enrollments per month and running cumulative per school.
    """
    from datetime import date
    today_year = date.today().year
    today_month = date.today().month
    current_sy = today_year if today_month >= 8 else today_year - 1

    if school_year_start is None:
        school_year_start = current_sy

    df = reenroll_df.copy()
    df["school_id"] = pd.to_numeric(df["school_id"], errors="coerce").fillna(0).astype(int)
    df["school_year_start"] = pd.to_numeric(df["school_year_start"], errors="coerce").fillna(0).astype(int)
    df = df[df["school_year_start"] == school_year_start]
    if school_ids:
        df = df[df["school_id"].isin(school_ids)]
    df = df[df["school_id"].isin(ACTIVE_SCHOOL_IDS)]

    if df.empty:
        return pd.DataFrame()

    df["entry_date"] = pd.to_datetime(df["entry_date"], errors="coerce")
    df = df.dropna(subset=["entry_date"])
    df["month_date"] = df["entry_date"].dt.to_period("M").dt.to_timestamp()
    df["month_label"] = df["entry_date"].dt.strftime("%b %Y")

    months = _school_year_months(school_year_start)
    month_order = {m["month_date"]: i for i, m in enumerate(months)}

    grp = df.groupby(["school_id", "school_abbr", "month_date", "month_label"], as_index=False).size()
    grp = grp.rename(columns={"size": "new_enrollments"})
    grp["month_order"] = grp["month_date"].map(month_order).fillna(999)
    grp = grp.sort_values(["school_id", "month_order"])
    grp["cumulative"] = grp.groupby("school_id")["new_enrollments"].cumsum()

    return grp.drop(columns=["month_order"])


# ── Grade Distribution ────────────────────────────────────────────────────────

def grade_distribution(funnel_df: pd.DataFrame, school_ids=None) -> pd.DataFrame:
    df = filter_funnel(funnel_df, school_ids)
    df["grade_level"] = pd.to_numeric(df["grade_level"], errors="coerce").fillna(-99).astype(int)
    result = df.groupby(["school_id", "school_abbr", "school_name", "grade_level", "grade_label"],
                        as_index=False)["total_enrolled"].sum()
    # Add sort key
    grade_order = {g: i for i, g in enumerate(GRADE_SORT_ORDER)}
    result["grade_sort"] = result["grade_level"].map(grade_order).fillna(99)
    return result.sort_values(["school_id", "grade_sort"])


def school_grade_heatmap_data(funnel_df: pd.DataFrame, school_ids=None) -> pd.DataFrame:
    """Pivot table: schools as rows, grades as columns, enrollment count as values."""
    df = grade_distribution(funnel_df, school_ids)
    pivot = df.pivot_table(
        index="school_abbr", columns="grade_label",
        values="total_enrolled", fill_value=0, aggfunc="sum"
    )
    # Order grade columns
    grade_label_order = ["PreK", "K", "1st", "2nd", "3rd", "4th", "5th",
                         "6th", "7th", "8th", "9th", "10th", "11th", "12th", "Graduated"]
    cols = [c for c in grade_label_order if c in pivot.columns]
    return pivot[cols]


# ── Retention Analysis ────────────────────────────────────────────────────────

def retention_heatmap_data(funnel_df: pd.DataFrame, school_ids=None) -> pd.DataFrame:
    """Pivot: school × grade, value = retention rate %."""
    df = filter_funnel(funnel_df, school_ids)
    df["retained_same_school"] = pd.to_numeric(df["retained_same_school"], errors="coerce").fillna(0)
    df["total_enrolled"] = pd.to_numeric(df["total_enrolled"], errors="coerce").fillna(0)
    df = df[df["total_enrolled"] > 0].copy()
    df["retention_rate"] = (df["retained_same_school"] / df["total_enrolled"] * 100).round(1)
    pivot = df.pivot_table(
        index="school_abbr", columns="grade_label",
        values="retention_rate", fill_value=np.nan, aggfunc="mean"
    )
    grade_label_order = ["PreK", "K", "1st", "2nd", "3rd", "4th", "5th",
                         "6th", "7th", "8th", "9th", "10th", "11th", "12th"]
    cols = [c for c in grade_label_order if c in pivot.columns]
    return pivot[cols]


def historical_retention(reenroll_df: pd.DataFrame, school_ids=None,
                         students_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    For each school and year, estimate retention as:
    students appearing in year N+1 at same school / students in year N.
    Covers 2015 onward.
    For the current school year, uses students_df (active students) instead of
    reenroll_df which won't have current-year records yet.
    """
    from datetime import date
    today = date.today()
    current_sy = today.year if today.month >= 8 else today.year - 1

    df = filter_reenroll(reenroll_df, school_ids, year_range=(2015, 2100))
    df["school_id"] = pd.to_numeric(df["school_id"], errors="coerce").fillna(0).astype(int)
    df["student_id"] = pd.to_numeric(df["student_id"], errors="coerce").fillna(0).astype(int)
    df = df[df["school_id"].isin(ACTIVE_SCHOOL_IDS)]

    # Build per-school student ID sets for the current year from students_df
    current_year_sids = {}  # school_id → set of student_ids
    if students_df is not None and not students_df.empty:
        sdf = students_df.copy()
        sdf["school_id"] = pd.to_numeric(sdf["school_id"], errors="coerce").fillna(0).astype(int)
        sdf["student_id"] = pd.to_numeric(sdf["student_id"], errors="coerce").fillna(0).astype(int)
        sdf["enroll_status"] = pd.to_numeric(sdf["enroll_status"], errors="coerce").fillna(-99).astype(int)
        sdf = sdf[sdf["enroll_status"] == 0]
        for sid in ACTIVE_SCHOOL_IDS:
            current_year_sids[sid] = set(sdf[sdf["school_id"] == sid]["student_id"])

    rows = []
    years = sorted(df["school_year_start"].unique())
    for year in years:
        next_year = year + 1
        use_current = (next_year == current_sy and bool(current_year_sids))
        if not use_current and next_year not in df["school_year_start"].values:
            continue
        for school_id in ACTIVE_SCHOOL_IDS:
            sids_now = set(df[(df["school_year_start"] == year) & (df["school_id"] == school_id)]["student_id"])
            if not sids_now:
                continue
            if use_current:
                sids_next = current_year_sids.get(school_id, set())
            else:
                sids_next = set(df[(df["school_year_start"] == next_year) & (df["school_id"] == school_id)]["student_id"])
            retained = len(sids_now & sids_next)
            rate = round(retained / len(sids_now) * 100, 1)
            rows.append({
                "school_year_start": year,
                "school_year_label": f"{year}-{year+1}",
                "school_id": school_id,
                "school_abbr": SCHOOL_MAP.get(school_id, {}).get("abbr", str(school_id)),
                "school_name": SCHOOL_MAP.get(school_id, {}).get("name", str(school_id)),
                "enrolled_this_year": len(sids_now),
                "retained_next_year": retained,
                "historical_retention_rate": rate,
            })

    return pd.DataFrame(rows)


# ── Not Decided Risk Table ────────────────────────────────────────────────────

def not_decided_students(students_df: pd.DataFrame, school_ids=None, grades=None) -> pd.DataFrame:
    """Active students where next_school_id == 0."""
    df = filter_students(students_df, school_ids, grades, active_only=True)
    df["next_school_id"] = pd.to_numeric(df["next_school_id"], errors="coerce").fillna(0).astype(int)
    nd = df[df["next_school_id"] == 0].copy()
    cols = ["student_id", "student_number", "last_first", "school_abbr",
            "grade_label", "entry_date", "retention_status"]
    available = [c for c in cols if c in nd.columns]
    return nd[available].sort_values(["school_abbr", "grade_label"])


# ── Cohort Progression (Sankey data) ─────────────────────────────────────────

def cohort_progression(reenroll_df: pd.DataFrame, school_id: int, from_year: int) -> pd.DataFrame:
    """
    For a given school and year, show where each grade's students
    appeared in the following year (same or different grade/school).
    """
    df = reenroll_df.copy()
    df["school_id"] = pd.to_numeric(df["school_id"], errors="coerce").fillna(0).astype(int)
    df["student_id"] = pd.to_numeric(df["student_id"], errors="coerce").fillna(0).astype(int)
    df["grade_level"] = pd.to_numeric(df["grade_level"], errors="coerce").fillna(-99).astype(int)
    df["school_year_start"] = pd.to_numeric(df["school_year_start"], errors="coerce").fillna(0).astype(int)

    year_n = df[(df["school_year_start"] == from_year) & (df["school_id"] == school_id)][
        ["student_id", "grade_level"]
    ].rename(columns={"grade_level": "from_grade"})

    year_n1 = df[df["school_year_start"] == from_year + 1][
        ["student_id", "grade_level", "school_id"]
    ].rename(columns={"grade_level": "to_grade", "school_id": "to_school_id"})

    merged = year_n.merge(year_n1, on="student_id", how="left")
    merged["to_grade"] = merged["to_grade"].fillna(-99).astype(int)
    merged["to_school_id"] = merged["to_school_id"].fillna(0).astype(int)
    merged["outcome"] = merged.apply(
        lambda r: (
            "Did Not Return" if r["to_school_id"] == 0 else
            "Same School" if r["to_school_id"] == school_id else
            SCHOOL_MAP.get(r["to_school_id"], {}).get("abbr", "Other Network")
        ),
        axis=1,
    )
    from shared.constants import GRADE_LABEL_MAP
    merged["from_grade_label"] = merged["from_grade"].map(GRADE_LABEL_MAP).fillna(merged["from_grade"].astype(str))
    merged["to_grade_label"] = merged["to_grade"].apply(lambda g: GRADE_LABEL_MAP.get(g, "—") if g != -99 else "—")

    summary = merged.groupby(["from_grade_label", "outcome"]).size().reset_index(name="students")
    return summary


# ── Enrollment History (reenroll + current year) ─────────────────────────────

def enrollment_history(
    reenroll_df: pd.DataFrame,
    students_df: pd.DataFrame,
    school_ids=None,
    year_range=None,
) -> pd.DataFrame:
    """
    Enrollment counts per school per year for the historical trend chart.
    Historical years: unique student_ids from reenroll_df.
    Current year: active students from students_df (reenroll records not yet populated).
    Returns columns compatible with enrollment_trend_line(): school_abbr, school_year_start,
    school_year_label, unique_students.
    """
    from datetime import date
    today = date.today()
    current_sy = today.year if today.month >= 8 else today.year - 1

    re = reenroll_df.copy()
    re["school_id"] = pd.to_numeric(re["school_id"], errors="coerce").fillna(0).astype(int)
    re["student_id"] = pd.to_numeric(re["student_id"], errors="coerce").fillna(0).astype(int)
    re["school_year_start"] = pd.to_numeric(re["school_year_start"], errors="coerce").fillna(0).astype(int)
    re = re[re["school_id"].isin(ACTIVE_SCHOOL_IDS)]
    if school_ids:
        re = re[re["school_id"].isin(school_ids)]
    if year_range:
        re = re[re["school_year_start"].between(year_range[0], year_range[1])]
    # Exclude current year from reenroll — students_df is the authoritative source for it
    hist = re[re["school_year_start"] != current_sy]

    parts = []
    if not hist.empty:
        grp = hist.groupby(
            ["school_year_start", "school_year_label", "school_id", "school_abbr"],
            as_index=False,
        ).agg(unique_students=("student_id", "nunique"))
        parts.append(grp)

    # Current year from students_df
    if not students_df.empty and (year_range is None or current_sy >= year_range[0]):
        sdf = students_df.copy()
        sdf["school_id"] = pd.to_numeric(sdf["school_id"], errors="coerce").fillna(0).astype(int)
        sdf["student_id"] = pd.to_numeric(sdf["student_id"], errors="coerce").fillna(0).astype(int)
        sdf["enroll_status"] = pd.to_numeric(sdf["enroll_status"], errors="coerce").fillna(-99).astype(int)
        sdf = sdf[sdf["enroll_status"] == 0]
        if school_ids:
            sdf = sdf[sdf["school_id"].isin(school_ids)]
        sdf = sdf[sdf["school_id"].isin(ACTIVE_SCHOOL_IDS)]
        current_rows = []
        for sid, g in sdf.groupby("school_id"):
            abbr = g["school_abbr"].iloc[0] if "school_abbr" in g.columns else SCHOOL_MAP.get(sid, {}).get("abbr", str(sid))
            current_rows.append({
                "school_year_start": current_sy,
                "school_year_label": f"{current_sy}-{current_sy + 1}",
                "school_id": sid,
                "school_abbr": abbr,
                "unique_students": int(g["student_id"].nunique()),
            })
        if current_rows:
            parts.append(pd.DataFrame(current_rows))

    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True).sort_values(["school_year_start", "school_id"])


# ── Projected Enrollment ──────────────────────────────────────────────────────

def projected_enrollment(summary_df: pd.DataFrame, funnel_df: pd.DataFrame,
                          school_ids=None) -> pd.DataFrame:
    """
    Simple projection: retained_same_school + avg historical intake.
    Returns one row per school.
    """
    kpi = funnel_kpis(funnel_df, school_ids)
    hist = enrollment_by_sy(summary_df, school_ids)

    rows = []
    active_schools = school_ids or ACTIVE_SCHOOL_IDS
    for sid in active_schools:
        school_hist = hist[hist["school_id"] == sid].sort_values("school_year_start")
        if len(school_hist) < 3:
            continue
        # Use only fully-completed historical years (exclude current year from avg)
        completed = school_hist.iloc[:-1] if len(school_hist) > 1 else school_hist
        recent = completed.tail(5)
        avg_enrollment = float(recent["unique_students"].mean())
        latest = int(school_hist.iloc[-1]["unique_students"])
        school_funnel = filter_funnel(funnel_df, school_ids=[sid])
        retained = _int(school_funnel["retained_same_school"].sum()) if not school_funnel.empty else 0
        # Estimated new intake = avg historical enrollment minus retained from prior year
        prior_retained_rate = retained / latest if latest > 0 else 0.8
        avg_new = max(0, round(avg_enrollment * (1 - prior_retained_rate)))
        projected = int(retained + avg_new)
        rows.append({
            "school_id": sid,
            "school_abbr": SCHOOL_MAP.get(sid, {}).get("abbr", str(sid)),
            "school_name": SCHOOL_MAP.get(sid, {}).get("name", str(sid)),
            "current_enrolled": latest,
            "retained_count": retained,
            "avg_historical_enrollment": int(avg_enrollment),
            "projected_enrollment": projected,
        })
    return pd.DataFrame(rows)


# ── SchoolMint Recruitment Pipeline metrics ───────────────────────────────────

_SM_FUNNEL_COLS = ["leads", "apps_submitted", "reg_submitted", "reg_approved"]


def sm_filter(
    recruitment_df: pd.DataFrame,
    school_abbrs=None,
    grades=None,
) -> pd.DataFrame:
    """Filter the summary_sm_recruitment table by school abbreviation and/or grade."""
    df = recruitment_df.copy()
    df["grade_level"] = pd.to_numeric(df["grade_level"], errors="coerce").fillna(-99).astype(int)
    for col in _SM_FUNNEL_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    if school_abbrs:
        df = df[df["school_abbr"].isin(school_abbrs)]
    if grades is not None and len(grades) > 0:
        df = df[df["grade_level"].isin(grades)]
    return df


def sm_funnel_kpis(
    recruitment_df: pd.DataFrame,
    school_abbrs=None,
    grades=None,
) -> dict:
    """
    Compute recruitment pipeline stage counts and conversion rates.
    Returns dict with keys: leads, apps_submitted, reg_submitted, reg_approved,
    rate_leads_to_apps, rate_apps_to_rc, rate_rc_to_ra,
    rate_apps_to_ra, rate_leads_to_rc, rate_leads_to_ra.
    """
    _zero = {
        "leads": 0, "apps_submitted": 0, "reg_submitted": 0, "reg_approved": 0,
        "rate_leads_to_apps": 0.0, "rate_apps_to_rc": 0.0, "rate_rc_to_ra": 0.0,
        "rate_apps_to_ra": 0.0, "rate_leads_to_rc": 0.0, "rate_leads_to_ra": 0.0,
    }
    if recruitment_df.empty:
        return _zero

    df = sm_filter(recruitment_df, school_abbrs, grades)
    if df.empty:
        return _zero

    leads   = _int(df["leads"].sum())
    apps    = _int(df["apps_submitted"].sum())
    reg_sub = _int(df["reg_submitted"].sum())
    reg_app = _int(df["reg_approved"].sum())

    def _rate(num, den):
        return round(num / den * 100, 1) if den else 0.0

    return {
        "leads":          leads,
        "apps_submitted": apps,
        "reg_submitted":  reg_sub,
        "reg_approved":   reg_app,
        "rate_leads_to_apps": _rate(apps,    leads),
        "rate_apps_to_rc":    _rate(reg_sub, apps),
        "rate_rc_to_ra":      _rate(reg_app, reg_sub),
        "rate_apps_to_ra":    _rate(reg_app, apps),
        "rate_leads_to_rc":   _rate(reg_sub, leads),
        "rate_leads_to_ra":   _rate(reg_app, leads),
    }


def sm_monthly_pace(
    apps_df: pd.DataFrame,
    school_abbrs=None,
) -> pd.DataFrame:
    """
    Count submitted applications per school per month from the raw SM applications table.
    Returns DataFrame with columns: school_abbr, month_date, month_label,
                                    apps_count, cumulative.
    """
    if apps_df.empty or "submitted_timestamp" not in apps_df.columns:
        return pd.DataFrame()

    df = apps_df.copy()

    # Filter to non-test submitted rows
    if "is_test_row" in df.columns:
        df = df[df["is_test_row"] != "True"]
    if "app_submitted" in df.columns:
        df = df[df["app_submitted"] == "Submitted"]

    if school_abbrs:
        df = df[df["school_abbr"].isin(school_abbrs)]

    df["_dt"] = pd.to_datetime(df["submitted_timestamp"], errors="coerce")
    df = df.dropna(subset=["_dt"])
    if df.empty:
        return pd.DataFrame()

    df["month_date"] = df["_dt"].dt.to_period("M").dt.to_timestamp()
    df["month_label"] = df["_dt"].dt.strftime("%b %Y")

    pace = (
        df.groupby(["school_abbr", "month_date", "month_label"])
        .size()
        .reset_index(name="apps_count")
        .sort_values(["school_abbr", "month_date"])
    )

    # Cumulative per school
    pace["cumulative"] = pace.groupby("school_abbr")["apps_count"].cumsum()

    return pace.reset_index(drop=True)
