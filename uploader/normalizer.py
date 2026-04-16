"""
Data normalization for PowerSchool CSV exports.
Cleans, joins, and derives columns for all 4 export tables.
"""

import io
import re as _re
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.constants import (
    SCHOOL_MAP, NETWORK_SCHOOL_IDS, GRADE_LABEL_MAP, ENROLL_STATUS_MAP, ACTIVE_SCHOOL_IDS,
    SM_SCHOOL_ABBR_MAP, SM_GRADE_MAP, GRADE_SORT_ORDER,
)


def _strip_prefix(df: pd.DataFrame) -> pd.DataFrame:
    """Remove TABLENAME. prefix from all column names (PS export format)."""
    df.columns = [c.split(".")[-1] if "." in c else c for c in df.columns]
    return df


def _read_csv(file_obj, encoding="utf-8") -> pd.DataFrame:
    """Read CSV from a file object or path, trying utf-8 then latin-1."""
    if hasattr(file_obj, "read"):
        raw = file_obj.read()
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        for enc in [encoding, "latin-1", "utf-8-sig"]:
            try:
                return pd.read_csv(io.BytesIO(raw), encoding=enc, dtype=str)
            except (UnicodeDecodeError, Exception):
                continue
        return pd.read_csv(io.BytesIO(raw), encoding="latin-1", dtype=str)
    else:
        for enc in [encoding, "latin-1", "utf-8-sig"]:
            try:
                return pd.read_csv(file_obj, encoding=enc, dtype=str)
            except (UnicodeDecodeError, Exception):
                continue
        return pd.read_csv(file_obj, encoding="latin-1", dtype=str)


def _read_file(file_obj) -> pd.DataFrame:
    """Read CSV or xlsx from a file-like object, auto-detecting by name attribute."""
    name = getattr(file_obj, "name", "") or ""
    if name.lower().endswith(".xlsx") or name.lower().endswith(".xls"):
        raw = file_obj.read() if hasattr(file_obj, "read") else open(file_obj, "rb").read()
        return pd.read_excel(io.BytesIO(raw), dtype=str)
    return _read_csv(file_obj)


def _school_name(school_id: int, field: str = "name") -> str:
    info = SCHOOL_MAP.get(school_id, {})
    return info.get(field, str(school_id))


def _grade_label(grade: int) -> str:
    return GRADE_LABEL_MAP.get(grade, str(grade))


def _school_year_from_date(date_series: pd.Series):
    """Derive school_year_start (int) from a date series. Aug+ = current year."""
    dt = pd.to_datetime(date_series, errors="coerce")
    year = dt.dt.year.fillna(0).astype(int)
    month = dt.dt.month.fillna(0).astype(int)
    return year.where(month >= 8, year - 1)


def normalize_schools(file_obj) -> tuple[pd.DataFrame, list]:
    """Normalize Schools export. Returns (df, warnings)."""
    warnings = []
    df = _read_csv(file_obj)
    df = _strip_prefix(df)

    # Rename to standard internal names
    rename = {
        "ID": "school_internal_id",
        "Name": "school_name",
        "Abbreviation": "school_abbr",
        "School_Number": "school_id",
        "Alternate_School_Number": "alt_school_number",
        "District_Number": "district_number",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    if "school_id" not in df.columns:
        warnings.append("Schools file missing School_Number column — cannot join to other tables.")
        return df, warnings

    df["school_id"] = pd.to_numeric(df["school_id"], errors="coerce").fillna(0).astype(int)
    df = df[df["school_id"] != 0]

    # Ensure abbr / name from constants where missing
    df["school_name"] = df.apply(
        lambda r: _school_name(r["school_id"], "name") if pd.isna(r.get("school_name")) or r.get("school_name", "") == ""
        else r.get("school_name", _school_name(r["school_id"], "name")),
        axis=1,
    )
    # school_abbr: use PS export value when present, fall back to SCHOOL_MAP
    df["school_abbr"] = df.apply(
        lambda r: _school_name(r["school_id"], "abbr") if pd.isna(r.get("school_abbr")) or r.get("school_abbr", "") == ""
        else r.get("school_abbr", _school_name(r["school_id"], "abbr")),
        axis=1,
    )

    return df, warnings


def normalize_terms(file_obj) -> tuple[pd.DataFrame, list]:
    """Normalize Terms export."""
    warnings = []
    df = _read_csv(file_obj)
    df = _strip_prefix(df)

    rename = {
        "FirstDay": "first_day",
        "LastDay": "last_day",
        "Name": "term_name",
        "NoOfDays": "num_days",
        "SchoolID": "school_id",
        "YearID": "year_id",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    df["school_id"] = pd.to_numeric(df.get("school_id", 0), errors="coerce").fillna(0).astype(int)
    df["year_id"] = pd.to_numeric(df.get("year_id", 0), errors="coerce").fillna(0).astype(int)
    df["num_days"] = pd.to_numeric(df.get("num_days", 0), errors="coerce").fillna(0).astype(int)
    df["first_day"] = pd.to_datetime(df.get("first_day"), errors="coerce").dt.strftime("%Y-%m-%d")
    df["last_day"] = pd.to_datetime(df.get("last_day"), errors="coerce").dt.strftime("%Y-%m-%d")

    # is_full_year: term name matches YYYY-YYYY pattern
    df["is_full_year"] = df["term_name"].str.match(r"^\d{4}-\d{4}$", na=False)

    # school_year_label from year_id: year_id 35 → 2025-2026
    # YearID = year - 1990; so 35 → 2025, 36 → 2026
    df["school_year_start"] = df["year_id"] + 1990
    df["school_year_label"] = df["school_year_start"].apply(
        lambda y: f"{y}-{y+1}" if y > 1990 else ""
    )
    df["school_name"] = df["school_id"].apply(lambda x: _school_name(x, "name"))
    df["school_abbr"] = df["school_id"].apply(lambda x: _school_name(x, "abbr"))

    return df, warnings


def normalize_students(file_obj, schools_df: pd.DataFrame = None) -> tuple[pd.DataFrame, list]:
    """Normalize Students export. Returns (df, warnings)."""
    warnings = []
    df = _read_csv(file_obj, encoding="latin-1")
    df = _strip_prefix(df)

    rename = {
        "ID": "student_id",
        "Student_Number": "student_number",
        "LastFirst": "last_first",
        "Enroll_Status": "enroll_status",
        "Enrollment_SchoolID": "enrollment_school_id",
        "SchoolID": "school_id",
        "Grade_Level": "grade_level",
        "EntryDate": "entry_date",
        "ExitDate": "exit_date",
        "SchoolEntryDate": "school_entry_date",
        "SchoolEntryGradeLevel": "school_entry_grade",
        "Next_School": "next_school_id",
        "CampusID": "campus_id",
        "GuardianEmail": "guardian_email",
        "Home_Phone": "home_phone",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    # Numeric conversions
    for col in ["student_id", "school_id", "grade_level", "enroll_status",
                "next_school_id", "enrollment_school_id", "school_entry_grade"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Drop the system placeholder record
    df = df[df["student_id"] != -100]

    # Date columns
    for col in ["entry_date", "exit_date", "school_entry_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    # Derived labels
    df["enroll_status_label"] = df["enroll_status"].apply(
        lambda x: ENROLL_STATUS_MAP.get(x, f"Status {x}")
    )
    df["grade_label"] = df["grade_level"].apply(_grade_label)

    # School name/abbr — use schools export data first, fall back to SCHOOL_MAP constants
    if schools_df is not None and not schools_df.empty and "school_id" in schools_df.columns:
        _sl = schools_df[["school_id", "school_name", "school_abbr"]].copy()
        _sl["school_id"] = pd.to_numeric(_sl["school_id"], errors="coerce").fillna(0).astype(int)
        _sl = _sl.drop_duplicates("school_id")
        df = df.merge(_sl, on="school_id", how="left")
        _mask = df["school_name"].isna() | (df["school_name"] == "")
        df.loc[_mask, "school_name"] = df.loc[_mask, "school_id"].apply(lambda x: _school_name(x, "name"))
        _mask = df["school_abbr"].isna() | (df["school_abbr"] == "")
        df.loc[_mask, "school_abbr"] = df.loc[_mask, "school_id"].apply(lambda x: _school_name(x, "abbr"))
        # Next school labels from the same lookup
        _nsl = _sl.rename(columns={"school_id": "next_school_id",
                                    "school_name": "next_school_name",
                                    "school_abbr": "next_school_abbr"})
        df = df.merge(_nsl, on="next_school_id", how="left")
        _mask = df["next_school_name"].isna() | (df["next_school_name"] == "")
        df.loc[_mask, "next_school_name"] = df.loc[_mask, "next_school_id"].apply(lambda x: _school_name(x, "name"))
        _mask = df["next_school_abbr"].isna() | (df["next_school_abbr"] == "")
        df.loc[_mask, "next_school_abbr"] = df.loc[_mask, "next_school_id"].apply(lambda x: _school_name(x, "abbr"))
    else:
        df["school_name"]      = df["school_id"].apply(lambda x: _school_name(x, "name"))
        df["school_abbr"]      = df["school_id"].apply(lambda x: _school_name(x, "abbr"))
        df["next_school_name"] = df["next_school_id"].apply(lambda x: _school_name(x, "name"))
        df["next_school_abbr"] = df["next_school_id"].apply(lambda x: _school_name(x, "abbr"))

    # Retention status (for active students; others get N/A)
    def retention_status(row):
        if row["enroll_status"] != 0:
            return "N/A"
        ns = row["next_school_id"]
        sid = row["school_id"]
        if ns == 0:
            return "Not Decided"
        if ns == sid:
            return "Retained"
        if ns == 999999:
            return "Graduating"
        if ns in NETWORK_SCHOOL_IDS:
            return "Network Transfer"
        return "External Transfer"

    df["retention_status"] = df.apply(retention_status, axis=1)

    if df["student_id"].isna().any() or (df["student_id"] == 0).sum() > 5:
        warnings.append(f"Students: {(df['student_id'] == 0).sum()} rows with student_id=0 found.")

    return df, warnings


def normalize_reenrollments(file_obj, schools_df: pd.DataFrame = None) -> tuple[pd.DataFrame, list]:
    """Normalize ReEnrollments export."""
    warnings = []
    df = _read_csv(file_obj)
    df = _strip_prefix(df)

    rename = {
        "ID": "reenroll_id",
        "StudentID": "student_id",
        "SchoolID": "school_id",
        "EnrollmentCode": "enrollment_code",
        "EntryDate": "entry_date",
        "ExitDate": "exit_date",
        "Grade_Level": "grade_level",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    for col in ["reenroll_id", "student_id", "school_id", "grade_level", "enrollment_code"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in ["entry_date", "exit_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # School year derivation
    df["school_year_start"] = _school_year_from_date(df["entry_date"])
    df["school_year_label"] = df["school_year_start"].apply(
        lambda y: f"{y}-{y+1}" if y > 0 else ""
    )

    # Format dates as strings after derivation
    for col in ["entry_date", "exit_date"]:
        if col in df.columns:
            df[col] = df[col].dt.strftime("%Y-%m-%d")

    df["grade_label"] = df["grade_level"].apply(_grade_label)
    # School name/abbr — use schools export data first, fall back to SCHOOL_MAP constants
    if schools_df is not None and not schools_df.empty and "school_id" in schools_df.columns:
        _sl = schools_df[["school_id", "school_name", "school_abbr"]].copy()
        _sl["school_id"] = pd.to_numeric(_sl["school_id"], errors="coerce").fillna(0).astype(int)
        _sl = _sl.drop_duplicates("school_id")
        df = df.merge(_sl, on="school_id", how="left")
        _mask = df["school_name"].isna() | (df["school_name"] == "")
        df.loc[_mask, "school_name"] = df.loc[_mask, "school_id"].apply(lambda x: _school_name(x, "name"))
        _mask = df["school_abbr"].isna() | (df["school_abbr"] == "")
        df.loc[_mask, "school_abbr"] = df.loc[_mask, "school_id"].apply(lambda x: _school_name(x, "abbr"))
    else:
        df["school_name"] = df["school_id"].apply(lambda x: _school_name(x, "name"))
        df["school_abbr"] = df["school_id"].apply(lambda x: _school_name(x, "abbr"))

    return df, warnings


def build_summary_enrollment(reenroll_df: pd.DataFrame,
                             students_df: pd.DataFrame = None,
                             schools_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Aggregate unique students and record counts by school × school_year.
    For the CURRENT school year, the ReEnrollments table only contains a handful
    of records because active enrollments live in the Students table until
    year-end close-out. We therefore inject the current year from active Students.
    School name/abbr comes from schools_df (schools export) when available,
    falling back to SCHOOL_MAP constants.
    """
    today = datetime.today()
    current_sy = today.year if today.month >= 8 else today.year - 1

    # Ensure key columns are numeric before comparisons
    re = reenroll_df.copy()
    re["school_id"]        = pd.to_numeric(re.get("school_id",        0), errors="coerce").fillna(0).astype(int)
    re["student_id"]       = pd.to_numeric(re.get("student_id",       0), errors="coerce").fillna(0).astype(int)
    re["school_year_start"]= pd.to_numeric(re.get("school_year_start",0), errors="coerce").fillna(0).astype(int)
    if "reenroll_id" in re.columns:
        re["reenroll_id"]  = pd.to_numeric(re["reenroll_id"],           errors="coerce").fillna(0).astype(int)

    # Historical: active schools, valid calendar years only, exclude current year
    # school_year_start < 2000 indicates a null/invalid entry_date — skip those rows
    reenroll_id_col = "reenroll_id" if "reenroll_id" in re.columns else "student_id"
    hist_mask = (
        re["school_id"].isin(ACTIVE_SCHOOL_IDS) &
        (re["school_year_start"] >= 2000) &
        (re["school_year_start"] < current_sy)
    )
    hist = (
        re[hist_mask]
        .groupby(["school_year_start", "school_year_label", "school_id"])
        .agg(
            unique_students=("student_id",     "nunique"),
            reenroll_records=(reenroll_id_col, "count"),
        )
        .reset_index()
    )

    # Current year: use active Students (enroll_status=0) counts per school
    rows = []
    if students_df is not None and not students_df.empty:
        stu = students_df.copy()
        stu["enroll_status"] = pd.to_numeric(stu.get("enroll_status", -1), errors="coerce").fillna(-1).astype(int)
        stu["school_id"]     = pd.to_numeric(stu.get("school_id",      0), errors="coerce").fillna(0).astype(int)
        stu["student_id"]    = pd.to_numeric(stu.get("student_id",     0), errors="coerce").fillna(0).astype(int)
        active = stu[stu["enroll_status"] == 0]
        for school_id, grp in active.groupby("school_id"):
            if school_id not in ACTIVE_SCHOOL_IDS:
                continue
            rows.append({
                "school_year_start": current_sy,
                "school_year_label": f"{current_sy}-{current_sy+1}",
                "school_id":         school_id,
                "unique_students":   grp["student_id"].nunique(),
                "reenroll_records":  len(grp),
                "data_source":       "students_active",
            })
    current_df = pd.DataFrame(rows)

    # Combine
    combined = pd.concat([hist, current_df], ignore_index=True)

    # Apply school labels: prefer schools_df (export data), fall back to SCHOOL_MAP constants
    if schools_df is not None and not schools_df.empty and "school_id" in schools_df.columns:
        _sl = schools_df[["school_id", "school_name", "school_abbr"]].copy()
        _sl["school_id"] = pd.to_numeric(_sl["school_id"], errors="coerce").fillna(0).astype(int)
        _sl = _sl.drop_duplicates("school_id")
        combined = combined.drop(columns=["school_name", "school_abbr"], errors="ignore")
        combined = combined.merge(_sl, on="school_id", how="left")
        _mask = combined["school_name"].isna() | (combined["school_name"].astype(str) == "")
        combined.loc[_mask, "school_name"] = combined.loc[_mask, "school_id"].apply(lambda x: _school_name(x, "name"))
        _mask = combined["school_abbr"].isna() | (combined["school_abbr"].astype(str) == "")
        combined.loc[_mask, "school_abbr"] = combined.loc[_mask, "school_id"].apply(lambda x: _school_name(x, "abbr"))
    else:
        combined["school_name"] = combined["school_id"].apply(lambda x: _school_name(x, "name"))
        combined["school_abbr"] = combined["school_id"].apply(lambda x: _school_name(x, "abbr"))

    return combined.sort_values(["school_year_start", "school_id"])


def build_summary_funnel(students_df: pd.DataFrame, reenroll_df: pd.DataFrame) -> pd.DataFrame:
    """
    Funnel counts per school × grade for currently active students (enroll_status=0).
    has_reenroll_record = student appears in ReEnrollments for school_year_start >= current.
    School name/abbr is taken directly from the already-resolved students_df columns
    (populated from the schools export during normalize_students), falling back to
    SCHOOL_MAP constants only when those columns are absent.
    """
    stu = students_df.copy()
    stu["enroll_status"]   = pd.to_numeric(stu.get("enroll_status",   -1), errors="coerce").fillna(-1).astype(int)
    stu["school_id"]       = pd.to_numeric(stu.get("school_id",        0), errors="coerce").fillna(0).astype(int)
    stu["student_id"]      = pd.to_numeric(stu.get("student_id",       0), errors="coerce").fillna(0).astype(int)
    stu["grade_level"]     = pd.to_numeric(stu.get("grade_level",      0), errors="coerce").fillna(0).astype(int)
    stu["next_school_id"]  = pd.to_numeric(stu.get("next_school_id",   0), errors="coerce").fillna(0).astype(int)
    active = stu[stu["enroll_status"] == 0].copy()

    # Determine current school year
    today = datetime.today()
    current_sy = today.year if today.month >= 8 else today.year - 1
    next_sy = current_sy + 1

    # Students with a reenrollment record for next year — cast to int first to avoid str/int mismatch
    re = reenroll_df.copy()
    re["school_year_start"] = pd.to_numeric(re.get("school_year_start", 0), errors="coerce").fillna(0).astype(int)
    re["student_id"]        = pd.to_numeric(re.get("student_id",        0), errors="coerce").fillna(0).astype(int)
    reenroll_next = set(re[re["school_year_start"] >= next_sy]["student_id"].unique())

    # Build per-school label map from the already-resolved active students
    # (school_name/abbr were set by normalize_students via the schools export)
    school_labels = {}
    if "school_abbr" in active.columns and "school_name" in active.columns:
        for sid, grp in active.groupby("school_id"):
            abbr_vals = grp["school_abbr"].dropna()
            name_vals = grp["school_name"].dropna()
            school_labels[sid] = {
                "school_name": name_vals.iloc[0] if not name_vals.empty else _school_name(sid, "name"),
                "school_abbr": abbr_vals.iloc[0] if not abbr_vals.empty else _school_name(sid, "abbr"),
            }

    rows = []
    for (school_id, grade), grp in active.groupby(["school_id", "grade_level"]):
        sids = set(grp["student_id"])
        slabels = school_labels.get(school_id, {})
        rows.append({
            "school_id":            school_id,
            "school_name":          slabels.get("school_name") or _school_name(school_id, "name"),
            "school_abbr":          slabels.get("school_abbr") or _school_name(school_id, "abbr"),
            "grade_level":          grade,
            "grade_label":          _grade_label(grade),
            "total_enrolled":       len(grp),
            "next_school_assigned": int((grp["next_school_id"] != 0).sum()),
            "has_reenroll_record":  len(sids & reenroll_next),
            "not_decided":          int((grp["next_school_id"] == 0).sum()),
            "retained_same_school": int((grp["retention_status"] == "Retained").sum()),
            "network_transfer":     int((grp["retention_status"] == "Network Transfer").sum()),
            "graduating":           int((grp["retention_status"] == "Graduating").sum()),
            "external_transfer":    int((grp["retention_status"] == "External Transfer").sum()),
        })

    return pd.DataFrame(rows).sort_values(["school_id", "grade_level"])


# ── SchoolMint normalizers ────────────────────────────────────────────────────

# Rename map for applications.csv duplicate column names.
# pandas auto-suffixes second occurrences with ".1".
_SM_APPS_RENAME = {
    "student_schoolmintId": "student_sm_id",
    "applicationId":        "application_id",
    "school_applying":      "school_abbr",
    "grade_applying":       "grade_applying_raw",
    # duplicated columns — first occurrence (application-level)
    "status":               "app_status",
    "status_timestamp":     "app_status_timestamp",
    "submitted":            "app_submitted",
    "id":                   "lottery_id",
    # duplicated columns — second occurrence (registration-level, pandas adds .1)
    "id.1":                 "reg_id",
    "status.1":             "reg_status",
    "submitted.1":          "reg_submitted",
    "status_timestamp.1":   "reg_status_timestamp",
}


def _validate_sm_apps_columns(df: pd.DataFrame, warnings: list) -> None:
    """Warn if expected duplicate-suffixed columns are missing from the applications CSV."""
    expected = ["id.1", "status.1", "submitted.1", "status_timestamp.1"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        warnings.append(
            f"applications.csv: expected duplicate-suffixed columns not found: {missing}. "
            "The file structure may have changed. Embedded registration data will be empty."
        )


def _sm_school_id(abbr_series: pd.Series) -> pd.Series:
    """Map school abbreviation strings to PowerSchool school_id integers."""
    normalized = abbr_series.str.strip().str.upper()
    return normalized.map(SM_SCHOOL_ABBR_MAP).fillna(0).astype(int)


def _sm_grade_level(grade_series: pd.Series, warnings: list, context: str) -> pd.Series:
    """Map SchoolMint grade strings to integer grade levels."""
    normalized = grade_series.str.strip()
    mapped = normalized.map(SM_GRADE_MAP)
    unmapped = normalized[mapped.isna() & normalized.notna() & (normalized != "")]
    if not unmapped.empty:
        unique_bad = unmapped.unique().tolist()
        warnings.append(
            f"{context}: unrecognized grade values {unique_bad} — stored as -99 (Unknown)."
        )
    return mapped.fillna(-99).astype(int)


def normalize_sm_applications(file_obj) -> tuple:
    """
    Normalize SchoolMint applications CSV export.
    Returns (df, warnings).
    The CSV has duplicate column headers; pandas auto-suffixes them with '.1'.
    """
    warnings = []
    df = _read_csv(file_obj)

    _validate_sm_apps_columns(df, warnings)

    # Ensure all expected renamed columns exist (guard against missing .1 variants)
    for col in ["id.1", "status.1", "submitted.1", "status_timestamp.1"]:
        if col not in df.columns:
            df[col] = ""

    df = df.rename(columns=_SM_APPS_RENAME)

    # Detect test/demo rows
    is_test = (
        df["student_last"].str.contains("Test", case=False, na=False) |
        df["school_abbr"].isna() |
        (df["school_abbr"].str.strip() == "")
    )
    test_count = is_test.sum()
    if test_count:
        warnings.append(f"{test_count} rows flagged as test/demo and excluded from summary.")
    df["is_test_row"] = is_test.map({True: "True", False: "False"})

    # Normalize school abbreviation, derive PS school_id
    df["school_abbr"] = df["school_abbr"].fillna("").str.strip().str.upper()
    unrecognized = df.loc[~is_test, "school_abbr"]
    unrecognized_schools = set(unrecognized[~unrecognized.isin(SM_SCHOOL_ABBR_MAP) & (unrecognized != "")].unique())
    if unrecognized_schools:
        warnings.append(f"applications.csv: unrecognized school abbreviations: {sorted(unrecognized_schools)}")
    df["school_id"] = _sm_school_id(df["school_abbr"])

    # Derive grade_level and grade_label
    df["grade_level"] = _sm_grade_level(df["grade_applying_raw"], warnings, "applications.csv")
    df["grade_label"] = df["grade_level"].map(lambda g: GRADE_LABEL_MAP.get(g, "Unknown") if g != -99 else "Unknown")

    # Canonical column order
    cols = [
        "student_sm_id", "student_last", "student_first",
        "school_abbr", "school_id",
        "grade_applying_raw", "grade_level", "grade_label",
        "application_id", "app_status", "app_status_timestamp",
        "app_submitted", "submitted_timestamp",
        "withdrawn", "withdrawn_reason",
        "lottery_id", "accepted", "timestamp_accepted", "timestamp_accepted_declined",
        "lottery_list",
        "reg_id", "reg_status", "reg_submitted", "reg_status_timestamp",
        "sis_export_timestamp",
        "email_guardian", "phone_guardian",
        "is_test_row",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df[[c for c in cols if c in df.columns]]

    return df, warnings


def normalize_sm_registrations(file_obj) -> tuple:
    """
    Normalize SchoolMint registrations CSV export.
    Returns (df, warnings).
    """
    warnings = []
    df = _read_csv(file_obj)

    rename = {
        "student_schoolmintId": "student_sm_id",
        "school_applying":      "school_abbr",
        "grade_applying":       "grade_applying_raw",
        "id":                   "reg_id",
        "type":                 "reg_type",
        "status":               "reg_status",
        "submitted":            "reg_submitted",
        "status_timestamp":     "reg_status_timestamp",
        # Keep SchoolMint's internal school_id as sm_school_id to avoid PS ID collision
        "school_id":            "sm_school_id",
    }
    df = df.rename(columns=rename)

    df["school_abbr"] = df["school_abbr"].fillna("").str.strip().str.upper()
    df["school_id"] = _sm_school_id(df["school_abbr"])
    df["grade_level"] = _sm_grade_level(df["grade_applying_raw"], warnings, "registrations.csv")
    df["grade_label"] = df["grade_level"].map(lambda g: GRADE_LABEL_MAP.get(g, "Unknown") if g != -99 else "Unknown")

    cols = [
        "student_sm_id", "student_last", "student_first",
        "school_abbr", "school_id",
        "grade_applying_raw", "grade_level", "grade_label",
        "reg_id", "reg_type", "reg_status", "reg_submitted",
        "reg_status_timestamp", "sis_export_timestamp", "sm_school_id",
        "email_guardian", "phone_guardian",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df[[c for c in cols if c in df.columns]]

    return df, warnings


def build_sm_recruitment_summary(apps_df: pd.DataFrame, regs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build per school_abbr × grade_level recruitment funnel summary.
    Columns: school_year, school_abbr, school_id, grade_level, grade_label,
             leads, apps_submitted, reg_submitted, reg_approved.
    """
    today = datetime.today()
    next_sy = (today.year if today.month >= 8 else today.year - 1) + 1
    school_year = f"{next_sy}-{next_sy + 1}"

    key = ["school_abbr", "grade_level"]

    # --- Leads: unique students per school×grade (non-test rows) ---
    if not apps_df.empty and "is_test_row" in apps_df.columns:
        apps_clean = apps_df[apps_df["is_test_row"] != "True"].copy()
    elif not apps_df.empty:
        apps_clean = apps_df.copy()
    else:
        apps_clean = pd.DataFrame(columns=["school_abbr", "grade_level", "student_sm_id", "app_submitted"])

    for col in ["grade_level"]:
        if col in apps_clean.columns:
            apps_clean[col] = pd.to_numeric(apps_clean[col], errors="coerce").fillna(-99).astype(int)

    leads_df = (
        apps_clean.groupby(key)["student_sm_id"]
        .nunique()
        .reset_index()
        .rename(columns={"student_sm_id": "leads"})
    )

    # --- Apps submitted ---
    if not apps_clean.empty and "app_submitted" in apps_clean.columns:
        apps_sub = apps_clean[apps_clean["app_submitted"] == "Submitted"]
    else:
        apps_sub = pd.DataFrame(columns=key)

    apps_sub_df = (
        apps_sub.groupby(key)
        .size()
        .reset_index(name="apps_submitted")
    )

    # --- Reg submitted / approved from applications embedded registration columns ---
    # The applications CSV embeds registration data via .1 duplicate columns
    # (renamed to reg_submitted / reg_status by _SM_APPS_RENAME), which use
    # confirmed values "Submitted" / "Approved". The standalone regs file may
    # lack a `submitted` column entirely, so apps_clean is the reliable source.
    if not apps_clean.empty:
        if "reg_submitted" in apps_clean.columns:
            reg_sub_df = (
                apps_clean[apps_clean["reg_submitted"] == "Submitted"]
                .groupby(key).size().reset_index(name="reg_submitted")
            )
        else:
            reg_sub_df = pd.DataFrame(columns=key + ["reg_submitted"])

        if "reg_status" in apps_clean.columns:
            reg_app_df = (
                apps_clean[apps_clean["reg_status"] == "Approved"]
                .groupby(key).size().reset_index(name="reg_approved")
            )
        else:
            reg_app_df = pd.DataFrame(columns=key + ["reg_approved"])
    else:
        reg_sub_df = pd.DataFrame(columns=key + ["reg_submitted"])
        reg_app_df = pd.DataFrame(columns=key + ["reg_approved"])

    # Outer-merge all four counts
    summary = leads_df
    for right, fill_col in [(apps_sub_df, "apps_submitted"), (reg_sub_df, "reg_submitted"), (reg_app_df, "reg_approved")]:
        summary = summary.merge(right, on=key, how="outer")

    for col in ["leads", "apps_submitted", "reg_submitted", "reg_approved"]:
        if col not in summary.columns:
            summary[col] = 0
        summary[col] = pd.to_numeric(summary[col], errors="coerce").fillna(0).astype(int)

    # Add labels
    summary["school_id"] = _sm_school_id(summary["school_abbr"].fillna(""))
    summary["grade_label"] = summary["grade_level"].map(
        lambda g: GRADE_LABEL_MAP.get(g, "Unknown") if g != -99 else "Unknown"
    )
    summary["school_year"] = school_year

    # Sort
    grade_order = {g: i for i, g in enumerate(GRADE_SORT_ORDER + [-99])}
    summary["_sort"] = summary["grade_level"].map(grade_order).fillna(99)
    summary = summary.sort_values(["school_abbr", "_sort"]).drop(columns=["_sort"])

    cols = ["school_year", "school_abbr", "school_id", "grade_level", "grade_label",
            "leads", "apps_submitted", "reg_submitted", "reg_approved"]
    for c in cols:
        if c not in summary.columns:
            summary[c] = 0
    return summary[cols].reset_index(drop=True)


# ── HubSpot enrollment funnel ─────────────────────────────────────────────────

# Output column order: Is_*/Duplicate_Flag → all raw HS columns → SM_Reg_* → SM_App_* → PS_*
_HS_FUNNEL_COLS = ["Is_Lead", "Is_App", "Is_Enrolled", "Duplicate_Flag"]
_HS_SM_REG_COLS = [
    "SM_Reg_Match", "SM_Reg_Match_Method", "SM_Reg_SchoolMint_ID",
    "SM_Reg_Record_ID", "SM_Reg_Student_Name", "SM_Reg_School",
    "SM_Reg_Grade", "SM_Reg_Status", "SM_Reg_Submitted_Date",
    "SM_Reg_Created_Date", "SM_Reg_Status_Timestamp",
]
_HS_SM_APP_COLS = [
    "SM_App_Match", "SM_App_Match_Method", "SM_App_SchoolMint_ID",
    "SM_App_Application_ID", "SM_App_Record_ID", "SM_App_Student_Name",
    "SM_App_School", "SM_App_Grade", "SM_App_Status",
    "SM_App_Submitted_Date", "SM_App_Created_Date", "SM_App_Decision_Date",
    "SM_App_Submitted_Timestamp",
    "SM_App_Withdrawn", "SM_App_Withdrawn_Reason",
    "SM_App_Accepted", "SM_App_Accepted_Timestamp",
]
_HS_PS_COLS = [
    "PS_Match", "PS_Match_Method", "PS_Student_ID", "PS_Student_Number",
    "PS_Student_Name", "PS_School", "PS_Campus_ID", "PS_Grade",
    "PS_Enrollment_Status", "PS_Enrollee_Type",
    "PS_Enrollment_Date", "PS_Exit_Date", "PS_Entry_Code", "PS_Enrollment_Type",
]


# ── Matching helpers ──────────────────────────────────────────────────────────

def _ne(s) -> str:
    """Normalize email: lowercase, stripped. Returns '' for null/empty."""
    if s is None or s != s or str(s).strip() == "":
        return ""
    return str(s).strip().lower()


def _np(s) -> str:
    """Normalize phone: digits only, last 10 digits. Returns '' if fewer than 10 digits."""
    if s is None or s != s or str(s).strip() == "":
        return ""
    d = _re.sub(r"\D", "", str(s))
    return d[-10:] if len(d) >= 10 else ""


def _nname(last, first) -> str:
    """Normalized name key: 'last|first' lowercased. Returns '' if both empty."""
    l = str(last).strip().lower() if last and last == last and str(last).strip() else ""
    f = str(first).strip().lower() if first and first == first and str(first).strip() else ""
    return f"{l}|{f}" if (l or f) else ""


def _nname_lastfirst(lastfirst_str) -> str:
    """Normalize a 'Last, First' string to 'last|first' key."""
    s = str(lastfirst_str or "").strip()
    if "," in s:
        parts = s.split(",", 1)
        return f"{parts[0].strip().lower()}|{parts[1].strip().lower()}"
    return s.lower()


def _build_sm_index(df: pd.DataFrame, email_col="email_guardian", phone_col="phone_guardian",
                    date_col="reg_status_timestamp"):
    """
    Build lookup dicts for SM data (apps or regs).
    Returns (by_email, by_phone, by_student_name) mapping normalized key → row dict.
    Sorted by date descending so first occurrence = most recent record.
    """
    if date_col in df.columns:
        df = df.sort_values(date_col, ascending=False, na_position="last")
    by_email: dict = {}
    by_phone: dict = {}
    by_student: dict = {}
    for _, row in df.iterrows():
        rd = row.to_dict()
        e = _ne(row.get(email_col, ""))
        if e and e not in by_email:
            by_email[e] = rd
        p = _np(row.get(phone_col, ""))
        if p and p not in by_phone:
            by_phone[p] = rd
        name = _nname(row.get("student_last", ""), row.get("student_first", ""))
        if name and name not in by_student:
            by_student[name] = rd
    return by_email, by_phone, by_student


def _build_ps_index(df: pd.DataFrame):
    """
    Build lookup dicts for PS students.
    Returns (by_email, by_phone, by_student_name) mapping normalized key → row dict.
    Sorted by entry_date descending so first occurrence = most recent record.
    """
    if "entry_date" in df.columns:
        df = df.sort_values("entry_date", ascending=False, na_position="last")
    by_email: dict = {}
    by_phone: dict = {}
    by_student: dict = {}
    for _, row in df.iterrows():
        rd = row.to_dict()
        e = _ne(row.get("guardian_email", ""))
        if e and e not in by_email:
            by_email[e] = rd
        p = _np(row.get("home_phone", ""))
        if p and p not in by_phone:
            by_phone[p] = rd
        name = _nname_lastfirst(row.get("last_first", ""))
        if name and name not in by_student:
            by_student[name] = rd
    return by_email, by_phone, by_student


def _match(by_email, by_phone, by_student,
           hs_email, hs_phones, hs_student_name, hs_guardian_name):
    """
    Priority-order lookup. Returns (row_dict_or_None, method_string).
    Priority: 1=Email, 2=Phone/Mobile, 3=Student name, 4=Guardian name fallback.
    """
    if hs_email and hs_email in by_email:
        return by_email[hs_email], "Email"
    for ph in hs_phones:
        if ph and ph in by_phone:
            return by_phone[ph], "Phone"
    if hs_student_name and hs_student_name in by_student:
        return by_student[hs_student_name], "Student Name"
    if hs_guardian_name and hs_guardian_name in by_student:
        return by_student[hs_guardian_name], "Guardian Name"
    return None, ""


def _s(v) -> str:
    """Safe stringify: returns '' for None/NaN."""
    if v is None or v != v:
        return ""
    return str(v)


def _sm_reg_row(rd, method: str) -> dict:
    if rd is None:
        return {c: "" for c in _HS_SM_REG_COLS}
    name = f"{_s(rd.get('student_last'))}, {_s(rd.get('student_first'))}".strip(", ")
    return {
        "SM_Reg_Match":             "Yes",
        "SM_Reg_Match_Method":      method,
        "SM_Reg_SchoolMint_ID":     _s(rd.get("student_sm_id")),
        "SM_Reg_Record_ID":         _s(rd.get("reg_id")),
        "SM_Reg_Student_Name":      name,
        "SM_Reg_School":            _s(rd.get("school_abbr")),
        "SM_Reg_Grade":             _s(rd.get("grade_label")),
        "SM_Reg_Status":            _s(rd.get("reg_status")),
        "SM_Reg_Submitted_Date":    _s(rd.get("reg_submitted")),
        "SM_Reg_Created_Date":      _s(rd.get("reg_submitted")),
        "SM_Reg_Status_Timestamp":  _s(rd.get("reg_status_timestamp")),
    }


def _sm_app_row(rd, method: str) -> dict:
    if rd is None:
        return {c: "" for c in _HS_SM_APP_COLS}
    name = f"{_s(rd.get('student_last'))}, {_s(rd.get('student_first'))}".strip(", ")
    return {
        "SM_App_Match":               "Yes",
        "SM_App_Match_Method":        method,
        "SM_App_SchoolMint_ID":       _s(rd.get("student_sm_id")),
        "SM_App_Application_ID":      _s(rd.get("application_id")),
        "SM_App_Record_ID":           _s(rd.get("lottery_id")),
        "SM_App_Student_Name":        name,
        "SM_App_School":              _s(rd.get("school_abbr")),
        "SM_App_Grade":               _s(rd.get("grade_label")),
        "SM_App_Status":              _s(rd.get("app_status")),
        "SM_App_Submitted_Date":      _s(rd.get("app_submitted")),
        "SM_App_Created_Date":        _s(rd.get("app_submitted")),
        "SM_App_Decision_Date":       _s(rd.get("app_status_timestamp")),
        "SM_App_Submitted_Timestamp": _s(rd.get("submitted_timestamp")),
        "SM_App_Withdrawn":           _s(rd.get("withdrawn")),
        "SM_App_Withdrawn_Reason":    _s(rd.get("withdrawn_reason")),
        "SM_App_Accepted":            _s(rd.get("accepted")),
        "SM_App_Accepted_Timestamp":  _s(rd.get("timestamp_accepted")),
    }


def _ps_row(rd, method: str, reenroll_ids: set) -> dict:
    if rd is None:
        return {c: "" for c in _HS_PS_COLS}
    try:
        status_int = int(float(_s(rd.get("enroll_status", ""))))
    except (ValueError, TypeError):
        status_int = -99
    enrollee_type = ""
    if status_int == 0:
        try:
            sid = int(float(_s(rd.get("student_id", 0))))
        except (ValueError, TypeError):
            sid = 0
        enrollee_type = "Re-enrollee" if sid in reenroll_ids else "New Enrollee"
    school = _s(rd.get("school_abbr") or rd.get("school_name") or rd.get("school_id"))
    grade  = _s(rd.get("grade_label") or rd.get("grade_level"))
    enroll_status = ENROLL_STATUS_MAP.get(status_int, _s(rd.get("enroll_status_label") or rd.get("enroll_status")))
    return {
        "PS_Match":             "Yes",
        "PS_Match_Method":      method,
        "PS_Student_ID":        _s(rd.get("student_id")),
        "PS_Student_Number":    _s(rd.get("student_number")),
        "PS_Student_Name":      _s(rd.get("last_first")),
        "PS_School":            school,
        "PS_Campus_ID":         _s(rd.get("campus_id")),
        "PS_Grade":             grade,
        "PS_Enrollment_Status": enroll_status,
        "PS_Enrollee_Type":     enrollee_type,
        "PS_Enrollment_Date":   _s(rd.get("entry_date")),
        "PS_Exit_Date":         _s(rd.get("exit_date")),
        "PS_Entry_Code":        _s(rd.get("school_entry_grade")),
        "PS_Enrollment_Type":   _s(rd.get("enrollment_code", "")),
    }


# ── Public API ────────────────────────────────────────────────────────────────

def normalize_hs_contacts(file_obj) -> tuple[pd.DataFrame, list]:
    """
    Load a raw HubSpot contacts CSV or xlsx export.
    Keeps ALL columns as-is — matching is done later in build_enrollment_funnel().
    Returns (df, warnings).
    """
    warnings = []
    df = _read_file(file_obj)
    if df.empty:
        warnings.append("[HubSpot] Contacts file is empty.")
        return df, warnings
    warnings.append(f"[HubSpot] {len(df):,} contacts loaded.")
    return df, warnings


def build_enrollment_funnel(
    hs_df: pd.DataFrame,
    sm_apps_df: pd.DataFrame,
    sm_regs_df: pd.DataFrame,
    students_df: pd.DataFrame,
    reenroll_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Reconcile HubSpot contacts against SchoolMint and PowerSchool.
    For each HS contact, match to SM registrations, SM applications, and PS students
    using 4-priority matching: Email → Phone/Mobile → Student Name → Guardian Name.

    Returns one row per HS contact with:
      FUNNEL_* flags | all raw HS columns | SM_Reg_* | SM_App_* | PS_*
    """
    # Set of student IDs with any reenrollment record (for New vs Re-enrollee)
    reenroll_ids: set = set()
    if not reenroll_df.empty and "student_id" in reenroll_df.columns:
        reenroll_ids = set(
            pd.to_numeric(reenroll_df["student_id"], errors="coerce")
            .dropna().astype(int).unique()
        )

    # Build lookup indexes (most recent record wins — sorted desc by date in _build_*_index)
    sm_reg_email, sm_reg_phone, sm_reg_student = (
        _build_sm_index(sm_regs_df, date_col="reg_status_timestamp") if not sm_regs_df.empty else ({}, {}, {})
    )
    sm_app_email, sm_app_phone, sm_app_student = (
        _build_sm_index(sm_apps_df, date_col="app_status_timestamp") if not sm_apps_df.empty else ({}, {}, {})
    )
    ps_email, ps_phone, ps_student = (
        _build_ps_index(students_df) if not students_df.empty else ({}, {}, {})
    )

    out_rows = []
    for _, hs in hs_df.iterrows():
        # Extract HS matching keys
        hs_email   = _ne(hs.get("Email"))
        hs_phone   = _np(hs.get("Phone Number"))
        hs_mobile  = _np(hs.get("Mobile Phone Number"))
        hs_phones  = [p for p in [hs_phone, hs_mobile] if p]

        hs_s_name  = _nname(hs.get("Student #1 Last Name"), hs.get("Student #1 First Name"))
        hs_g_name  = _nname(hs.get("Last Name"), hs.get("First Name"))

        # Match each system
        sm_reg_rd, sm_reg_method = _match(
            sm_reg_email, sm_reg_phone, sm_reg_student,
            hs_email, hs_phones, hs_s_name, hs_g_name,
        )
        sm_app_rd, sm_app_method = _match(
            sm_app_email, sm_app_phone, sm_app_student,
            hs_email, hs_phones, hs_s_name, hs_g_name,
        )
        ps_rd, ps_method = _match(
            ps_email, ps_phone, ps_student,
            hs_email, hs_phones, hs_s_name, hs_g_name,
        )

        # Build output sections
        sm_reg_cols = _sm_reg_row(sm_reg_rd, sm_reg_method)
        sm_app_cols = _sm_app_row(sm_app_rd, sm_app_method)
        ps_cols     = _ps_row(ps_rd, ps_method, reenroll_ids)

        # Derive funnel flags
        ps_enrolled = ps_rd is not None and _s(ps_rd.get("enroll_status")) == "0"
        funnel = {
            "Is_Lead":        True,
            "Is_App":         sm_reg_rd is not None or sm_app_rd is not None,
            "Is_Enrolled":    ps_enrolled,
            "Duplicate_Flag": False,  # filled in post-loop
        }

        out_rows.append({**funnel, **hs.to_dict(), **sm_reg_cols, **sm_app_cols, **ps_cols})

    if not out_rows:
        return pd.DataFrame()

    df = pd.DataFrame(out_rows)

    # Post-loop: flag duplicate HS contacts that matched the same SM/PS record
    for id_col in ["SM_Reg_Record_ID", "SM_App_Application_ID", "PS_Student_ID"]:
        if id_col in df.columns:
            non_empty = df[id_col] != ""
            dups = non_empty & df[id_col].duplicated(keep=False)
            df.loc[dups, "Duplicate_Flag"] = True

    return df


def build_hs_funnel_summary(hs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the enrollment funnel summary table.
    Returns DataFrame with columns: Stage, Count, % of HubSpot Leads.
    """
    n_leads = len(hs_df)

    def pct(n):
        if n_leads == 0:
            return ""
        return round(n / n_leads, 4)  # stored as decimal; format in Sheets

    sm_mask      = hs_df["Is_App"] == True
    ps_mask      = hs_df["PS_Match"] == "Yes"
    enrolled_mask = hs_df["Is_Enrolled"] == True
    no_match_mask = (~hs_df["Is_App"]) & (hs_df["PS_Match"] != "Yes")

    enrollee_type = hs_df.loc[enrolled_mask, "PS_Enrollee_Type"].str.strip().str.lower()
    n_new      = int((enrollee_type.isin(["new", "new enrollee"])).sum())
    n_reenroll = int((enrollee_type.isin(["re-enroll", "re-enrollee", "re-enrollment", "reenroll", "reenrollee"])).sum())
    n_enrolled = int(enrolled_mask.sum())

    rows = [
        {"Stage": "1. HubSpot Leads (All Contacts)",                        "Count": n_leads,                    "% of HubSpot Leads": ""},
        {"Stage": "2. SchoolMint \u2014 App or Registration found",          "Count": int(sm_mask.sum()),         "% of HubSpot Leads": pct(int(sm_mask.sum()))},
        {"Stage": "3. PowerSchool \u2014 Any record found",                  "Count": int(ps_mask.sum()),         "% of HubSpot Leads": pct(int(ps_mask.sum()))},
        {"Stage": "4. PowerSchool \u2014 Currently Enrolled (status = 0)",   "Count": n_enrolled,                 "% of HubSpot Leads": pct(n_enrolled)},
        {"Stage": "\u21b3 New Enrollees",                                    "Count": n_new,                      "% of HubSpot Leads": pct(n_new)},
        {"Stage": "\u21b3 Re-enrollees",                                     "Count": n_reenroll,                 "% of HubSpot Leads": pct(n_reenroll)},
        {"Stage": "5. No match in any system",                               "Count": int(no_match_mask.sum()),   "% of HubSpot Leads": pct(int(no_match_mask.sum()))},
        {"Stage": "Run timestamp",                                           "Count": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), "% of HubSpot Leads": ""},
    ]
    return pd.DataFrame(rows)


def normalize_all(
    students_file, reenroll_file,
    schools_file=None, terms_file=None,
    existing_schools_df=None, existing_terms_df=None,
    sm_applications_file=None,
    sm_registrations_file=None,
    hs_contacts_file=None,
    existing_funnel_df=None,
) -> dict:
    """
    Run all normalizers and build summaries.
    schools_file and terms_file are optional; when not provided the function
    falls back to existing_schools_df / existing_terms_df (read from Sheets),
    then to empty DataFrames with names resolved from SCHOOL_MAP constants.
    Returns dict with keys: students, reenrollments, schools, terms,
    summary_enrollment, summary_funnel, all_warnings, upload_timestamp.
    """
    all_warnings = []

    # Schools
    if schools_file is not None:
        schools_df, w = normalize_schools(schools_file)
        all_warnings.extend([f"[Schools] {x}" for x in w])
    elif existing_schools_df is not None and not existing_schools_df.empty:
        schools_df = existing_schools_df.copy()
        if "school_id" in schools_df.columns:
            schools_df["school_id"] = pd.to_numeric(
                schools_df["school_id"], errors="coerce"
            ).fillna(0).astype(int)
        all_warnings.append("[Schools] Using existing data from Google Sheet (no new file uploaded).")
    else:
        schools_df = pd.DataFrame()
        all_warnings.append("[Schools] No schools file or existing data — names resolved from constants.")

    # Terms
    if terms_file is not None:
        terms_df, w = normalize_terms(terms_file)
        all_warnings.extend([f"[Terms] {x}" for x in w])
    elif existing_terms_df is not None and not existing_terms_df.empty:
        terms_df = existing_terms_df.copy()
        all_warnings.append("[Terms] Using existing data from Google Sheet (no new file uploaded).")
    else:
        terms_df = pd.DataFrame()
        all_warnings.append("[Terms] No terms file or existing data.")

    students_df, w = normalize_students(students_file, schools_df)
    all_warnings.extend([f"[Students] {x}" for x in w])

    reenroll_df, w = normalize_reenrollments(reenroll_file, schools_df)
    all_warnings.extend([f"[ReEnrollments] {x}" for x in w])

    summary_enrollment = build_summary_enrollment(reenroll_df, students_df, schools_df)
    summary_funnel = build_summary_funnel(students_df, reenroll_df)

    # SchoolMint (optional)
    sm_apps_df = pd.DataFrame()
    sm_regs_df = pd.DataFrame()
    sm_recruitment_df = pd.DataFrame()

    if sm_applications_file is not None:
        sm_apps_df, w = normalize_sm_applications(sm_applications_file)
        all_warnings.extend([f"[SM-Apps] {x}" for x in w])

    if sm_registrations_file is not None:
        sm_regs_df, w = normalize_sm_registrations(sm_registrations_file)
        all_warnings.extend([f"[SM-Regs] {x}" for x in w])

    if not sm_apps_df.empty or not sm_regs_df.empty:
        sm_recruitment_df = build_sm_recruitment_summary(sm_apps_df, sm_regs_df)

    # HubSpot (optional) — match against SM and PS data already normalized above
    hs_contacts_df = pd.DataFrame()
    hs_funnel_summary_df = pd.DataFrame()

    # Columns added by build_enrollment_funnel (not raw HS data)
    _derived_cols = set(
        _HS_FUNNEL_COLS + _HS_SM_REG_COLS + _HS_SM_APP_COLS + _HS_PS_COLS
    )

    raw_hs_df = pd.DataFrame()
    if hs_contacts_file is not None:
        raw_hs_df, w = normalize_hs_contacts(hs_contacts_file)
        all_warnings.extend(w)
    elif existing_funnel_df is not None and not existing_funnel_df.empty:
        # Re-use stored HS contact rows; strip derived match columns so they
        # are re-computed against the latest PS/SM data
        hs_cols = [c for c in existing_funnel_df.columns if c not in _derived_cols]
        raw_hs_df = existing_funnel_df[hs_cols].copy()
        all_warnings.append(
            f"[HubSpot] No new file — refreshing match columns for "
            f"{len(raw_hs_df):,} existing contacts using updated PS/SM data."
        )

    if not raw_hs_df.empty:
        hs_contacts_df = build_enrollment_funnel(
            raw_hs_df, sm_apps_df, sm_regs_df, students_df, reenroll_df
        )
        n_sm = int(hs_contacts_df["Is_App"].sum())
        n_ps = int((hs_contacts_df["PS_Match"] == "Yes").sum())
        n_en = int(hs_contacts_df["Is_Enrolled"].sum())
        all_warnings.append(
            f"[HubSpot] Funnel built: {len(hs_contacts_df):,} contacts — "
            f"{n_sm:,} in SchoolMint, {n_ps:,} in PowerSchool, {n_en:,} currently enrolled."
        )
        hs_funnel_summary_df = build_hs_funnel_summary(hs_contacts_df)

    return {
        "students": students_df,
        "reenrollments": reenroll_df,
        "schools": schools_df,
        "terms": terms_df,
        "summary_enrollment": summary_enrollment,
        "summary_funnel": summary_funnel,
        "sm_applications": sm_apps_df,
        "sm_registrations": sm_regs_df,
        "sm_recruitment": sm_recruitment_df,
        "hs_contacts": hs_contacts_df,
        "hs_funnel_summary": hs_funnel_summary_df,
        "all_warnings": all_warnings,
        "upload_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
    }
