"""Shared constants for Ingenium Re-enrollment apps."""

# School number → display info
# Key: School_Number (the join key used in all PS exports)
SCHOOL_MAP = {
    118760: {"name": "Barack Obama Charter School", "abbr": "BOCS"},
    121137: {"name": "Ingenium Charter School", "abbr": "ICS"},
    127985: {"name": "Ingenium Charter Middle School", "abbr": "ICMS"},
    129825: {"name": "Clemente Charter School", "abbr": "CCS"},
    137240: {"name": "Ingenium Clarion Charter Middle School", "abbr": "ICCMS"},
    1279858: {"name": "Ingenium Wings Independent Study", "abbr": "IWIS"},
    13724060: {"name": "Ingenium Schools Attendance Recovery", "abbr": "IS-AR"},
    555555: {"name": "Ingenium Schools Attendance Recovery", "abbr": "IS-AR2"},
    999999: {"name": "Graduated Students", "abbr": "GRAD"},
    0: {"name": "District / Unknown", "abbr": "DIST"},
    1: {"name": "System", "abbr": "SYS"},
}

# Active instructional schools shown in reports
ACTIVE_SCHOOL_IDS = [118760, 121137, 127985, 129825, 137240]

# All network school IDs (for distinguishing intra-network transfers)
NETWORK_SCHOOL_IDS = set(SCHOOL_MAP.keys()) - {0, 1, 999999}

GRADE_LABEL_MAP = {
    -1: "PreK",
    0: "K",
    1: "1st",
    2: "2nd",
    3: "3rd",
    4: "4th",
    5: "5th",
    6: "6th",
    7: "7th",
    8: "8th",
    9: "9th",
    10: "10th",
    11: "11th",
    12: "12th",
    99: "Graduated",
}

GRADE_SORT_ORDER = [-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 99]

ENROLL_STATUS_MAP = {
    -2: "Inactive",
    -1: "Pre-registered",
    0: "Active",
    1: "Inactive",
    2: "Transferred Out",
    3: "Graduated",
    4: "Historical Import",
}

RETENTION_STATUS_ORDER = [
    "Retained",
    "Network Transfer",
    "Graduating",
    "Not Decided",
    "External Transfer",
    "N/A",
]

# Colors for charts
SCHOOL_COLORS = {
    "ICS":   "#1f77b4",
    "ICMS":  "#ff7f0e",
    "CCS":   "#2ca02c",
    "BOCS":  "#d62728",
    "ICCMS": "#9467bd",
    "IWIS":  "#8c564b",
    "GRAD":  "#7f7f7f",
    "DIST":  "#bcbd22",
}

RETENTION_COLORS = {
    "Retained":         "#2ca02c",
    "Network Transfer": "#ff7f0e",
    "Graduating":       "#9467bd",
    "Not Decided":      "#d62728",
    "External Transfer":"#8c564b",
    "N/A":              "#bcbd22",
}

# SchoolMint abbreviation → PowerSchool school_id
SM_SCHOOL_ABBR_MAP = {
    "BOCS":  118760,
    "ICS":   121137,
    "ICMS":  127985,
    "CCS":   129825,
    "ICCMS": 137240,
}

# SchoolMint grade string → integer grade level (matches GRADE_LABEL_MAP)
SM_GRADE_MAP = {
    "TK": -1,
    "K":   0,
    "1":   1,
    "2":   2,
    "3":   3,
    "4":   4,
    "5":   5,
    "6":   6,
    "7":   7,
    "8":   8,
}

# Google Sheets tab names
SHEET_TABS = {
    "raw_students":                    "raw_students",
    "raw_reenrollments":               "raw_reenrollments",
    "raw_schools":                     "raw_schools",
    "raw_terms":                       "raw_terms",
    "summary_enrollment_by_sy":        "summary_enrollment_by_school_year",
    "summary_funnel_current":          "summary_funnel_current",
    "upload_log":                      "upload_log",
    # SchoolMint recruitment pipeline
    "raw_sm_applications":             "raw_sm_applications",
    "raw_sm_registrations":            "raw_sm_registrations",
    "summary_sm_recruitment":          "summary_sm_recruitment",
    # HubSpot enrollment funnel
    "enrollment_funnel":               "enrollment_funnel",
    "funnel_summary":                  "funnel_summary",
}
