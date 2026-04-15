# Ingenium Re-enrollment Reporting System

Two Streamlit apps:
- **Uploader** — parses PowerSchool CSV exports and pushes to Google Sheets
- **Dashboard** — reads from Google Sheets and shows interactive visualizations

---

## Folder Structure

```
Ing_Reenroll/
├── uploader/          App 1 — CSV uploader
├── dashboard/         App 2 — reporting dashboard
├── shared/            constants shared by both apps
├── credentials/       place service_account.json here (git-ignored)
└── README.md
```

---

## 1. Google Cloud Setup (one-time)

### Step 1 — Create a project
1. Go to https://console.cloud.google.com
2. Click the project dropdown (top-left) → **New Project**
3. Name it `IngeniumReenroll` → **Create**

### Step 2 — Enable APIs
1. In the left menu: **APIs & Services → Library**
2. Search for and enable **Google Sheets API**
3. Search for and enable **Google Drive API**

### Step 3 — Create a service account
1. Go to **IAM & Admin → Service Accounts**
2. Click **Create Service Account**
3. Name: `reenroll-uploader` → **Create and Continue** → **Done**

### Step 4 — Download the JSON key
1. Click the service account you just created
2. Go to the **Keys** tab
3. **Add Key → Create new key → JSON** → Download

### Step 5 — Place the key file
Move the downloaded JSON file to:
```
C:/Claude_Projects/Ing_Reenroll/credentials/service_account.json
```

### Step 6 — Share your Google Drive folder
1. Open the `client_email` value from the JSON file (looks like `xyz@project.iam.gserviceaccount.com`)
2. In Google Drive, right-click the folder where the Sheet lives → **Share**
3. Paste the service account email → set role to **Editor** → **Send**

> If the Sheet doesn't exist yet, the uploader will create it automatically
> when you click "Push to Google Sheets" — as long as the service account
> has Editor access to the target Drive folder.

---

## 2. Install Dependencies

```bash
cd C:/Claude_Projects/Ing_Reenroll
C:/Python314/python.exe -m pip install streamlit pandas plotly gspread gspread-dataframe google-auth
```

---

## 3. Run the Uploader

```bash
cd C:/Claude_Projects/Ing_Reenroll/uploader
C:/Python314/python.exe -m streamlit run app.py
```

Export these four tables from PowerSchool and upload them:
- `Students_export_*.csv`
- `ReEnrollments_export_*.csv`
- `Schools_export*.csv`
- `Terms_export*.csv`

---

## 4. Run the Dashboard

```bash
cd C:/Claude_Projects/Ing_Reenroll/dashboard
C:/Python314/python.exe -m streamlit run app.py
```

Paste the Google Sheet URL in the sidebar when prompted.

---

## Google Sheets Structure

The uploader creates/overwrites these tabs:

| Tab | Contents |
|-----|----------|
| `raw_students` | All students with derived columns (retention status, school name, grade label) |
| `raw_reenrollments` | All historical enrollment records with school year labels |
| `raw_schools` | School lookup table |
| `raw_terms` | School calendar terms |
| `summary_enrollment_by_school_year` | Unique students per school per year (for trend charts) |
| `summary_funnel_current` | Funnel counts per school × grade (for funnel charts) |
| `upload_log` | Audit log — one row appended per upload (never overwritten) |

---

## Data Notes

- **School join key**: `School_Number` in Schools table = `SchoolID` in all other tables
- **Students file encoding**: `latin-1` (special characters in student names)
- **Active students**: `Enroll_Status = 0`
- **Grade levels**: -1=PreK, 0=K, 1–8=1st–8th, 99=Graduated
- **School years**: Derived from entry date; Aug+ = current year start
- **Zero re-enrollment records for next year** is normal at the start of enrollment season
