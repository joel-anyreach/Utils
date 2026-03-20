# 🎬 Anyreach Creator Recruitment Pipeline

A fully automated YouTube creator outreach pipeline with a Streamlit GUI.

**Flow:** YouTube Discovery → Email Enrichment → Role-Email Flagging → Reoon Verification → Google Sheets Dedup → Instantly Campaign Upload

---

## Features

| Feature | Description |
|---|---|
| **Keyword Search** | Discover YouTube channels by niche via Apify |
| **Playlist Extraction** | Pull every unique creator from a YouTube playlist (no Apify credits) |
| **Channel Lookup** | Enrich specific channel URLs directly (no Apify credits) |
| **Email Enrichment** | Scrape public emails via a second Apify actor |
| **Role Email Flagging** | Detects and flags generic/role emails (e.g. `info@`, `support@`, `contact@`) in output |
| **Reoon Verification** | Optionally verify emails via Reoon API before pushing to Instantly |
| **Google Sheets Dedup** | Writes new leads only; skips channels already in the sheet |
| **Instantly Upload** | Pushes verified new leads directly into your campaign |
| **Dry Run Mode** | Full enrichment run without writing to Sheet or Instantly |
| **Subscriber Filters** | Min/max subscriber range applied at discovery time |
| **Language Filter** | Filter by language in Apify keyword search |
| **Live Log Streaming** | Real-time pipeline output in the Streamlit UI |
| **CSV Export + Download** | Enriched results saved locally and available as a one-click download |

---

## Project Structure

```
YT_Test/
├── app.py                  # Streamlit GUI
├── pipeline.py             # CLI entrypoint (called by app.py as a subprocess)
├── config.py               # Environment variable loader
├── requirements.txt        # Python dependencies
├── .env.example            # Template — copy to .env and fill in values
├── pipeline/
│   ├── apify_client.py     # Apify channel discovery
│   ├── apifym_client.py    # Apify email enrichment
│   ├── email_utils.py      # Role-email detection helpers
│   ├── instantly_client.py # Instantly API upload
│   ├── models.py           # Shared data models
│   ├── reoon_client.py     # Reoon email verification
│   ├── sheets_client.py    # Google Sheets read/write
│   └── youtube_client.py   # YouTube Data API helpers
└── exports/                # Auto-created; holds per-run CSV exports (git-ignored)
```

---

## Setup

### 1. Clone & install dependencies

```bash
git clone https://github.com/joel-anyreach/Utils.git
cd Utils
python -m pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in all values:

| Variable | Description |
|---|---|
| `APIFY_API_TOKEN` | Apify API token (used for both discovery and email enrichment) |
| `APIFY_ACTOR_ID` | Channel discovery actor (default: `badruddeen/youtube-niche-channel-finder`) |
| `APIFY_EMAIL_ACTOR_ID` | Email enrichment actor (default: `endspec/youtube-instant-email-scraper`) |
| `APIFY_MAX_RESULTS` | Default max channels per keyword search |
| `YOUTUBE_API_KEY` | YouTube Data API v3 key |
| `INSTANTLY_API_KEY` | Instantly API key |
| `INSTANTLY_CAMPAIGN_ID` | Target campaign UUID in Instantly |
| `REOON_API_KEY` | Reoon EmailVerifier key *(optional — only needed with Verify toggle)* |
| `GOOGLE_SHEET_ID` | Google Sheets ID (from the URL) |
| `SHEET_TAB_NAME` | Sheet tab name (default: `Leads`) |

### 3. Google OAuth credentials

Download an **OAuth 2.0 Client** credentials JSON from [Google Cloud Console](https://console.cloud.google.com/) and save it as `oauth_credentials.json` in the project root.

On first run, a browser window will open to authorise access. The token is cached as `oauth_token.json` for subsequent runs.

> ⚠️ Both `oauth_credentials.json` and `oauth_token.json` are git-ignored — never commit them.

---

## Running the app

```bash
streamlit run app.py
```

The GUI opens at **http://localhost:8501**.

### CLI usage (advanced)

```bash
# Keyword search
python pipeline.py --query "solar energy" --max-results 100

# Playlist extraction
python pipeline.py --playlist-url "https://www.youtube.com/playlist?list=PLxxxxxx"

# Specific channels
python pipeline.py --channel-url https://youtube.com/@mkbhd --channel-url https://youtube.com/@linus

# Common flags
#   --dry-run          Skip Sheet + Instantly writes
#   --export-csv PATH  Save enriched results to a CSV
#   --verify-emails    Run Reoon verification before upload
#   --min-subs N       Minimum subscriber count
#   --max-subs N       Maximum subscriber count
#   --language CODE    Language filter (keyword mode only, e.g. en, es)
#   --strict-match     Strict keyword match (keyword mode only)
```

---

## Output columns (CSV / results table)

| Column | Description |
|---|---|
| `channel_id` | YouTube channel ID |
| `channel_name` | Channel display name |
| `channel_handle` | YouTube handle (`@...`) |
| `channel_url` | Full channel URL |
| `email` | Discovered email address |
| `email_source` | Where the email came from (`apify_public` / `enrichment`) |
| `is_role_email` | `yes` if the email is a generic role address (flagged, not blocked) |
| `reoon_status` | Reoon verification result (`valid`, `invalid`, `disposable`, etc.) |
| `subscriber_count` | Subscriber count |
| `total_views` | Total channel views |
| `total_videos_count` | Number of uploaded videos |
| `country` | Country code |
| `niche` | Search keyword / niche label |
| `status` | Pipeline status for this lead |

---

## API credits

| Service | When credits are used |
|---|---|
| **Apify** | Keyword search (channel discovery) + email enrichment actor runs |
| **YouTube Data API** | Playlist extraction, channel URL lookup, subscriber metadata |
| **Reoon** | Only when "Verify (Reoon)" toggle is enabled |
| **Instantly** | Leads pushed to campaign (Instantly billing applies) |

---

## License

MIT
