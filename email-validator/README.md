# Email Validator

A three-phase email validation and company enrichment web app built with Python and Streamlit.

## Features

- **Phase 1 – Free local checks** (instant, no API credits)
  - Email format validation
  - MX record lookup
  - Disposable domain detection (150+ domains)
  - Role-based email detection
  - Duplicate detection

- **Phase 2 – API verification** (only verifies emails that passed Phase 1)
  - Reoon Email Verifier
  - ZeroBounce
  - NeverBounce
  - Hunter.io

- **Phase 3 – Company enrichment** (powered by Gemini 2.5 Flash Lite)
  - Scrapes company homepage for free (plain HTTP)
  - Parses first & last name from email address
  - Classifies company into 13 industry categories
  - Generates 2-3 sentence company description
  - Infers likely pain points / operational challenges
  - Best-effort job title extraction from company team pages
  - Skips role emails automatically (info@, contact@, etc.)
  - Domain-level deduplication — one API call per unique domain
  - Free tier (15 RPM) and Paid tier (unlimited) modes

- CSV upload or paste emails directly
- Real-time progress bar and speed display
- Export results at any phase
- API key manager with labels, credit display, and provider switcher

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the app

```bash
streamlit run app.py
```

Opens at `http://localhost:8501`

### 3. Add API keys

In the sidebar, go to each provider tab:
1. Enter a **label** (e.g. "Production Key")
2. Paste your **API key**
3. Click **Save Key** → **Test Key** → **Set as Active Provider**

Keys are saved to `keys.json` locally (gitignored).

---

## Getting API Keys

| Provider | Sign Up | Free Tier |
|---|---|---|
| **Reoon** | https://emailverifier.reoon.com | 100 free credits |
| **ZeroBounce** | https://www.zerobounce.net | 100 free credits/mo |
| **NeverBounce** | https://neverbounce.com | Pay-as-you-go |
| **Hunter.io** | https://hunter.io | 25 free verifications/mo |
| **Gemini** | https://aistudio.google.com/apikey | Free tier: ~1,500 req/day |

---

## Workflow

1. **Upload** a CSV (with an `email` column) or **paste** emails
2. **Run Phase 1** — instant local checks, no API cost
3. *(Optional)* Select active provider → **Proceed to Phase 2** for API verification
4. *(Optional)* Add Gemini key → **Run Enrichment** for company data
5. **Export** results at any phase as CSV

---

## Output Columns

### Phase 1 & 2
| Column | Description |
|---|---|
| `email` | Email address |
| `status` | Valid / Invalid / Risky / Catch-all / Role-based / Disposable / Spam Trap / Duplicate / Unknown |
| `failure_reason` | Why it failed (if applicable) |
| `phase` | 1 or 2 |
| `provider` | Local or API provider name |
| `mailbox_exists` | Yes / No / Unknown |
| `is_role_based` | Yes / No |
| `is_disposable` | Yes / No |
| `is_duplicate` | Yes / No |
| `mx_found` | Yes / No / Unknown |
| `confidence_score` | Score if provided by API |

### Phase 3 (Enriched Export)
| Column | Description |
|---|---|
| `first_name` | Parsed from email local part |
| `last_name` | Parsed from email local part |
| `job_title` | Best-effort from company team page |
| `industry` | One of 13 classified categories |
| `company_description` | 2-3 sentence company summary |
| `pain_point_hint` | Inferred operational/CS challenges |

### Industry Categories
`Healthcare` · `MSP` · `SaaS` · `Technology` · `eCommerce` · `Communications` · `Financial` · `Education` · `Energy & Utilities` · `Insurance` · `BPO` · `Travel & Hospitality` · `General (Unclassified)`

---

## Streamlit Cloud Deployment

1. Push repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io) → New app → select repo → `app.py`
3. In **Settings → Secrets**, paste contents of `.streamlit/secrets.toml.example` with real keys filled in
4. Deploy — share the `*.streamlit.app` URL with your team
