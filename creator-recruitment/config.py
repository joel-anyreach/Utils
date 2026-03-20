import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    # Apify (used for both channel discovery AND email enrichment)
    apify_api_token: str
    apify_actor_id: str          # channel discovery actor
    apify_email_actor_id: str    # email enrichment actor
    apify_max_results: int
    youtube_api_key: str         # required by the discovery actor

    # Instantly
    instantly_api_key: str
    instantly_campaign_id: str

    # Reoon EmailVerifier (optional — only needed when --verify-emails is used)
    reoon_api_key: str

    # Google Sheets (OAuth as demo@anyreach.ai)
    google_sheet_id: str
    google_oauth_credentials_file: str
    google_oauth_token_file: str
    sheet_tab_name: str


def load_config() -> Config:
    load_dotenv()

    missing = []

    def require(key: str) -> str:
        val = os.getenv(key, "").strip()
        if not val:
            missing.append(key)
        return val

    def optional_int(key: str, default: int) -> int:
        val = os.getenv(key, "").strip()
        return int(val) if val else default

    cfg = Config(
        apify_api_token=require("APIFY_API_TOKEN"),
        apify_actor_id=os.getenv("APIFY_ACTOR_ID", "badruddeen/youtube-niche-channel-finder"),
        apify_email_actor_id=os.getenv("APIFY_EMAIL_ACTOR_ID", "endspec/youtube-instant-email-scraper"),
        apify_max_results=optional_int("APIFY_MAX_RESULTS", 200),
        youtube_api_key=require("YOUTUBE_API_KEY"),

        instantly_api_key=require("INSTANTLY_API_KEY"),
        instantly_campaign_id=require("INSTANTLY_CAMPAIGN_ID"),

        reoon_api_key=os.getenv("REOON_API_KEY", "").strip(),

        google_sheet_id=require("GOOGLE_SHEET_ID"),
        google_oauth_credentials_file=os.getenv("GOOGLE_OAUTH_CREDENTIALS_FILE", "oauth_credentials.json"),
        google_oauth_token_file=os.getenv("GOOGLE_OAUTH_TOKEN_FILE", "oauth_token.json"),
        sheet_tab_name=os.getenv("SHEET_TAB_NAME", "Leads"),
    )

    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Copy .env.example to .env and fill in all values."
        )

    creds_file = cfg.google_oauth_credentials_file
    if not os.path.exists(creds_file):
        raise EnvironmentError(
            f"Google OAuth credentials file not found: {creds_file}\n"
            "Download OAuth client credentials from Google Cloud Console\n"
            "and save as oauth_credentials.json in the project root."
        )

    return cfg
