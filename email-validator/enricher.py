"""
Phase 3 — Company Enrichment.

For each non-role email:
  1. Parse first/last name from email local part (free, local)
  2. Fetch company homepage (plain HTTP, free)
  3. Call Gemini 2.5 Flash Lite for description, pain point hint,
     industry classification, and team titles (one call per domain)

Role emails are skipped — their enrichment columns are left blank.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Callable, Dict, List, Optional

import aiohttp
from bs4 import BeautifulSoup

# Re-use role prefix set from validators to avoid duplication
try:
    from validators import ROLE_PREFIXES
except ImportError:
    ROLE_PREFIXES: frozenset = frozenset()

# ── Constants ─────────────────────────────────────────────────────────────────

GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.5-flash-lite:generateContent"
)

INDUSTRY_CLASSES = [
    "Healthcare",
    "MSP",
    "SaaS",
    "Technology",
    "eCommerce",
    "Communications",
    "Financial",
    "Education",
    "Energy & Utilities",
    "Insurance",
    "BPO",
    "Travel & Hospitality",
    "General (Unclassified)",
]

_EMPTY_ENRICHMENT = {
    "first_name": "",
    "last_name": "",
    "job_title": "",
    "industry": "",
    "company_description": "",
    "pain_point_hint": "",
}

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_PROMPT_TEMPLATE = """Website: {domain}
Content: {content}

Respond ONLY with a valid JSON object with exactly four keys:
1. "description": A 2-3 sentence summary of what this company does, who they serve, and their value proposition. Be concise and specific.
2. "pain_point_hint": In 1-2 sentences, infer what customer service or operational challenges this company likely faces based on their business model and site content (e.g. high churn, support volume, onboarding complexity, manual workflows). Be specific to their industry/model.
3. "industry": Classify this company into EXACTLY ONE of these categories: Healthcare | MSP | SaaS | Technology | eCommerce | Communications | Financial | Education | Energy & Utilities | Insurance | BPO | Travel & Hospitality | General (Unclassified). Return only the category label, nothing else.
4. "team_titles": A JSON array of up to 5 job titles found on the page for real named individuals (e.g. ["CEO", "Head of Customer Success", "CTO"]). Return an empty array [] if none found. Do NOT include generic department names — only titled individuals named on the site.

If content is insufficient, use "Unable to determine from website." for description and pain_point_hint, "General (Unclassified)" for industry, and [] for team_titles."""


# ── Name parsing ──────────────────────────────────────────────────────────────

def parse_name_from_email(email: str) -> dict[str, str]:
    """
    Best-effort first/last name extraction from email local part.
    Returns {"first_name": "...", "last_name": "..."}.
    Role emails and unrecognised patterns return {"first_name": "", "last_name": ""}.
    """
    try:
        local = email.split("@")[0].lower().strip()
    except Exception:
        return {"first_name": "", "last_name": ""}

    if local in ROLE_PREFIXES:
        return {"first_name": "", "last_name": ""}

    # Split on common separators
    parts = [p for p in re.split(r"[.\-_]", local) if p]

    if not parts:
        return {"first_name": "", "last_name": ""}

    if len(parts) == 1:
        token = parts[0]
        # Single initial only — skip (e.g. "j")
        if len(token) == 1:
            return {"first_name": "", "last_name": ""}
        # No separator — treat whole token as first name (can't reliably split "jdoe")
        return {"first_name": token.title(), "last_name": ""}

    if len(parts) == 2:
        a, b = parts
        # "jdoe" style → initial + last name
        if len(a) == 1:
            return {"first_name": "", "last_name": b.title()}
        # "john.d" style → first + initial
        if len(b) == 1:
            return {"first_name": a.title(), "last_name": ""}
        # "john.doe" style
        return {"first_name": a.title(), "last_name": b.title()}

    # 3+ parts — take first and last
    return {"first_name": parts[0].title(), "last_name": parts[-1].title()}


# ── Homepage fetching ─────────────────────────────────────────────────────────

async def fetch_page_text(domain: str, session: aiohttp.ClientSession) -> str:
    """
    Fetch and parse a company's homepage. Returns clean text ≤1500 chars.
    Tries https first, falls back to http. Returns "" on any failure.
    """
    headers = {"User-Agent": _USER_AGENT}
    timeout = aiohttp.ClientTimeout(total=10)

    for scheme in ("https", "http"):
        url = f"{scheme}://{domain}"
        try:
            async with session.get(
                url, headers=headers, timeout=timeout,
                allow_redirects=True, ssl=False,
            ) as resp:
                if resp.status >= 400:
                    continue
                html = await resp.text(errors="replace")
            break
        except Exception:
            html = ""
    else:
        return ""

    if not html:
        return ""

    try:
        soup = BeautifulSoup(html, "lxml")

        # Extract meta description first
        meta = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
        meta_text = ""
        if meta and meta.get("content"):
            meta_text = meta["content"].strip()

        # Remove noisy tags
        for tag in soup(["script", "style", "nav", "footer", "header",
                         "aside", "noscript", "svg", "iframe"]):
            tag.decompose()

        body_text = " ".join(soup.get_text(" ", strip=True).split())

        combined = (meta_text + " " + body_text).strip()
        return combined[:1500]

    except Exception:
        return ""


# ── Gemini enrichment ─────────────────────────────────────────────────────────

async def enrich_domain(
    domain: str,
    page_text: str,
    gemini_key: str,
    session: aiohttp.ClientSession,
) -> dict:
    """
    Call Gemini 2.5 Flash Lite with the scraped page text.
    Returns {"description", "pain_point_hint", "industry", "team_titles"}.
    """
    _fail = {
        "description": "Enrichment failed",
        "pain_point_hint": "",
        "industry": "General (Unclassified)",
        "team_titles": [],
    }

    content_input = page_text if page_text else f"(No page content available for {domain})"
    prompt = _PROMPT_TEMPLATE.format(domain=domain, content=content_input)

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 512},
    }

    try:
        async with session.post(
            GEMINI_URL,
            params={"key": gemini_key},
            json=payload,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status != 200:
                return _fail
            data = await resp.json(content_type=None)
    except Exception:
        return _fail

    try:
        raw_text = data["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError, TypeError):
        return _fail

    # Strip markdown code fences if present
    raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text.strip(), flags=re.IGNORECASE)
    raw_text = re.sub(r"\s*```$", "", raw_text.strip())

    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        return _fail

    # Normalise industry
    industry = str(parsed.get("industry", "")).strip()
    if industry not in INDUSTRY_CLASSES:
        industry = "General (Unclassified)"

    # Ensure team_titles is a list of strings
    raw_titles = parsed.get("team_titles", [])
    team_titles = [str(t) for t in raw_titles] if isinstance(raw_titles, list) else []

    return {
        "description": str(parsed.get("description", "")).strip(),
        "pain_point_hint": str(parsed.get("pain_point_hint", "")).strip(),
        "industry": industry,
        "team_titles": team_titles,
    }


# ── Batch enrichment ──────────────────────────────────────────────────────────

def _pick_job_title(first_name: str, team_titles: list[str]) -> str:
    """
    Pick the best job title from a domain's team_titles list.
    Prefers a title associated with the first name if it appears nearby;
    otherwise returns the first (most prominent) title in the list.
    """
    if not team_titles:
        return ""
    return team_titles[0]


async def enrich_batch(
    emails: list[str],
    gemini_key: str,
    tier: str = "free",
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> list[dict]:
    """
    Enrich a list of emails with company data.

    - Role emails are skipped (blank enrichment columns returned).
    - Domains are deduplicated — one Gemini call per unique domain.
    - tier: "free" (15 RPM, Semaphore(1) + 4s delay) or "paid" (Semaphore(5)).

    Returns a list of dicts (one per input email), each containing:
      email, first_name, last_name, job_title,
      industry, company_description, pain_point_hint
    """
    if tier == "paid":
        sem = asyncio.Semaphore(5)
        delay = 0.0
    else:
        sem = asyncio.Semaphore(1)
        delay = 4.0  # ~15 RPM

    # Identify role vs non-role emails
    def _is_role(email: str) -> bool:
        try:
            local = email.split("@")[0].lower().strip()
            return local in ROLE_PREFIXES
        except Exception:
            return False

    non_role_emails = [e for e in emails if not _is_role(e)]
    unique_domains = list(dict.fromkeys(
        e.split("@")[1].lower() for e in non_role_emails if "@" in e
    ))
    total_domains = len(unique_domains)

    domain_results: Dict[str, dict] = {}
    done_count = 0

    async with aiohttp.ClientSession() as session:
        async def _process_domain(domain: str):
            nonlocal done_count
            async with sem:
                page_text = await fetch_page_text(domain, session)
                result = await enrich_domain(domain, page_text, gemini_key, session)
                domain_results[domain] = result
                done_count += 1
                if progress_callback:
                    progress_callback(done_count, total_domains)
                if delay > 0:
                    await asyncio.sleep(delay)

        tasks = [_process_domain(d) for d in unique_domains]
        await asyncio.gather(*tasks)

    # Build per-email output rows
    output: list[dict] = []
    for email in emails:
        if _is_role(email):
            row = dict(_EMPTY_ENRICHMENT)
            row["email"] = email
            output.append(row)
            continue

        names = parse_name_from_email(email)
        domain = email.split("@")[1].lower() if "@" in email else ""
        d = domain_results.get(domain, {})

        row = {
            "email": email,
            "first_name": names["first_name"],
            "last_name": names["last_name"],
            "job_title": _pick_job_title(names["first_name"], d.get("team_titles", [])),
            "industry": d.get("industry", ""),
            "company_description": d.get("description", ""),
            "pain_point_hint": d.get("pain_point_hint", ""),
        }
        output.append(row)

    return output
