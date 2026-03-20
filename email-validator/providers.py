"""
Phase 2 — API provider integrations.
All responses are normalized to the unified result format.
"""

import asyncio
import aiohttp
from typing import Dict, List, Optional, Tuple


# ─────────────────────────────────────────────────────────────────────────────
# UNIFIED STATUS MAP HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _yn(val) -> str:
    if val is True or str(val).lower() in ("true", "yes", "1"):
        return "Yes"
    if val is False or str(val).lower() in ("false", "no", "0"):
        return "No"
    return "Unknown"


# ─────────────────────────────────────────────────────────────────────────────
# REOON
# ─────────────────────────────────────────────────────────────────────────────

async def reoon_verify(email: str, api_key: str, session: aiohttp.ClientSession) -> Dict:
    url = "https://emailverifier.reoon.com/api/v1/verify"
    params = {"email": email, "key": api_key, "mode": "power"}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json(content_type=None)
    except Exception as e:
        return _error_result(email, "Reoon", str(e))

    status_raw = str(data.get("status", "")).lower()
    is_valid    = data.get("is_valid_syntax", False)
    is_disp     = data.get("is_disposable", False)
    is_role     = data.get("is_role", False)
    mx_found    = data.get("mx_found", False)
    mbox        = data.get("is_deliverable", None)
    score       = data.get("quality_score", "")

    # Map to unified status
    if status_raw in ("valid", "safe"):
        status = "Valid"
        reason = ""
    elif status_raw == "disposable":
        status, reason = "Disposable", "Disposable domain detected by Reoon"
    elif status_raw in ("invalid", "dead"):
        status, reason = "Invalid", data.get("reason", "Invalid per Reoon")
    elif status_raw in ("catch_all", "catch-all"):
        status, reason = "Catch-all", "Catch-all domain"
    elif status_raw == "role_based":
        status, reason = "Role-based", "Role-based address"
    elif status_raw == "spamtrap":
        status, reason = "Spam Trap", "Spam trap detected"
    else:
        status, reason = "Unknown", f"Reoon status: {status_raw}"

    mailbox = "Yes" if mbox is True else ("No" if mbox is False else "Unknown")

    return {
        "status": status,
        "failure_reason": reason,
        "provider": "Reoon",
        "mailbox_exists": mailbox,
        "is_role_based": _yn(is_role),
        "is_disposable": _yn(is_disp),
        "mx_found": _yn(mx_found),
        "confidence_score": str(score) if score != "" else "",
    }


async def reoon_credits(api_key: str, session: aiohttp.ClientSession) -> Tuple[bool, int | str]:
    """Returns (valid, credits_remaining)."""
    url = "https://emailverifier.reoon.com/api/v1/get-credits"
    try:
        async with session.get(url, params={"key": api_key},
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json(content_type=None)
        if data.get("status") == "success":
            return True, data.get("credits_remaining", "N/A")
        return False, data.get("message", "Invalid key")
    except Exception as e:
        return False, str(e)


# ─────────────────────────────────────────────────────────────────────────────
# ZEROBOUNCE
# ─────────────────────────────────────────────────────────────────────────────

async def zerobounce_verify(email: str, api_key: str, session: aiohttp.ClientSession) -> Dict:
    url = "https://api.zerobounce.net/v2/validate"
    params = {"apikey": api_key, "email": email}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json(content_type=None)
    except Exception as e:
        return _error_result(email, "ZeroBounce", str(e))

    zb_status = str(data.get("status", "")).lower()
    sub_status = str(data.get("sub_status", "")).lower()

    status_map = {
        "valid":        ("Valid",      ""),
        "invalid":      ("Invalid",    sub_status or "Invalid per ZeroBounce"),
        "catch-all":    ("Catch-all",  "Catch-all domain"),
        "unknown":      ("Unknown",    "Could not determine deliverability"),
        "spamtrap":     ("Spam Trap",  "Spam trap address"),
        "abuse":        ("Invalid",    "Abuse/complaint address"),
        "do_not_mail":  ("Invalid",    sub_status or "Do not mail"),
    }
    status, reason = status_map.get(zb_status, ("Unknown", f"ZeroBounce: {zb_status}"))

    return {
        "status": status,
        "failure_reason": reason,
        "provider": "ZeroBounce",
        "mailbox_exists": "Yes" if zb_status == "valid" else "No" if zb_status == "invalid" else "Unknown",
        "is_role_based": _yn(data.get("is_role", False)),
        "is_disposable": _yn(sub_status == "disposable"),
        "mx_found": "Yes" if data.get("mx_found") else "No",
        "confidence_score": "",
    }


async def zerobounce_credits(api_key: str, session: aiohttp.ClientSession) -> Tuple[bool, int | str]:
    url = "https://api.zerobounce.net/v2/getcredits"
    try:
        async with session.get(url, params={"apikey": api_key},
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json(content_type=None)
        credits = data.get("Credits", -1)
        if credits == -1:
            return False, data.get("error", "Invalid key")
        return True, int(credits)
    except Exception as e:
        return False, str(e)


# ─────────────────────────────────────────────────────────────────────────────
# NEVERBOUNCE
# ─────────────────────────────────────────────────────────────────────────────

async def neverbounce_verify(email: str, api_key: str, session: aiohttp.ClientSession) -> Dict:
    url = "https://api.neverbounce.com/v4/single/check"
    params = {"key": api_key, "email": email}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json(content_type=None)
    except Exception as e:
        return _error_result(email, "NeverBounce", str(e))

    nb_result = str(data.get("result", "")).lower()
    status_map = {
        "valid":       ("Valid",      ""),
        "invalid":     ("Invalid",    "Invalid per NeverBounce"),
        "disposable":  ("Disposable", "Disposable domain"),
        "catchall":    ("Catch-all",  "Catch-all domain"),
        "unknown":     ("Unknown",    "Could not determine deliverability"),
    }
    status, reason = status_map.get(nb_result, ("Unknown", f"NeverBounce: {nb_result}"))

    return {
        "status": status,
        "failure_reason": reason,
        "provider": "NeverBounce",
        "mailbox_exists": "Yes" if nb_result == "valid" else "No" if nb_result == "invalid" else "Unknown",
        "is_role_based": "Unknown",
        "is_disposable": "Yes" if nb_result == "disposable" else "No",
        "mx_found": "Unknown",
        "confidence_score": str(data.get("numeric_code", "")),
    }


async def neverbounce_credits(api_key: str, session: aiohttp.ClientSession) -> Tuple[bool, int | str]:
    url = "https://api.neverbounce.com/v4/account/info"
    try:
        async with session.get(url, params={"key": api_key},
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json(content_type=None)
        if data.get("status") == "success":
            credits = data.get("credits_info", {}).get("free_credits_remaining", "N/A")
            return True, credits
        return False, data.get("message", "Invalid key")
    except Exception as e:
        return False, str(e)


# ─────────────────────────────────────────────────────────────────────────────
# HUNTER.IO
# ─────────────────────────────────────────────────────────────────────────────

async def hunter_verify(email: str, api_key: str, session: aiohttp.ClientSession) -> Dict:
    url = "https://api.hunter.io/v2/email-verifier"
    params = {"email": email, "api_key": api_key}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = (await r.json(content_type=None)).get("data", {})
    except Exception as e:
        return _error_result(email, "Hunter", str(e))

    h_status = str(data.get("status", "")).lower()
    h_result = str(data.get("result", "")).lower()
    score    = data.get("score", "")

    if h_result == "deliverable":
        status, reason = "Valid", ""
    elif h_result == "undeliverable":
        status, reason = "Invalid", "Undeliverable per Hunter"
    elif h_result == "risky":
        status, reason = "Risky", "Risky address per Hunter"
    elif h_result == "unknown":
        status, reason = "Unknown", "Could not determine deliverability"
    else:
        status, reason = "Unknown", f"Hunter result: {h_result}"

    return {
        "status": status,
        "failure_reason": reason,
        "provider": "Hunter",
        "mailbox_exists": "Yes" if h_result == "deliverable" else "No" if h_result == "undeliverable" else "Unknown",
        "is_role_based": "Unknown",
        "is_disposable": _yn(data.get("disposable", False)),
        "mx_found": "Unknown",
        "confidence_score": str(score) if score != "" else "",
    }


async def hunter_credits(api_key: str, session: aiohttp.ClientSession) -> Tuple[bool, int | str]:
    url = "https://api.hunter.io/v2/account"
    try:
        async with session.get(url, params={"api_key": api_key},
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = (await r.json(content_type=None)).get("data", {})
        requests_left = data.get("requests", {}).get("available", None)
        if requests_left is None:
            return False, "Invalid key or no data"
        return True, requests_left
    except Exception as e:
        return False, str(e)


# ─────────────────────────────────────────────────────────────────────────────
# DISPATCHER
# ─────────────────────────────────────────────────────────────────────────────

PROVIDER_VERIFY = {
    "Reoon":       reoon_verify,
    "ZeroBounce":  zerobounce_verify,
    "NeverBounce": neverbounce_verify,
    "Hunter":      hunter_verify,
}

PROVIDER_CREDITS = {
    "Reoon":       reoon_credits,
    "ZeroBounce":  zerobounce_credits,
    "NeverBounce": neverbounce_credits,
    "Hunter":      hunter_credits,
}


def _error_result(email: str, provider: str, error: str) -> Dict:
    return {
        "status": "Unknown",
        "failure_reason": f"API error: {error}",
        "provider": provider,
        "mailbox_exists": "Unknown",
        "is_role_based": "Unknown",
        "is_disposable": "Unknown",
        "mx_found": "Unknown",
        "confidence_score": "",
    }


async def check_credits(provider: str, api_key: str) -> Tuple[bool, int | str]:
    """Test an API key and return (is_valid, credits_remaining)."""
    async with aiohttp.ClientSession() as session:
        fn = PROVIDER_CREDITS.get(provider)
        if fn:
            return await fn(api_key, session)
        return False, "Unknown provider"


async def verify_batch(
    emails: List[str],
    provider: str,
    api_key: str,
    concurrency: int = 5,
    progress_callback=None,
) -> List[Dict]:
    """
    Verify a list of emails using the chosen provider.
    Returns list of partial result dicts (to be merged with Phase 1 results).
    progress_callback(done, total) called after each email completes.
    """
    fn = PROVIDER_VERIFY.get(provider)
    if not fn:
        return [_error_result(e, provider, "Unknown provider") for e in emails]

    results = [None] * len(emails)
    semaphore = asyncio.Semaphore(concurrency)
    done_count = 0

    async def _verify_one(idx: int, email: str, session: aiohttp.ClientSession):
        nonlocal done_count
        async with semaphore:
            res = await fn(email, api_key, session)
            results[idx] = res
            done_count += 1
            if progress_callback:
                progress_callback(done_count, len(emails))

    async with aiohttp.ClientSession() as session:
        tasks = [_verify_one(i, e, session) for i, e in enumerate(emails)]
        await asyncio.gather(*tasks)

    return results
