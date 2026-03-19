"""
Optional Stage 2.5: Verify email addresses via Reoon EmailVerifier API.

API docs: https://emailverifier.reoon.com
Endpoint: GET https://emailverifier.reoon.com/api/v1/verify?email=EMAIL&key=KEY&mode=quick

Status values returned by Reoon:
  safe         — deliverable, not a role/disposable
  valid        — passes syntax + DNS, deliverability uncertain
  accept_all   — catch-all domain (accepts everything)
  unknown      — couldn't determine
  invalid      — bad format or non-existent mailbox
  disposable   — disposable/temporary email service
  role_account — role-based address (info@, support@, etc.)
"""
from __future__ import annotations

import time

import requests

from pipeline.models import ChannelRecord

REOON_VERIFY_URL = "https://emailverifier.reoon.com/api/v1/verify"

# Statuses that block upload to Instantly
BLOCKED_STATUSES: frozenset[str] = frozenset({"invalid", "disposable"})

# HTTP / response signals that indicate credit exhaustion
_CREDIT_EXHAUSTED_HTTP_CODES = {402}
_CREDIT_EXHAUSTED_STATUSES = {"credits_exhausted", "no_credits", "insufficient_credits"}


class ReoonCreditsExhaustedError(Exception):
    """Raised when the Reoon API reports that the account has no remaining credits."""

    def __init__(self, partial_count: int) -> None:
        self.partial_count = partial_count
        super().__init__(
            f"Reoon credits exhausted after verifying {partial_count} email(s)"
        )


def verify_emails(records: list[ChannelRecord], api_key: str) -> list[ChannelRecord]:
    """
    Call the Reoon API for every record that has an email address.

    Mutates each record's ``reoon_status`` field in-place.
    Records whose email was not reached (e.g. after credits exhausted) keep
    ``reoon_status = None`` and are treated as unblocked by the caller.

    Raises:
        ReoonCreditsExhaustedError: when the API signals no remaining credits.
                                    Already-verified records are preserved.
    """
    to_verify = [r for r in records if r.email]
    total = len(to_verify)
    print(f"  Verifying {total} email(s) via Reoon...")

    verified_count = 0
    for record in to_verify:
        try:
            resp = requests.get(
                REOON_VERIFY_URL,
                params={"email": record.email, "key": api_key, "mode": "quick"},
                timeout=15,
            )
        except requests.RequestException as exc:
            print(f"    [WARN] Network error verifying {record.email}: {exc}")
            record.reoon_status = "error"
            verified_count += 1
            time.sleep(0.2)
            continue

        # Credit exhaustion via HTTP status
        if resp.status_code in _CREDIT_EXHAUSTED_HTTP_CODES:
            raise ReoonCreditsExhaustedError(partial_count=verified_count)

        if resp.status_code != 200:
            print(f"    [WARN] Reoon returned HTTP {resp.status_code} for {record.email}")
            record.reoon_status = "error"
            verified_count += 1
            time.sleep(0.2)
            continue

        try:
            data = resp.json()
        except Exception:
            record.reoon_status = "error"
            verified_count += 1
            time.sleep(0.2)
            continue

        status = data.get("status", "unknown")

        # Credit exhaustion signalled in the response body
        if status in _CREDIT_EXHAUSTED_STATUSES:
            raise ReoonCreditsExhaustedError(partial_count=verified_count)

        record.reoon_status = status
        verified_count += 1
        time.sleep(0.2)  # ~5 req/sec — well within Reoon's limits

    return records
