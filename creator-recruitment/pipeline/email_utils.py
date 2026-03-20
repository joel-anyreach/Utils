"""
Utility helpers for email classification.
"""

ROLE_PREFIXES: frozenset[str] = frozenset({
    "info", "contact", "hello", "hi", "support", "admin", "sales",
    "marketing", "team", "office", "media", "press", "pr", "business",
    "collab", "collaboration", "partnership", "partnerships",
    "booking", "bookings", "help", "service", "services", "general",
    "mail", "noreply", "no-reply", "enquiry", "enquiries", "inquiry",
    "jobs", "careers", "legal", "finance", "billing", "accounts",
    "hr", "feedback", "newsletter", "news", "management", "manager",
})


def is_role_email(email: str) -> bool:
    """Return True if the email's local-part matches a known role prefix."""
    if not email:
        return False
    local = email.split("@")[0].lower().strip()
    return local in ROLE_PREFIXES
