"""
Phase 1 — free local email checks.
  - Format validation
  - MX record lookup
  - Disposable domain detection
  - Role-based address detection
  - Duplicate detection
"""

import re
import asyncio
from typing import List, Dict

try:
    import dns.resolver
    DNS_AVAILABLE = True
except ImportError:
    DNS_AVAILABLE = False

# ── Regex ────────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)

# ── Role-based prefixes ───────────────────────────────────────────────────────
ROLE_PREFIXES = {
    "info", "admin", "support", "noreply", "no-reply", "hello", "contact",
    "sales", "marketing", "billing", "webmaster", "postmaster", "team",
    "help", "abuse", "hr", "finance", "legal", "press", "media", "jobs",
    "careers", "office", "mail", "email", "newsletter", "news", "notifications",
    "notification", "alerts", "alert", "security", "privacy", "compliance",
    "service", "services", "enquiries", "enquiry", "feedback", "customercare",
    "customerservice", "care", "invoices", "invoice", "accounts", "account",
    "payments", "payment", "orders", "order", "returns", "return", "spam",
    "do-not-reply", "donotreply", "unsubscribe", "bounce", "bounces",
    "devnull", "dev-null", "root", "hostmaster", "usenet", "news",
    "uucp", "ftp", "www", "daemon", "mailer-daemon",
}

# ── Disposable domains (150+ entries) ─────────────────────────────────────────
DISPOSABLE_DOMAINS = {
    # Classic disposable services
    "mailinator.com", "guerrillamail.com", "guerrillamail.net",
    "guerrillamail.org", "guerrillamail.biz", "guerrillamail.de",
    "guerrillamail.info", "guerrillamailblock.com",
    "tempmail.com", "temp-mail.org", "temp-mail.io", "tempr.email",
    "throwam.com", "throwaway.email", "throwam.com",
    "yopmail.com", "yopmail.fr", "cool.fr.nf", "jetable.fr.nf",
    "nospam.ze.tc", "nomail.xl.cx", "mega.zik.dj", "speed.1s.fr",
    "courriel.fr.nf", "moncourrier.fr.nf", "monemail.fr.nf",
    "monmail.fr.nf",
    "sharklasers.com", "guerrillamailblock.com", "grr.la",
    "guerrillamail.info", "spam4.me",
    "trashmail.com", "trashmail.at", "trashmail.io", "trashmail.me",
    "trashmail.net", "trashmail.org", "trashmailer.com", "trashmail.xyz",
    "dispostable.com", "disposableaddress.com", "mailnull.com",
    "spamgourmet.com", "spamgourmet.net", "spamgourmet.org",
    "maildrop.cc", "mailnull.com", "spamex.com",
    "fakeinbox.com", "fakeinbox.net", "fakemailgenerator.com",
    "mailexpire.com", "mailme.lv", "mailme24.com", "mail-temporaire.fr",
    "filzmail.com", "easytrashmail.com", "anonaddy.com",
    "spamhereplease.com", "spamherelots.com", "binkmail.com",
    "bobmail.info", "chammy.info", "devnullmail.com", "discard.email",
    "discardmail.com", "discardmail.de", "dodgit.com",
    "donemail.ru", "dump-email.info", "dumpandjunk.com",
    "dumpmail.de", "emaildienst.de", "emailsensei.com",
    "emailtemporanea.com", "emailtemporario.com.br",
    "emailthe.net", "emailtmp.com", "emailwarden.com",
    "ephemail.net", "etranquil.com", "etranquil.net",
    "etranquil.org", "explodemail.com", "fastacura.com",
    "fightallspam.com", "fizmail.com", "fleckens.hu",
    "frapmail.com", "garliclife.com", "gelitik.in",
    "get1mail.com", "getonemail.com", "getonemail.net",
    "ghosttexter.de", "girlsundertheinfluence.com",
    "gishpuppy.com", "gowikibooks.com", "gowikicampus.com",
    "gowikicars.com", "gowikifilms.com", "gowikigames.com",
    "gowikimusic.com", "gowikinetwork.com", "gowikitravel.com",
    "grandmamail.com", "grandmasmail.com",
    "haltospam.com", "herp.in", "hidemail.de",
    "hidzz.com", "hmamail.com", "hopemail.biz",
    "ieh-mail.de", "ihateyoualot.info", "iheartspam.org",
    "imails.info", "inoutmail.de", "inoutmail.eu",
    "inoutmail.info", "inoutmail.net", "instant-mail.de",
    "instantemailaddress.com", "internet-e-mail.de",
    "internet-mail.de", "internetemails.net", "inwind.it",
    "ipoo.org", "irish2me.com", "jetable.com",
    "jetable.net", "jetable.org", "joojoo.be",
    "junk1.tk", "kasmail.com", "kaspop.com",
    "killmail.com", "killmail.net", "klassmaster.com",
    "klzlk.com", "knol-power.nl", "kurzepost.de",
    "letthemeatspam.com", "lhsdv.com", "lifebyfood.com",
    "link2mail.net", "litedrop.com", "lol.ovpn.to",
    "lolfreak.net", "lookugly.com", "lortemail.dk",
    "lovemeleaveme.com", "lr78.com", "lukop.dk",
    "m4ilweb.info", "maboard.com", "mail114.net",
    "mail1a.de", "mail21.cc", "mail2rss.org",
    "mail333.com", "mailbidon.com", "mailbiz.biz",
    "mailblocks.com", "mailbucket.org", "mailc.net",
    "mailcat.biz", "mailcatch.com", "mailchu.com",
    "mailclean.net", "mailcorner.eu", "maildu.de",
    "maileater.com", "mailed.in", "mailfa.tk",
    "mailforspam.com", "mailfree.ga", "mailfreeonline.com",
    "mailguard.me", "mailimate.com", "mailin8r.com",
    "mailinater.com", "mailincubator.com", "mailismagic.com",
    "mailme.gq", "mailme.ir", "mailnew.com",
    "mailnull.com", "mailpick.biz", "mailproxsy.com",
    "mailquack.com", "mailrock.biz", "mailscrap.com",
    "mailseal.de", "mailshell.com", "mailsiphon.com",
    "mailslapping.com", "mailslite.com", "mailsnull.com",
    "mailsoul.com", "mailsucker.net", "mailtemp.info",
    "mailtome.de", "mailtothis.com", "mailtrash.net",
    "mailtv.net", "mailtv.tv", "mailzilla.com",
    "mailzilla.org", "meinspamschutz.de", "meltmail.com",
    "mierdamail.com", "mintemail.com", "moncourrier.fr.nf",
    "monemail.fr.nf", "monmail.fr.nf",
    "mt2009.com", "mt2014.com", "mx0.wwwnew.eu",
    "mycleaninbox.net", "mypartyclip.de", "myphantomemail.com",
    "myspaceinc.com", "myspaceinc.net", "myspaceinc.org",
    "myspacepimpedup.com", "myspamless.com", "mytempemail.com",
    "mytempmail.com", "mytrashmail.com", "nabuma.com",
    "netmails.com", "netmails.net", "netzidiot.de",
    "nh3.ro", "nice-4u.com", "nincsmail.hu",
    "nnh.com", "noblepioneer.com", "nogmailspam.info",
    "nomail.pw", "nomail.xl.cx", "nomail2me.com",
    "nomorespamemails.com", "nonspam.eu", "nonspammer.de",
    "noref.in", "nospam.ze.tc", "nospamfor.us",
    "nospammail.net", "nospamthanks.info", "notmailinator.com",
    "nowhere.org", "nowmymail.com",
    # Additional modern services
    "10minutemail.com", "10minutemail.net", "10minutemail.org",
    "10minutemail.de", "10minutemail.nl",
    "20minutemail.com", "20minutemail.it",
    "33mail.com", "spamgourmet.com",
    "mohmal.com", "tempmailaddress.com",
    "tempinbox.com", "spambox.us", "spambox.info",
    "spamdecoy.net", "spamfree24.org", "spamfree.eu",
    "spamgob.com", "spamhereplease.com",
    "mailinator2.com", "getairmail.com",
    "throwam.com", "throwam.net",
    "crazymailing.com", "deadaddress.com",
    "discard.email", "discardmail.com",
}


def _mx_lookup(domain: str) -> bool:
    """Return True if domain has MX records."""
    if not DNS_AVAILABLE:
        return True  # Assume valid if dns module not available
    try:
        dns.resolver.resolve(domain, "MX")
        return True
    except Exception:
        return False


def validate_phase1(emails: List[str]) -> List[Dict]:
    """
    Run Phase 1 checks on a list of email strings.
    Returns list of result dicts (unified format).
    """
    seen = {}
    results = []

    for raw in emails:
        email = raw.strip().lower()

        if not email:
            continue

        result = {
            "email": email,
            "status": "Valid",
            "failure_reason": "",
            "phase": 1,
            "provider": "Local",
            "mailbox_exists": "Unknown",
            "is_role_based": "No",
            "is_disposable": "No",
            "is_duplicate": "No",
            "mx_found": "Unknown",
            "confidence_score": "",
        }

        # ── Duplicate check ──────────────────────────────────────────────────
        if email in seen:
            result["status"] = "Duplicate"
            result["is_duplicate"] = "Yes"
            result["failure_reason"] = "Duplicate email address"
            results.append(result)
            continue
        seen[email] = True

        # ── Format check ─────────────────────────────────────────────────────
        if not EMAIL_RE.match(email):
            result["status"] = "Invalid"
            result["failure_reason"] = "Invalid email format"
            results.append(result)
            continue

        local, domain = email.rsplit("@", 1)

        # ── Disposable domain ────────────────────────────────────────────────
        if domain in DISPOSABLE_DOMAINS:
            result["status"] = "Disposable"
            result["is_disposable"] = "Yes"
            result["failure_reason"] = "Disposable/throwaway email domain"
            results.append(result)
            continue

        # ── Role-based check ─────────────────────────────────────────────────
        if local in ROLE_PREFIXES:
            result["is_role_based"] = "Yes"
            result["status"] = "Role-based"
            result["failure_reason"] = "Role-based address (e.g. info@, admin@)"

        # ── MX record check ──────────────────────────────────────────────────
        mx_ok = _mx_lookup(domain)
        result["mx_found"] = "Yes" if mx_ok else "No"
        if not mx_ok:
            result["status"] = "Invalid"
            result["failure_reason"] = "No MX records found for domain"
            results.append(result)
            continue

        results.append(result)

    return results


def split_emails(raw_text: str) -> List[str]:
    """Split pasted text into individual email addresses."""
    import re as _re
    # Split on whitespace, commas, semicolons, newlines
    parts = _re.split(r"[\s,;]+", raw_text)
    return [p.strip() for p in parts if p.strip()]
