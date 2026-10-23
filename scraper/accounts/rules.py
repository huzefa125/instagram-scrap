"""Account / proxy rules check: run before the first login so mistakes cost nothing."""

import re
from collections import Counter
from urllib.parse import urlparse

from ..core.config import AccountConfig

USERNAME = re.compile(r"[A-Za-z0-9._]{1,30}")
SCHEMES = ("http", "https", "socks5", "socks5h")


def check_accounts(accounts: list[AccountConfig]) -> list[tuple[str, str]]:
    """Return (level, message) pairs; level is "error" or "warn"."""
    problems: list[tuple[str, str]] = []
    if not accounts:
        return [("error", "No accounts configured (accounts.json or IG_USERNAME in .env)")]

    names = Counter(a.username.lower() for a in accounts)
    proxies = Counter(a.proxy for a in accounts if a.proxy)

    for acc in accounts:
        who = acc.username or "(blank username)"
        if not acc.username or not acc.password:
            problems.append(("error", f"{who}: username and password are both required"))
        if "@" in acc.username:
            problems.append(("error", f"{who}: use the Instagram username, not an email address"))
        elif acc.username and not USERNAME.fullmatch(acc.username):
            problems.append(("error", f"{who}: not a valid Instagram username"))
        if names[acc.username.lower()] > 1:
            problems.append(("error", f"{who}: listed more than once"))

        if not acc.proxy:
            problems.append(("error", f"{who}: no proxy; every account needs its own stable proxy"))
            continue
        parsed = urlparse(acc.proxy)
        if parsed.scheme not in SCHEMES or not parsed.hostname or not parsed.port:
            problems.append(("error", f"{who}: proxy must look like http://user:pass@host:port"))
        elif not parsed.username:
            problems.append(("warn", f"{who}: proxy has no username/password"))
        if proxies[acc.proxy] > 1:
            problems.append(("error", f"{who}: shares its proxy with another account"))
    return problems
