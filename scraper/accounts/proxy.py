"""Proxy health: watch each account's proxy and hold the account back when it is unhealthy.

Proxies are never rotated or replaced here, and traffic never falls back to the machine's own IP:
a stable egress IP per account is the safety rule. A bad proxy only pauses its account until a
person fixes it (or confirms a deliberate change with `proxy --accept-ip`).
"""

import logging
import time
from datetime import datetime, timedelta, timezone

import requests

from ..observability import span

log = logging.getLogger(__name__)
IP_CHECK_URL = "https://api.ipify.org/"
FAILURES_BEFORE_DOWN = 2


def probe(proxy: str, timeout: float = 10.0) -> tuple[str, int]:
    """Return (egress IP, latency ms) seen through `proxy`; raises requests errors on failure."""
    started = time.perf_counter()
    response = requests.get(IP_CHECK_URL, proxies={"http": proxy, "https": proxy}, timeout=timeout)
    response.raise_for_status()
    return response.text.strip(), round((time.perf_counter() - started) * 1000)


class ProxyMonitor:
    def __init__(self, store, proxies: dict[str, str], every_minutes: int = 30, prober=probe):
        self.store = store
        self.proxies = proxies
        self.every = timedelta(minutes=every_minutes)
        self.prober = prober

    def check(self, username: str, force: bool = False) -> dict:
        """Check one account's proxy (at most every `every_minutes` unless forced)."""
        doc = self.store.get("proxy_health", username) or {}
        proxy = self.proxies.get(username, "")
        if not proxy:
            return {"status": "none", "ok": True}
        now = datetime.now(timezone.utc)
        last = doc.get("checked_at")
        if not force and last and now - datetime.fromisoformat(last) < self.every:
            return doc

        fields: dict = {"checked_at": now.isoformat()}
        try:
            with span("proxy.check", username):
                ip, latency = self.prober(proxy)
        except (requests.RequestException, OSError) as exc:
            failures = doc.get("failures", 0) + 1
            down = failures >= FAILURES_BEFORE_DOWN
            fields.update(failures=failures, error=type(exc).__name__, ok=not down,
                          status="down" if down else "flaky")
            if failures == FAILURES_BEFORE_DOWN:
                log.error("[%s] proxy is down (%s). The account is paused until it works again",
                          username, fields["error"])
        else:
            expected = doc.get("expected_ip") or ip
            changed = ip != expected
            fields.update(ip=ip, expected_ip=expected, latency_ms=latency, failures=0, error=None,
                          ok=not changed, status="ip_changed" if changed else "ok")
            if changed and doc.get("status") != "ip_changed":
                log.error("[%s] proxy IP changed from %s to %s. The account is paused; confirm with "
                          "`python -m scraper proxy --account %s --accept-ip`", username, expected, ip, username)
        return self.store.upsert("proxy_health", username, fields)

    def accept_ip(self, username: str) -> dict:
        """A person confirms the proxy's current IP is the intended one."""
        ip, latency = self.prober(self.proxies[username])
        return self.store.upsert("proxy_health", username, {
            "ip": ip, "expected_ip": ip, "latency_ms": latency, "failures": 0, "error": None, "ok": True,
            "status": "ok", "checked_at": datetime.now(timezone.utc).isoformat()})

    def healthy(self, username: str) -> bool:
        return bool(self.check(username).get("ok", True))
