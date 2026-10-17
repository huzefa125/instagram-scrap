"""Per-account safety state: freeze after blocks, daily request budget, error log."""

import uuid
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse

# Only these last_json fields are kept; never cookies, tokens or credentials.
LAST_JSON_FIELDS = ("message", "feedback_message", "feedback_title", "error_type", "status", "spam")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def proxy_host(proxy_url: str) -> str | None:
    if not proxy_url:
        return None
    parsed = urlparse(proxy_url)
    return f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname


class AccountState:
    def __init__(self, store, username: str, proxy_url: str = ""):
        self.store = store
        self.username = username or "default"
        self.proxy = proxy_host(proxy_url)

    def get(self) -> dict:
        return self.store.get("account_state", self.username) or {"_id": self.username}

    # --- freeze -----------------------------------------------------------

    def frozen(self) -> tuple[bool, str | None, str | None]:
        doc = self.get()
        until = doc.get("frozen_until")
        if until and datetime.fromisoformat(until) > _now():
            return True, doc.get("freeze_reason"), until
        return False, None, None

    def freeze(self, hours: float, reason: str) -> str:
        until = (_now() + timedelta(hours=hours)).isoformat()
        self.store.upsert("account_state", self.username, {"frozen_until": until, "freeze_reason": reason})
        return until

    # --- server lock: one account runs on one server at a time -------------

    def acquire_lock(self, owner: str, ttl: timedelta) -> bool:
        """Take the lock if free or expired, or keep it if `owner` already holds it."""
        self.store.insert_if_missing("account_state", self.username, {"lock_until": None})
        now = _now()
        took = self.store.claim("account_state", {"_id": self.username},
                                {"lock_owner": owner, "lock_until": (now + ttl).isoformat()},
                                now=now.isoformat(), lease_field="lock_until")
        if took:
            return True
        if self.get().get("lock_owner") == owner:  # restarted before our own lock expired
            self.renew_lock(owner, ttl)
            return True
        return False

    def renew_lock(self, owner: str, ttl: timedelta) -> None:
        self.store.upsert("account_state", self.username,
                          {"lock_owner": owner, "lock_until": (_now() + ttl).isoformat()})

    def release_lock(self, owner: str) -> None:
        if self.get().get("lock_owner") == owner:
            self.store.upsert("account_state", self.username, {"lock_owner": None, "lock_until": None})

    def lock_holder(self) -> str | None:
        doc = self.get()
        until = doc.get("lock_until")
        return doc.get("lock_owner") if until and until > _now().isoformat() else None

    def mark_needs_login(self, reason: str) -> None:
        self.store.upsert("account_state", self.username, {"needs_login": True, "needs_login_reason": reason})

    def clear_needs_login(self) -> None:
        self.store.upsert("account_state", self.username, {"needs_login": False, "needs_login_reason": None})

    # --- daily budget -----------------------------------------------------

    def requests_today(self) -> int:
        doc = self.get()
        return doc.get("requests_today", 0) if doc.get("day") == date.today().isoformat() else 0

    def count_request(self) -> None:
        self.store.upsert("account_state", self.username, {
            "day": date.today().isoformat(),
            "requests_today": self.requests_today() + 1,
        })

    # --- error log --------------------------------------------------------

    def log_event(self, method: str, exc: Exception, last_json: dict | None, status_code: int | None,
                  action: str) -> None:
        last_json = last_json or {}
        self.store.upsert("events", uuid.uuid4().hex, {
            "at": _now().isoformat(),
            "account": self.username,
            "proxy": self.proxy,
            "method": method,
            "exception": type(exc).__name__,
            "message": str(exc)[:300],
            "http_status": status_code,
            "last_json": {k: last_json[k] for k in LAST_JSON_FIELDS if k in last_json},
            "action": action,
        })
