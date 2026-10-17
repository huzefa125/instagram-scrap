"""One collection run across all accounts, shared by the CLI and the API."""

import csv
import logging
import re
from pathlib import Path

from instagrapi.exceptions import ClientError

from .account import AccountState
from .collectors import Collector
from .config import AccountConfig, Config, load_accounts
from .jobqueue import WorkQueue
from .worker import LOCK_TTL, Account, Worker
from .ratelimit import RateGuard
from .session import SessionManager, session_file

log = logging.getLogger(__name__)


class CannotStart(Exception):
    """No account can work right now (all frozen, out of budget, or not logged in)."""


def read_usernames(path: Path) -> list[str]:
    """Accepts a CSV with a 'username' column, or one username / profile URL per line."""
    text = path.read_text(encoding="utf-8-sig")
    lines = text.splitlines()
    if lines and "username" in lines[0].lower().split(","):
        raw = [row.get("username") or row.get("Username") or "" for row in csv.DictReader(lines)]
    else:
        raw = lines
    return clean_usernames(raw)


def resolve_targets(targets: list[str]) -> list[str]:
    """Each target is a username, @name, profile URL, or a CSV/text file of them."""
    raw = []
    for target in targets:
        path = Path(target)
        raw.extend(read_usernames(path) if path.is_file() else [target])
    return clean_usernames(raw)


def clean_usernames(raw: list[str]) -> list[str]:
    names, seen = [], set()
    for value in raw:
        value = (value or "").strip()
        match = re.search(r"instagram\.com/([^/?#]+)", value)
        name = (match.group(1) if match else value).lstrip("@").strip().lower()
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    return names


def daily_limit(config: Config, account: AccountConfig) -> int:
    return account.max_requests_per_day or config.max_requests_per_day


def why_not_ready(config: Config, store, account: AccountConfig, force: bool,
                  need_session: bool) -> str | None:
    """Reason this account can't work now, or None if it can."""
    state = AccountState(store, account.username, account.proxy)
    frozen, reason, until = state.frozen()
    if frozen and not force:
        return f"frozen until {until}: {reason}"
    if state.requests_today() >= daily_limit(config, account):
        return f"daily budget used ({state.requests_today()}/{daily_limit(config, account)})"
    if need_session and not session_file(config, account.username).exists():
        return "no saved session (run `python -m scraper login`)"
    return None


def build_accounts(config: Config, store, force: bool, interactive: bool,
                   always_on: bool = False) -> tuple[list[Account], dict[str, str]]:
    """Lock and log in every usable account of THIS server. Returns (accounts, {skipped: reason}).

    always_on (the `worker` service): frozen or out-of-budget accounts still start and sleep until
    they can work. One-off runs skip them instead.
    """
    ready, skipped = [], {}
    configured = load_accounts(config)
    if not configured:
        raise CannotStart("No accounts. Add IG_USERNAME_1... to .env, or create accounts.json.")
    for acc in configured:
        state = AccountState(store, acc.username, acc.proxy)
        if always_on:
            reason = None if interactive or session_file(config, acc.username).exists()                 else "no saved session (run `python -m scraper login`)"
        else:
            reason = why_not_ready(config, store, acc, force, need_session=not interactive)
        if not reason and state.get().get("needs_login") and not interactive:
            reason = f"needs login: {state.get().get('needs_login_reason')} (run `python -m scraper login`)"
        if not reason:
            holder = state.lock_holder()
            if holder and holder != config.worker_id:
                reason = f"already running on server {holder}"
            elif not state.acquire_lock(config.worker_id, LOCK_TTL):
                reason = "could not lock account"
        if reason:
            skipped[acc.username] = reason
            continue

        session = SessionManager(config, acc)
        try:
            client = session.login(interactive=interactive)
        except ClientError as exc:
            state.release_lock(config.worker_id)
            skipped[acc.username] = f"login failed: {type(exc).__name__}"
            continue
        state.clear_needs_login()
        guard = RateGuard(config, session, state, daily_limit(config, acc), max_per_run=None if always_on else -1)
        ready.append(Account(acc.username, acc.role, Collector(client, guard), state,
                             requests=lambda g=guard: g.requests, max_per_day=daily_limit(config, acc)))
    for name, reason in skipped.items():
        log.warning("Skipping account %s: %s", name, reason)
    if not ready:
        raise CannotStart("No account can run: " + "; ".join(f"{n}: {r}" for n, r in skipped.items()))
    return ready, skipped


def run_collection(config: Config, store, usernames: list[str], force: bool = False,
                   interactive: bool = True) -> dict:
    """Pilot / dev: enqueue, then work with this server's accounts until the queue is empty."""
    queued = WorkQueue(store).enqueue(usernames)
    accounts, skipped = build_accounts(config, store, force, interactive)
    result = Worker(config, store, accounts, config.worker_id, until_empty=True).run()
    result["enqueued"] = queued
    result["skipped_accounts"] = skipped
    result["requests"] = sum(a["requests"] for a in result["accounts"].values())
    stops = [f"{n}: {a['stopped']}" for n, a in result["accounts"].items() if a["stopped"]]
    work_left = result["influencers_left"] or result["audience_left"]
    result["stopped"] = "; ".join(stops) if stops and work_left else None
    log.info("Run finished: %d requests, %d influencers left, %s engagers left",
             result["requests"], result["influencers_left"], result["audience_left"])
    return result


def run_worker(config: Config, store) -> dict:
    """Always-on worker for a server: runs until stopped (Ctrl+C / service stop)."""
    accounts, _ = build_accounts(config, store, force=False, interactive=False, always_on=True)
    return Worker(config, store, accounts, config.worker_id).run()
