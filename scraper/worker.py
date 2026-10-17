"""Worker: runs this server's Instagram accounts against the shared queue.

  python -m scraper worker               always on (run it as a service on each server)
  python -m scraper run influencers.csv  enqueue + work until the queue is empty (pilot / dev)

One thread per account, one request at a time per account. Each loop an account:
  1. sleeps if it is frozen (until the freeze ends) or out of today's budget (until tomorrow)
  2. claims an influencer job (engagement / both) or an engager job (audience / both)
  3. sleeps WORKER_POLL_SECONDS when there is nothing to do

Every server runs the same code; MongoDB claims make sure no job is done twice, and account
locks make sure no account runs on two servers at once.
"""

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from instagrapi.exceptions import ClientError, NotFoundError, PrivateAccount

from .account import AccountState
from .checkpoint import Checkpoints
from .config import Config
from .jobqueue import WorkQueue
from .pipeline import AudienceWorker, InfluencerWorker
from .ratelimit import StopRun

log = logging.getLogger(__name__)

LOCK_TTL = timedelta(minutes=3)
LOCK_HEARTBEAT_SECONDS = 60
COOLDOWN_SECONDS = 1800          # after "throttled" / "network" stops
MAX_ENRICH_ATTEMPTS = 3


@dataclass
class Account:
    name: str
    role: str                                  # engagement | audience | both
    collector: Any
    state: AccountState
    requests: Callable[[], int] = lambda: 0
    max_per_day: int = 500
    stats: dict = field(default_factory=lambda: {"influencers": 0, "profiles": 0, "stopped": None})


def _seconds_until(iso: str) -> float:
    return max(0.0, (datetime.fromisoformat(iso) - datetime.now(timezone.utc)).total_seconds())


def _seconds_until_tomorrow() -> float:
    now = datetime.now()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
    return (tomorrow - now).total_seconds()


class Worker:
    def __init__(self, config: Config, store, accounts: list[Account], worker_id: str,
                 until_empty: bool = False, poll_seconds: float | None = None):
        self.config = config
        self.store = store
        self.accounts = accounts
        self.worker_id = worker_id
        self.until_empty = until_empty
        self.poll_seconds = config.poll_seconds if poll_seconds is None else poll_seconds
        self.queue = WorkQueue(store)
        self.checkpoints = Checkpoints(store)
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def _sleep(self, seconds: float) -> None:
        self._stop.wait(seconds)

    # --- one unit of work ---------------------------------------------------

    def _do_influencer(self, acc: Account) -> bool:
        username = self.queue.claim_influencer(f"{self.worker_id}/{acc.name}")
        if username is None:
            return False
        owner = f"{self.worker_id}/{acc.name}"
        log.info("[%s] @%s", acc.name, username)
        try:
            InfluencerWorker(self.config, self.store, acc.collector).process(
                username, heartbeat=lambda: self.queue.renew_influencer(username, owner))
            acc.stats["influencers"] += 1
        except StopRun:
            raise
        except (NotFoundError, PrivateAccount) as exc:
            self.checkpoints.mark_skipped(username, type(exc).__name__)
        except ClientError as exc:
            log.error("[%s] @%s failed: %s", acc.name, username, exc)
            # Retry & Checkpoint -> Job Queue: status "retry" makes it claimable again (max 3 attempts)
            self.checkpoints.mark_failed(username, f"{type(exc).__name__}: {exc}")
        finally:
            self.queue.release_influencer(username)
        return True

    def _do_audience(self, acc: Account) -> bool:
        user_id = self.queue.claim_audience(f"{self.worker_id}/{acc.name}")
        if user_id is None:
            return False
        try:
            AudienceWorker(self.config, self.store, acc.collector).enrich(user_id)
            acc.stats["profiles"] += 1
        except StopRun:
            self.queue.release_audience(user_id)
            raise
        except ClientError as exc:
            doc = self.store.get("audience", user_id) or {}
            attempts = doc.get("enrich_attempts", 0) + 1
            self.store.upsert("audience", user_id, {
                "enrich_attempts": attempts,
                "error": f"{type(exc).__name__}: {exc}"[:200],
                "enriched": attempts >= MAX_ENRICH_ATTEMPTS,
                "lease_until": None, "claimed_by": None,
            })
        return True

    # --- account loop -------------------------------------------------------

    def _wait_if_unavailable(self, acc: Account) -> bool:
        """Sleep through freezes and spent budgets. Returns False if the account should exit."""
        frozen, reason, until = acc.state.frozen()
        if frozen:
            if self.until_empty:
                acc.stats["stopped"] = f"frozen until {until}: {reason}"
                return False
            log.info("[%s] frozen until %s, sleeping", acc.name, until)
            self._sleep(min(_seconds_until(until) + 5, 3600))
            return True
        if acc.state.requests_today() >= acc.max_per_day:
            if self.until_empty:
                acc.stats["stopped"] = f"daily budget used ({acc.max_per_day})"
                return False
            log.info("[%s] daily budget used, sleeping until tomorrow", acc.name)
            self._sleep(min(_seconds_until_tomorrow(), 3600))
            return True
        return True

    def _account_loop(self, acc: Account) -> None:
        while not self._stop.is_set():
            if not self._wait_if_unavailable(acc):
                return
            if self._stop.is_set():
                return
            frozen, _, _ = acc.state.frozen()
            if frozen or acc.state.requests_today() >= acc.max_per_day:
                continue  # just woke from a sleep that hasn't finished yet

            try:
                did = False
                if acc.role in ("engagement", "both"):
                    did = self._do_influencer(acc)
                if not did and acc.role in ("audience", "both"):
                    did = self._do_audience(acc)
            except StopRun as exc:
                acc.stats["stopped"] = str(exc)
                log.warning("[%s] %s", acc.name, exc)
                if exc.kind == "session":
                    acc.state.mark_needs_login(str(exc))
                    return
                if exc.kind == "budget_run" or self.until_empty:
                    return
                if exc.kind in ("throttled", "network", "other"):
                    self._sleep(COOLDOWN_SECONDS)
                continue  # frozen / budget_day: handled at the top of the loop

            if not did:
                if self.until_empty and self.queue.is_drained():
                    return
                self._sleep(self.poll_seconds)

    def _run_account(self, acc: Account) -> None:
        try:
            self._account_loop(acc)
        except Exception as exc:  # noqa: BLE001 - one broken account must not stop the others
            log.exception("[%s] crashed", acc.name)
            acc.stats["stopped"] = f"crashed: {type(exc).__name__}: {exc}"

    def _lock_heartbeat(self) -> None:
        while not self._stop.wait(LOCK_HEARTBEAT_SECONDS):
            for acc in self.accounts:
                acc.state.renew_lock(self.worker_id, LOCK_TTL)

    # --- run ----------------------------------------------------------------

    def run(self) -> dict:
        log.info("Worker %s starting with %d accounts: %s", self.worker_id, len(self.accounts),
                 {a.name: a.role for a in self.accounts})
        threads = [threading.Thread(target=self._run_account, args=(a,), name=a.name, daemon=True)
                   for a in self.accounts]
        beat = threading.Thread(target=self._lock_heartbeat, name="lock-heartbeat", daemon=True)
        beat.start()
        for t in threads:
            t.start()
        try:
            for t in threads:
                while t.is_alive():
                    t.join(timeout=1)
        except KeyboardInterrupt:
            log.warning("Stopping: finishing current requests, releasing jobs and locks")
            self.stop()
            for t in threads:
                t.join(timeout=120)
        finally:
            self.stop()
            for acc in self.accounts:
                acc.state.release_lock(self.worker_id)

        counts = self.queue.counts()
        return {
            "accounts": {a.name: {"role": a.role, "requests": a.requests(), **a.stats} for a in self.accounts},
            "influencers_left": counts["influencers_pending"],
            "audience_left": counts["audience_waiting"],
        }
