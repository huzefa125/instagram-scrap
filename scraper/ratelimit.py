"""Rate Limit / Challenge Handler.

Every Instagram request goes through RateGuard.call(). Each kind of response is handled
differently, following instagrapi's best-practices guide:

  ClientThrottledError (429)    back off 5 / 15 / 30 min, then stop
  PleaseWaitFewMinutes          one 30 min cooldown; a second one stops and freezes the account 6 h
  ClientConnectionError         network problem: short retries, then stop (no freeze)
  FeedbackRequired              action blocked: stop, freeze 24 h
  ChallengeRequired             stop, freeze 12 h; verify in the Instagram app (never auto-resolved)
  LoginRequired                 log in again with the same device once, then stop

Every exception is written to the `events` collection.
"""

import logging
import random
import time
from typing import Callable, TypeVar

from instagrapi.exceptions import (
    ChallengeRequired,
    ClientConnectionError,
    ClientError,
    ClientThrottledError,
    FeedbackRequired,
    LoginRequired,
    PleaseWaitFewMinutes,
    RateLimitError,
)

from .account import AccountState
from .config import Config
from .session import SessionManager

log = logging.getLogger(__name__)
T = TypeVar("T")

THROTTLE_BACKOFF = [300, 900, 1800]   # 429: 5, 15, 30 minutes
NETWORK_RETRIES = [10, 30, 60]        # timeouts / connection resets
PLEASE_WAIT_COOLDOWN = 1800           # 30 minutes
FREEZE_HOURS = {"please_wait": 6, "feedback": 24, "challenge": 12}


class StopRun(Exception):
    """This account must stop for now. `kind` tells the worker what to do next:

      budget_day  daily budget used          -> sleep until tomorrow
      budget_run  per-run budget used        -> end this run
      frozen      challenge / block / please-wait twice -> sleep until the freeze ends
      throttled   still 429 after backoffs   -> cool down, then continue
      network     network keeps failing      -> cool down, then continue
      session     login lost and re-login failed -> needs a person: run `login` again
    """

    def __init__(self, message: str, kind: str = "other"):
        super().__init__(message)
        self.kind = kind


class RateGuard:
    def __init__(self, config: Config, session: SessionManager, account: AccountState,
                 max_per_day: int | None = None, max_per_run: int | None = -1):
        self.config = config
        self.session = session
        self.account = account
        self.max_per_day = max_per_day or config.max_requests_per_day
        # -1 = use MAX_REQUESTS_PER_RUN; None = no per-run limit (always-on worker)
        self.max_per_run = config.max_requests_per_run if max_per_run == -1 else max_per_run
        self.requests = 0
        self.please_wait_seen = False

    def _check_budget(self) -> None:
        if self.max_per_run is not None and self.requests >= self.max_per_run:
            raise StopRun(f"Run budget reached ({self.max_per_run} requests)", "budget_run")
        if self.account.requests_today() >= self.max_per_day:
            raise StopRun(f"Daily budget reached ({self.max_per_day} requests today)", "budget_day")

    def _record(self, method: str, exc: Exception, action: str) -> None:
        client = self.session.client
        response = getattr(client, "last_response", None)
        self.account.log_event(
            method, exc,
            last_json=getattr(client, "last_json", None),
            status_code=getattr(response, "status_code", None),
            action=action,
        )

    def _stop_and_freeze(self, kind: str, method: str, exc: Exception, reason: str) -> StopRun:
        until = self.account.freeze(FREEZE_HOURS[kind], reason)
        self._record(method, exc, f"stop, frozen until {until}")
        return StopRun(f"{reason}. Account frozen until {until}.", "frozen")

    def call(self, fn: Callable[..., T], *args, **kwargs) -> T:
        method = getattr(fn, "__name__", "request")
        relogged = False
        throttle_attempt = 0
        network_attempt = 0
        while True:
            self._check_budget()
            time.sleep(random.uniform(self.config.delay_min, self.config.delay_max))
            self.requests += 1
            self.account.count_request()
            try:
                return fn(*args, **kwargs)

            except ChallengeRequired as exc:
                raise self._stop_and_freeze(
                    "challenge", method, exc,
                    "Instagram asked for verification. Open the account in the Instagram app and confirm it",
                ) from exc

            except FeedbackRequired as exc:
                last_json = getattr(self.session.client, "last_json", None) or {}
                message = last_json.get("feedback_message") or str(exc)
                raise self._stop_and_freeze("feedback", method, exc, f"Action blocked: {message}") from exc

            except LoginRequired as exc:
                if relogged:
                    self._record(method, exc, "stop")
                    raise StopRun("Session lost again after re-login. Check the account in the app", "session") from exc
                self._record(method, exc, "relogin")
                log.warning("Session dropped, logging in again with the same device")
                try:
                    self.session.relogin()
                except ClientError as relogin_exc:
                    self._record("relogin", relogin_exc, "stop")
                    raise StopRun(f"Re-login failed: {type(relogin_exc).__name__}", "session") from relogin_exc
                relogged = True

            except (PleaseWaitFewMinutes, RateLimitError) as exc:
                if self.please_wait_seen:
                    raise self._stop_and_freeze(
                        "please_wait", method, exc, f"Instagram asked to slow down twice ({type(exc).__name__})",
                    ) from exc
                self.please_wait_seen = True
                self._record(method, exc, f"cooldown {PLEASE_WAIT_COOLDOWN // 60} min")
                log.warning("Instagram asked to slow down, cooling down %d min", PLEASE_WAIT_COOLDOWN // 60)
                time.sleep(PLEASE_WAIT_COOLDOWN)

            except ClientThrottledError as exc:
                if throttle_attempt >= len(THROTTLE_BACKOFF):
                    self._record(method, exc, "stop")
                    raise StopRun(f"Still throttled (429) after {throttle_attempt} backoffs", "throttled") from exc
                wait = THROTTLE_BACKOFF[throttle_attempt]
                self._record(method, exc, f"backoff {wait // 60} min")
                log.warning("Throttled (429), waiting %d min", wait // 60)
                time.sleep(wait)
                throttle_attempt += 1

            except ClientConnectionError as exc:
                if network_attempt >= len(NETWORK_RETRIES):
                    self._record(method, exc, "stop")
                    raise StopRun(f"Network keeps failing: {exc}", "network") from exc
                wait = NETWORK_RETRIES[network_attempt]
                self._record(method, exc, f"retry in {wait}s")
                log.warning("Network error, retrying in %ds", wait)
                time.sleep(wait)
                network_attempt += 1

            except ClientError as exc:
                # Not a block (user not found, private account, ...): log it, let the pipeline decide.
                self._record(method, exc, "raised")
                raise
