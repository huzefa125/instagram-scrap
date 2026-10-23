"""Daily account activity plans with explicit targets and approved comments.

Plans are stored per account and day. The existing account worker executes one due
action at a time through its RateGuard, so activity shares the same session,
proxy, lock, and request budget as collection work.
"""

import json
import logging
import random
import re
import time as _time
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

from ..observability import span
from .rate_limit import StopRun
from .stream import stream_reel
from .viewer import NotLoggedIn

log = logging.getLogger(__name__)
KINDS = ("profile", "reel_check", "like", "comment", "feed_watch", "feed_comment")
FEED_KINDS = {"feed_watch", "feed_comment"}
WRITE_KINDS = {"like", "comment", "feed_comment"}
FEED_REELS_PER_WATCH = 6                # default; each feed_watch picks a random count in this range
FEED_REELS_RANGE = (4, 9)
FEED_SCROLLS_RANGE = (0, 10)           # home-feed scrolls before searching for a profile; 0 is allowed
SCROLL_PAUSE = (2.0, 9.0)              # seconds spent on each feed screen
PROFILE_VISIT_CHANCE = (0.0, 0.4)      # each watch draws its own chance in this range (about 2 in 10 on average)
MAX_COMMENT_LENGTH = 150
LINK_OR_MENTION = re.compile(r"https?://|www\.|@\w", re.IGNORECASE)


class NothingToDo(Exception):
    """An action had no eligible target (for example, no approved feed comment)."""


def validate_comment(text: str) -> str:
    text = text.strip()
    if not text or len(text) > MAX_COMMENT_LENGTH or LINK_OR_MENTION.search(text):
        raise ValueError(f"Comment must be 1-{MAX_COMMENT_LENGTH} characters with no links or @mentions")
    return text
USERNAME = re.compile(r"^[A-Za-z0-9._]+$")
REEL_CODE = re.compile(r"^[A-Za-z0-9_-]+$")


def reel_code(url: str) -> str:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if (parsed.scheme != "https" or parsed.hostname not in ("instagram.com", "www.instagram.com")
            or len(parts) != 2 or parts[0] != "reel" or not REEL_CODE.fullmatch(parts[1])):
        raise ValueError(f"Expected an https://www.instagram.com/reel/CODE/ URL: {url}")
    return parts[1]


def _clock(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid local time {value!r}; use HH:MM") from exc


@dataclass(frozen=True)
class ActivitySettings:
    actions_per_account: int
    daily_mix: dict[str, int]
    window_start: time
    window_end: time
    min_gap_minutes: int
    profiles: list[str]
    reels: list[str]
    likes: list[str]
    comments_by_account: dict[str, list[dict[str, str]]]


def load_activity_settings(path: Path) -> ActivitySettings:
    if not path.exists():
        raise ValueError(f"Activity file not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    mix = {kind: int(raw.get("daily_mix", {}).get(kind, 0)) for kind in KINDS}
    count = int(raw.get("actions_per_account", 5))
    if count < 1 or sum(mix.values()) != count or any(value < 0 for value in mix.values()):
        raise ValueError("daily_mix must contain nonnegative counts summing to actions_per_account")
    start = _clock(raw.get("window_start", "09:00"))
    end = _clock(raw.get("window_end", "21:00"))
    if start >= end:
        raise ValueError("window_end must be after window_start on the same day")
    gap = int(raw.get("min_gap_minutes", 30))
    if gap < 0:
        raise ValueError("min_gap_minutes must be nonnegative")

    profiles = [str(value).lstrip("@").strip() for value in raw.get("profiles", [])]
    if any(not USERNAME.fullmatch(value) for value in profiles):
        raise ValueError("profiles must contain Instagram usernames")
    reels = [str(value).strip() for value in raw.get("reels", [])]
    likes = [str(value).strip() for value in raw.get("likes", [])]
    for url in reels + likes:
        reel_code(url)
    comments = raw.get("comments_by_account", {})
    if not isinstance(comments, dict):
        raise ValueError("comments_by_account must map each account username to approved comments")
    for account, items in comments.items():
        if not isinstance(items, list):
            raise ValueError(f"comments_by_account[{account}] must be a list")
        for item in items:
            if not isinstance(item, dict) or not isinstance(item.get("reel_url"), str):
                raise ValueError(f"Every comment for {account} needs a reel_url")
            reel_code(item["reel_url"])
            if not isinstance(item.get("text"), str) or not item["text"].strip():
                raise ValueError(f"Every comment for {account} needs approved text")
    return ActivitySettings(count, mix, start, end, gap, profiles, reels, likes, comments)


def make_daily_actions(settings: ActivitySettings, account: str, day: date, offset_seconds: int,
                       used: set[tuple[str, str]] | None = None, rng=None,
                       now: datetime | None = None) -> list[dict]:
    """Choose targets and random times; this function makes no Instagram calls."""
    used = used or set()
    rng = rng or random.SystemRandom()
    local_tz = timezone(timedelta(seconds=offset_seconds))
    start = datetime.combine(day, settings.window_start, local_tz)
    end = datetime.combine(day, settings.window_end, local_tz)
    if now is not None and now.astimezone(local_tz).date() == day:
        start = max(start, now.astimezone(local_tz) + timedelta(minutes=5))
    if end - start < timedelta(minutes=settings.min_gap_minutes * (settings.actions_per_account - 1)):
        raise ValueError("Not enough time remains in today's activity window")

    choices: list[dict] = []
    pools = {
        "profile": [{"kind": "profile", "target": name} for name in dict.fromkeys(settings.profiles)],
        "reel_check": [{"kind": "reel_check", "target": url} for url in dict.fromkeys(settings.reels)],
        "like": [{"kind": "like", "target": url} for url in dict.fromkeys(settings.likes)
                 if ("like", reel_code(url)) not in used],
        "feed_watch": [{"kind": "feed_watch", "target": "explore"}],
        "feed_comment": [{"kind": "feed_comment", "target": "approved-candidate"}],
        "comment": [{"kind": "comment", "target": item["reel_url"], "text": item["text"].strip()}
                    for item in settings.comments_by_account.get(account, [])
                    if ("comment", reel_code(item["reel_url"])) not in used],
    }
    for kind, amount in settings.daily_mix.items():
        candidates = pools[kind]
        if kind in FEED_KINDS:
            choices.extend(dict(candidates[0]) for _ in range(amount))
            continue
        if amount > len(candidates):
            raise ValueError(f"{account}: need {amount} unused {kind} targets, have {len(candidates)}")
        choices.extend(rng.sample(candidates, amount))
    rng.shuffle(choices)

    # Sample action times, then space nearby samples by at least the configured gap.
    total = (end - start).total_seconds()
    gap_seconds = settings.min_gap_minutes * 60
    slack = total - gap_seconds * (len(choices) - 1)
    offsets = sorted(rng.uniform(0, slack) for _ in choices)
    for index, (action, offset) in enumerate(zip(choices, offsets)):
        due = start + timedelta(seconds=offset + index * gap_seconds)
        action["due_at"] = due.astimezone(timezone.utc).isoformat()
        action["status"] = "queued"
    return choices


class ActivityRunner:
    def __init__(self, store, settings: ActivitySettings, offset_seconds: int, viewer_factory=None):
        self.store = store
        self.viewer_factory = viewer_factory   # account name -> a viewer (browser or phone), or None for API-only
        self.settings = settings
        self.offset_seconds = offset_seconds
        self.local_tz = timezone(timedelta(seconds=offset_seconds))
        self.invalid_plans: set[tuple[str, date]] = set()

    def _plan(self, account: str, day: date, now: datetime) -> dict | None:
        plan_id = f"{account}:{day.isoformat()}"
        existing = self.store.get("activity_plans", plan_id)
        if existing is not None:
            return existing
        if (account, day) in self.invalid_plans:
            return None
        used = set()
        for kind, urls in (("like", self.settings.likes),
                           ("comment", [item["reel_url"] for item in
                                        self.settings.comments_by_account.get(account, [])])):
            used.update((kind, reel_code(url)) for url in urls
                        if self.store.get("activity_used", f"{account}:{kind}:{reel_code(url)}"))
        try:
            actions = make_daily_actions(self.settings, account, day, self.offset_seconds, used, now=now)
        except ValueError as exc:
            log.error("Activity plan for %s unavailable: %s", account, exc)
            self.invalid_plans.add((account, day))
            return None
        self.store.insert_if_missing("activity_plans", plan_id, {
            "account": account, "day": day.isoformat(), "actions": actions,
            "created_at": now.isoformat(), "last_executed_at": None,
        })
        return self.store.get("activity_plans", plan_id)

    def process_due(self, acc) -> bool:
        """Run at most one due action. Returns True when an action was attempted."""
        now = datetime.now(timezone.utc)
        local = now.astimezone(self.local_tz)
        if not self.settings.window_start <= local.time() < self.settings.window_end:
            return False
        plan = self._plan(acc.name, local.date(), now)
        if plan is None:
            return False
        last = plan.get("last_executed_at")
        if last and now - datetime.fromisoformat(last) < timedelta(minutes=self.settings.min_gap_minutes):
            return False

        actions = plan["actions"]
        changed = False
        for action in actions:
            if action["status"] == "in_progress" and action.get("started_at"):
                if now - datetime.fromisoformat(action["started_at"]) >= timedelta(minutes=30):
                    action["status"] = "uncertain" if action["kind"] in WRITE_KINDS else "queued"
                    changed = True
        if changed:
            self.store.upsert("activity_plans", plan["_id"], {"actions": actions})

        action = next((item for item in actions
                       if item["status"] == "queued" and item["due_at"] <= now.isoformat()), None)
        if action is None:
            return False
        action["status"] = "in_progress"
        action["started_at"] = now.isoformat()
        self.store.upsert("activity_plans", plan["_id"], {"actions": actions})
        try:
            with span(f"activity.{action['kind']}", acc.name):
                if action["kind"] == "feed_watch":
                    self._feed_watch(acc, acc.collector.client, acc.collector.guard)
                elif action["kind"] == "feed_comment":
                    self._feed_comment(acc, acc.collector.client, acc.collector.guard, action)
                else:
                    self._execute(acc, action)
        except StopRun:
            action["status"] = "uncertain" if action["kind"] in WRITE_KINDS else "queued"
            self.store.upsert("activity_plans", plan["_id"], {"actions": actions})
            raise
        except NothingToDo as exc:
            action["status"] = "skipped"
            action["error"] = str(exc)[:200]
            log.info("[%s] activity %s skipped: %s", acc.name, action["kind"], exc)
        except Exception as exc:  # noqa: BLE001 - one activity must not kill the account worker
            action["status"] = "uncertain" if action["kind"] in WRITE_KINDS else "failed"
            action["error"] = f"{type(exc).__name__}: {exc}"[:200]
            log.exception("[%s] activity %s failed", acc.name, action["kind"])
        else:
            action["status"] = "done"
            if action["kind"] in WRITE_KINDS and action["kind"] not in FEED_KINDS:
                code = reel_code(action["target"])
                self.store.insert_if_missing("activity_used", f"{acc.name}:{action['kind']}:{code}", {
                    "account": acc.name, "kind": action["kind"], "reel_url": action["target"],
                    "done_at": datetime.now(timezone.utc).isoformat(),
                })
            log.info("[%s] activity %s complete: %s", acc.name, action["kind"], action["target"])
        self.store.upsert("activity_plans", plan["_id"], {
            "actions": actions, "last_executed_at": datetime.now(timezone.utc).isoformat(),
        })
        return True

    @staticmethod
    def _execute(acc, action: dict) -> None:
        client = acc.collector.client
        guard = acc.collector.guard
        if action["kind"] == "profile":
            ActivityRunner._browse_to_profile(client, guard, action["target"])
            return

        code = reel_code(action["target"])
        media = guard.call(client.media_info, client.media_pk_from_code(code))
        if media.product_type != "clips":
            raise ValueError(f"Target is not a Reel: {action['target']}")
        if action["kind"] == "like":
            if guard.call(client.media_like, str(media.id)) is not True:
                raise ValueError("Instagram did not confirm the like")
        elif action["kind"] == "comment":
            comment = guard.call(client.media_comment, str(media.id), action["text"])
            if comment is None:
                raise ValueError("Instagram did not confirm the comment")
        # reel_check fetches Reel metadata only; it does not claim playback or a view.

    @staticmethod
    def _browse_to_profile(client, guard, target: str) -> None:
        """Reach a profile the way a person does: scroll the home feed 0-10 times, search for the
        name (sometimes typing a shorter prefix first), then open it from the results."""
        rng = random.SystemRandom()
        max_id = None
        for i in range(rng.randint(*FEED_SCROLLS_RANGE)):
            try:
                page = guard.call(client.get_timeline_feed, "cold_start_fetch" if i == 0 else "pagination", max_id)
                max_id = (page or {}).get("next_max_id")
            except StopRun:
                raise
            except Exception as exc:  # noqa: BLE001 - a feed hiccup must not stop the visit
                log.info("Feed scroll skipped: %s", type(exc).__name__)
                break
            _time.sleep(rng.uniform(*SCROLL_PAUSE))
        if len(target) > 4 and rng.random() < 0.5:
            try:
                guard.call(client.search_users, target[:rng.randint(3, len(target) - 1)])
            except StopRun:
                raise
            except Exception as exc:  # noqa: BLE001
                log.info("Partial search skipped: %s", type(exc).__name__)
            _time.sleep(rng.uniform(1.5, 5.0))
        guard.call(client.search_users, target)
        _time.sleep(rng.uniform(1.5, 5.0))
        guard.call(client.user_info_by_username, target)

    def _feed_watch(self, acc, client, guard) -> None:
        """Watch feed Reels and queue them as candidates for human review.

        With a browser or phone viewer the Reels are really played; otherwise a few Explore Reels
        are only marked as seen through the API (no playback).
        """
        viewer = self.viewer_factory(acc.name) if self.viewer_factory else None
        reel_count = random.SystemRandom().randint(*FEED_REELS_RANGE)
        if viewer is not None:
            # A logged-out profile or app is never retried automatically: repeated login
            # screens look suspicious. A person logs in once with `view --login`.
            if (self.store.get("account_state", acc.name) or {}).get("browser_needs_login"):
                raise NothingToDo("Viewer needs a manual login (python -m scraper view --account NAME --login [--mobile])")
            try:
                watched = viewer.watch(reel_count)
            except NotLoggedIn as exc:
                self.store.upsert("account_state", acc.name, {"browser_needs_login": True})
                raise NothingToDo(str(exc)) from exc
            found = [{"code": r.code, "author": r.author, "caption": r.caption,
                      "like_count": None, "comment_count": None, "watched_seconds": r.watched_seconds}
                     for r in watched if r.code]   # app viewers expose no Reel link: watch only
        else:
            medias = guard.call(client.explore_reels, reel_count)
            reels = [m for m in medias if m.product_type == "clips"]
            if not reels:
                raise NothingToDo("Explore returned no Reels")
            streamed: dict[str, dict] = {}
            for m in reels:
                try:   # real video bytes over the account's proxy, at playback speed
                    streamed[str(m.pk)] = stream_reel(str(m.video_url or ""), m.video_duration,
                                                      getattr(client, "proxy", "") or "")
                except Exception as exc:  # noqa: BLE001 - a CDN hiccup must not fail the whole watch
                    log.info("[%s] could not stream %s: %s", acc.name, m.code, type(exc).__name__)
            guard.call(client.clip_seen, [str(m.pk) for m in reels if str(m.pk) in streamed] or
                       [str(m.pk) for m in reels])
            found = [{"code": m.code, "author": m.user.username, "caption": (m.caption_text or "")[:500],
                      "like_count": m.like_count, "comment_count": m.comment_count,
                      "watched_seconds": streamed.get(str(m.pk), {}).get("seconds"),
                      "watched_fraction": streamed.get(str(m.pk), {}).get("fraction"),
                      "playback": "api-stream" if str(m.pk) in streamed else None}
                     for m in reels]
        for item in found:
            self.store.insert_if_missing("reel_candidates", f"{acc.name}:{item['code']}", {
                **item, "account": acc.name, "reel_url": f"https://www.instagram.com/reel/{item['code']}/",
                "status": "pending", "comment": None,
                "found_at": datetime.now(timezone.utc).isoformat(),
            })
        self._visit_authors(guard, client, found)

    @staticmethod
    def _visit_authors(guard, client, found: list[dict]) -> None:
        """Look up some Reel authors' profiles (no follow). The chance is drawn fresh for every watch."""
        rng = random.SystemRandom()
        chance = rng.uniform(*PROFILE_VISIT_CHANCE)
        authors = list(dict.fromkeys(item["author"] for item in found if item.get("author")))
        for author in authors:
            if rng.random() >= chance:
                continue
            try:
                guard.call(client.user_info_by_username, author)
            except StopRun:
                raise
            except Exception as exc:  # noqa: BLE001 - a missing or private author must not fail the watch
                log.info("Profile visit to %s skipped: %s", author, type(exc).__name__)

    def _feed_comment(self, acc, client, guard, action: dict) -> None:
        """Post one human-approved comment on a reel found in this account's feed."""
        candidate = next((c for c in self.store.find("reel_candidates")
                          if c["account"] == acc.name and c["status"] == "approved"), None)
        if candidate is None:
            raise NothingToDo("No approved feed comment waiting")
        text = validate_comment(candidate["comment"] or "")
        action["target"] = candidate["reel_url"]
        self.store.upsert("reel_candidates", candidate["_id"], {"status": "posting"})
        try:
            media = guard.call(client.media_info, client.media_pk_from_code(candidate["code"]))
            comment = guard.call(client.media_comment, str(media.id), text)
            if comment is None:
                raise ValueError("Instagram did not confirm the comment")
        except StopRun:
            self.store.upsert("reel_candidates", candidate["_id"], {"status": "uncertain"})
            raise
        except Exception:
            self.store.upsert("reel_candidates", candidate["_id"], {"status": "uncertain"})
            raise
        self.store.upsert("reel_candidates", candidate["_id"], {
            "status": "posted", "posted_at": datetime.now(timezone.utc).isoformat()})
