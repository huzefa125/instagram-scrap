"""Offline tests for daily activity scheduling and execution."""

import random
import unittest
from collections import Counter
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from scraper.accounts.activity import (
    ActivityRunner, ActivitySettings, load_activity_settings, make_daily_actions,
)
from scraper.core.config import _numbered_env_accounts
from scraper.storage.store import JsonStore


def settings() -> ActivitySettings:
    reel = lambda code: f"https://www.instagram.com/reel/{code}/"
    return ActivitySettings(
        5, {"profile": 1, "reel_check": 2, "like": 1, "comment": 1},
        time(9), time(21), 30, ["profile_one"],
        [reel("watchA"), reel("watchB")], [reel("likeA"), reel("likeB")],
        {"account_a": [{"reel_url": reel("commentA"), "text": "Nice work!"}]},
    )


class ActivityTests(unittest.TestCase):
    def test_five_randomly_timed_actions_per_account(self) -> None:
        day = date(2026, 10, 20)
        plan = make_daily_actions(settings(), "account_a", day, 19800,
                                  rng=random.Random(11))
        self.assertEqual(len(plan), 5)
        self.assertEqual(Counter(action["kind"] for action in plan), settings().daily_mix)
        due = [datetime.fromisoformat(action["due_at"]) for action in plan]
        self.assertEqual(due, sorted(due))
        self.assertTrue(all(b - a >= timedelta(minutes=30) for a, b in zip(due, due[1:])))
        self.assertTrue(all(datetime(2026, 10, 20, 9, tzinfo=timezone(timedelta(hours=5, minutes=30)))
                            <= when.astimezone(timezone(timedelta(hours=5, minutes=30)))
                            < datetime(2026, 10, 20, 21, tzinfo=timezone(timedelta(hours=5, minutes=30)))
                            for when in due))
        second = make_daily_actions(settings(), "account_a", day, 19800,
                                    rng=random.Random(12))
        self.assertNotEqual([a["due_at"] for a in plan], [a["due_at"] for a in second])

    def test_write_targets_are_not_reused(self) -> None:
        with self.assertRaisesRegex(ValueError, "unused like targets"):
            make_daily_actions(settings(), "account_a", date(2026, 10, 20), 0,
                               used={("like", "likeA"), ("like", "likeB")})

    def test_account_needs_approved_comment(self) -> None:
        with self.assertRaisesRegex(ValueError, "unused comment targets"):
            make_daily_actions(settings(), "account_b", date(2026, 10, 20), 0)

    def test_failed_plan_does_not_disable_future_days(self) -> None:
        empty = ActivitySettings(5, settings().daily_mix, time(9), time(21), 30,
                                 [], [], [], {})
        with TemporaryDirectory() as root:
            runner = ActivityRunner(JsonStore(Path(root)), empty, 0)
            now = datetime(2026, 10, 20, 10, tzinfo=timezone.utc)
            self.assertIsNone(runner._plan("account_a", now.date(), now))
            self.assertIn(("account_a", now.date()), runner.invalid_plans)
            self.assertIsNone(runner._plan("account_a", now.date() + timedelta(days=1),
                                           now + timedelta(days=1)))
            self.assertIn(("account_a", now.date() + timedelta(days=1)), runner.invalid_plans)

    def test_execution_uses_guard_and_only_approved_text(self) -> None:
        calls = []

        class Guard:
            def call(self, fn, *args):
                calls.append((fn.__name__, args))
                return fn(*args)

        class Client:
            def get_timeline_feed(self, reason, max_id):
                return {"next_max_id": "m"}

            def search_users(self, query):
                return []

            def user_info_by_username(self, name):
                return name

            def media_pk_from_code(self, code):
                return code

            def media_info(self, code):
                return SimpleNamespace(product_type="clips", id=code)

            def media_like(self, media_id):
                return True

            def media_comment(self, media_id, text):
                return SimpleNamespace(id="comment-1")

        acc = SimpleNamespace(collector=SimpleNamespace(client=Client(), guard=Guard()))
        reel = "https://www.instagram.com/reel/approved/"
        with patch("scraper.accounts.activity._time.sleep"):
            ActivityRunner._execute(acc, {"kind": "profile", "target": "profile_one"})
        names = [name for name, _ in calls]
        self.assertEqual(names[-2:], ["search_users", "user_info_by_username"])   # opened from search, never first
        self.assertTrue(set(names[:-2]) <= {"get_timeline_feed", "search_users"})
        self.assertLessEqual(names.count("get_timeline_feed"), 10)
        calls.clear()
        ActivityRunner._execute(acc, {"kind": "reel_check", "target": reel})
        ActivityRunner._execute(acc, {"kind": "like", "target": reel})
        ActivityRunner._execute(acc, {"kind": "comment", "target": reel, "text": "Nice work!"})
        self.assertEqual([name for name, _ in calls], [
            "media_info", "media_info", "media_like",
            "media_info", "media_comment",
        ])
        self.assertEqual(calls[-1][1], ("approved", "Nice work!"))
        self.assertNotIn("media_seen", [name for name, _ in calls])

    def test_example_needs_real_targets_before_use(self) -> None:
        example = Path(__file__).resolve().parent.parent / "activity.example.json"
        loaded = load_activity_settings(example)
        with self.assertRaisesRegex(ValueError, "unused profile targets"):
            make_daily_actions(loaded, "account_a", date(2026, 10, 20), 0)

    def test_numbered_accounts_can_exceed_200(self) -> None:
        with patch.dict("os.environ", {
            "IG_USERNAME_1": "first", "IG_PASSWORD_1": "test",
            "IG_USERNAME_201": "later", "IG_PASSWORD_201": "test",
        }, clear=True):
            self.assertEqual([account.username for account in _numbered_env_accounts()],
                             ["first", "later"])

    def test_due_like_is_persisted_and_not_replayed(self) -> None:
        class Guard:
            def call(self, fn, *args):
                return fn(*args)

        class Client:
            def __init__(self):
                self.likes = 0

            def media_pk_from_code(self, code):
                return code

            def media_info(self, code):
                return SimpleNamespace(product_type="clips", id=code)

            def media_like(self, media_id):
                self.likes += 1
                return True

        client = Client()
        account = SimpleNamespace(name="account_a", collector=SimpleNamespace(client=client, guard=Guard()))
        today = datetime.now(timezone.utc).date()
        plan_id = f"account_a:{today.isoformat()}"
        url = "https://www.instagram.com/reel/likeA/"
        action = {"kind": "like", "target": url, "status": "queued",
                  "due_at": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()}
        with TemporaryDirectory() as root:
            store = JsonStore(Path(root))
            store.upsert("activity_plans", plan_id, {"actions": [action], "last_executed_at": None})
            one = replace(settings(), actions_per_account=1,
                          daily_mix={"profile": 0, "reel_check": 0, "like": 1, "comment": 0},
                          window_start=time.min, window_end=time.max, min_gap_minutes=0)
            runner = ActivityRunner(store, one, 0)
            self.assertTrue(runner.process_due(account))
            self.assertFalse(runner.process_due(account))
            self.assertEqual(client.likes, 1)
            self.assertEqual(store.get("activity_plans", plan_id)["actions"][0]["status"], "done")
            self.assertIsNotNone(store.get("activity_used", "account_a:like:likeA"))


if __name__ == "__main__":
    unittest.main()
