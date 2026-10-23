import random
import unittest
from datetime import date

from scraper.accounts.activity import ActivitySettings, make_daily_actions, validate_comment
from datetime import time


def settings():
    mix = {"profile": 1, "reel_check": 0, "like": 0, "comment": 0, "feed_watch": 3, "feed_comment": 1}
    return ActivitySettings(5, mix, time(9), time(21), 30, ["nasa"], [], [], {})


class FeedActivityTest(unittest.TestCase):
    def test_feed_actions_need_no_configured_targets(self):
        actions = make_daily_actions(settings(), "acc", date(2026, 10, 21), 0, rng=random.Random(1))
        kinds = sorted(a["kind"] for a in actions)
        self.assertEqual(kinds, ["feed_comment", "feed_watch", "feed_watch", "feed_watch", "profile"])

    def test_comment_validation(self):
        self.assertEqual(validate_comment("  Great edit!  "), "Great edit!")
        for bad in ("", "x" * 151, "see https://x.co", "thanks @someone"):
            with self.assertRaises(ValueError):
                validate_comment(bad)


if __name__ == "__main__":
    unittest.main()
