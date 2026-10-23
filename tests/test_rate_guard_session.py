import unittest
from types import SimpleNamespace
from unittest.mock import patch

from instagrapi.exceptions import ChallengeRequired, ClientUnauthorizedError

from scraper.accounts.rate_limit import RateGuard, StopRun


class FakeAccount:
    username = "acc"

    def __init__(self):
        self.count = 0
        self.events = []

    def effective_limit(self, max_per_day, config):
        return 100

    def requests_today(self):
        return self.count

    def count_request(self, n=1):
        self.count += n

    def require_manual_verification(self, reason):
        self.parked = reason

    def log_event(self, method, exc, last_json, status_code, action):
        self.events.append(action)


class UnauthorizedTest(unittest.TestCase):
    def test_401_relogs_in_once_then_stops_instead_of_burning_budget(self):
        config = SimpleNamespace(max_requests_per_day=100, max_requests_per_run=300, delay_min=0, delay_max=0,
                                 warmup_delay_min=0, warmup_delay_max=0)
        logins = []
        session = SimpleNamespace(client=SimpleNamespace(last_json={}, last_response=None),
                                  relogin=lambda: logins.append(1))
        account = FakeAccount()
        guard = RateGuard(config, session, account, max_per_day=100, max_per_run=None)
        calls = []

        def lookup():
            calls.append(1)
            raise ClientUnauthorizedError("401 login_required")

        with patch("scraper.accounts.rate_limit.time.sleep"):
            with self.assertRaises(StopRun) as ctx:
                guard.call(lookup)
        self.assertEqual(ctx.exception.kind, "session")
        self.assertEqual(len(logins), 1)
        self.assertEqual(len(calls), 2)       # first try + one try after re-login, nothing more
        self.assertEqual(account.count, 2)


class ChallengeTest(unittest.TestCase):
    def test_challenge_parks_account_for_manual_verification(self):
        config = SimpleNamespace(max_requests_per_day=100, max_requests_per_run=None, delay_min=0, delay_max=0,
                                 warmup_delay_min=0, warmup_delay_max=0)
        session = SimpleNamespace(client=SimpleNamespace(last_json={}, last_response=None))
        account = FakeAccount()
        guard = RateGuard(config, session, account, max_per_day=100, max_per_run=None)

        def blocked():
            raise ChallengeRequired()

        with patch("scraper.accounts.rate_limit.time.sleep"):
            with self.assertRaises(StopRun) as ctx:
                guard.call(blocked)
        self.assertEqual(ctx.exception.kind, "frozen")
        self.assertTrue(hasattr(account, "parked"))
        self.assertEqual(account.count, 1)     # no retry


class HttpCountTest(unittest.TestCase):
    def test_one_call_with_many_pages_uses_budget_per_http_request(self):
        config = SimpleNamespace(max_requests_per_day=100, max_requests_per_run=None, delay_min=0, delay_max=0,
                                 warmup_delay_min=0, warmup_delay_max=0)
        session = SimpleNamespace(client=SimpleNamespace(last_json={}, last_response=None), http_requests=0)
        account = FakeAccount()
        guard = RateGuard(config, session, account, max_per_day=100, max_per_run=None)

        def five_pages():
            session.http_requests += 5

        with patch("scraper.accounts.rate_limit.time.sleep"):
            guard.call(five_pages)
        self.assertEqual(account.count, 5)


if __name__ == "__main__":
    unittest.main()
