"""Offline tests for proxy health and tracing."""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import requests

from scraper.accounts.proxy import ProxyMonitor
from scraper.observability import tracing
from scraper.storage.store import JsonStore


class FakeProber:
    def __init__(self, *results):
        self.results = list(results)

    def __call__(self, proxy):
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result, 40


class ProxyMonitorTests(unittest.TestCase):
    def monitor(self, tmp, *results, proxies=None):
        store = JsonStore(Path(tmp))
        proxies = {"acc": "http://u:p@1.2.3.4:80"} if proxies is None else proxies
        return ProxyMonitor(store, proxies, every_minutes=30, prober=FakeProber(*results)), store

    def test_first_success_pins_expected_ip(self):
        with TemporaryDirectory() as tmp:
            monitor, store = self.monitor(tmp, "9.9.9.9")
            self.assertTrue(monitor.healthy("acc"))
            self.assertEqual(store.get("proxy_health", "acc")["expected_ip"], "9.9.9.9")

    def test_ip_change_holds_account_until_accepted(self):
        with TemporaryDirectory() as tmp:
            monitor, _ = self.monitor(tmp, "9.9.9.9", "8.8.8.8", "8.8.8.8")
            monitor.check("acc", force=True)
            self.assertFalse(monitor.check("acc", force=True)["ok"])
            self.assertTrue(monitor.accept_ip("acc")["ok"])

    def test_down_only_after_two_failures(self):
        with TemporaryDirectory() as tmp:
            error = requests.ConnectionError("x")
            monitor, _ = self.monitor(tmp, "9.9.9.9", error, error, "9.9.9.9")
            monitor.check("acc", force=True)
            self.assertTrue(monitor.check("acc", force=True)["ok"])     # one blip is tolerated
            self.assertFalse(monitor.check("acc", force=True)["ok"])    # second failure pauses it
            self.assertTrue(monitor.check("acc", force=True)["ok"])     # recovers on its own

    def test_checks_are_rate_limited_and_no_proxy_is_fine(self):
        with TemporaryDirectory() as tmp:
            monitor, _ = self.monitor(tmp, "9.9.9.9")
            monitor.check("acc", force=True)
            monitor.check("acc")  # within 30 minutes: would raise IndexError if it probed again
            self.assertTrue(monitor.healthy("other"))


class TracingTests(unittest.TestCase):
    def tearDown(self):
        tracing._dir = None

    def test_noop_until_configured(self):
        with tracing.span("x"):
            pass

    def test_spans_nest_and_record_errors(self):
        with TemporaryDirectory() as tmp:
            tracing.configure(Path(tmp))
            with tracing.span("activity.like", "acc"):
                with tracing.span("ig.media_info", "acc"):
                    pass
                with self.assertRaises(ValueError):
                    with tracing.span("ig.media_like", "acc"):
                        raise ValueError("boom")
            spans = tracing.read_spans(Path(tmp), account="acc")
            self.assertEqual(len(spans), 3)
            self.assertEqual(len({s["trace"] for s in spans}), 1)
            parent = next(s for s in spans if s["name"] == "activity.like")
            self.assertEqual({s["parent"] for s in spans if s is not parent}, {parent["span"]})
            summary = tracing.summarize(spans)
            self.assertEqual(summary["ig.media_like"]["errors"], 1)
            self.assertEqual(summary["ig.media_info"]["errors"], 0)


if __name__ == "__main__":
    unittest.main()
