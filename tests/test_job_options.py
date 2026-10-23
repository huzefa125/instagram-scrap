import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from scraper.collection.pipeline import InfluencerWorker
from scraper.jobs.queue import WorkQueue
from scraper.storage.store import JsonStore


class JobOptionsTest(unittest.TestCase):
    def test_per_request_numbers_override_env_defaults(self):
        with TemporaryDirectory() as root:
            store = JsonStore(Path(root))
            WorkQueue(store).enqueue(["a"], options={"enrich_profiles": 200, "likers": 50, "posts": None})
            WorkQueue(store).enqueue(["b"])
            config = SimpleNamespace(audience_profiles_per_influencer=300, likers_per_post=10)
            worker = InfluencerWorker.__new__(InfluencerWorker)
            worker.store, worker.config = store, config
            self.assertEqual(worker._enrich_cap("a"), 200)
            self.assertEqual(worker._option("a", "likers", config.likers_per_post), 50)
            self.assertEqual(worker._option("a", "posts", 7), 7)          # not given: default
            self.assertEqual(worker._enrich_cap("b"), 300)                # no options: .env default

    def test_zero_means_everyone_and_options_update_while_still_queued(self):
        with TemporaryDirectory() as root:
            store = JsonStore(Path(root))
            queue = WorkQueue(store)
            queue.enqueue(["a"])
            queue.enqueue(["a"], options={"enrich_profiles": 0})
            worker = InfluencerWorker.__new__(InfluencerWorker)
            worker.store, worker.config = store, SimpleNamespace(audience_profiles_per_influencer=300)
            self.assertEqual(worker._enrich_cap("a"), 0)


class PrivateSkippedTest(unittest.TestCase):
    def test_private_engagers_are_never_queued_for_lookup(self):
        with TemporaryDirectory() as root:
            store = JsonStore(Path(root))
            WorkQueue(store).enqueue(["a"], options={"enrich_profiles": 0})
            worker = InfluencerWorker.__new__(InfluencerWorker)
            worker.store, worker.config = store, SimpleNamespace(audience_profiles_per_influencer=0)
            worker._record_audience({"pk": 1, "username": "open", "is_private": False}, "a", "like")
            worker._record_audience({"pk": 2, "username": "closed", "is_private": True}, "a", "like")
            self.assertTrue(store.get("audience", "1")["wanted"])
            self.assertFalse(store.get("audience", "2")["wanted"])

    def test_sample_picks_only_public_engagers(self):
        with TemporaryDirectory() as root:
            store = JsonStore(Path(root))
            WorkQueue(store).enqueue(["a"], options={"enrich_profiles": 10})
            worker = InfluencerWorker.__new__(InfluencerWorker)
            worker.store, worker.config = store, SimpleNamespace(audience_profiles_per_influencer=10)
            for pk in range(1, 7):
                worker._record_audience({"pk": pk, "username": f"u{pk}", "is_private": pk % 2 == 0}, "a", "like")
            worker._mark_sample("a", [str(pk) for pk in range(1, 7)])
            wanted = sorted(d for d in "123456" if store.get("audience", d)["wanted"])
            self.assertEqual(wanted, ["1", "3", "5"])


if __name__ == "__main__":
    unittest.main()
