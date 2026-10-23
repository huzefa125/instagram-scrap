"""Fair request allocation without Instagram or MongoDB access."""

import random
import time
import unittest
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from scraper.jobs.distribution import FairDistributor
from scraper.jobs.worker import Account, Worker
from scraper.storage.store import JsonStore


@dataclass
class FakeAccount:
    name: str
    role: str
    count: int = 0

    def requests(self) -> int:
        return self.count


class DistributionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.accounts = [FakeAccount("engagement", "engagement")]
        self.accounts += [FakeAccount(f"audience-{i}", "audience") for i in range(4)]
        self.distributor = FairDistributor(self.accounts, rng=random.Random(7))
        for account in self.accounts:
            self.distributor.set_ready(account.name, True)

    def assign(self, kind: str, times: int) -> list[str]:
        selected = []
        order = list(self.accounts)
        rng = random.Random(21)
        for _ in range(times):
            rng.shuffle(order)
            for account in order:
                if self.distributor.try_acquire(account.name, kind):
                    account.count += 1
                    selected.append(account.name)
                    self.distributor.release(account.name)
                    break
            else:
                self.fail("No eligible account received the next turn")
        return selected

    def test_400_audience_requests_are_random_and_equal(self) -> None:
        selected = self.assign("audience", 400)
        self.assertEqual([account.count for account in self.accounts], [0, 100, 100, 100, 100])
        self.assertGreater(len(set(selected[:4])), 1)

    def test_engagement_stays_with_engagement_account(self) -> None:
        self.assertEqual(set(self.assign("engagement", 20)), {"engagement"})

    def test_all_five_can_share_when_roles_are_both(self) -> None:
        for account in self.accounts:
            account.role = "both"
        self.assign("engagement", 400)
        self.assertEqual([account.count for account in self.accounts], [80] * 5)

    def test_job_costs_balance_by_requests(self) -> None:
        order = self.accounts[1:]
        for job in range(240):
            for account in order:
                if self.distributor.try_acquire(account.name, "audience"):
                    account.count += 1 + (job % 2)
                    self.distributor.release(account.name)
                    break
            else:
                self.fail("No eligible account received the next turn")
        counts = [account.count for account in self.accounts[1:]]
        self.assertEqual(sum(counts), 360)
        self.assertLessEqual(max(counts) - min(counts), 2)

    def test_unavailable_account_is_excluded(self) -> None:
        self.distributor.set_ready("audience-0", False)
        self.assign("audience", 400)
        self.assertEqual(self.accounts[1].count, 0)
        self.assertEqual(sorted(account.count for account in self.accounts[2:]), [133, 133, 134])

    def test_busy_account_is_excluded(self) -> None:
        self.assertTrue(any(self.distributor.try_acquire(account.name, "audience")
                            for account in self.accounts[1:]))
        busy = next(iter(self.distributor._busy))
        self.assertFalse(self.distributor.try_acquire(busy, "audience"))
        self.distributor.release(busy)


class WorkerDistributionTests(unittest.TestCase):
    def test_worker_splits_audience_lookups(self) -> None:
        class FakeState:
            def __init__(self, collector):
                self.collector = collector

            def frozen(self):
                return False, None, None

            def effective_limit(self, max_per_day, config):
                return max_per_day

            def requests_today(self):
                return self.collector.count

            def renew_lock(self, owner, ttl):
                pass

            def release_lock(self, owner):
                pass

        class FakeCollector:
            def __init__(self):
                self.count = 0

            def public_profile(self, user_id):
                time.sleep(0.001)
                self.count += 1
                return {"pk": user_id, "is_private": False, "media_count": 0}

        with TemporaryDirectory() as root:
            store = JsonStore(Path(root))
            for user_id in range(80):
                store.upsert("audience", str(user_id), {
                    "wanted": True, "enriched": False, "lease_until": None,
                })
            collectors = [FakeCollector() for _ in range(4)]
            accounts = [Account(f"audience-{i}", "audience", collector, FakeState(collector),
                                requests=lambda c=collector: c.count, max_per_day=1000)
                        for i, collector in enumerate(collectors)]
            config = SimpleNamespace(poll_seconds=0.01, audience_recent_posts=0)
            result = Worker(config, store, accounts, "test-server", until_empty=True,
                            poll_seconds=0.01).run()

            counts = [collector.count for collector in collectors]
            self.assertEqual(sum(counts), 80)
            self.assertLessEqual(max(counts) - min(counts), 2)
            self.assertEqual(result["audience_left"], 0)


if __name__ == "__main__":
    unittest.main()
