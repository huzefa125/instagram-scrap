"""Random, role-aware turns for accounts on one worker server.

Among idle accounts that can do a kind of job, choose one with the fewest
Instagram requests so far in this worker run. Randomize ties. A turn only
reserves the next queue claim; it does not change the durable job lease.
"""

import random
import threading


class FairDistributor:
    def __init__(self, accounts: list, rng=None):
        self.accounts = {account.name: account for account in accounts}
        self.rng = rng or random.SystemRandom()
        self._condition = threading.Condition()
        self._ready: set[str] = set()
        self._busy: set[str] = set()
        self._reserved: dict[str, str] = {}

    def set_ready(self, name: str, ready: bool) -> None:
        with self._condition:
            if ready:
                self._ready.add(name)
            else:
                self._ready.discard(name)
                for kind, reserved in list(self._reserved.items()):
                    if reserved == name:
                        del self._reserved[kind]
            self._condition.notify_all()

    def try_acquire(self, name: str, kind: str) -> bool:
        """Reserve one claim for this account, if it is the selected account."""
        with self._condition:
            candidates = [account for account in self.accounts.values()
                          if account.name in self._ready and account.name not in self._busy
                          and account.role in (kind, "both")]
            if not candidates or name not in {account.name for account in candidates}:
                return False

            chosen = self._reserved.get(kind)
            if chosen not in {account.name for account in candidates}:
                least = min(account.requests() for account in candidates)
                tied = [account.name for account in candidates if account.requests() == least]
                chosen = self.rng.choice(tied)
                self._reserved[kind] = chosen
            if chosen != name:
                return False

            self._busy.add(name)
            del self._reserved[kind]
            self._condition.notify_all()
            return True

    def release(self, name: str) -> None:
        with self._condition:
            self._busy.discard(name)
            self._condition.notify_all()

    def wait(self, seconds: float) -> None:
        with self._condition:
            self._condition.wait(timeout=seconds)
