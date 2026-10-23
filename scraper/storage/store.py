"""Storage layer. MongoDB when MONGO_URI is set (e.g. MongoDB Atlas), JSON files otherwise.

Collections:
  influencers    one doc per influencer (profile, post_ids, audience_ids)
  posts          one doc per influencer post, with liker ids and comments
  audience       one doc per unique engaging user (deduplicated across influencers)
  checkpoints    pipeline progress per influencer
  account_state  freeze + daily request count per Instagram account
  events         Instagram error log
"""

import json
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Iterator

from ..core.config import Config


def _replace(src: Path, dst: Path, attempts: int = 20) -> None:
    """os.replace, retried briefly: on Windows, antivirus / indexer / OneDrive can hold a
    just-written file open for a moment, which makes the swap fail with Access denied."""
    for i in range(attempts):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if i == attempts - 1:
                raise
            time.sleep(0.05 * (i + 1))


def _read(path: Path, attempts: int = 20) -> str:
    """read_text, retried briefly for the same Windows file-locking reason as _replace."""
    for i in range(attempts):
        try:
            return path.read_text(encoding="utf-8")
        except PermissionError:
            if i == attempts - 1:
                raise
            time.sleep(0.05 * (i + 1))


def _matches(doc: dict, match: dict) -> bool:
    """Equality match; a list value means 'one of'."""
    for key, want in match.items():
        have = doc.get(key)
        if isinstance(want, list):
            if have not in want:
                return False
        elif have != want:
            return False
    return True


def _mongo_query(match: dict) -> dict:
    return {k: ({"$in": v} if isinstance(v, list) else v) for k, v in match.items()}


class JsonStore:
    """Local JSON files, for development and the pilot. Use MongoDB for multiple servers."""

    def __init__(self, root: Path):
        self.root = root
        # Several account threads write at once; read-modify-write must not interleave.
        self._lock = threading.RLock()

    def _path(self, collection: str, doc_id: str) -> Path:
        safe = re.sub(r"[^A-Za-z0-9_.-]", "_", str(doc_id))
        return self.root / collection / f"{safe}.json"

    def get(self, collection: str, doc_id: str) -> dict | None:
        path = self._path(collection, doc_id)
        with self._lock:
            if not path.exists():
                return None
            return json.loads(_read(path))

    def get_many(self, collection: str, doc_ids: list[str]) -> dict[str, dict]:
        out = {}
        for doc_id in doc_ids:
            doc = self.get(collection, doc_id)
            if doc is not None:
                out[str(doc_id)] = doc
        return out

    def upsert(self, collection: str, doc_id: str, fields: dict) -> dict:
        with self._lock:
            doc = self.get(collection, doc_id) or {"_id": str(doc_id)}
            doc.update(fields)
            path = self._path(collection, doc_id)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(doc, ensure_ascii=False, default=str), encoding="utf-8")
            _replace(tmp, path)
            return doc

    def insert_if_missing(self, collection: str, doc_id: str, fields: dict) -> bool:
        """Create the doc only if it doesn't exist (atomic). Returns True if created."""
        with self._lock:
            if self._path(collection, doc_id).exists():
                return False
            self.upsert(collection, doc_id, fields or {"created": True})
            return True

    def add_to_set(self, collection: str, doc_id: str, field: str, value: Any) -> None:
        with self._lock:
            doc = self.get(collection, doc_id) or {}
            values = doc.get(field, [])
            if value not in values:
                self.upsert(collection, doc_id, {field: values + [value]})
            elif not doc:
                self.upsert(collection, doc_id, {field: values})

    def find(self, collection: str) -> Iterator[dict]:
        folder = self.root / collection
        if not folder.exists():
            return
        for path in list(folder.glob("*.json")):
            with self._lock:  # never read while another thread swaps the file in
                try:
                    doc = json.loads(_read(path))
                except FileNotFoundError:
                    continue
            yield doc

    def find_ids(self, collection: str, field: str, value: Any, limit: int = 0) -> list[str]:
        ids = [d["_id"] for d in self.find(collection) if d.get(field) == value]
        return ids[:limit] if limit else ids

    def count(self, collection: str) -> int:
        folder = self.root / collection
        return len(list(folder.glob("*.json"))) if folder.exists() else 0

    def count_where(self, collection: str, match: dict) -> int:
        return sum(1 for d in self.find(collection) if _matches(d, match))

    def update_many(self, collection: str, doc_ids: list[str], fields: dict) -> None:
        with self._lock:
            for doc_id in doc_ids:
                if self.get(collection, doc_id) is not None:
                    self.upsert(collection, doc_id, fields)

    def claim(self, collection: str, match: dict, update: dict, now: str,
              lease_field: str = "lease_until", sort_field: str | None = None) -> dict | None:
        """Atomically take one doc that matches and whose lease is empty or expired; apply `update`."""
        with self._lock:
            best = None
            for doc in self.find(collection):
                if not _matches(doc, match) or (doc.get(lease_field) or "") >= now:
                    continue
                if sort_field is None:
                    best = doc
                    break
                if best is None or (doc.get(sort_field) or "") < (best.get(sort_field) or ""):
                    best = doc
            return self.upsert(collection, best["_id"], update) if best else None


class MongoStore:
    BATCH = 1000

    def __init__(self, uri: str, db_name: str):
        from pymongo import ASCENDING, MongoClient

        self.db = MongoClient(uri, serverSelectionTimeoutMS=10_000)[db_name]
        self.db.command("ping")  # fail fast on a wrong URI / IP not allowed in Atlas
        # Queue claims (see jobqueue.py) and status counts
        self.db.checkpoints.create_index([("status", ASCENDING), ("lease_until", ASCENDING), ("enqueued_at", ASCENDING)])
        self.db.audience.create_index([("wanted", ASCENDING), ("enriched", ASCENDING), ("lease_until", ASCENDING)])
        self.db.posts.create_index([("influencer", ASCENDING)])
        self.db.events.create_index([("at", ASCENDING)])

    def get(self, collection: str, doc_id: str) -> dict | None:
        return self.db[collection].find_one({"_id": str(doc_id)})

    def get_many(self, collection: str, doc_ids: list[str]) -> dict[str, dict]:
        ids = [str(i) for i in doc_ids]
        out = {}
        for start in range(0, len(ids), self.BATCH):
            for doc in self.db[collection].find({"_id": {"$in": ids[start:start + self.BATCH]}}):
                out[doc["_id"]] = doc
        return out

    def upsert(self, collection: str, doc_id: str, fields: dict) -> dict:
        from pymongo import ReturnDocument

        fields = {k: v for k, v in fields.items() if k != "_id"}
        return self.db[collection].find_one_and_update(
            {"_id": str(doc_id)}, {"$set": fields}, upsert=True, return_document=ReturnDocument.AFTER,
        )

    def insert_if_missing(self, collection: str, doc_id: str, fields: dict) -> bool:
        fields = {k: v for k, v in fields.items() if k != "_id"} or {"created": True}  # $setOnInsert can't be empty
        result = self.db[collection].update_one({"_id": str(doc_id)}, {"$setOnInsert": fields}, upsert=True)
        return result.upserted_id is not None

    def count_where(self, collection: str, match: dict) -> int:
        return self.db[collection].count_documents(_mongo_query(match))

    def update_many(self, collection: str, doc_ids: list[str], fields: dict) -> None:
        ids = [str(i) for i in doc_ids]
        for start in range(0, len(ids), self.BATCH):
            self.db[collection].update_many({"_id": {"$in": ids[start:start + self.BATCH]}}, {"$set": fields})

    def claim(self, collection: str, match: dict, update: dict, now: str,
              lease_field: str = "lease_until", sort_field: str | None = None) -> dict | None:
        """Atomically take one doc that matches and whose lease is empty or expired; apply `update`.
        Safe across threads and servers: MongoDB applies find_one_and_update as one operation."""
        from pymongo import ReturnDocument

        query = _mongo_query(match)
        query["$or"] = [{lease_field: None}, {lease_field: {"$lt": now}}]
        return self.db[collection].find_one_and_update(
            query, {"$set": update},
            sort=[(sort_field, 1)] if sort_field else None,
            return_document=ReturnDocument.AFTER,
        )

    def add_to_set(self, collection: str, doc_id: str, field: str, value: Any) -> None:
        self.db[collection].update_one({"_id": str(doc_id)}, {"$addToSet": {field: value}}, upsert=True)

    def find(self, collection: str) -> Iterator[dict]:
        yield from self.db[collection].find()

    def find_ids(self, collection: str, field: str, value: Any, limit: int = 0) -> list[str]:
        cursor = self.db[collection].find({field: value}, {"_id": 1})
        if limit:
            cursor = cursor.limit(limit)
        return [d["_id"] for d in cursor]

    def count(self, collection: str) -> int:
        return self.db[collection].count_documents({})


def open_store(config: Config) -> JsonStore | MongoStore:
    if config.mongo_uri:
        return MongoStore(config.mongo_uri, config.mongo_db)
    return JsonStore(config.data_dir)
