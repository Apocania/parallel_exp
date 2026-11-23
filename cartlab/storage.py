from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path

from . import crdt


class Store:
    def __init__(self, path, node_id, joining=False):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.node_id = node_id
        self.lock = threading.RLock()
        self.clock_offset = 0
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS carts (id TEXT PRIMARY KEY, state TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events
              (seq INTEGER PRIMARY KEY AUTOINCREMENT, timestamp REAL, kind TEXT, detail TEXT);
            CREATE TABLE IF NOT EXISTS cas (key TEXT PRIMARY KEY, value TEXT, version INTEGER);
            CREATE TABLE IF NOT EXISTS migration
              (id TEXT PRIMARY KEY, total INTEGER, digest TEXT);
            CREATE TABLE IF NOT EXISTS chunks
              (job TEXT, part INTEGER, data BLOB, PRIMARY KEY(job, part));
        """)
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO meta VALUES ('actor', ?)",
                            (node_id + "@" + uuid.uuid4().hex[:12],))
            self.db.execute("INSERT OR IGNORE INTO meta VALUES ('counter', '0')")
            self.db.execute("INSERT OR IGNORE INTO meta VALUES ('ready', ?)", ("0" if joining else "1",))
        self.actor = self.meta("actor")

    def meta(self, key):
        with self.lock:
            return self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()[0]

    @property
    def ready(self):
        return self.meta("ready") == "1"

    def event(self, kind, detail):
        self.db.execute("INSERT INTO events(timestamp,kind,detail) VALUES (?,?,?)",
                        (time.time() + self.clock_offset, kind, crdt.canonical(detail)))

    def _get(self, cart_id):
        row = self.db.execute("SELECT state FROM carts WHERE id=?", (cart_id,)).fetchone()
        return json.loads(row[0]) if row else crdt.empty()

    def _put(self, cart_id, state):
        self.db.execute("INSERT INTO carts VALUES (?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state",
                        (cart_id, crdt.canonical(state)))

    def read(self, cart_id):
        with self.lock:
            return self._get(cart_id)

    def operate(self, cart_id, operation, request_id):
        with self.lock, self.db:
            if not self.ready:
                raise RuntimeError("node is importing; not ready to serve")
            state = self._get(cart_id)
            counter = max(int(self.meta("counter")), state["vv"].get(self.actor, 0)) + 1
            updated, duplicate = crdt.apply(state, operation, request_id, self.actor, counter)
            if not duplicate:
                self._put(cart_id, updated)
                self.db.execute("UPDATE meta SET value=? WHERE key='counter'", (str(counter),))
            self.event("deduplicated" if duplicate else "local_write",
                       {"cart_id": cart_id, "request_id": request_id, "operation": operation,
                        "ack_replicas": 1, "vv": updated["vv"]})
        # Return only after the SQLite transaction has committed.
        return {"duplicate": duplicate, "request_id": request_id,
                "ack": {"node": self.node_id, "durable_replicas": 1, "persisted": True},
                "cart": crdt.public_view(updated)}

    def snapshot(self):
        with self.lock:
            return {row[0]: json.loads(row[1]) for row in self.db.execute("SELECT id,state FROM carts")}

    def merge_snapshot(self, snapshot, source):
        if not isinstance(snapshot, dict):
            raise ValueError("snapshot must be a map")
        changed = []
        with self.lock, self.db:
            if not self.ready:
                raise RuntimeError("node is importing; ordinary sync is disabled")
            for cart_id, remote in snapshot.items():
                local = self._get(cart_id)
                joined = crdt.merge(local, remote)
                if crdt.canonical(joined) != crdt.canonical(local):
                    relationship = crdt.relation(local["vv"], remote["vv"])
                    self._put(cart_id, joined)
                    self.event("merge", {"cart_id": cart_id, "source": source,
                                         "relation": relationship, "vv": joined["vv"]})
                    changed.append({"cart_id": cart_id, "relation": relationship})
        return changed

    def events(self, limit=15):
        with self.lock:
            return [{"seq": r[0], "timestamp": r[1], "kind": r[2], "detail": json.loads(r[3])}
                    for r in self.db.execute("SELECT seq,timestamp,kind,detail FROM events ORDER BY seq DESC LIMIT ?", (limit,))]

    def cas_read(self, key):
        with self.lock:
            row = self.db.execute("SELECT value,version FROM cas WHERE key=?", (key,)).fetchone()
            return {"value": json.loads(row[0]), "version": row[1]} if row else {"value": None, "version": 0}

    def compare_swap(self, key, expected, value):
        with self.lock, self.db:
            if not self.ready:
                raise RuntimeError("node is importing")
            current = self.cas_read(key)
            if current["version"] != expected:
                return {"swapped": False, **current}
            self.db.execute("INSERT INTO cas VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,version=excluded.version",
                            (key, crdt.canonical(value), expected + 1))
            self.event("local_cas", {"key": key, "version": expected + 1})
            return {"swapped": True, "value": value, "version": expected + 1}

    def migration_begin(self, job, total, digest):
        if not 1 <= total <= 10000:
            raise ValueError("invalid chunk count")
        with self.lock, self.db:
            if self.ready:
                raise ValueError("migration target must be a joining node")
            self.db.execute("DELETE FROM chunks")
            self.db.execute("DELETE FROM migration")
            self.db.execute("INSERT INTO migration VALUES (?,?,?)", (job, total, digest))
            self.event("import_started", {"job": job, "total": total})

    def migration_chunk(self, job, part, encoded):
        data = base64.b64decode(encoded, validate=True)
        with self.lock, self.db:
            row = self.db.execute("SELECT total FROM migration WHERE id=?", (job,)).fetchone()
            if not row or not 0 <= part < row[0]:
                raise ValueError("unknown migration or chunk")
            self.db.execute("INSERT INTO chunks VALUES (?,?,?) ON CONFLICT(job,part) DO UPDATE SET data=excluded.data", (job, part, data))

    def migration_status(self):
        with self.lock:
            row = self.db.execute("SELECT id,total FROM migration LIMIT 1").fetchone()
            if not row:
                return None
            done = self.db.execute("SELECT COUNT(*) FROM chunks WHERE job=?", (row[0],)).fetchone()[0]
            return {"job": row[0], "total": row[1], "received": done}

    def migration_commit(self, job):
        with self.lock, self.db:
            row = self.db.execute("SELECT total,digest FROM migration WHERE id=?", (job,)).fetchone()
            if not row:
                raise ValueError("unknown migration")
            chunks = self.db.execute("SELECT data FROM chunks WHERE job=? ORDER BY part", (job,)).fetchall()
            if len(chunks) != row[0]:
                raise ValueError("incomplete snapshot")
            raw = b"".join(c[0] for c in chunks)
            if hashlib.sha256(raw).hexdigest() != row[1]:
                raise ValueError("snapshot checksum mismatch")
            snapshot = json.loads(raw)
            for cart_id, state in snapshot.items():
                self._put(cart_id, crdt.merge(self._get(cart_id), state))
            self.db.execute("UPDATE meta SET value='1' WHERE key='ready'")
            self.db.execute("DELETE FROM chunks WHERE job=?", (job,))
            self.db.execute("DELETE FROM migration WHERE id=?", (job,))
            self.event("import_committed", {"job": job, "carts": len(snapshot)})

    def close(self):
        with self.lock:
            self.db.close()
