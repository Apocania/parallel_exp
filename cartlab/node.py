from __future__ import annotations

import argparse
import threading
import time
from urllib.parse import parse_qs, urlparse

from .crdt import public_view
from .http_util import JsonHandler, LabHTTPServer, request, valid_id
from .storage import Store


class NodeHandler(JsonHandler):
    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            store = self.server.store
            if parsed.path == "/health":
                self.send_json({"node": store.node_id, "ready": store.ready, "actor": store.actor})
            elif parsed.path == "/state":
                cart_id = valid_id(query.get("cart_id", ["demo"])[0])
                self.send_json({"node": store.node_id, "ready": store.ready, "actor": store.actor,
                                "clock_offset": store.clock_offset,
                                "wall_time": time.time() + store.clock_offset,
                                "cart": public_view(store.read(cart_id)), "events": store.events(),
                                "migration": store.migration_status()})
            elif parsed.path == "/internal/snapshot":
                self.send_json({"carts": store.snapshot()})
            elif parsed.path == "/cas":
                self.send_json(store.cas_read(valid_id(query.get("key", ["example"])[0])))
            else:
                self.send_json({"error": "not found"}, 404)
        except (ValueError, KeyError, TypeError) as exc:
            self.send_json({"error": str(exc)}, 400)

    def do_POST(self):
        try:
            body = self.body()
            store = self.server.store
            if self.path == "/cart/operate":
                result = store.operate(valid_id(body["cart_id"]), body["operation"], valid_id(body["request_id"]))
                self.server.schedule_sync([body["cart_id"]], reason="retry" if result["duplicate"] else "write",
                                          causes=[body["request_id"]])
                self.send_json(result)
            elif self.path == "/internal/merge":
                changed = store.merge_snapshot(body["carts"], body["source"])
                if changed:
                    self.server.schedule_sync([item["cart_id"] for item in changed], reason="merge",
                                              causes=body.get("causes", []))
                self.send_json({"changed": changed})
            elif self.path == "/internal/sync-now":
                self.server.schedule_sync()
                self.send_json({"ok": True})
            elif self.path == "/internal/clock":
                offset = body["offset"]
                if type(offset) is not int or not -3600 <= offset <= 3600:
                    raise ValueError("offset outside experiment range")
                store.clock_offset = offset
                self.send_json({"offset": store.clock_offset})
            elif self.path == "/cas":
                if type(body["expected"]) is not int or body["expected"] < 0:
                    raise ValueError("invalid expected version")
                result = store.compare_swap(valid_id(body["key"]), body["expected"], body["value"])
                self.send_json(result, 200 if result["swapped"] else 409)
            elif self.path == "/internal/migration/begin":
                store.migration_begin(body["job"], body["total"], body["digest"])
                self.send_json({"ok": True})
            elif self.path == "/internal/migration/chunk":
                store.migration_chunk(body["job"], body["part"], body["data"])
                self.send_json({"ok": True})
            elif self.path == "/internal/migration/commit":
                store.migration_commit(body["job"])
                self.server.schedule_sync()
                self.send_json({"ok": True})
            else:
                self.send_json({"error": "not found"}, 404)
        except RuntimeError as exc:
            self.send_json({"error": str(exc)}, 503)
        except (ValueError, KeyError, TypeError) as exc:
            self.send_json({"error": str(exc)}, 409 if "request_id reused" in str(exc) else 400)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--database", required=True)
    parser.add_argument("--manager", required=True)
    parser.add_argument("--interval", type=float, default=10)
    parser.add_argument("--joining", action="store_true")
    args = parser.parse_args()
    store = Store(args.database, args.id, args.joining)
    server = LabHTTPServer(("127.0.0.1", args.port), NodeHandler)
    server.daemon_threads = True
    server.store = store
    wake = threading.Event()
    pending_lock = threading.Lock()
    dirty = set()
    reasons, pending_causes = set(), set()
    full_sync = [True]

    def schedule_sync(cart_ids=None, reason="recovery", causes=()):
        with pending_lock:
            reasons.add(reason)
            pending_causes.update(causes)
            if cart_ids is None:
                full_sync[0] = True
            else:
                dirty.update(cart_ids)
            wake.set()

    server.schedule_sync = schedule_sync

    def sync():
        while True:
            triggered = wake.wait(args.interval)
            with pending_lock:
                cart_ids = set(dirty)
                send_all = full_sync[0] or not triggered
                dirty.clear()
                full_sync[0] = False
                reason = next((r for r in ("write", "retry", "recovery", "merge") if r in reasons), "repair")
                causes = sorted(pending_causes)
                reasons.clear()
                pending_causes.clear()
                wake.clear()
            if not store.ready:
                continue
            snapshot = store.snapshot() if send_all else {cart_id: store.read(cart_id) for cart_id in cart_ids}
            if not snapshot:
                continue
            try:
                request(args.manager + "/api/transport",
                        {"source": args.id, "carts": snapshot, "reason": reason, "causes": causes}, timeout=2)
            except (OSError, ValueError):
                # Next round sends the full state again: transient loss is repaired.
                pass

    threading.Thread(target=sync, daemon=True, name="state-sync").start()
    print(f"Node {args.id}: http://127.0.0.1:{args.port} actor={store.actor}", flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
