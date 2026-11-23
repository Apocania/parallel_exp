"""Local lab supervisor and observable message relay (not a production gateway)."""
from __future__ import annotations

import argparse
import base64
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import hashlib
import heapq
import json
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import uuid
from urllib.parse import parse_qs, urlencode, urlparse

from .crdt import CATALOG, canonical, relation, public_view
from .http_util import JsonHandler, LabHTTPServer, request, valid_id
from .observations import MessageJournal

ROOT = Path(__file__).resolve().parent.parent


class Lab:
    def __init__(self, port, node_base, data_dir, interval=10):
        self.port, self.node_base, self.interval = port, node_base, interval
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.changed = threading.Condition(self.lock)
        self.revision = 0
        self.message_revision = 0
        self.journal = MessageJournal()
        self.processes = {}
        self.database_paths = {}
        self.log_files = {}
        self.blocked = set()
        self.delay_ms, self.copies, self.reorder = 0, 1, False
        self.queue, self.sequence = [], 0
        self.pending_repairs = {}
        self.broadcast_sequence = 0
        self.write_rounds = {}
        self.counters = {"enqueued": 0, "delivered": 0, "dropped": 0, "changed": 0, "overtaken": 0}
        self.last_delivered = {}
        self.events = deque(maxlen=300)
        self.event_seq = 0
        self.drop_reply = set()
        self.stopping = threading.Event()
        self.cart_id = "demo"
        self.migration = None
        self.migration_thread = None
        self.migration_cancel = threading.Event()
        self.migration_continue = threading.Event()
        self.started = time.time()
        self.record("lab_started", "实验控制台已启动；写成功表示 1 个本地副本已持久化")

    def url(self, node):
        if node not in "ABCD" or len(node) != 1:
            raise ValueError("unknown node")
        return f"http://127.0.0.1:{self.node_base + ord(node) - ord('A')}"

    def call(self, node, path, data=None, timeout=2):
        return request(self.url(node) + path, data, timeout)

    def record(self, kind, message, **detail):
        with self.lock:
            self.event_seq += 1
            self.events.append({"seq": self.event_seq, "timestamp": time.time(),
                                "kind": kind, "message": message, "detail": detail})
            self.invalidate()

    def invalidate(self):
        with self.changed:
            self.revision += 1
            self.changed.notify_all()

    def notify_messages(self):
        with self.changed:
            self.message_revision += 1
            self.changed.notify_all()

    def messages(self, cart_id=None):
        with self.lock:
            return {"cart_id": cart_id or self.cart_id, "message_revision": self.message_revision,
                    "network": self.network_view(), "repair_interval": self.interval,
                    **self.journal.snapshot(cart_id or self.cart_id)}

    def kick_sync(self):
        # Trigger ordinary replication. The relay still applies EVERY fault.
        def kick(node):
            if self.running(node):
                try:
                    self.call(node, "/internal/sync-now", {}, timeout=.5)
                except (OSError, ValueError):
                    pass
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(kick, "ABCD"))

    def running(self, node):
        with self.lock:
            return node in self.processes and self.processes[node].poll() is None

    def start_node(self, node, joining=False):
        self.url(node)
        with self.lock:
            if self.running(node):
                return
            stream = open(self.data_dir / f"node-{node}.log", "ab")
            args = [sys.executable, "-u", "-m", "cartlab.node", "--id", node,
                    "--port", str(self.node_base + ord(node) - ord('A')),
                    "--database", str(self.database_paths.get(node, self.data_dir / f"node-{node}.sqlite3")),
                    "--manager", f"http://127.0.0.1:{self.port}", "--interval", str(self.interval)]
            if joining:
                args.append("--joining")
            flags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
            proc = subprocess.Popen(args, cwd=ROOT, stdout=stream, stderr=stream, creationflags=flags)
            self.processes[node] = proc
            old_stream = self.log_files.get(node)
            if old_stream:
                old_stream.close()
            self.log_files[node] = stream
        for _ in range(60):
            if proc.poll() is not None:
                raise RuntimeError(f"node {node} failed to start; inspect node-{node}.log")
            try:
                if self.call(node, "/health", timeout=.2)[0] == 200:
                    self.record("node_started", f"节点 {node} 已启动", node=node, pid=proc.pid)
                    self.kick_sync()
                    return
            except OSError:
                pass
            self.stopping.wait(.05)
        raise RuntimeError(f"node {node} startup timed out")

    def stop_node(self, node):
        self.url(node)
        with self.lock:
            proc = self.processes.get(node)
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=4)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
            self.record("node_stopped", f"节点 {node} 进程已停止，磁盘数据保留", node=node)

    def configure_network(self, body):
        with self.lock:
            if body.get("heal"):
                self.blocked.clear()
                self.delay_ms, self.copies, self.reorder = 0, 1, False
                self.write_rounds.clear()
            if "isolate" in body:
                node = body["isolate"]
                self.url(node)
                self.blocked = {(a, b) for a in "ABCD" for b in "ABCD" if a != b and node in (a, b)}
            if "delay_ms" in body:
                self.delay_ms = max(0, min(30000, int(body["delay_ms"])))
            if "copies" in body:
                self.copies = max(1, min(3, int(body["copies"])))
            if "reorder" in body:
                self.reorder = bool(body["reorder"])
                self.write_rounds.clear()
            self.record("network", "网络故障设置已更新", **self.network_view())
        if body.get("heal"):
            self.kick_sync()
        return self.network_view()

    def network_view(self):
        with self.lock:
            return {"blocked": [list(p) for p in sorted(self.blocked)], "delay_ms": self.delay_ms,
                    "copies": self.copies, "reorder": self.reorder, "queued": len(self.queue),
                    "counters": dict(self.counters)}

    def enqueue(self, source, carts, reason="repair", causes=()):
        self.url(source)
        with self.lock:
            self.broadcast_sequence += 1
            broadcast = self.broadcast_sequence
            if reason == "repair":
                self.journal.background["broadcasts"] += 1
            # Only explicit writes determine the demonstration's ordering.
            # Background heartbeats cannot accidentally select the slow round.
            extra = 0
            if self.reorder and reason in ("write", "retry"):
                self.write_rounds[source] = self.write_rounds.get(source, 0) + 1
                extra = 2.0 if self.write_rounds[source] % 2 else 0
            interesting = False
            contents = public_view(carts[self.cart_id]) if self.cart_id in carts else None
            for target in "ABCD":
                if source == target or (target == "D" and not self.running(target)):
                    continue
                # One periodic batch per link is enough while the previous
                # batch is still in flight. Explicit writes/retries/recovery
                # are never coalesced; the next heartbeat carries full state.
                repair_key = (source, target)
                if reason == "repair" and self.pending_repairs.get(repair_key, 0):
                    continue
                for copy in range(self.copies):
                    if len(self.queue) >= 2000:
                        self.counters["dropped"] += 1
                        continue
                    self.sequence += 1
                    due = time.monotonic() + self.delay_ms / 1000 + extra + copy * .03
                    status = "offline" if not self.running(target) else "blocked" if (source, target) in self.blocked else "queued"
                    trace = {"group": broadcast, "source": source, "target": target,
                             "copy": copy + 1, "copies": self.copies, "status": status,
                             "reason": reason, "causes": list(causes),
                             "contents": {"items": [{"sku": i["sku"], "quantity": i["quantity"]}
                                                    for i in contents["items"]],
                                          "removed": contents["tombstone_count"]} if contents else None,
                             "cart_id": self.cart_id, "created_at": time.time(),
                             "due_at": time.time() + self.delay_ms / 1000 + extra + copy * .03,
                             "request_ids": list(carts.get(self.cart_id, {}).get("requests", {}))}
                    if self.cart_id in carts:
                        interesting = self.journal.attach(trace, carts) or interesting
                    if status != "queued":
                        self.counters["dropped"] += 1
                        if reason == "repair":
                            self.journal.background["dropped"] += 1
                        continue
                    heapq.heappush(self.queue, (due, self.sequence, source, target, carts, trace))
                    if reason == "repair":
                        self.pending_repairs[repair_key] = self.pending_repairs.get(repair_key, 0) + 1
                    self.counters["enqueued"] += 1
            if interesting:
                self.notify_messages()

    def relay(self):
        while not self.stopping.is_set():
            message = None
            with self.lock:
                if self.queue and self.queue[0][0] <= time.monotonic():
                    message = heapq.heappop(self.queue)
                    if message[5]["reason"] == "repair":
                        key = (message[2], message[3])
                        self.pending_repairs[key] -= 1
            if message is None:
                self.stopping.wait(.01)
                continue
            _, sequence, source, target, carts, trace = message
            with self.lock:
                if (source, target) in self.blocked or not self.running(target):
                    self.counters["dropped"] += 1
                    trace["status"] = "dropped"
                    if trace.get("tracked"):
                        self.notify_messages()
                    continue
            try:
                code, result = self.call(target, "/internal/merge",
                                         {"source": source, "carts": carts, "causes": trace["causes"]}, timeout=.8)
                with self.lock:
                    if code != 200:
                        self.counters["dropped"] += 1
                        trace["status"] = "rejected"
                        if trace.get("tracked"):
                            self.notify_messages()
                        continue
                    self.counters["delivered"] += 1
                    trace["status"] = "delivered"
                    trace["delivered_at"] = time.time()
                    trace["changed"] = any(c["cart_id"] == trace["cart_id"] for c in result["changed"])
                    new_receipt = self.journal.delivered(trace, carts)
                    if trace["reason"] == "repair":
                        self.journal.background["delivered"] += 1
                        self.journal.background["changed"] += int(bool(result["changed"]))
                    key = (source, target)
                    if sequence < self.last_delivered.get(key, 0):
                        self.counters["overtaken"] += 1
                        trace["out_of_order"] = True
                    self.last_delivered[key] = max(sequence, self.last_delivered.get(key, 0))
                    if result["changed"]:
                        self.counters["changed"] += 1
                        self.record("merged", f"{source} → {target}：副本状态已合并",
                                    source=source, target=target, carts=result["changed"])
                    if trace.get("tracked") or new_receipt:
                        self.notify_messages()
            except (OSError, ValueError):
                with self.lock:
                    self.counters["dropped"] += 1
                    trace["status"] = "dropped"
                    if trace.get("tracked"):
                        self.notify_messages()

    def node_state(self, node, cart_id):
        if not self.running(node):
            return {"node": node, "online": False, "ready": False}
        try:
            code, result = self.call(node, "/state?" + urlencode({"cart_id": cart_id}), timeout=.6)
            return {**result, "online": code == 200, "pid": self.processes[node].pid,
                    "port": self.node_base + ord(node) - ord('A')}
        except (OSError, ValueError):
            return {"node": node, "online": False, "ready": False}

    def status(self, cart_id=None):
        cart_id = cart_id or self.cart_id
        with ThreadPoolExecutor(max_workers=4) as pool:
            nodes = list(pool.map(lambda n: self.node_state(n, cart_id), "ABCD"))
        serving = [node for node in nodes if node.get("online") and node.get("ready")]
        converged = len(serving) >= 2 and len({n["cart"]["state_hash"] for n in serving}) == 1
        pairs = [{"left": a["node"], "right": b["node"],
                  "relation": relation(a["cart"]["vv"], b["cart"]["vv"])}
                 for i, a in enumerate(serving) for b in serving[i + 1:]]
        with self.lock:
            return {"cart_id": cart_id, "revision": self.revision, "nodes": nodes, "network": self.network_view(),
                    "catalog": CATALOG, "events": list(reversed(self.events))[:80],
                    "converged": converged, "compared_nodes": [n["node"] for n in serving],
                    "relations": pairs, "migration": dict(self.migration) if self.migration else None,
                    **self.messages(cart_id),
                    "drop_reply": sorted(self.drop_reply), "uptime_seconds": round(time.time() - self.started)}

    def operation(self, body):
        node = body["node"]
        payload = {"cart_id": valid_id(body.get("cart_id", self.cart_id)),
                   "request_id": valid_id(body["request_id"]), "operation": body["operation"]}
        record = attempt = None
        with self.lock:
            if payload["cart_id"] == self.cart_id:
                record, attempt = self.journal.begin(payload["cart_id"], payload["request_id"], node, payload["operation"])
                self.notify_messages()
        try:
            code, result = self.call(node, "/cart/operate", payload)
        except OSError:
            if record is not None:
                with self.lock:
                    self.journal.finish(record, attempt, "unknown")
                    self.notify_messages()
            raise
        with self.lock:
            lost = code == 200 and node in self.drop_reply
            if lost:
                self.drop_reply.remove(node)
            if record is not None:
                self.journal.finish(record, attempt, "reply_lost" if lost else "acknowledged" if code == 200 else "rejected",
                                    result.get("duplicate", False))
                self.notify_messages()
        if code == 200:
            self.record("deduplicated" if result["duplicate"] else "write_ack",
                        f"{node}：{'识别重复请求' if result['duplicate'] else '本地落盘完成，确认 1 个副本'}",
                        node=node, request_id=payload["request_id"], cart_id=payload["cart_id"])
            with self.lock:
                if lost:
                    self.record("response_lost", f"{node}：模拟写入响应在返回途中丢失", request_id=payload["request_id"], cart_id=payload["cart_id"])
                    return 504, {"error": "响应丢失模拟：结果未知，请使用同一请求 ID 重试。",
                                 "ambiguous": True, "request_id": payload["request_id"]}
        return code, result

    def new_experiment(self):
        self.cancel_migration()
        self.stop_node("D")
        self.configure_network({"heal": True})
        with self.lock:
            self.drop_reply.clear()
            self.migration = None
            self.journal = MessageJournal()
            self.notify_messages()
            self.cart_id = "demo-" + uuid.uuid4().hex[:10]
            cart_id = self.cart_id
        for node in "ABC":
            if not self.running(node):
                self.start_node(node)
            self.call(node, "/internal/clock", {"offset": 0})
        code, result = self.operation({"node": "A", "cart_id": cart_id, "request_id": uuid.uuid4().hex,
                                       "operation": {"kind": "add", "sku": "milk"}})
        if code != 200:
            raise RuntimeError("could not seed experiment")
        self.record("experiment", "新实验已准备：同一用户，初始购物车包含 1 份牛奶", cart_id=cart_id)
        return {"cart_id": cart_id, "seed": result}

    def cancel_migration(self):
        self.migration_cancel.set()
        if self.migration_thread and self.migration_thread.is_alive():
            self.migration_thread.join(timeout=3)
            if self.migration_thread.is_alive():
                raise RuntimeError("previous migration is still stopping")

    def begin_migration(self, fresh=False, pause_after=None):
        if fresh:
            self.cancel_migration()
            self.stop_node("D")
            directory = self.data_dir / "import-runs"
            directory.mkdir(exist_ok=True)
            self.database_paths["D"] = directory / ("D-" + uuid.uuid4().hex + ".sqlite3")
        with self.lock:
            if self.migration_thread and self.migration_thread.is_alive():
                raise ValueError("migration already running")
        self.start_node("D", joining=True)
        if self.call("D", "/health")[1]["ready"]:
            raise ValueError("D 已完成加入；重新选择迁移实验可使用新数据库再次演示")
        code, snapshot = self.call("A", "/internal/snapshot")
        if code != 200:
            raise RuntimeError("source unavailable")
        raw = canonical(snapshot["carts"]).encode()
        # At least 8 chunks make mid-transfer failure observable, even with a
        # small demo cart. A fresh snapshot is taken on each retry.
        size = max(1, (len(raw) + 7) // 8)
        chunks = [raw[i:i + size] for i in range(0, len(raw), size)]
        if pause_after is not None and (type(pause_after) is not int or not 0 < pause_after < len(chunks)):
            raise ValueError("pause_after must be between the first and final chunk")
        job = uuid.uuid4().hex
        code, result = self.call("D", "/internal/migration/begin",
                                 {"job": job, "total": len(chunks), "digest": hashlib.sha256(raw).hexdigest()})
        if code != 200:
            raise ValueError(result.get("error", "migration refused"))
        self.migration = {"job": job, "status": "running", "sent": 0, "total": len(chunks)}
        self.migration_cancel = threading.Event()
        self.migration_continue = threading.Event()
        cancelled = self.migration_cancel
        continued = self.migration_continue
        self.record("migration", "D 开始导入快照；完成校验和提交前不提供购物车服务")

        def transfer():
            try:
                for i, chunk in enumerate(chunks):
                    if self.stopping.is_set():
                        return
                    if cancelled.wait(.5):
                        raise RuntimeError("import cancelled for a new experiment")
                    code, result = self.call("D", "/internal/migration/chunk",
                                             {"job": job, "part": i, "data": base64.b64encode(chunk).decode()})
                    if code != 200:
                        raise RuntimeError(result.get("error", "chunk rejected"))
                    with self.lock:
                        self.migration["sent"] = i + 1
                        self.invalidate()
                    if pause_after == i + 1:
                        with self.lock:
                            self.migration["status"] = "paused"
                            self.invalidate()
                        while not continued.wait(.1):
                            if cancelled.is_set() or self.stopping.is_set() or not self.running("D"):
                                raise RuntimeError("target stopped at the import checkpoint")
                        with self.lock:
                            self.migration["status"] = "running"
                code, result = self.call("D", "/internal/migration/commit", {"job": job})
                if code != 200:
                    raise RuntimeError(result.get("error", "commit rejected"))
                with self.lock:
                    self.migration["status"] = "complete"
                self.record("migration", "D 快照已完整提交，开始正常同步并接收请求")
                self.kick_sync()
            except (OSError, ValueError, RuntimeError) as exc:
                with self.lock:
                    self.migration["status"] = "interrupted"
                    self.migration["error"] = str(exc)
                self.record("migration_interrupted", "D 导入中断；已接收片段仍处于暂存区，可重新导入")

        self.migration_thread = threading.Thread(target=transfer, daemon=True)
        self.migration_thread.start()
        return dict(self.migration)

    def close(self):
        self.stopping.set()
        self.migration_cancel.set()
        self.invalidate()
        for node in "ABCD":
            self.stop_node(node)
        for stream in self.log_files.values():
            stream.close()


class LabHandler(JsonHandler):
    def stream_events(self):
        lab = self.server.lab
        self.connection.settimeout(3)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        previous = previous_messages = -1
        try:
            while not lab.stopping.is_set():
                with lab.changed:
                    lab.changed.wait_for(lambda: lab.revision != previous or lab.message_revision != previous_messages
                                         or lab.stopping.is_set(), timeout=10)
                    revision, message_revision = lab.revision, lab.message_revision
                if lab.stopping.is_set():
                    return
                events = []
                if revision != previous:
                    events.append(f"event: change\ndata: {{\"revision\":{revision}}}\n\n")
                if message_revision != previous_messages:
                    events.append(f"event: messages\ndata: {{\"message_revision\":{message_revision}}}\n\n")
                self.wfile.write(("".join(events) or ": heartbeat\n\n").encode())
                self.wfile.flush()
                previous = revision
                previous_messages = message_revision
        except (OSError, ValueError):
            pass

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            lab = self.server.lab
            if parsed.path == "/api/events":
                self.stream_events()
            elif parsed.path == "/api/status":
                self.send_json(lab.status(valid_id(query["cart_id"][0]) if "cart_id" in query else None))
            elif parsed.path == "/api/messages":
                self.send_json(lab.messages(valid_id(query["cart_id"][0]) if "cart_id" in query else None))
            elif parsed.path == "/api/health":
                self.send_json({"ok": True, "service": "cartlab"})
            elif parsed.path == "/api/cas":
                node = query.get("node", ["A"])[0]
                code, result = lab.call(node, "/cas?" + urlencode({"key": valid_id(query.get("key", ["example"])[0])}))
                self.send_json(result, code)
            elif parsed.path == "/guide":
                self.send_bytes((ROOT / "docs" / "实验讲解详解.html").read_bytes(), "text/html; charset=utf-8")
            elif parsed.path in ("/", "/app.js", "/scenarios.js", "/lessons.js", "/style.css"):
                file = "index.html" if parsed.path == "/" else parsed.path[1:]
                mime = "text/html" if parsed.path == "/" else "text/css" if file.endswith(".css") else "application/javascript"
                self.send_bytes((ROOT / "web" / file).read_bytes(), mime + "; charset=utf-8")
            elif parsed.path == "/favicon.ico":
                self.send_bytes(b"", "image/x-icon", 204)
            else:
                self.send_json({"error": "not found"}, 404)
        except (ValueError, KeyError, TypeError) as exc:
            self.send_json({"error": str(exc)}, 400)
        except OSError as exc:
            self.send_json({"error": "节点暂不可用", "detail": str(exc)}, 503)

    def do_POST(self):
        try:
            body = self.body()
            lab = self.server.lab
            if self.path == "/api/transport":
                lab.enqueue(body["source"], body["carts"], body.get("reason", "repair"), body.get("causes", []))
                self.send_json({"accepted": True})
            elif self.path == "/api/operation":
                code, result = lab.operation(body)
                self.send_json(result, code)
            elif self.path == "/api/network":
                self.send_json(lab.configure_network(body))
            elif self.path == "/api/experiment/new":
                self.send_json(lab.new_experiment())
            elif self.path == "/api/node":
                node = body["node"]
                if body["action"] == "stop":
                    lab.stop_node(node)
                elif body["action"] == "start":
                    lab.start_node(node, joining=node == "D")
                else:
                    raise ValueError("unknown action")
                self.send_json({"ok": True})
            elif self.path == "/api/fault/reply":
                lab.url(body["node"])
                with lab.lock:
                    lab.drop_reply.add(body["node"])
                lab.record("fault", f"已设置：丢弃 {body['node']} 的下一次成功写入响应")
                self.send_json({"ok": True})
            elif self.path == "/api/fault/clock":
                code, result = lab.call(body["node"], "/internal/clock", {"offset": body["offset"]})
                lab.record("clock", f"节点 {body['node']} 的实验时钟偏移设为 {body['offset']} 秒")
                self.send_json(result, code)
            elif self.path == "/api/cas":
                code, result = lab.call(body["node"], "/cas", body)
                self.send_json(result, code)
            elif self.path == "/api/migration/start":
                self.send_json(lab.begin_migration(fresh=bool(body.get("fresh")), pause_after=body.get("pause_after")))
            elif self.path == "/api/migration/resume":
                lab.migration_continue.set()
                self.send_json({"ok": True})
            elif self.path == "/api/lab/shutdown":
                self.send_json({"ok": True})
                threading.Thread(target=self.server.shutdown, daemon=True).start()
            else:
                self.send_json({"error": "not found"}, 404)
        except (ValueError, KeyError, TypeError) as exc:
            self.send_json({"error": str(exc)}, 400)
        except (OSError, RuntimeError) as exc:
            self.send_json({"error": "操作未能确认完成", "detail": str(exc), "ambiguous": self.path == "/api/operation"}, 503)


def main():
    parser = argparse.ArgumentParser(description="Distributed shopping cart laboratory")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8760)
    parser.add_argument("--node-base-port", type=int, default=8761)
    parser.add_argument("--data-dir", default=str(ROOT / ".lab-data"))
    parser.add_argument("--sync-interval", type=float, default=10.0)
    args = parser.parse_args()
    if args.sync_interval <= 0:
        parser.error("sync interval must be positive")
    lab = Lab(args.port, args.node_base_port, args.data_dir, args.sync_interval)
    server = LabHTTPServer((args.host, args.port), LabHandler)
    server.daemon_threads = True
    server.lab = lab
    def stop_signal(_signum, _frame):
        threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, stop_signal)
    try:
        for node in "ABC":
            lab.start_node(node)
        threading.Thread(target=lab.relay, daemon=True, name="message-relay").start()
        print(f"CartLab ready: http://127.0.0.1:{args.port}", flush=True)
        server.serve_forever(poll_interval=.2)
    except KeyboardInterrupt:
        pass
    finally:
        lab.close()
        server.server_close()


if __name__ == "__main__":
    main()
