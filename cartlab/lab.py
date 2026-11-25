"""Local lab supervisor and observable message relay (not a production gateway)."""
from __future__ import annotations

import argparse
import base64
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
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


def bounded(value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f"数值须为 {low} 至 {high} 的整数")
    return value


class Lab:
    def __init__(self, port, node_base, data_dir, interval=10):
        self.port, self.node_base, self.interval = port, node_base, interval
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.delivery_lock = threading.RLock()
        self.delivery_waiters = 0
        self.lifecycle_lock = threading.RLock()
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
        self.manual = False
        self.repair_enabled = True
        self.link_rules = {}
        self.recent_packets = deque(maxlen=120)
        self.original_paths = {}
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
        self.migration_permit = False
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
            m = self.migration
            if m and m["status"] in ("paused", "running") and node in (m["source"], m["target"]):
                self.migration_cancel.set()
                self.changed.notify_all()
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=4)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
            self.record("node_stopped", f"节点 {node} 进程已停止，磁盘数据保留", node=node)

    @contextmanager
    def control_delivery(self):
        # Give operator commands the next turn after the current HTTP merge.
        with self.lock:
            self.delivery_waiters += 1
        try:
            with self.delivery_lock:
                yield
        finally:
            with self.lock:
                self.delivery_waiters -= 1

    def configure_network(self, body):
        # Finish a delivery already in progress before acknowledging a pause or cut.
        with self.control_delivery(), self.lock:
            if body.get("heal"):
                self.blocked.clear()
                self.delay_ms, self.copies, self.reorder = 0, 1, False
                self.write_rounds.clear()
                self.link_rules.clear()
                self.manual = False
                self.repair_enabled = True
            if "isolate" in body:
                node = body["isolate"]
                self.url(node)
                self.blocked.update((a, b) for a in "ABCD" for b in "ABCD" if a != b and node in (a, b))
            if "reconnect" in body:
                node = body["reconnect"]
                self.url(node)
                self.blocked = {pair for pair in self.blocked if node not in pair}
            if "link" in body:
                link = body["link"]
                source, target = link["source"], link["target"]
                self.check_pair(source, target)
                pairs = [(source, target)]
                if link.get("both"):
                    pairs.append((target, source))
                for pair in pairs:
                    if link.get("open"):
                        self.blocked.discard(pair)
                    else:
                        self.blocked.add(pair)
            if "rule" in body:
                rule = body["rule"]
                pair = (rule["source"], rule["target"])
                self.check_pair(*pair)
                if rule.get("clear"):
                    self.link_rules.pop(pair, None)
                else:
                    self.link_rules[pair] = {
                        "delay_ms": bounded(rule["delay_ms"], 0, 120000),
                        "copies": bounded(rule["copies"], 1, 10),
                        "drop_next": bounded(rule["drop_next"], 0, 100),
                    }
            if "manual" in body:
                self.manual = bool(body["manual"])
            if "repair_enabled" in body:
                self.repair_enabled = bool(body["repair_enabled"])
            if "delay_ms" in body:
                self.delay_ms = max(0, min(30000, int(body["delay_ms"])))
            if "copies" in body:
                self.copies = max(1, min(3, int(body["copies"])))
            if "reorder" in body:
                self.reorder = bool(body["reorder"])
                self.write_rounds.clear()
            self.record("network", "网络故障设置已更新", **self.network_view())
        if body.get("heal") or "reconnect" in body or body.get("link", {}).get("open"):
            self.kick_sync()
        return self.network_view()

    def network_view(self):
        with self.lock:
            return {"blocked": [list(p) for p in sorted(self.blocked)], "delay_ms": self.delay_ms,
                    "copies": self.copies, "reorder": self.reorder, "queued": len(self.queue),
                    "manual": self.manual, "repair_enabled": self.repair_enabled,
                    "rules": [dict(source=a, target=b, **rule) for (a,b),rule in self.link_rules.items()],
                    "counters": dict(self.counters)}

    def check_pair(self, source, target):
        self.url(source)
        self.url(target)
        if source == target:
            raise ValueError("请选择两个不同节点")

    def enqueue(self, source, carts, reason="repair", causes=(), destination=None, *, sent_at=None):
        self.url(source)
        with self.lock:
            if reason == "repair" and not self.repair_enabled:
                return
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
            for target in destination or "ABCD":
                if source == target or (destination is None and target == "D" and not self.running(target)):
                    continue
                # One periodic batch per link is enough while the previous
                # batch is still in flight. Explicit writes/retries/recovery
                # are never coalesced; the next heartbeat carries full state.
                repair_key = (source, target)
                if reason == "repair" and self.pending_repairs.get(repair_key, 0):
                    continue
                rule = self.link_rules.get(repair_key, {})
                copies = rule.get("copies", self.copies)
                delay = rule.get("delay_ms", self.delay_ms) / 1000 + extra
                for copy in range(copies):
                    self.sequence += 1
                    wait = delay + copy * .03
                    due = time.monotonic() + wait
                    status = "offline" if not self.running(target) else "blocked" if (source, target) in self.blocked else "queued"
                    if status == "queued" and rule.get("drop_next", 0):
                        status = "dropped"
                        rule["drop_next"] -= 1
                    if status == "queued" and len(self.queue) >= 2000:
                        status = "dropped"
                    trace = {"id": self.sequence, "group": broadcast, "source": source, "target": target,
                             "copy": copy + 1, "copies": copies, "status": status,
                             "reason": reason, "causes": list(causes),
                             "contents": {"items": [{"sku": i["sku"], "quantity": i["quantity"]}
                                                    for i in contents["items"]],
                                          "removed": contents["tombstone_count"]} if contents else None,
                             "cart_id": self.cart_id, "created_at": time.time(), "sent_at": sent_at,
                             "due_at": time.time() + wait,
                             "request_ids": list(carts.get(self.cart_id, {}).get("requests", {}))}
                    if self.cart_id in carts:
                        interesting = self.journal.attach(trace, carts) or interesting
                    if status != "queued":
                        self.failed_delivery(trace, status)
                        continue
                    heapq.heappush(self.queue, (due, self.sequence, source, target, carts, trace))
                    if reason == "repair":
                        self.pending_repairs[repair_key] = self.pending_repairs.get(repair_key, 0) + 1
                    self.counters["enqueued"] += 1
            if interesting:
                self.notify_messages()
            return broadcast

    @staticmethod
    def packet_view(trace):
        fields = ("id", "group", "source", "target", "reason", "status", "sent_at", "due_at",
                  "copy", "copies", "contents", "delivered_at", "changed", "out_of_order", "copied_from")
        return {name: trace[name] for name in fields if name in trace}

    def dispatch_view(self):
        with self.lock:
            return {"cart_id": self.cart_id, "network": self.network_view(),
                    "pending": [self.packet_view(m[5]) for m in sorted(self.queue, key=lambda m: m[1])],
                    "recent": [self.packet_view(t) for t in reversed(self.recent_packets)
                               if t["status"] != "queued"]}

    def clear_dispatch(self, cart_id, scope):
        with self.control_delivery(), self.lock:
            if cart_id != self.cart_id:
                raise ValueError("实验已复位，请刷新队列后重试")
            if scope == "history":
                count = len(self.recent_packets)
                self.recent_packets.clear()
            elif scope == "pending":
                count = len(self.queue)
                while self.queue:
                    self.failed_delivery(self.take_packet()[5], "dropped")
            else:
                raise ValueError("未知清理范围")
            self.notify_messages()
            return {"cleared": count}

    def send_snapshot(self, source, target):
        self.check_pair(source, target)
        cart_id = self.cart_id
        code, state = self.call(source, "/state?" + urlencode({"cart_id": cart_id}))
        if code != 200 or not state["ready"]:
            raise ValueError("来源节点当前不可服务")
        with self.lock:
            if cart_id != self.cart_id:
                raise ValueError("实验已复位，请重新发送")
            group = self.enqueue(source, {cart_id: state["cart"]["raw"]}, "manual", destination=target,
                                 sent_at=state["wall_time"])
            return {"group": group}

    def take_packet(self, index=0):
        if index == 0:
            packet = heapq.heappop(self.queue)
        else:
            packet = self.queue.pop(index)
            heapq.heapify(self.queue)
        if packet[5]["reason"] == "repair":
            self.pending_repairs[(packet[2], packet[3])] -= 1
        return packet

    def adjust_packet(self, body):
        action = body["action"]
        with self.control_delivery():
            with self.lock:
                index = next((i for i, packet in enumerate(self.queue) if packet[1] == body["id"]), None)
                if index is None:
                    raise ValueError("该消息已离开队列，请查看投递结果")
                due, seq, source, target, carts, trace = self.queue[index]
                if action == "delay":
                    seconds = bounded(body["delay_ms"], 0, 120000) / 1000
                    trace["due_at"] = time.time() + seconds
                    self.queue[index] = (time.monotonic() + seconds, seq, source, target, carts, trace)
                    heapq.heapify(self.queue)
                elif action == "duplicate":
                    if len(self.queue) >= 2000:
                        raise ValueError("队列已满")
                    self.sequence += 1
                    self.broadcast_sequence += 1
                    clone = dict(trace, id=self.sequence, group=self.broadcast_sequence, copied_from=seq,
                                 reason="duplicate", copy=1, copies=1, created_at=time.time())
                    self.journal.attach(clone, carts)
                    heapq.heappush(self.queue, (due, self.sequence, source, target, carts, clone))
                    self.counters["enqueued"] += 1
                    trace = clone
                elif action in ("deliver", "drop"):
                    packet = self.take_packet(index)
                    if action == "drop":
                        self.failed_delivery(trace, "dropped")
                else:
                    raise ValueError("未知消息操作")
                self.notify_messages()
            if action == "deliver":
                self.deliver(packet)
            with self.lock:
                return self.packet_view(trace)

    def failed_delivery(self, trace, status):
        self.counters["dropped"] += 1
        trace["status"] = status
        if trace["reason"] != "repair" or trace.get("tracked"):
            self.recent_packets.append(trace)
        if trace["reason"] == "repair":
            self.journal.background["dropped"] += 1
        if trace.get("tracked"):
            self.notify_messages()

    def relay(self):
        while not self.stopping.is_set():
            with self.delivery_lock:
                with self.lock:
                    ready = (self.queue and not self.manual and not self.delivery_waiters
                             and self.queue[0][0] <= time.monotonic())
                    packet = self.take_packet() if ready else None
                if packet is not None:
                    self.deliver(packet)
            if packet is None:
                self.stopping.wait(.01)

    def deliver(self, packet):
        _, sequence, source, target, carts, trace = packet
        with self.lock:
            if (source, target) in self.blocked or not self.running(target):
                self.failed_delivery(trace, "blocked" if (source, target) in self.blocked else "offline")
                return
        try:
            code, result = self.call(target, "/internal/merge",
                                     {"source": source, "carts": carts, "causes": trace["causes"]}, timeout=2)
        except (OSError, ValueError):
            with self.lock:
                self.failed_delivery(trace, "dropped")
            return
        with self.lock:
            if code != 200:
                self.failed_delivery(trace, "rejected")
                return
            self.counters["delivered"] += 1
            trace["status"] = "delivered"
            trace["delivered_at"] = time.time()
            if trace["reason"] != "repair" or trace.get("tracked"):
                self.recent_packets.append(trace)
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

    def operation(self, body, *, cas=None):
        node = body["node"]
        payload = {"cart_id": valid_id(body.get("cart_id", self.cart_id)),
                   "request_id": valid_id(body["request_id"]), "operation": body["operation"]}
        if cas is not None:
            payload.update(cas)
        record = attempt = None
        with self.lock:
            if payload["cart_id"] == self.cart_id:
                record, attempt = self.journal.begin(payload["cart_id"], payload["request_id"], node, payload["operation"])
                self.notify_messages()
        try:
            code, result = self.call(node, "/cas" if cas is not None else "/cart/operate", payload)
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

    def new_experiment(self, seed=True):
        with self.lifecycle_lock:
            self.cancel_migration()
            self.stop_node("D")
            # Reattach the pre-import databases if a free experiment replaced
            # an ordinary replica. Every staging database remains on disk.
            for node, path in self.original_paths.items():
                self.stop_node(node)
                self.database_paths[node] = path
            self.original_paths.clear()
            with self.control_delivery(), self.lock:
                self.queue.clear()
                self.pending_repairs.clear()
                self.recent_packets.clear()
                self.last_delivered.clear()
                self.drop_reply.clear()
                self.migration = None
                self.journal = MessageJournal()
                self.cart_id = "demo-" + uuid.uuid4().hex[:10]
                cart_id = self.cart_id
                self.events.clear()
                self.counters = dict.fromkeys(self.counters, 0)
                self.notify_messages()
            self.configure_network({"heal": True})
            for node in "ABC":
                if not self.running(node):
                    self.start_node(node)
                self.call(node, "/internal/clock", {"offset": 0})
            result = None
            if seed:
                code, result = self.operation({"node": "A", "cart_id": cart_id, "request_id": uuid.uuid4().hex,
                                               "operation": {"kind": "add", "sku": "milk"}})
                if code != 200:
                    raise RuntimeError("could not seed experiment")
            detail = "初始购物车包含 1 份牛奶" if seed else "空购物车，链路与时钟已复位"
            self.record("experiment", "新实验已准备：" + detail, cart_id=cart_id)
            return {"cart_id": cart_id, "seed": result}

    def cancel_migration(self):
        self.migration_cancel.set()
        with self.changed:
            self.changed.notify_all()
        if self.migration_thread and self.migration_thread.is_alive():
            self.migration_thread.join(timeout=4)
            if self.migration_thread.is_alive():
                raise RuntimeError("previous migration is still stopping")

    def begin_migration(self, fresh=False, pause_after=None, source="A", target="D", manual=False):
        self.check_pair(source, target)
        with self.lock:
            if self.migration and self.migration["status"] in ("paused", "running"):
                raise ValueError("当前迁移仍在进行，请先中断或完成")
            if (source, target) in self.blocked:
                raise ValueError("迁移方向的链路已断开")
        if not self.running(source) or not self.call(source, "/health")[1]["ready"]:
            raise ValueError("请选择可服务的快照来源")
        code, snapshot = self.call(source, "/internal/snapshot")
        if code != 200:
            raise RuntimeError("source unavailable")
        raw = canonical(snapshot["carts"]).encode()
        total = min(8, len(raw))
        chunks = [raw[i * len(raw) // total:(i + 1) * len(raw) // total] for i in range(total)]
        if pause_after is not None:
            bounded(pause_after, 1, total - 1)
        self.cancel_migration()
        if fresh:
            self.stop_node(target)
            directory = self.data_dir / "import-runs"
            directory.mkdir(exist_ok=True)
            self.original_paths.setdefault(target, self.database_paths.get(target, self.data_dir / f"node-{target}.sqlite3"))
            self.database_paths[target] = directory / (target + "-" + uuid.uuid4().hex + ".sqlite3")
        self.start_node(target, joining=True)
        if self.call(target, "/health")[1]["ready"]:
            raise ValueError("目标已经在服务，请使用新的导入库")
        job = uuid.uuid4().hex
        code, result = self.call(target, "/internal/migration/begin",
                                 {"job": job, "total": total, "digest": hashlib.sha256(raw).hexdigest()})
        if code != 200:
            raise ValueError(result.get("error", "migration refused"))
        with self.lock:
            self.migration = {"job": job, "source": source, "target": target, "status": "paused" if manual else "running",
                              "manual": manual, "sent": 0, "total": total}
            self.migration_permit = False
            self.migration_cancel = threading.Event()
            job_state = self.migration
            cancelled = self.migration_cancel
        self.record("migration", f"{source} → {target} 已取得快照；目标进入暂存阶段")

        def transfer():
            try:
                for index in range(total + 1):
                    with self.changed:
                        if job_state["manual"] and not self.migration_permit:
                            job_state["status"] = "paused"
                            self.invalidate()
                        while job_state["manual"] and not self.migration_permit and not cancelled.is_set():
                            self.changed.wait(.2)
                        self.migration_permit = False
                        job_state["status"] = "running"
                    if cancelled.is_set() or self.stopping.is_set():
                        raise RuntimeError("操作员中断或节点已停止")
                    if not job_state["manual"] and cancelled.wait(.45):
                        raise RuntimeError("导入已中断")
                    with self.lock:
                        if (source, target) in self.blocked or not self.running(source):
                            raise RuntimeError("来源离线或迁移链路已断开")
                    if index == total:
                        code, result = self.call(target, "/internal/migration/commit", {"job": job})
                    else:
                        code, result = self.call(target, "/internal/migration/chunk",
                                                 {"job": job, "part": index,
                                                  "data": base64.b64encode(chunks[index]).decode()})
                    if code != 200:
                        raise RuntimeError(result.get("error", "目标拒收"))
                    with self.lock:
                        if index < total:
                            job_state["sent"] = index + 1
                            if pause_after == index + 1:
                                job_state["manual"] = True
                        self.invalidate()
                with self.lock:
                    job_state["status"] = "complete"
                self.record("migration", f"{target} 已校验并提交完整快照，开始补同步")
                self.kick_sync()
            except (OSError, ValueError, RuntimeError) as exc:
                with self.lock:
                    job_state.update(status="interrupted", error=str(exc))
                self.record("migration_interrupted", f"{target} 导入中断，暂存数据保留")

        self.migration_thread = threading.Thread(target=transfer, daemon=True)
        self.migration_thread.start()
        with self.lock:
            return dict(job_state)

    def control_migration(self, action, job=None):
        with self.changed:
            m = self.migration
            if not m or m["status"] not in ("paused", "running") or (job and job != m["job"]):
                raise ValueError("当前没有可操作的迁移任务")
            if action in ("step", "commit"):
                if m["status"] != "paused":
                    raise ValueError("请等待当前分片完成")
                if (m["sent"] == m["total"]) != (action == "commit"):
                    raise ValueError("先接收全部分片，再校验提交")
                self.migration_permit = True
                m["status"] = "running"
            elif action == "resume":
                m.update(manual=False, status="running")
            elif action == "pause":
                m["manual"] = True
            elif action == "cancel":
                self.migration_cancel.set()
            else:
                raise ValueError("未知迁移操作")
            self.invalidate()
            return dict(m)

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
            elif parsed.path == "/api/dispatch":
                self.send_json(lab.dispatch_view())
            elif parsed.path == "/api/health":
                self.send_json({"ok": True, "service": "cartlab"})
            elif parsed.path == "/api/cas":
                node = query.get("node", ["A"])[0]
                code, result = lab.call(node, "/cas?" + urlencode({"key": valid_id(query.get("key", ["example"])[0])}))
                self.send_json(result, code)
            elif parsed.path == "/guide":
                self.send_bytes((ROOT / "docs" / "实验讲解详解.html").read_bytes(), "text/html; charset=utf-8")
            elif parsed.path in ("/", "/free", "/free.js", "/free.css", "/app.js", "/scenarios.js", "/lessons.js", "/style.css"):
                file = {"/": "index.html", "/free": "free.html"}.get(parsed.path, parsed.path[1:])
                mime = "text/html" if file.endswith(".html") else "text/css" if file.endswith(".css") else "application/javascript"
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
                lab.enqueue(body["source"], body["carts"], body.get("reason", "repair"), body.get("causes", []),
                            sent_at=body.get("sent_at"))
                self.send_json({"accepted": True})
            elif self.path == "/api/operation":
                code, result = lab.operation(body)
                self.send_json(result, code)
            elif self.path == "/api/network":
                self.send_json(lab.configure_network(body))
            elif self.path == "/api/experiment/new":
                self.send_json(lab.new_experiment(seed=body.get("seed", True)))
            elif self.path == "/api/dispatch/send":
                self.send_json(lab.send_snapshot(body["source"], body["target"]))
            elif self.path == "/api/dispatch/clear":
                self.send_json(lab.clear_dispatch(body["cart_id"], body["scope"]))
            elif self.path == "/api/dispatch/packet":
                self.send_json(lab.adjust_packet(body))
            elif self.path == "/api/node":
                node = body["node"]
                with lab.lifecycle_lock:
                    if body["action"] == "stop":
                        lab.stop_node(node)
                    elif body["action"] == "start":
                        lab.start_node(node, joining=body.get("joining", node == "D"))
                    else:
                        raise ValueError("unknown action")
                self.send_json({"ok": True})
            elif self.path == "/api/fault/reply":
                lab.url(body["node"])
                with lab.lock:
                    if body.get("enabled", True):
                        lab.drop_reply.add(body["node"])
                    else:
                        lab.drop_reply.discard(body["node"])
                lab.record("fault", f"{body['node']} 的响应丢失设置已更新")
                self.send_json({"ok": True})
            elif self.path == "/api/fault/clock":
                code, result = lab.call(body["node"], "/internal/clock", {"offset": body["offset"]})
                if code == 200:
                    lab.record("clock", f"节点 {body['node']} 的实验时钟偏移设为 {body['offset']} 秒")
                self.send_json(result, code)
            elif self.path == "/api/cas":
                if "cart_id" in body:
                    award = {"cart_id": body["cart_id"], "node": body["node"], "request_id": uuid.uuid4().hex,
                             "operation": {"kind": "add", "sku": "apple"}}
                    cas = {field: body[field] for field in ("key", "expected", "value")}
                    code, result = lab.operation(award, cas=cas)
                else:
                    code, result = lab.call(body["node"], "/cas", body)
                self.send_json(result, code)
            elif self.path == "/api/migration/start":
                with lab.lifecycle_lock:
                    result = lab.begin_migration(fresh=bool(body.get("fresh")), pause_after=body.get("pause_after"),
                                                 source=body.get("source", "A"), target=body.get("target", "D"),
                                                 manual=bool(body.get("manual")))
                self.send_json(result)
            elif self.path in ("/api/migration/resume", "/api/migration/control"):
                with lab.lifecycle_lock:
                    self.send_json(lab.control_migration("resume" if self.path.endswith("/resume") else body["action"],
                                                         body.get("job")))
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
    parser.add_argument("--cart-id", type=valid_id, default="demo", help="open an existing cart on startup")
    parser.add_argument("--sync-interval", type=float, default=10.0)
    args = parser.parse_args()
    if args.sync_interval <= 0:
        parser.error("sync interval must be positive")
    lab = Lab(args.port, args.node_base_port, args.data_dir, args.sync_interval)
    lab.cart_id = args.cart_id
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
