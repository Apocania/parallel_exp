"""Bounded, in-memory teaching evidence. Never used to decide CRDT state."""
from collections import OrderedDict, deque
from copy import deepcopy
import time

from .crdt import fingerprint, normalize_operation


class MessageJournal:
    """Called under the supervisor lock; pins useful messages to user intents."""
    def __init__(self):
        self.operations = OrderedDict()
        self.raw = deque(maxlen=160)
        self.background = {"broadcasts": 0, "delivered": 0, "changed": 0, "dropped": 0}

    def begin(self, cart_id, request_id, node, operation):
        operation = normalize_operation(operation)
        digest = fingerprint(operation)
        key = (cart_id, request_id, digest)
        if key not in self.operations:
            self.operations[key] = {
                "cart_id": cart_id, "request_id": request_id, "fingerprint": digest,
                "node": node, "operation": operation, "created_at": time.time(),
                "attempts": [], "stored_on": [], "traces": [],
            }
            while len(self.operations) > 100:
                self.operations.popitem(last=False)
        record = self.operations[key]
        attempt = {"node": node, "status": "sending", "started_at": time.time()}
        record["attempts"].append(attempt)
        return record, attempt

    def finish(self, record, attempt, status, duplicate=False):
        attempt.update(status=status, duplicate=duplicate, finished_at=time.time())
        if status in ("acknowledged", "reply_lost") and attempt["node"] not in record["stored_on"]:
            record["stored_on"].append(attempt["node"])
            record["stored_on"].sort()

    def matching(self, cart_id, carts):
        receipts = carts.get(cart_id, {}).get("requests", {})
        return [r for r in self.operations.values() if r["cart_id"] == cart_id
                and r["fingerprint"] in receipts.get(r["request_id"], [])]

    def attach(self, trace, carts):
        self.raw.append(trace)
        tracked = False
        for record in self.matching(trace["cart_id"], carts):
            caused = record["request_id"] in trace["causes"]
            missing = trace["target"] not in record["stored_on"]
            if not caused and not missing:
                continue
            same_link = [t for t in record["traces"]
                         if (t["source"], t["target"]) == (trace["source"], trace["target"])]
            # Preserve all copies in the chosen group, and a later recovery
            # after a real failure. Periodic duplicates cannot replace evidence.
            same_group = any(t["group"] == trace["group"] for t in same_link)
            explicit = caused and trace["reason"] in ("write", "retry")
            pending = any(t["status"] == "queued" for t in same_link)
            failed = bool(same_link and same_link[-1]["status"] in
                          ("blocked", "offline", "dropped", "rejected"))
            recovery = missing and not pending and (not same_link or
                       (failed and trace["status"] == "queued"))
            if len(record["traces"]) < 48 and (explicit or same_group or recovery):
                record["traces"].append(trace)
                tracked = True
        trace["tracked"] = tracked
        return tracked

    def delivered(self, trace, carts):
        changed = False
        for record in self.matching(trace["cart_id"], carts):
            if trace["target"] not in record["stored_on"]:
                record["stored_on"].append(trace["target"])
                record["stored_on"].sort()
                changed = True
        return changed

    def snapshot(self, cart_id):
        return deepcopy({
            "operations": [r for r in self.operations.values() if r["cart_id"] == cart_id],
            "transmissions": [t for t in self.raw if t["cart_id"] == cart_id],
            "background": self.background,
        })
