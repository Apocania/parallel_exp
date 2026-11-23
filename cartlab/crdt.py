"""State-based observed-remove multiset: each add represents one item unit.

Only monotone metadata is replicated. Deletes retain observed tags forever;
classroom-sized experiments deliberately do not implement garbage collection.
"""
from __future__ import annotations

import hashlib
import json

CATALOG = {
    "milk": {"name": "鲜牛奶", "emoji": "🥛", "price": 1200},
    "bread": {"name": "全麦面包", "emoji": "🍞", "price": 1800},
    "apple": {"name": "红苹果", "emoji": "🍎", "price": 600},
    "cookie": {"name": "曲奇饼干", "emoji": "🍪", "price": 2400},
    "coffee": {"name": "咖啡豆", "emoji": "☕", "price": 5800},
    "orange": {"name": "鲜橙", "emoji": "🍊", "price": 800},
}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def empty():
    return {"adds": {}, "removed": [], "vv": {}, "requests": {}}


def validate(state):
    if not isinstance(state, dict) or set(state) != {"adds", "removed", "vv", "requests"}:
        raise ValueError("invalid CRDT state")
    if not all(isinstance(state[k], dict) for k in ("adds", "vv", "requests")):
        raise ValueError("invalid state maps")
    if not isinstance(state["removed"], list) or not all(isinstance(t, str) for t in state["removed"]):
        raise ValueError("invalid removed tags")
    for tag, sku in state["adds"].items():
        if not isinstance(tag, str) or sku not in CATALOG:
            raise ValueError("invalid addition")
    for actor, count in state["vv"].items():
        if not isinstance(actor, str) or type(count) is not int or count < 0:
            raise ValueError("invalid version vector")
    for rid, variants in state["requests"].items():
        if not isinstance(rid, str) or not isinstance(variants, list) or not all(isinstance(v, str) for v in variants):
            raise ValueError("invalid request receipts")


def merge(left, right):
    """Join: union facts/receipts, componentwise maximum of version vectors."""
    validate(left)
    validate(right)
    adds = dict(left["adds"])
    for tag, sku in right["adds"].items():
        if tag in adds and adds[tag] != sku:
            raise ValueError("a unique add tag cannot identify two products")
        adds[tag] = sku
    requests = {key: set(value) for key, value in left["requests"].items()}
    for key, value in right["requests"].items():
        requests.setdefault(key, set()).update(value)
    return {
        "adds": adds,
        "removed": sorted(set(left["removed"]) | set(right["removed"])),
        "vv": {key: max(left["vv"].get(key, 0), right["vv"].get(key, 0))
               for key in left["vv"].keys() | right["vv"].keys()},
        "requests": {key: sorted(value) for key, value in requests.items()},
    }


def relation(left, right):
    keys = left.keys() | right.keys()
    less = any(left.get(k, 0) < right.get(k, 0) for k in keys)
    greater = any(left.get(k, 0) > right.get(k, 0) for k in keys)
    if less and greater:
        return "concurrent"
    if less:
        return "before"
    if greater:
        return "after"
    return "equal"


def live_tags(state, sku=None):
    removed = set(state["removed"])
    return sorted(tag for tag, product in state["adds"].items()
                  if tag not in removed and (sku is None or sku == product))


def normalize_operation(operation):
    if not isinstance(operation, dict):
        raise ValueError("operation must be an object")
    kind, sku = operation.get("kind"), operation.get("sku")
    if kind not in ("add", "remove") or sku not in CATALOG:
        raise ValueError("unknown operation or product")
    observed = operation.get("observed", [])
    if not isinstance(observed, list) or not all(isinstance(t, str) for t in observed):
        raise ValueError("observed must be a list of add tags")
    if kind == "add" and observed:
        raise ValueError("add does not accept observed tags")
    return {"kind": kind, "sku": sku, "observed": sorted(set(observed))}


def fingerprint(operation):
    return hashlib.sha256(canonical(operation).encode()).hexdigest()


def apply(state, operation, request_id, actor, counter):
    operation = normalize_operation(operation)
    digest = fingerprint(operation)
    if request_id in state["requests"]:
        if state["requests"][request_id] != [digest]:
            raise ValueError("request_id reused with a different operation")
        return state, True
    if operation["kind"] == "remove":
        for tag in operation["observed"]:
            if state["adds"].get(tag) != operation["sku"]:
                raise ValueError("unknown observed tag; read this node before removing")
    result = merge(state, empty())
    if operation["kind"] == "add":
        # The same request retried at an isolated second node creates the SAME
        # item unit. The fingerprint also exposes ID misuse without tag clashes.
        tag = request_id + ":" + digest[:20]
        result["adds"][tag] = operation["sku"]
    else:
        result["removed"] = sorted(set(result["removed"]) | set(operation["observed"]))
    result["vv"][actor] = counter
    result["requests"][request_id] = [digest]
    return result, False


def public_view(state):
    items = []
    for sku, product in CATALOG.items():
        tags = live_tags(state, sku)
        if tags:
            items.append({"sku": sku, **product, "quantity": len(tags), "tags": tags})
    return {
        "items": items,
        "quantity": sum(item["quantity"] for item in items),
        "total_cents": sum(item["quantity"] * item["price"] for item in items),
        "vv": state["vv"],
        "add_count": len(state["adds"]),
        "tombstone_count": len(state["removed"]),
        "request_count": len(state["requests"]),
        "request_conflicts": sorted(k for k, v in state["requests"].items() if len(v) > 1),
        "state_hash": hashlib.sha256(canonical(state).encode()).hexdigest(),
        "raw": state,
    }
