import copy
import random
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from cartlab import crdt
from cartlab.storage import Store


def add(state, node, counter, rid, sku="apple"):
    return crdt.apply(state, {"kind": "add", "sku": sku}, rid, node, counter)[0]


class CRDTTests(unittest.TestCase):
    def test_concurrent_additions_preserve_both_quantities(self):
        a = add(crdt.empty(), "A", 1, "request-a")
        b = add(crdt.empty(), "B", 1, "request-b")
        self.assertEqual(crdt.public_view(crdt.merge(a, b))["quantity"], 2)
        self.assertEqual(crdt.relation(a["vv"], b["vv"]), "concurrent")

    def test_observed_remove_keeps_concurrent_add_and_later_delete_works(self):
        original = add(crdt.empty(), "A", 1, "old")
        deleted = crdt.apply(original, {"kind": "remove", "sku": "apple", "observed": crdt.live_tags(original)}, "remove-old", "A", 2)[0]
        added = add(original, "B", 1, "new")
        merged = crdt.merge(deleted, added)
        self.assertEqual(crdt.public_view(merged)["quantity"], 1)
        final = crdt.apply(merged, {"kind": "remove", "sku": "apple", "observed": crdt.live_tags(merged)}, "remove-new", "A", 3)[0]
        self.assertEqual(crdt.public_view(crdt.merge(final, original))["quantity"], 0)

    def test_two_removals_of_same_unit_remove_only_one_unit(self):
        state = add(add(crdt.empty(), "A", 1, "one"), "A", 2, "two")
        operation = {"kind": "remove", "sku": "apple", "observed": crdt.live_tags(state)[:1]}
        a = crdt.apply(state, operation, "remove-a", "A", 3)[0]
        b = crdt.apply(state, operation, "remove-b", "B", 1)[0]
        self.assertEqual(crdt.public_view(crdt.merge(a, b))["quantity"], 1)

    def test_request_retry_at_isolated_nodes_has_one_item_effect(self):
        a = add(crdt.empty(), "A", 1, "same-request")
        b = add(crdt.empty(), "B", 1, "same-request")
        self.assertEqual(crdt.public_view(crdt.merge(a, b))["quantity"], 1)
        retry, duplicate = crdt.apply(a, {"kind": "add", "sku": "apple"}, "same-request", "A", 2)
        self.assertTrue(duplicate)
        self.assertEqual(a, retry)

    def test_request_id_misuse_rejected_locally_and_detected_after_partition(self):
        a = add(crdt.empty(), "A", 1, "reused")
        with self.assertRaisesRegex(ValueError, "reused"):
            add(a, "A", 2, "reused", "milk")
        b = add(crdt.empty(), "B", 1, "reused", "milk")
        self.assertEqual(crdt.public_view(crdt.merge(a, b))["request_conflicts"], ["reused"])

    def test_random_message_reordering_duplication_and_join_laws(self):
        rng = random.Random(20261121)
        replicas = {actor: crdt.empty() for actor in "ABC"}
        snapshots = []
        counters = dict.fromkeys("ABC", 0)
        for index in range(90):
            actor = rng.choice("ABC")
            counters[actor] += 1
            current = replicas[actor]
            tags = crdt.live_tags(current, "apple")
            operation = {"kind": "remove", "sku": "apple", "observed": tags[:1]} if tags and rng.random() < .35 else {"kind": "add", "sku": "apple"}
            replicas[actor] = crdt.apply(current, operation, f"request-{index}", actor, counters[actor])[0]
            if rng.random() < .3:
                other = rng.choice("ABC")
                replicas[other] = crdt.merge(replicas[other], replicas[actor])
            snapshots.append(copy.deepcopy(replicas[actor]))
        a,b,c = replicas.values()
        self.assertEqual(crdt.merge(a,b), crdt.merge(b,a))
        self.assertEqual(crdt.merge(crdt.merge(a,b),c), crdt.merge(a,crdt.merge(b,c)))
        self.assertEqual(crdt.merge(a,a), a)
        expected = crdt.merge(crdt.merge(a,b),c)
        for _ in range(5):
            shuffled = snapshots * 3
            rng.shuffle(shuffled)
            state = crdt.empty()
            for snapshot in shuffled:
                state = crdt.merge(state,snapshot)
            self.assertEqual(state,expected)

    def test_vector_partial_order(self):
        self.assertEqual(crdt.relation({"A":1},{"A":2,"B":1}), "before")
        self.assertEqual(crdt.relation({"A":2,"B":1},{"A":1}), "after")
        self.assertEqual(crdt.relation({"A":1},{"A":1}), "equal")


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "node.sqlite3"
        self.store = Store(self.path,"A")

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_restart_preserves_receipt_clock_actor_and_tombstones(self):
        first = self.store.operate("cart", {"kind":"add","sku":"milk"}, "add-once")
        self.store.operate("cart", {"kind":"remove","sku":"milk","observed":first["cart"]["items"][0]["tags"]}, "remove-once")
        actor, clock = self.store.actor, self.store.meta("counter")
        self.store.close()
        self.store = Store(self.path,"A")
        self.assertEqual(self.store.actor,actor)
        self.assertEqual(self.store.meta("counter"),clock)
        retry = self.store.operate("cart", {"kind":"add","sku":"milk"}, "add-once")
        self.assertTrue(retry["duplicate"])
        self.assertEqual(retry["cart"]["quantity"],0)

    def test_failed_operation_does_not_advance_counter_or_write_partial_state(self):
        with self.assertRaises(ValueError):
            self.store.operate("cart", {"kind":"remove","sku":"milk","observed":["unknown"]}, "bad")
        self.assertEqual(self.store.meta("counter"),"0")
        self.assertEqual(self.store.read("cart"),crdt.empty())

    def test_cas_competing_threads_exactly_one_wins(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda v:self.store.compare_swap("key",0,v),["phone","desktop"]))
        self.assertEqual(sum(r["swapped"] for r in results),1)

    def test_incomplete_migration_is_not_visible_and_checksum_is_checked(self):
        self.store.close()
        self.store = Store(Path(self.temp.name)/"joining.sqlite3","D",joining=True)
        self.store.migration_begin("job",2,"bad-hash")
        self.store.migration_chunk("job",0,"e30=")
        with self.assertRaisesRegex(ValueError,"incomplete"):
            self.store.migration_commit("job")
        self.assertFalse(self.store.ready)
        with self.assertRaises(RuntimeError):
            self.store.operate("cart",{"kind":"add","sku":"milk"},"not-ready")
        self.store.migration_chunk("job",1,"IA==")
        with self.assertRaisesRegex(ValueError,"checksum"):
            self.store.migration_commit("job")
        self.assertEqual(self.store.snapshot(),{})


if __name__ == "__main__":
    unittest.main()
