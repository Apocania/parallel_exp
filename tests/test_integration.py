"""Real independent-process tests. No mock store and no fabricated outcomes."""
import json
import os
import random
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from urllib.request import urlopen
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

from cartlab.http_util import request

ROOT = Path(__file__).resolve().parent.parent


class LabCase(unittest.TestCase):
    SYNC_INTERVAL = .35
    REPORT = "integration-results.json"
    @classmethod
    def setUpClass(cls):
        (ROOT / ".test-data").mkdir(exist_ok=True)
        cls.temp = tempfile.TemporaryDirectory(prefix="integration-", dir=ROOT / ".test-data")
        for _ in range(100):
            # This Windows host uses 1024..60000 for outgoing ephemeral TCP.
            # Do not select bind(0)'s neighbours: startup HTTP calls can claim
            # the next node's port between probing and process startup.
            port = random.SystemRandom().randrange(60100, 64900)
            sockets = []
            try:
                for candidate in range(port, port + 5):
                    s = socket.socket()
                    sockets.append(s)
                    s.bind(("127.0.0.1", candidate))
                cls.port = port
                break
            except OSError:
                continue
            finally:
                for s in sockets:
                    s.close()
        else:
            raise RuntimeError("could not reserve test ports")
        cls.base = f"http://127.0.0.1:{cls.port}"
        cls.log = open(Path(cls.temp.name) / "supervisor.log", "w", encoding="utf-8")
        flags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        cls.process = subprocess.Popen([sys.executable, "-u", "run.py", "--port", str(cls.port),
                                       "--node-base-port", str(cls.port + 1), "--data-dir", cls.temp.name,
                                       "--sync-interval", str(cls.SYNC_INTERVAL)], cwd=ROOT, stdout=cls.log, stderr=cls.log,
                                      creationflags=flags)
        cls.results = []
        try:
            for _ in range(100):
                if cls.process.poll() is not None:
                    raise RuntimeError("lab startup failed")
                try:
                    if request(cls.base + "/api/health", timeout=.3)[0] == 200:
                        return
                except OSError:
                    time.sleep(.1)
            raise RuntimeError("lab startup timed out")
        except Exception:
            cls.process.terminate()
            cls.log.close()
            raise

    @classmethod
    def tearDownClass(cls):
        try:
            request(cls.base + "/api/lab/shutdown", {}, timeout=3)
            cls.process.wait(timeout=10)
        finally:
            if cls.process.poll() is None:
                cls.process.kill()
            cls.log.close()
        path = ROOT / "artifacts" / cls.REPORT
        path.parent.mkdir(exist_ok=True)
        path.write_text(json.dumps({
            "run_at": datetime.now(timezone(timedelta(hours=8))).isoformat(),
            "python": sys.version.split()[0], "platform": sys.platform,
            "transport": "real HTTP between independent node processes; faults injected by relay",
            "sync_interval_seconds": cls.SYNC_INTERVAL, "tests": cls.results,
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        cls.temp.cleanup()

    def setUp(self):
        self.started = time.monotonic()
        self.evidence = {}
        self.addCleanup(self.record_result)
        code, initial = request(self.base + "/api/experiment/new", {}, timeout=6)
        self.assertEqual(code, 200, initial)
        self.cart_id = initial["cart_id"]
        self.wait_converged()

    def record_result(self):
        result = self._outcome.result
        failed = any(test is self for test, _ in result.failures + result.errors)
        self.results.append({"name": self._testMethodName, "description": self._testMethodDoc,
                             "status": "failed" if failed else "passed",
                             "elapsed_seconds": round(time.monotonic() - self.started, 3),
                             "cart_id": getattr(self, "cart_id", None), "evidence": self.evidence})

    def call(self, path, body=None):
        return request(self.base + path, body, timeout=5)

    def state(self):
        return self.call("/api/status?cart_id=" + self.cart_id)[1]

    def node(self, name):
        return next(n for n in self.state()["nodes"] if n["node"] == name)

    def operate(self, node, kind="add", sku="apple", observed=None, request_id=None, cart_id=None):
        return self.call("/api/operation", {"node": node, "cart_id": cart_id or self.cart_id,
            "request_id": request_id or uuid.uuid4().hex,
            "operation": {"kind": kind, "sku": sku, "observed": observed or []}})

    def until(self, predicate, timeout=12):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(.12)
        self.fail("condition did not become true before deadline")

    def wait_converged(self, timeout=12, nodes="ABC"):
        def check():
            state = self.state()
            expected = [n for n in state["nodes"] if n["node"] in nodes]
            if all(n.get("online") and n.get("ready") for n in expected) and len({n["cart"]["state_hash"] for n in expected}) == 1:
                return state
        try:
            return self.until(check, timeout)
        except AssertionError:
            latest=self.state()
            diagnostic={"network":latest["network"],"nodes":[{"node":n["node"],"ready":n.get("ready"),"cart":n.get("cart")} for n in latest["nodes"]]}
            self.evidence["convergence_timeout"]=diagnostic
            self.fail("convergence timeout: "+json.dumps(diagnostic,ensure_ascii=True))

    def quantity(self, node, sku):
        return next((i["quantity"] for i in node["cart"]["items"] if i["sku"] == sku), 0)


class DistributedIntegrationTests(LabCase):
    def test_10_partition_accepts_both_writes_and_converges(self):
        """分区两侧可写，恢复后不同商品均保留。"""
        self.call("/api/network", {"isolate":"A"})
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(self.operate,"A",sku="apple"), pool.submit(self.operate,"B",sku="cookie")]
            codes = [f.result()[0] for f in futures]
        self.assertEqual(codes,[200,200])
        a,b = self.node("A"),self.node("B")
        self.assertEqual(self.quantity(a,"cookie"),0)
        self.assertEqual(self.quantity(b,"apple"),0)
        self.evidence["partition_vectors"] = {"A":a["cart"]["vv"],"B":b["cart"]["vv"]}
        self.call("/api/network", {"heal":True})
        final = self.wait_converged()
        self.assertEqual(final["nodes"][0]["cart"]["quantity"],3)
        self.evidence["final_items"] = final["nodes"][0]["cart"]["items"]

    def test_20_concurrent_add_remove_and_later_delete(self):
        """并发新添加存活；观察新标记后可删除，旧状态不会复活商品。"""
        _, initial = self.operate("A")
        tags = next(i["tags"] for i in initial["cart"]["items"] if i["sku"]=="apple")
        self.wait_converged()
        old = self.node("A")["cart"]["raw"]
        self.call("/api/network", {"isolate":"A"})
        self.assertEqual(self.operate("A","remove",observed=tags)[0],200)
        self.assertEqual(self.operate("B")[0],200)
        self.call("/api/network", {"heal":True})
        self.wait_converged()
        a = self.node("A")
        self.assertEqual(self.quantity(a,"apple"),1)
        tags = next(i["tags"] for i in a["cart"]["items"] if i["sku"]=="apple")
        self.operate("A","remove",observed=tags)
        self.wait_converged()
        # Deliberately deliver an old pre-deletion full snapshot again.
        self.call("/api/transport", {"source":"B","carts":{self.cart_id:old}})
        time.sleep(.3)
        self.assertEqual(self.quantity(self.node("A"),"apple"),0)
        self.evidence = {"concurrent_add_survived":True,"later_delete_quantity":0,"old_snapshot_did_not_resurrect":True}

    def test_30_lost_response_retry_and_restart_deduplication(self):
        """写成功后丢响应，重试及重启后重试均不重复添加。"""
        rid = uuid.uuid4().hex
        self.call("/api/fault/reply", {"node":"A"})
        code, result = self.operate("A",request_id=rid)
        self.assertEqual(code,504)
        self.assertTrue(result["ambiguous"])
        code, result = self.operate("A",request_id=rid)
        self.assertEqual(code,200)
        self.assertTrue(result["duplicate"])
        self.call("/api/node", {"node":"A","action":"stop"})
        self.call("/api/node", {"node":"A","action":"start"})
        _, after = self.operate("A",request_id=rid)
        self.assertTrue(after["duplicate"])
        self.assertEqual(next(i["quantity"] for i in after["cart"]["items"] if i["sku"]=="apple"),1)
        self.evidence = {"first_response":504,"retry_duplicate":True,"retry_after_restart_duplicate":True,"apple_quantity":1}

    def test_35_retry_at_other_isolated_node_has_one_effect(self):
        """同一个添加请求跨分区重试，合并后仍只有一件商品。"""
        self.call("/api/network", {"isolate":"A"})
        rid=uuid.uuid4().hex
        self.assertEqual(self.operate("A",request_id=rid)[0],200)
        self.assertEqual(self.operate("B",request_id=rid)[0],200)
        self.call("/api/network", {"heal":True})
        self.wait_converged()
        self.assertEqual(self.quantity(self.node("A"),"apple"),1)
        self.evidence={"cross_node_retry_quantity":1}

    def test_40_ten_second_delay_reordering_and_triplicate_delivery(self):
        """消息延迟至少 10 秒、重复三次且乱序，最终仍收敛。"""
        self.call("/api/network", {"delay_ms":10000,"copies":3,"reorder":True})
        started=time.monotonic()
        _, result=self.operate("A")
        tags=next(i["tags"] for i in result["cart"]["items"] if i["sku"]=="apple")
        self.call("/api/transport", {"source":"A","carts":{self.cart_id:result["cart"]["raw"]}})
        self.assertEqual(self.quantity(self.node("B"),"apple"),0)
        self.operate("A","remove",observed=tags)
        self.operate("B",sku="bread")
        before=self.state()["network"]["counters"]
        final=self.wait_converged(timeout=35)
        elapsed=time.monotonic()-started
        self.assertGreaterEqual(elapsed,9.5)
        self.assertEqual(self.quantity(final["nodes"][0],"apple"),0)
        self.assertEqual(self.quantity(final["nodes"][0],"bread"),1)
        self.until(lambda:self.state()["network"]["counters"]["overtaken"]>before["overtaken"],timeout=8)
        self.evidence={"convergence_seconds":round(elapsed,3),"configured_delay_ms":10000,"copies":3,
                       "observed_out_of_order":self.state()["network"]["counters"]["overtaken"]-before["overtaken"],
                       "deleted_apple_quantity":0,"bread_quantity":1}
        self.call("/api/network", {"heal":True})

    def test_50_crash_recovery_preserves_local_write_and_catches_up(self):
        """停止副本后其余节点继续写；重启恢复本地提交并补同步。"""
        self.call("/api/network", {"isolate":"A"})
        _, receipt=self.operate("A",sku="coffee")
        self.assertEqual(receipt["ack"]["durable_replicas"],1)
        self.call("/api/node", {"node":"A","action":"stop"})
        self.assertFalse(self.node("A")["online"])
        self.assertEqual(self.operate("B",sku="orange")[0],200)
        self.call("/api/node", {"node":"A","action":"start"})
        self.assertEqual(self.quantity(self.node("A"),"coffee"),1)
        self.call("/api/network", {"heal":True})
        self.wait_converged()
        a=self.node("A")
        self.assertEqual(self.quantity(a,"orange"),1)
        self.evidence={"ack_replicas":1,"local_coffee_recovered":True,"remote_orange_caught_up":True}

    def test_60_clock_rollback_does_not_reverse_causality(self):
        """墙上时间回拨 60 秒，逻辑计数仍递增。"""
        before=self.node("A")
        self.call("/api/fault/clock", {"node":"A","offset":-60})
        self.operate("A",sku="bread")
        after=self.node("A")
        actor=before["actor"]
        self.assertLess(after["wall_time"],before["wall_time"]-55)
        self.assertGreater(after["cart"]["vv"][actor],before["cart"]["vv"].get(actor,0))
        self.wait_converged()
        self.evidence={"clock_offset_seconds":-60,"counter_before":before["cart"]["vv"].get(actor,0),
                       "counter_after":after["cart"]["vv"][actor]}

    def test_70_local_cas_race_and_cross_node_limit(self):
        """单节点 CAS 只有一方成功；不同节点本地 CAS 可以同时成功。"""
        key="cas-"+uuid.uuid4().hex
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda v:self.call("/api/cas",{"node":"A","key":key,"expected":0,"value":v}),["one","two"]))
        self.assertEqual(sorted(code for code,_ in results),[200,409])
        self.call("/api/network", {"isolate":"A"})
        key="independent-"+uuid.uuid4().hex
        results=[self.call("/api/cas",{"node":n,"key":key,"expected":0,"value":n})[0] for n in "AB"]
        self.assertEqual(results,[200,200])
        self.evidence={"same_node_status_codes":[200,409],"separate_node_status_codes":results,
                       "guarantee":"single-node atomicity only"}

    def test_cas_apple_awards_visible_in_cart(self):
        """同节点发一个苹果，跨节点分区发放后合并出两个苹果。"""
        for nodes, count in [("AA", 1), ("AB", 2)]:
            code, initial = self.call("/api/experiment/new", {"seed": False})
            self.assertEqual(code, 200)
            self.cart_id = initial["cart_id"]
            if nodes == "AB":
                self.call("/api/network", {"isolate": "A"})
            with ThreadPoolExecutor(max_workers=2) as pool:
                claims = [pool.submit(self.call, "/api/cas", {
                    "node": node, "cart_id": self.cart_id, "key": self.cart_id + ".apple",
                    "expected": 0, "value": client,
                }) for node, client in zip(nodes, ["phone", "desktop"])]
                codes = sorted(claim.result()[0] for claim in claims)
            self.assertEqual(codes, [200, 409] if nodes == "AA" else [200, 200])
            if nodes == "AB":
                for node in nodes:
                    self.assertEqual(self.quantity(self.node(node), "apple"), 1)
                self.call("/api/network", {"heal": True})
            state = self.wait_converged()
            for node in state["nodes"]:
                if node["node"] in "ABC":
                    self.assertEqual(self.quantity(node, "apple"), count)

    def test_80_hot_key_exactly_half_of_concurrent_requests(self):
        """24 个并发请求中 12 个访问同一购物车，无丢失更新。"""
        hot="hot-"+uuid.uuid4().hex
        jobs=[hot if i%2==0 else "cold-"+uuid.uuid4().hex for i in range(24)]
        with ThreadPoolExecutor(max_workers=8) as pool:
            results=list(pool.map(lambda c:self.operate("A",cart_id=c),jobs))
        self.assertTrue(all(code==200 for code,_ in results))
        state=self.call("/api/status?cart_id="+hot)[1]
        self.assertEqual(state["nodes"][0]["cart"]["quantity"],12)
        self.evidence={"requests":24,"hot_key_requests":12,"hot_key_share":0.5,"errors":0,"hot_quantity":12}

    def test_90_migration_crash_staging_and_retry(self):
        """迁移中途宕机不暴露不完整副本；重试后加入并补齐快照后的更新。"""
        code,_=self.call("/api/migration/start",{"fresh":True,"pause_after":4})
        self.assertEqual(code,200)
        self.until(lambda:(self.state()["migration"] or {}).get("status")=="paused")
        status=self.state()["migration"]
        self.assertEqual(status["sent"],4)
        time.sleep(.7)
        self.assertEqual(self.state()["migration"]["sent"],4)
        self.assertFalse(self.node("D")["ready"])
        self.assertEqual(self.operate("D")[0],503)
        self.call("/api/node",{"node":"D","action":"stop"})
        self.until(lambda:self.state()["migration"]["status"]=="interrupted")
        self.call("/api/node",{"node":"D","action":"start"})
        self.assertFalse(self.node("D")["ready"])
        self.assertEqual(self.node("D")["cart"]["quantity"],0)
        self.assertEqual(self.call("/api/migration/start",{})[0],200)
        self.operate("B",sku="coffee")
        self.until(lambda:self.state()["migration"]["status"]=="complete",timeout=15)
        self.wait_converged(nodes="ABCD")
        self.assertEqual(self.quantity(self.node("D"),"coffee"),1)
        # A completed run must be repeatable without deleting the original DB.
        self.assertEqual(self.call("/api/migration/start",{"fresh":True,"pause_after":4})[0],200)
        self.until(lambda:self.state()["migration"]["status"]=="paused")
        self.assertFalse(self.node("D")["ready"])
        self.call("/api/migration/resume",{})
        self.until(lambda:self.state()["migration"]["status"]=="complete",timeout=15)
        self.wait_converged(nodes="ABCD")
        self.assertEqual(len(list((Path(self.temp.name)/"import-runs").glob("*.sqlite3"))),2)
        self.evidence={"chunks_before_crash":status["sent"],"total_chunks":status["total"],
                       "incomplete_node_refused_write":True,"ready_after_retry":True,"post_snapshot_update_caught_up":True,
                       "paused_until_action":True,"fresh_run_repeatable_and_previous_database_preserved":True}

    def test_95_new_experiment_cancels_paused_import_and_clears_faults(self):
        """切换实验会解除故障，取消暂停导入，保留旧购物车。"""
        old_cart=self.cart_id
        self.call("/api/migration/start",{"fresh":True,"pause_after":4})
        self.until(lambda:self.state()["migration"]["status"]=="paused")
        self.call("/api/network",{"isolate":"A","copies":3,"delay_ms":10000,"reorder":True})
        self.call("/api/fault/clock",{"node":"A","offset":-60})
        self.call("/api/fault/reply",{"node":"A"})
        code,created=self.call("/api/experiment/new",{})
        self.assertEqual(code,200)
        self.cart_id=created["cart_id"]
        s=self.wait_converged()
        self.assertNotEqual(self.cart_id,old_cart)
        self.assertIsNone(s["migration"])
        self.assertEqual(s["drop_reply"],[])
        self.assertEqual(s["network"]["blocked"],[])
        self.assertEqual(s["network"]["delay_ms"],0)
        self.assertEqual(s["network"]["copies"],1)
        self.assertFalse(self.node("D")["online"])
        self.assertEqual(self.node("A")["clock_offset"],0)
        self.assertEqual(self.quantity(self.node("A"),"milk"),1)
        old=self.call("/api/status?cart_id="+old_cart)[1]
        self.assertEqual(self.quantity(old["nodes"][0],"milk"),1)
        self.evidence={"faults_cleared":True,"paused_import_cancelled":True,"old_cart_preserved":True}


class ImmediateSyncTests(LabCase):
    # A 60-second heartbeat makes reliance on periodic polling fail these tests.
    SYNC_INTERVAL = 60
    REPORT = "realtime-results.json"

    def test_10_bidirectional_writes_do_not_wait_for_heartbeat(self):
        """心跳设为 60 秒，两端添加和删除仍在 3 秒内传播。"""
        measured=[]
        for origin,kind in [("A","add"),("B","remove")]:
            tags=[]
            if kind=="remove":
                tags=next(i["tags"] for i in self.node("B")["cart"]["items"] if i["sku"]=="apple")
            started=time.monotonic()
            self.assertEqual(self.operate(origin,kind=kind,observed=tags)[0],200)
            s=self.wait_converged(timeout=3)
            self.assertEqual(self.quantity(s["nodes"][0],"apple"),1 if kind=="add" else 0)
            measured.append(round((time.monotonic()-started)*1000,2))
        self.evidence={"heartbeat_seconds":60,"write_to_observed_convergence_ms":measured}

    def test_20_heal_triggers_catchup_without_new_write(self):
        """恢复通信立即触发补同步，无需新操作或等待心跳。"""
        self.call("/api/network",{"isolate":"A"})
        self.operate("A")
        self.operate("B",sku="cookie")
        self.assertEqual(self.quantity(self.node("A"),"cookie"),0)
        started=time.monotonic()
        self.call("/api/network",{"heal":True})
        s=self.wait_converged(timeout=3)
        self.assertEqual(s["nodes"][0]["cart"]["quantity"],3)
        self.evidence={"heal_to_observed_convergence_ms":round((time.monotonic()-started)*1000,2)}

    def test_30_restart_triggers_catchup_without_heartbeat(self):
        """重启 A 后立即补齐宕机期间在 B 的写入。"""
        self.call("/api/node",{"node":"A","action":"stop"})
        self.operate("B",sku="bread")
        self.wait_converged(nodes="BC")
        self.call("/api/node",{"node":"A","action":"start"})
        self.wait_converged(timeout=3)
        self.assertEqual(self.quantity(self.node("A"),"bread"),1)
        self.evidence={"restart_caught_up_without_heartbeat":True}

    def test_40_event_stream_notifies_real_changes(self):
        """浏览器事件流在写入后推送版本通知，实际副本能读到更新。"""
        with urlopen(self.base+"/api/events",timeout=3) as stream:
            self.assertIn("text/event-stream",stream.headers["Content-Type"])
            def next_revision():
                event = None
                while True:
                    line=stream.readline().decode()
                    if line.startswith("event:"):
                        event=line[6:].strip()
                    if line.startswith("data:") and event == "change":
                        return json.loads(line[5:])["revision"]
                    if not line:
                        self.fail("event stream closed unexpectedly")
            first=next_revision()
            self.operate("A",sku="orange")
            updated=next_revision()
            self.assertGreater(updated,first)
            self.wait_converged(timeout=3)
            self.assertEqual(self.quantity(self.node("B"),"orange"),1)
        self.evidence={"first_revision":first,"notification_revision":updated,"peer_read_confirmed":True}

    def test_50_explicit_reordering_without_heartbeat(self):
        """心跳 60 秒，指定的新删除快照先到、旧添加快照后到，苹果不复活。"""
        self.call("/api/network", {"delay_ms":10000,"reorder":True})
        _, added=self.operate("A")
        def packet(rid):
            records=self.call("/api/messages")[1]["operations"]
            record=next((o for o in records if o["request_id"]==rid),None)
            return next((t for t in record["traces"] if t["source"]=="A" and t["target"]=="B" and t["reason"]=="write"),None) if record else None
        old=self.until(lambda:packet(added["request_id"]))
        tags=next(i["tags"] for i in added["cart"]["items"] if i["sku"]=="apple")
        _, removed=self.operate("A","remove",observed=tags)
        fresh=self.until(lambda:packet(removed["request_id"]))
        self.assertEqual(self.node("B")["cart"]["tombstone_count"],0)
        self.until(lambda:packet(added["request_id"])["status"]=="delivered" and
                   packet(removed["request_id"])["status"]=="delivered",timeout=20)
        old,fresh=packet(added["request_id"]),packet(removed["request_id"])
        self.assertGreater(old["delivered_at"],fresh["delivered_at"])
        self.wait_converged(timeout=3)
        self.assertEqual(self.quantity(self.node("B"),"apple"),0)
        self.evidence={"old_group":old["group"],"new_group":fresh["group"],"new_arrived_first":True,
                       "late_old_snapshot_resurrected_apple":False,"heartbeat_seconds":60}


class MessageEvidenceTests(LabCase):
    SYNC_INTERVAL = .5
    REPORT = "message-results.json"

    def records(self):
        return self.call("/api/messages")[1]

    def test_10_idle_repairs_do_not_replace_or_notify_operation_evidence(self):
        """大量后台补同步继续运行，但关键记录和界面通知版本保持不变。"""
        _, result=self.operate("A",sku="orange")
        rid=result["request_id"]
        self.wait_converged()
        self.until(lambda:all(t["status"]!="queued" for o in self.records()["operations"] for t in o["traces"]))
        before=self.records()
        revision=self.state()["revision"]
        baseline=before["background"]["broadcasts"]
        self.until(lambda:self.records()["background"]["broadcasts"]>=baseline+90,timeout=25)
        after=self.records()
        self.assertEqual(before["operations"],after["operations"])
        self.assertEqual(before["message_revision"],after["message_revision"])
        self.assertEqual(revision,self.state()["revision"])
        record=next(o for o in after["operations"] if o["request_id"]==rid)
        self.assertEqual(record["stored_on"],list("ABC"))
        self.assertTrue(record["traces"])
        self.assertFalse(any(t["group"]==record["traces"][0]["group"] for t in after["transmissions"]))
        self.evidence={"background_broadcasts_observed":after["background"]["broadcasts"]-baseline,
                       "raw_ring_rotated":True,"pinned_operation_unchanged":True,"state_notifications_unchanged":True}

    def test_20_partition_recovery_preserves_failed_and_successful_evidence(self):
        """分区受阻记录保留，恢复后另有真实送达记录。"""
        self.call("/api/network",{"isolate":"A"})
        _,result=self.operate("A")
        rid=result["request_id"]
        def record():
            return next(o for o in self.records()["operations"] if o["request_id"]==rid)
        self.until(lambda:any(t["target"]=="B" and t["status"]=="blocked" for t in record()["traces"]))
        blocked=next(t["group"] for t in record()["traces"] if t["target"]=="B" and t["status"]=="blocked")
        self.call("/api/network",{"heal":True})
        self.wait_converged()
        self.until(lambda:record()["stored_on"]==list("ABC"))
        final=record()
        self.assertTrue(any(t["group"]==blocked and t["status"]=="blocked" for t in final["traces"]))
        self.assertTrue(any(t["target"]=="B" and t["status"]=="delivered" for t in final["traces"]))
        self.evidence={"failed_message_preserved":True,"recovery_delivery_observed":True}

    def test_30_three_copies_and_retry_are_distinct_evidence(self):
        """三份同步消息与同 ID 的两次客户端请求分别记录，商品仍只有一件。"""
        self.call("/api/network",{"copies":3})
        self.call("/api/fault/reply",{"node":"A"})
        rid=uuid.uuid4().hex
        self.assertEqual(self.operate("A",sku="orange",request_id=rid)[0],504)
        code,retried=self.operate("A",sku="orange",request_id=rid)
        self.assertEqual(code,200)
        self.assertTrue(retried["duplicate"])
        self.wait_converged()
        def record():
            return next(o for o in self.records()["operations"] if o["request_id"]==rid)
        self.until(lambda:all(t["status"]!="queued" for t in record()["traces"]))
        final=record()
        self.assertEqual([a["status"] for a in final["attempts"]],["reply_lost","acknowledged"])
        self.assertTrue(final["attempts"][1]["duplicate"])
        a_to_b=[t for t in final["traces"] if t["source"]=="A" and t["target"]=="B"]
        group=a_to_b[0]["group"]
        self.assertEqual([t["copy"] for t in a_to_b if t["group"]==group and t["status"]=="delivered"],[1,2,3])
        self.assertEqual(self.quantity(self.node("B"),"orange"),1)
        self.evidence={"client_attempts":2,"same_group_copies_delivered":3,"orange_quantity":1}


class FreeControlTests(LabCase):
    SYNC_INTERVAL = 60
    REPORT = "free-control-results.json"

    def ok(self, path, body=None):
        code, data = self.call(path, body)
        self.assertEqual(code, 200, data)
        return data

    def test_manual_reorder_copy_drop_and_delay(self):
        """只点选指定消息：先送删除、再送旧添加，复制和丢弃均能独立核对。"""
        self.ok("/api/network", {"manual": True, "repair_enabled": False})
        _, added = self.operate("A")
        def queued_write(removed):
            return next((p for p in self.ok("/api/dispatch")["pending"]
                         if (p["source"], p["target"], p["reason"]) == ("A", "B", "write")
                         and p["contents"]["removed"] == removed
                         and (removed or any(i["sku"] == "apple" for i in p["contents"]["items"]))), None)
        old = self.until(lambda: queued_write(0))
        tags = next(i["tags"] for i in added["cart"]["items"] if i["sku"] == "apple")
        self.ok("/api/fault/clock", {"node":"A","offset":-60})
        self.operate("A", "remove", "apple", tags)
        new = self.until(lambda: queued_write(1))
        self.assertGreater(old["sent_at"] - new["sent_at"], 50)
        self.assertGreaterEqual(new["due_at"], old["due_at"])
        self.assertEqual(self.node("B")["cart"]["tombstone_count"], 0)
        def adjust(packet, action, **extra):
            return self.ok("/api/dispatch/packet", {"id": packet["id"], "action": action, **extra})
        self.assertGreater(adjust(old, "delay", delay_ms=12000)["due_at"], time.time()+10)
        copy = adjust(new, "duplicate")
        self.assertEqual(copy["copied_from"], new["id"])
        self.assertEqual(copy["sent_at"], new["sent_at"])
        self.assertEqual(adjust(copy, "drop")["status"], "dropped")
        self.assertTrue(adjust(new, "deliver")["changed"])
        self.assertEqual(self.node("B")["cart"]["tombstone_count"], 1)
        late = adjust(old, "deliver")
        self.assertTrue(late["out_of_order"])
        self.assertFalse(late["changed"])
        self.assertEqual(self.quantity(self.node("B"), "apple"), 0)
        self.assertEqual(self.call("/api/dispatch/packet", {"id": old["id"], "action": "deliver"})[0], 400)

    def test_clear_dispatch_history_and_pending(self):
        """清理历史保留待投递包，批量丢弃可核对，旧实验不能清理新实验。"""
        self.ok("/api/network", {"manual": True, "repair_enabled": False})
        group = self.ok("/api/dispatch/send", {"source": "A", "target": "B"})["group"]
        packet = next(p for p in self.ok("/api/dispatch")["pending"] if p["group"] == group)
        copy = self.ok("/api/dispatch/packet", {"id": packet["id"], "action": "duplicate"})
        self.ok("/api/dispatch/packet", {"id": copy["id"], "action": "drop"})
        path = "/api/dispatch/clear"
        history = {"scope": "history", "cart_id": self.cart_id}
        self.assertGreater(self.ok(path, history)["cleared"], 0)
        self.assertEqual(self.ok(path, history)["cleared"], 0)
        self.assertEqual(self.call(path, {"scope": "pending", "cart_id": "stale"})[0], 400)
        view = self.ok("/api/dispatch")
        self.assertFalse(view["recent"])
        self.assertIn(packet["id"], [p["id"] for p in view["pending"]])
        result = self.ok(path, {"scope": "pending", "cart_id": self.cart_id})
        self.assertEqual(result["cleared"], len(view["pending"]))
        view = self.ok("/api/dispatch")
        self.assertEqual(view["network"]["queued"], 0)
        self.assertFalse(view["pending"])
        self.assertIn((packet["id"], "dropped"), [(p["id"], p["status"]) for p in view["recent"]])
        self.assertEqual(self.quantity(self.node("A"), "milk"), 1)

    def test_directed_rules_and_reset(self):
        """两条方向独立设置；复位清除积压与故障、启用三个默认节点并保留旧车。"""
        self.ok("/api/network", {"manual": True, "repair_enabled": False})
        self.ok("/api/network", {"link": {"source":"C", "target":"B", "open":False}})
        self.ok("/api/network", {"isolate":"A"})
        self.ok("/api/network", {"rule":{"source":"B","target":"C","copies":3,"delay_ms":12000,"drop_next":1}})
        group = self.ok("/api/dispatch/send", {"source":"B","target":"C"})["group"]
        q = self.ok("/api/dispatch")
        self.assertEqual(len([p for p in q["pending"] if p["group"] == group]), 2)
        self.assertEqual([p["status"] for p in q["recent"] if p["group"] == group], ["dropped"])
        # Reconnect after the manual send so recovery traffic cannot consume drop_next.
        net = self.ok("/api/network", {"reconnect":"A"})
        self.assertEqual(net["blocked"], [["C","B"]])
        self.ok("/api/fault/clock", {"node":"B","offset":-137})
        self.ok("/api/fault/reply", {"node":"C"})
        self.ok("/api/node", {"node":"A","action":"stop"})
        self.ok("/api/node", {"node":"D","action":"start","joining":False})
        old_id = self.cart_id
        self.cart_id = self.ok("/api/experiment/new", {"seed":False})["cart_id"]
        state = self.state()
        self.assertFalse(state["network"]["manual"])
        self.assertTrue(state["network"]["repair_enabled"])
        self.assertEqual(state["network"]["blocked"], [])
        self.assertEqual(state["network"]["rules"], [])
        self.assertEqual(state["drop_reply"], [])
        self.assertEqual(state["compared_nodes"], list("ABC"))
        self.assertTrue(all(n["cart"]["quantity"] == 0 and n["clock_offset"] == 0 for n in state["nodes"] if n["online"]))
        self.assertFalse(any(p["group"] == group for p in self.ok("/api/dispatch")["pending"]))
        self.assertEqual(self.ok("/api/status?cart_id="+old_id)["nodes"][0]["cart"]["quantity"], 1)

    def test_operator_migration_retry_and_explicit_commit(self):
        """B 向 C 导入；断链后分片中断，重取快照，收齐前后分别验证服务边界。"""
        config = {"source":"B", "target":"C", "fresh":True, "manual":True}
        m = self.ok("/api/migration/start", config)
        def control(action):
            return self.ok("/api/migration/control", {"job":m["job"],"action":action})
        def at(sent):
            latest = self.state()["migration"]
            return latest["sent"] == sent and latest["status"] == "paused"
        self.assertTrue(at(0))
        control("step")
        self.until(lambda: at(1))
        self.ok("/api/network", {"link":{"source":"B","target":"C","open":False}})
        control("step")
        self.until(lambda: self.state()["migration"]["status"] == "interrupted")
        self.assertEqual(self.operate("C")[0],503)
        self.ok("/api/network", {"link":{"source":"B","target":"C","open":True}})
        m=self.ok("/api/migration/start", {**config, "fresh":False})
        self.assertEqual(self.call("/api/migration/control", {"job":m["job"],"action":"commit"})[0],400)
        for sent in range(1,m["total"]+1):
            control("step")
            self.until(lambda: at(sent))
        self.assertFalse(self.node("C")["ready"])
        control("commit")
        self.until(lambda: self.state()["migration"]["status"] == "complete")
        self.assertTrue(self.node("C")["ready"])
        self.wait_converged()


if __name__ == "__main__":
    unittest.main()
