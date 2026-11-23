"""Reproducible small benchmark; 50% of operations target one hot cart."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
import json
import math
from pathlib import Path
import sys
import time
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
from cartlab.http_util import request


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--url",default="http://127.0.0.1:8760")
    parser.add_argument("--requests",type=int,default=120)
    parser.add_argument("--workers",type=int,default=8)
    parser.add_argument("--output",default="artifacts/benchmark.json")
    args=parser.parse_args()
    if args.requests<=0 or args.requests%2 or args.workers<=0:
        parser.error("requests must be a positive even number; workers must be positive")
    batch=uuid.uuid4().hex[:12]
    hot="bench-hot-"+batch
    def work(index):
        cart=hot if index%2==0 else f"bench-cold-{batch}-{index}"
        payload={"node":"A","cart_id":cart,"request_id":uuid.uuid4().hex,
                 "operation":{"kind":"add","sku":"apple"}}
        started=time.perf_counter()
        try:
            code,_=request(args.url+"/api/operation",payload,timeout=15)
        except (OSError,ValueError):
            code=0
        return {"latency_ms":(time.perf_counter()-started)*1000,"status":code,"hot":index%2==0}
    started=time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results=list(pool.map(work,range(args.requests)))
    elapsed=time.perf_counter()-started
    latencies=sorted(r["latency_ms"] for r in results)
    state=request(args.url+"/api/status?cart_id="+hot,timeout=5)[1]
    node=next(n for n in state["nodes"] if n["node"]=="A")
    expected=sum(r["status"]==200 and r["hot"] for r in results)
    actual=node["cart"]["quantity"]
    report={"run_at":datetime.now(timezone(timedelta(hours=8))).isoformat(),
            "requests":args.requests,"workers":args.workers,"hot_key_share":0.5,
            "duration_seconds":round(elapsed,3),"throughput_requests_per_second":round(args.requests/elapsed,2),
            "p50_ms":round(latencies[math.ceil(len(latencies)*.5)-1],2),
            "p95_ms":round(latencies[math.ceil(len(latencies)*.95)-1],2),
            "errors":sum(r["status"]!=200 for r in results),"hot_expected":expected,"hot_actual":actual,
            "hot_count_correct":expected==actual,
            "method":"one local host, all client writes via supervisor to node A; half target one cart; full-state background replication; no performance SLA"}
    path=Path(args.output);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(report,ensure_ascii=False,indent=2))
    if report["errors"] or not report["hot_count_correct"]:
        sys.exit(1)


if __name__=="__main__":
    main()
