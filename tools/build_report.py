"""Turn measured integration/benchmark JSON into a readable experiment record."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main():
    data=json.loads((ROOT/"artifacts/integration-results.json").read_text(encoding="utf-8"))
    realtime=json.loads((ROOT/"artifacts/realtime-results.json").read_text(encoding="utf-8"))
    messages=json.loads((ROOT/"artifacts/message-results.json").read_text(encoding="utf-8"))
    browser=json.loads((ROOT/"artifacts/message-browser-results.json").read_text(encoding="utf-8"))
    bench=json.loads((ROOT/"artifacts/benchmark.json").read_text(encoding="utf-8"))
    tests=data["tests"]+realtime["tests"]+messages["tests"]
    passed=sum(t["status"]=="passed" for t in tests)
    lines=["# 实测实验记录", "", "本文件由 `python -X utf8 tools/build_report.py` 根据本地运行产生的 JSON 整理。没有用预期结果替代实测。", "",
           "## 运行环境", "", f"- 集成测试时间：{data['run_at']}",
           f"- Python：{data['python']}；平台：{data['platform']}",
           "- 独立 Python 节点进程、独立 SQLite 数据库、真实本地 HTTP 通信。",
           "- 写入后立即触发复制，SSE 通知页面读取实际状态；周期同步只作为遗漏修复。",
           f"- 故障测试修复周期：{data['sync_interval_seconds']} 秒；即时同步专项故意设置为 {realtime['sync_interval_seconds']} 秒；消息观测测试为 {messages['sync_interval_seconds']} 秒。",
           "- 故障在消息层注入；停止进程保留磁盘；时钟回拨使用可注入偏移。", "",
           "## 集成测试结果", "", f"本轮 JSON 中记录 {len(tests)} 项进程间测试，{passed} 项通过。完整测试命令还包含 11 项数据结构和持久化单元测试；完整运行状态以终端退出码为准。", "",
           "| 场景 | 状态 | 用时（秒） |", "|---|---|---:|"]
    for test in tests:
        lines.append(f"| {test['description']} | {'通过' if test['status']=='passed' else '失败'} | {test['elapsed_seconds']} |")
    lines.extend(["", "## 关键观测", ""])
    for test in tests:
        lines.extend([f"### {test['name']}", "", test["description"], "", "```json",
                      json.dumps(test["evidence"],ensure_ascii=False,indent=2), "```", ""])
    lines.extend(["## 既有热点压测记录", "", f"实测时间：{bench['run_at']}", "",
                  "此处保留压测原运行日期。本次同步与界面修改后未重跑压测，不用这些旧数字证明新版性能。", "",
                  "| 指标 | 结果 |", "|---|---:|",
                  f"| 请求数 | {bench['requests']} |", f"| 并发工作线程 | {bench['workers']} |",
                  f"| 热点请求占比 | {bench['hot_key_share']:.0%} |",
                  f"| 用时 | {bench['duration_seconds']} 秒 |",
                  f"| 吞吐量 | {bench['throughput_requests_per_second']} 请求/秒 |",
                  f"| P50 | {bench['p50_ms']} 毫秒 |", f"| P95 | {bench['p95_ms']} 毫秒 |",
                  f"| 错误请求 | {bench['errors']} |", f"| 热点商品预期数量 | {bench['hot_expected']} |",
                  f"| 热点商品实际数量 | {bench['hot_actual']} |", "",
                  "方法：全部请求经控制器访问 A，一半访问同一购物车，另一半访问不同购物车。全状态复制在后台运行。该结果只描述这次开发机运行，没有均匀负载对照，不能推断热点的独立因果影响或生产环境容量。", "",
                  "## 浏览器操作证据", "", f"浏览器检查时间（UTC）：{browser['checked_at']}", "",
                  "本轮验证消息面板、即时同步、分区恢复、重复投递、响应丢失、指定乱序，以及全部 11 份讲解的展示和导航。以下来自实际页面操作；浏览器观测耗时包含工具开销，不是纯网络延迟。", "",
                  "| 检查 | 结果 |", "|---|---|"])
    for check in browser["checks"]:
        lines.append(f"| {check['name']} | {'通过' if check['status']=='passed' else '失败'} |")
    lines.extend(["", "- [本轮浏览器检查记录](../artifacts/message-browser-results.json)",
                  "- [新版消息面板截图](../artifacts/message-tracking.jpg)",
                  "- [实验讲解页面截图](../artifacts/experiment-explanation.jpg)",
                  "- [先前版本全部 10 场景浏览器记录](../artifacts/browser-guided-results.json)（保留历史，不代表本轮重新执行）", "",
                  "## 限制与复跑说明", "",
                  "1. 测试验证了有限故障序列，不等同于形式化证明；CRDT 收敛还依赖合法状态和充分传播。",
                  "2. 页面状态是多次独立读取，不是全局原子快照。",
                  "3. Docker 当前未在本机验证；验证路径是 Windows 本地 Python。",
                  "4. 迁移场景可在一半处暂停，也可使用新 D 数据库重复演示；旧数据库保留。",
                  "5. 正式报告建议在你们的展示机器上复跑，再记录自己的日期、配置与结果。", ""])
    output=ROOT/"docs/实验记录.md"
    output.write_text("\n".join(lines),encoding="utf-8")
    print(output)


if __name__=="__main__":
    main()
