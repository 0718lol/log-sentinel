#!/usr/bin/env python3
# LogSentinel 核心机制回归测试：Token 鉴权 / 告警触发与自动恢复 / 告警冷却 / 离线告警按节点隔离。
# 运行: python scripts/verify_core_fixes.py（已安装 requirements.txt 后执行）
# 脚本会以临时数据库、独立端口和缩短的冷却时间启动真实 server.py，全部断言通过后自动清理。

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TOKEN = "test-token-123"
COOLDOWN_SECONDS = 6.0
OFFLINE_TIMEOUT_SECONDS = 2

passed = 0


def check(condition, label):
    global passed
    if not condition:
        raise AssertionError(f"验证失败: {label}")
    passed += 1
    print(f"  ✓ {label}")


def section(title):
    print(f"\n● {title}")


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def make_entry(level, message, source):
    return {"timestamp": utc_now_iso(), "level": level, "message": message, "source": source}


class Client:
    def __init__(self, port):
        self.base = f"http://127.0.0.1:{port}"

    def request(self, method, path, body=None, token=TOKEN):
        headers = {}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if token:
            headers["X-API-Token"] = token
        req = urllib.request.Request(
            self.base + path,
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers=headers,
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read() or b"null")
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"null")

    def ingest(self, entries, token=TOKEN):
        return self.request("POST", "/api/logs/ingest", entries, token=token)


def main():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    tmp = tempfile.mkdtemp(prefix="logsentinel-verify-")
    server_log = open(os.path.join(tmp, "server.log"), "w", encoding="utf-8")
    env = {
        **os.environ,
        "PORT": str(port),
        "LOG_SENTINEL_DB_PATH": os.path.join(tmp, "verify.db"),
        "LOG_SENTINEL_TOKEN": TOKEN,
        "ALERT_COOLDOWN_SECONDS": str(COOLDOWN_SECONDS),
    }
    proc = subprocess.Popen(
        [sys.executable, "server.py"],
        cwd=ROOT, env=env, stdout=server_log, stderr=subprocess.STDOUT,
    )
    client = Client(port)

    try:
        section("启动与可选 Token 鉴权")
        deadline = time.time() + 20
        while True:
            try:
                status, _ = client.request("GET", "/api/rules")
                if status == 200:
                    break
            except Exception:
                pass
            if time.time() > deadline:
                server_log.flush()
                raise RuntimeError(f"服务器 20 秒内未就绪，日志尾部:\n{open(server_log.name, encoding='utf-8').read()[-800:]}")
            time.sleep(0.3)
        check(True, f"服务器以临时端口 {port} 启动")

        entry = make_entry("ERROR", "boom", "test-node")
        no_token, _ = client.ingest([entry], token=None)
        check(no_token == 401, "未携带令牌的写入请求被拒绝 (401)")
        bad_token, _ = client.ingest([entry], token="wrong-token")
        check(bad_token == 401, "错误令牌的写入请求被拒绝 (401)")
        ok_token, data = client.ingest([entry], token=TOKEN)
        check(ok_token == 200 and data["status"] == "ok", "携带正确令牌可正常写入")
        status, _ = client.request("GET", "/api/logs", token=TOKEN)
        check(status == 200, "只读接口无需令牌即可访问（看板保持可用）")

        section("告警触发与自动恢复")
        client.ingest([make_entry("ERROR", f"disk failure #{i}", "test-node") for i in range(5)])
        status, alerts = client.request("GET", "/api/alerts")
        error_alerts = [a for a in alerts if "High Error Rate" in a["rule_name"]]
        check(len(error_alerts) == 1 and error_alerts[0]["status"] == "active", "10 秒内 5 条 ERROR 触发告警")
        # 窗口本身有迟滞：ERROR 在 10 秒窗口内老化之前条件一直成立，需等待老化后再评估
        time.sleep(10.5)
        client.ingest([make_entry("INFO", "all good", "test-node")])
        status, alerts = client.request("GET", "/api/alerts")
        error_alerts = [a for a in alerts if "High Error Rate" in a["rule_name"]]
        check(len(error_alerts) == 1 and error_alerts[0]["status"] == "resolved", "条件恢复后告警自动转为 resolved")

        section(f"告警冷却机制 (冷却 {COOLDOWN_SECONDS}s, 3s 短窗口规则)")
        status, _ = client.request("POST", "/api/rules", {
            "name": "Burst Test", "condition_type": "count",
            "threshold": 2, "window_seconds": 3, "level_filter": "ERROR",
        })
        check(status == 200, "创建 3 秒窗口、阈值 2 的 Burst Test 规则")
        client.ingest([make_entry("ERROR", "burst 1", "test-node"), make_entry("ERROR", "burst 2", "test-node")])
        status, alerts = client.request("GET", "/api/alerts")
        burst_alerts = [a for a in alerts if a["rule_name"] == "Burst Test"]
        check(len(burst_alerts) == 1 and burst_alerts[0]["status"] == "active", "首次触发正常告警")
        time.sleep(3.5)  # 让触发日志老化出 3s 窗口
        client.ingest([make_entry("INFO", "calm down", "test-node")])
        status, alerts = client.request("GET", "/api/alerts")
        burst_alerts = [a for a in alerts if a["rule_name"] == "Burst Test"]
        check(burst_alerts[0]["status"] == "resolved", "窗口清空后告警自动恢复")
        client.ingest([make_entry("ERROR", "burst 3", "test-node"), make_entry("ERROR", "burst 4", "test-node")])
        status, alerts = client.request("GET", "/api/alerts")
        burst_alerts = [a for a in alerts if a["rule_name"] == "Burst Test"]
        check(len(burst_alerts) == 1 and burst_alerts[0]["status"] == "resolved",
              "恢复后立即再触发被冷却抑制（此时已无活跃告警，抑制只能来自冷却）")
        time.sleep(2.7)  # 跨过冷却期
        client.ingest([make_entry("ERROR", "burst 5", "test-node"), make_entry("ERROR", "burst 6", "test-node")])
        status, alerts = client.request("GET", "/api/alerts")
        burst_alerts = [a for a in alerts if a["rule_name"] == "Burst Test"]
        check(len(burst_alerts) == 2 and burst_alerts[0]["status"] == "active", "冷却期结束后允许再次触发")

        section("离线告警按节点精确隔离")
        status, rules = client.request("GET", "/api/rules")
        offline_rule = next(r for r in rules if r["condition_type"] == "agent_offline")
        status, _ = client.request("DELETE", f"/api/rules/{offline_rule['id']}")
        check(status == 200, "默认离线规则已删除")
        status, _ = client.request("POST", "/api/rules", {
            "name": "Offline Fast", "condition_type": "agent_offline",
            "threshold": OFFLINE_TIMEOUT_SECONDS, "window_seconds": 30,
        })
        check(status == 200, "创建 2 秒超时的离线规则")

        client.ingest([make_entry("INFO", "node-1 heartbeat", "node-1")])
        time.sleep(8.0)  # 心跳检查器每 5 秒扫描一次
        status, alerts = client.request("GET", "/api/alerts")
        # 此前的 test-node 早已停止心跳,它也会正常产生离线告警,这里只断言 node-1 的那一条
        node1_alerts = [a for a in alerts if a["rule_name"] == "Offline Fast" and "[node-1]" in a["message"]]
        testnode_alerts = [a for a in alerts if a["rule_name"] == "Offline Fast" and "[test-node]" in a["message"]]
        check(len(node1_alerts) == 1 and node1_alerts[0]["status"] == "active", "超时后对 node-1 恰好产生一条离线告警")
        check(len(testnode_alerts) == 1 and testnode_alerts[0]["status"] == "active", "此前静默的 test-node 也被独立检出")

        client.ingest([make_entry("INFO", "node-10 heartbeat", "node-10")])
        time.sleep(0.5)
        status, alerts = client.request("GET", "/api/alerts")
        node1_alerts = [a for a in alerts if a["rule_name"] == "Offline Fast" and "[node-1]" in a["message"]]
        testnode_alerts = [a for a in alerts if a["rule_name"] == "Offline Fast" and "[test-node]" in a["message"]]
        check(node1_alerts[0]["status"] == "active", "node-10 上线不会误恢复 node-1 的告警")

        client.ingest([make_entry("INFO", "node-1 back", "node-1")])
        time.sleep(0.5)
        status, alerts = client.request("GET", "/api/alerts")
        node1_alerts = [a for a in alerts if a["rule_name"] == "Offline Fast" and "[node-1]" in a["message"]]
        testnode_alerts = [a for a in alerts if a["rule_name"] == "Offline Fast" and "[test-node]" in a["message"]]
        check(node1_alerts[0]["status"] == "resolved", "node-1 重新上线后其告警精确恢复")
        check(testnode_alerts[0]["status"] == "active", "其余离线节点 (test-node) 的告警不受影响")

        section("历史日志查询")
        status, logs = client.request("GET", "/api/logs?limit=10")
        check(status == 200 and isinstance(logs, list) and len(logs) > 0, "GET /api/logs 返回已落盘日志")

        print(f"\n全部 {passed} 项断言通过 ✅")
    finally:
        proc.kill()
        server_log.close()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
