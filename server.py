import asyncio
import os
import json
import logging
import re
from datetime import datetime, timedelta
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
import collections

import database

# Regex to parse CPU and Memory from Agent heartbeat logs
METRIC_REGEX = re.compile(r"Agent heartbeat metrics:\s+CPU\s+([\d\.]+)%,\s+RAM\s+([\d\.]+)MB")

# Latest performance metrics received from the agent
latest_metrics = {"cpu": 0.0, "ram": 0.0}

# Track last active epoch timestamps of each agent node
agent_last_seen = {}

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LogServer")

app = FastAPI(title="Log Alert Server")

# Initialize database
database.init_db()

# Bounded queue in memory for sliding window analysis (holds logs from last 60 seconds)
# Each entry is a dict: {"timestamp": float_epoch, "level": str, "message": str, "source": str}
sliding_window = collections.deque()
window_lock = asyncio.Lock()

# Store active websocket connections
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"New client connected. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"Client disconnected. Total connections: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        # Broadcast message as JSON
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)
        
        for conn in disconnected:
            self.disconnect(conn)

manager = ConnectionManager()

# Map to store last triggered alert timestamps to prevent spam (Alert Suppression)
# Key: rule_id (int), Value: timestamp (float)
last_alert_timestamps = {}
ALERT_COOLDOWN_SECONDS = 10.0

# ----------------- Models -----------------
class LogEntry(BaseModel):
    timestamp: str # ISO-8601 string
    level: str
    message: str
    source: str

class RuleCreate(BaseModel):
    name: str
    condition_type: str # 'count', 'keyword', 'cpu_threshold', or 'ram_threshold'
    threshold: int
    window_seconds: int
    level_filter: Optional[str] = None
    keyword_filter: Optional[str] = None

# ----------------- Helper Functions -----------------
def parse_iso_to_epoch(iso_str: str) -> float:
    """Parse ISO timestamp string to epoch float."""
    try:
        # Strip trailing Z and parse
        if iso_str.endswith("Z"):
            iso_str = iso_str[:-1]
        dt = datetime.fromisoformat(iso_str)
        return dt.timestamp()
    except Exception:
        return datetime.utcnow().timestamp()

async def clean_old_logs(now_epoch: float):
    """Remove logs older than 60 seconds from memory sliding window."""
    async with window_lock:
        cutoff = now_epoch - 60.0
        while sliding_window and sliding_window[0]["epoch"] < cutoff:
            sliding_window.popleft()

async def evaluate_rules(now_epoch: float):
    """Evaluate all active alerting rules against the sliding window with state transitions (Auto-Resolve)."""
    rules = database.get_rules()
    active_rules = [r for r in rules if r["is_active"] == 1 and r["condition_type"] != "agent_offline"]
    
    async with window_lock:
        # Convert sliding window to a local list for fast querying
        current_logs = list(sliding_window)

    for rule in active_rules:
        rule_id = rule["id"]
        rule_name = rule["name"]
        cond_type = rule["condition_type"]
        window_sec = rule["window_seconds"]
        threshold = rule["threshold"]
        lvl_filter = rule["level_filter"]
        kw_filter = rule["keyword_filter"]

        # Check if this rule currently has an active alert in the database
        active_alert = database.get_active_alert_by_rule(rule_id)

        # Check if trigger condition is met
        is_triggered = False
        alert_msg = ""

        if cond_type == "cpu_threshold":
            current_cpu = latest_metrics["cpu"]
            if current_cpu >= threshold:
                is_triggered = True
                alert_msg = f"Rule [{rule_name}] triggered. CPU load at {current_cpu:.1f}% (Threshold: {threshold}%)."
        elif cond_type == "ram_threshold":
            current_ram = latest_metrics["ram"]
            if current_ram >= threshold:
                is_triggered = True
                alert_msg = f"Rule [{rule_name}] triggered. Memory usage at {current_ram:.1f}MB (Threshold: {threshold}MB)."
        else:
            # Traditional log window count or keyword checks
            window_start = now_epoch - window_sec
            matching_count = 0
            for log in current_logs:
                if log["epoch"] >= window_start:
                    if lvl_filter and log["level"].upper() != lvl_filter.upper():
                        continue
                    if kw_filter and kw_filter.lower() not in log["message"].lower():
                        continue
                    matching_count += 1
            
            if matching_count >= threshold:
                is_triggered = True
                alert_msg = f"Rule [{rule_name}] triggered. Found {matching_count} matching logs in last {window_sec}s (Threshold: {threshold})."

        # State transition handling
        if is_triggered:
            # Transition: Normal -> Alerting (Only trigger if not already active)
            if not active_alert:
                alert_id = database.save_alert(rule_id, rule_name, alert_msg)
                logger.warning(f"ALERT TRIGGERED: {alert_msg}")

                # Broadcast new alert
                alert_payload = {
                    "type": "alert",
                    "data": {
                        "id": alert_id,
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "rule_name": rule_name,
                        "rule_id": rule_id,
                        "message": alert_msg,
                        "status": "active"
                    }
                }
                await manager.broadcast(alert_payload)
                asyncio.create_task(send_webhook_alert(rule_name, alert_msg))
        else:
            # Transition: Alerting -> Resolved (Auto-resolve if condition is cleared)
            if active_alert:
                database.resolve_alert_by_rule(rule_id)
                logger.info(f"ALERT AUTO-RESOLVED: Rule [{rule_name}] is now normal.")
                
                recovery_msg = f"告警恢复: 规则 [{rule_name}] 的触发条件已恢复正常。"
                # Broadcast recovery
                resolve_payload = {
                    "type": "alert_resolved",
                    "data": {
                        "rule_id": rule_id,
                        "alert_id": active_alert["id"],
                        "message": recovery_msg
                    }
                }
                await manager.broadcast(resolve_payload)
                asyncio.create_task(send_webhook_alert(rule_name, recovery_msg))

async def send_webhook_alert(rule_name: str, message: str):
    """Send Webhook alert dispatch dynamically supporting Feishu and DingTalk."""
    webhook_url = database.get_setting("webhook_url")
    if not webhook_url:
        return

    # Check if it is a recovery message (has '恢复' or 'Recovered' or 'normal')
    is_recovery = any(x in message for x in ["恢复", "RECOVERED", "normal", "已恢复"])
    title = "🟢 告警恢复" if is_recovery else "🚨 告警触发"
    template_color = "green" if is_recovery else "red"

    # Determine if Feishu
    if "feishu" in webhook_url or "larksuite" in webhook_url:
        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": title
                    },
                    "template": template_color
                },
                "elements": [
                    {
                        "tag": "markdown",
                        "content": f"**监控规则**: {rule_name}\n**告警描述**: {message}\n**发生时间**: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
                    }
                ]
            }
        }
    # Determine if DingTalk
    elif "dingtalk" in webhook_url or "oapi.dingtalk.com" in webhook_url:
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": f"## {title}\n- **监控规则**: {rule_name}\n- **告警描述**: {message}\n- **发生时间**: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
            }
        }
    # Generic custom webhook fallback
    else:
        payload = {
            "title": title,
            "rule_name": rule_name,
            "message": message,
            "text": f"[{title}] 规则: {rule_name}. 详情: {message}"
        }

    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            res = await client.post(webhook_url, json=payload, timeout=5.0)
            logger.info(f"Webhook alert sent. Status: {res.status_code}, Response: {res.text[:100]}")
    except Exception as e:
        logger.error(f"Failed to send webhook alert: {e}")


# ----------------- Routes -----------------

@app.post("/api/logs/ingest")
async def ingest_logs(entries: List[LogEntry]):
    """Ingest logs from Agent, save to database, update sliding window, and broadcast."""
    if not entries:
        return {"status": "ok", "count": 0}

    now_epoch = datetime.utcnow().timestamp()
    db_entries = []
    ws_log_data = []

    # Update agent last seen heartbeat timestamp
    source = entries[0].source
    agent_last_seen[source] = now_epoch

    for entry in entries:
        epoch = parse_iso_to_epoch(entry.timestamp)
        log_dict = {
            "timestamp": entry.timestamp,
            "level": entry.level,
            "message": entry.message,
            "source": entry.source
        }
        db_entries.append(log_dict)
        ws_log_data.append(log_dict)

        # Parse metrics if it's an Agent heartbeat log
        match = METRIC_REGEX.search(entry.message)
        if match:
            try:
                latest_metrics["cpu"] = float(match.group(1))
                latest_metrics["ram"] = float(match.group(2))
            except Exception as e:
                logger.error(f"Error parsing metrics from log: {e}")

        # Append to sliding window for rule matching
        async with window_lock:
            sliding_window.append({
                "epoch": epoch,
                "level": entry.level,
                "message": entry.message,
                "source": entry.source
            })

    # Save to SQLite database
    database.save_logs(db_entries)

    # Broadcast to Web UI via WebSockets
    await manager.broadcast({
        "type": "logs",
        "data": ws_log_data
    })

    # Check and Auto-Resolve Agent Node Offline Alert if it was previously offline
    rules = database.get_rules()
    offline_rule = next((r for r in rules if r["condition_type"] == "agent_offline" and r["is_active"] == 1), None)
    if offline_rule:
        rule_id = offline_rule["id"]
        active_offline_alert = database.get_active_agent_offline_alert(rule_id, source)
        if active_offline_alert:
            database.resolve_agent_offline_alert(rule_id, source)
            logger.info(f"AGENT RECOVERED: Agent [{source}] is back online.")
            
            # Broadcast recovery
            resolve_payload = {
                "type": "alert_resolved",
                "data": {
                    "rule_id": rule_id,
                    "alert_id": active_offline_alert["id"],
                    "message": f"告警恢复: 采集端 [{source}] 已重新上线，连接恢复正常。"
                }
            }
            await manager.broadcast(resolve_payload)

    # Trigger async analysis
    await clean_old_logs(now_epoch)
    await evaluate_rules(now_epoch)

    return {"status": "ok", "count": len(entries)}

@app.get("/api/logs")
async def get_logs(limit: int = 100):
    """Retrieve historical logs from SQLite."""
    return database.get_recent_logs(limit)

@app.get("/api/rules")
async def get_rules():
    """Retrieve all alerting rules."""
    return database.get_rules()

@app.post("/api/rules")
async def create_rule(rule: RuleCreate):
    """Add a new alerting rule."""
    success = database.add_rule(
        name=rule.name,
        condition_type=rule.condition_type,
        threshold=rule.threshold,
        window_seconds=rule.window_seconds,
        level_filter=rule.level_filter,
        keyword_filter=rule.keyword_filter
    )
    if not success:
        raise HTTPException(status_code=400, detail="Rule name already exists")
    return {"status": "ok"}

@app.delete("/api/rules/{rule_id}")
async def delete_rule(rule_id: int):
    """Delete a rule."""
    database.delete_rule(rule_id)
    return {"status": "ok"}

# Model for Webhook configuration request
class WebhookConfig(BaseModel):
    webhook_url: str

@app.get("/api/settings/webhook")
async def get_webhook_setting():
    url = database.get_setting("webhook_url")
    return {"webhook_url": url}

@app.post("/api/settings/webhook")
async def save_webhook_setting(req: WebhookConfig):
    database.save_setting("webhook_url", req.webhook_url)
    return {"status": "ok"}

@app.post("/api/settings/webhook/test")
async def test_webhook_setting(req: WebhookConfig):
    url = req.webhook_url
    if not url:
        raise HTTPException(status_code=400, detail="Webhook URL is empty")
    
    title = "🔔 告警通道测试"
    message = "这是一条来自 Antigravity 日志智能分析系统的告警通道测试消息。收到此卡片说明通道配置成功！"
    
    # Format payload
    if "feishu" in url or "larksuite" in url:
        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": title
                    },
                    "template": "blue"
                },
                "elements": [
                    {
                        "tag": "markdown",
                        "content": f"**状态**: 连接正常\n**详情**: {message}\n**测试时间**: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
                    }
                ]
            }
        }
    elif "dingtalk" in url or "oapi.dingtalk.com" in url:
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": f"## {title}\n- **状态**: 连接正常\n- **详情**: {message}\n- **测试时间**: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
            }
        }
    else:
        payload = {
            "title": title,
            "message": message,
            "text": f"[{title}] {message}"
        }

    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            res = await client.post(url, json=payload, timeout=5.0)
            if res.status_code >= 400:
                raise HTTPException(status_code=400, detail=f"Webhook server returned error: {res.status_code}")
            return {"status": "ok", "response": res.text[:200]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to webhook URL: {str(e)}")

@app.get("/api/alerts")
async def get_alerts(limit: int = 50):
    """Get recent alerts."""
    return database.get_recent_alerts(limit)

@app.post("/api/alerts/{alert_id}/dismiss")
async def dismiss_alert(alert_id: int):
    """Mark alert as dismissed."""
    database.dismiss_alert(alert_id)
    return {"status": "ok"}

# WebSocket endpoint for real-time dashboard updates
@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Keep connection open, wait for client messages if any
        while True:
            data = await websocket.receive_text()
            # Respond to ping/pong or filter commands if needed
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Serve static dashboard files
# Create static directory if it does not exist on start
os.makedirs(os.path.join(os.path.dirname(__file__), "static"), exist_ok=True)

@app.get("/")
async def read_index():
    index_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "Server is running. Put index.html in the static folder."}

app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")

# ----------------- Heartbeat Background Checker -----------------
async def check_agent_heartbeats():
    """Background task to periodically scan and alert on offline agents."""
    logger.info("Agent Heartbeat Checker background task started.")
    while True:
        await asyncio.sleep(5.0)
        try:
            now = datetime.utcnow().timestamp()
            rules = database.get_rules()
            offline_rule = next((r for r in rules if r["condition_type"] == "agent_offline" and r["is_active"] == 1), None)
            if not offline_rule:
                continue

            rule_id = offline_rule["id"]
            rule_name = offline_rule["name"]
            timeout = offline_rule["threshold"] # threshold represents the heartbeat timeout in seconds (default 30)

            for source, last_seen in list(agent_last_seen.items()):
                if now - last_seen > timeout:
                    # Agent Offline Detected!
                    active_alert = database.get_active_agent_offline_alert(rule_id, source)
                    if not active_alert:
                        alert_msg = f"Agent [{source}] has not sent heartbeats for over {timeout}s (Last active: {datetime.fromtimestamp(last_seen).strftime('%H:%M:%S')})."
                        alert_id = database.save_alert(rule_id, rule_name, alert_msg)
                        logger.warning(f"ALERT TRIGGERED: {alert_msg}")

                        # Broadcast offline alert
                        alert_payload = {
                            "type": "alert",
                            "data": {
                                "id": alert_id,
                                "timestamp": datetime.utcnow().isoformat() + "Z",
                                "rule_name": rule_name,
                                "rule_id": rule_id,
                                "message": alert_msg,
                                "status": "active"
                            }
                        }
                        await manager.broadcast(alert_payload)
                        asyncio.create_task(send_webhook_alert(rule_name, alert_msg))
        except Exception as e:
            logger.error(f"Error in heartbeat checker loop: {e}")

@app.on_event("startup")
async def startup_event():
    # Launch agent heartbeat checker in the background event loop
    asyncio.create_task(check_agent_heartbeats())

if __name__ == "__main__":
    import uvicorn
    # Start FastAPI server on port 8001
    uvicorn.run(app, host="0.0.0.0", port=8001)
