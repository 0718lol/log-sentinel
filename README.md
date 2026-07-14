# LogSentinel: 轻量级分布式日志采集与实时告警系统

这是一个高效、高可靠的分布式日志分析与实时告警系统，旨在解决生产环境中**多节点日志汇聚慢、故障感知滞后、并发写入可能引发系统崩溃 (OOM)** 等核心痛点。

本项目专为开发实习岗位简历量身定制，核心实现了 **Agent 采集端背压控制 (Backpressure)、内存批合并上报 (Batching)、网络故障指数退避重试 (Exponential Backoff with Jitter)、滑动窗口规则判断** 等企业级高可靠特性。

---

## 🚀 项目核心亮点 (写在简历上的加分项)

- **有界队列背压机制 (Backpressure)**：Agent 采集端内部设计了有界环形缓冲区（Bounded Queue）。当服务器不可用、网络抖动或返回 HTTP 429 (Too Many Requests) 时，Agent 会自动降低或暂停日志读取速率，直至服务器恢复，物理性隔离并防范 Agent 内存溢出（OOM）。
- **重试容灾设计 (Exponential Backoff with Jitter)**：网络异常时，Agent 使用带有随机抖动的指数退避算法进行重试，避免重试流量瞬间冲垮服务器（“惊群效应”）。
- **滑动窗口告警引擎 (Sliding Window Engine)**：服务端基于内存双端队列设计了滑动窗口判定算法，支持按指定时间区间、特定级别或关键字进行流式频次判定（如：“10秒内出现超过5次 ERROR 则告警”）。
- **告警风暴收敛 (Alert Suppression)**：系统对同一个告警规则实施冷却机制（Cooldown），在设定时间内避免发送重复告警，防止告警通道过载。
- **现代化极客风看板 (Glassmorphic Web Dashboard)**：前端使用 WebSocket 建立实时双向数据流通道，结合 Canvas 绘制动态 EPS（每秒事件数）曲线，并支持终端控制台过滤、限制最大渲染行数以保护浏览器内存。

---

## 🛠️ 技术栈

- **采集端 (Agent)**：Python 3.9+ / Asyncio (异步非阻塞 I/O) / Psutil (系统资源指标上报)
- **服务端 (Log Server)**：FastAPI (异步 Web 框架) / Uvicorn (高性能 ASGI 服务器) / SQLite (轻量级本地化持久存储)
- **前端看板 (Web UI)**：HTML5 / Vanilla CSS (极客暗黑玻璃拟物风) / Vanilla JS / WebSockets / Chart.js (实时数据可视化)

---

## 📂 项目结构

```
creation/
├── .venv/                  # Python 虚拟环境
├── static/                 # 前端静态资源
│   ├── index.html          # 可视化监控大屏
│   ├── style.css           # 玻璃拟物暗黑风样式
│   └── script.js           # WebSocket通信与图表更新逻辑
├── requirements.txt        # 依赖列表
├── database.py             # SQLite 初始化与 CRUD 逻辑
├── server.py               # FastAPI 服务端与滑动窗口规则引擎
├── agent.py                # 异步日志采集代理 (带背压与退避重试)
├── generator.py            # 业务系统日志流模拟发生器
└── README.md               # 项目使用说明书
```

---

## 🚦 运行与调试步骤

为了方便地在本地查看完整数据流和容灾效果，请按以下步骤开启多个终端运行：

### 1. 初始化依赖环境
在 `creation` 根目录下，激活虚拟环境并安装依赖：
```bash
# 1. 进入项目根目录
cd "/Users/wanganchang/creation"

# 2. 激活虚拟环境
source .venv/bin/activate

# 3. 安装依赖包
pip install -r requirements.txt
```

### 2. 启动核心日志服务器 (Server)
服务器将监听 `8001` 端口，初始化 SQLite 数据库：
```bash
python server.py
```
*启动后，在浏览器访问 `http://localhost:8001` 即可打开可视化大屏。*

### 3. 启动日志发生模拟器 (Generator)
模拟业务系统向 `app.log` 持续输出日志（每秒 10 条）：
```bash
python generator.py --rate 10 --file app.log
```

### 4. 启动日志采集端 (Agent)
Agent 将实时监听 `app.log` 文件，批量压缩合并日志并上报至服务端：
```bash
python agent.py --file app.log --server http://localhost:8001 --source my-mac-app
```
*此时，回到网页控制台即可看到源源不断的实时日志和吞吐折线图！*

---

## 🧪 核心特色功能验证演示 (面试加分演示)

### 验证 1：背压与退避重试 (Backpressure & Retry)
1. 正常运行系统，此时网页端 EPS 大约为 10，Agent 正常上报。
2. 在服务器运行终端，使用 `Ctrl+C` 强行关闭 `server.py` 服务。
3. 观察 Agent 终端输出，它会报告网络请求失败，并进入 **Backoff Active (退避重试)** 状态，重试等待时间逐渐变长（1s -> 2s -> 4s -> 8s...）。
4. 由于 Agent 发送受阻，本地 `Queue` 队列积压行数开始上升。当队列达到 1000 行上限时，Agent 将暂停读取 `app.log`，不再消耗文件，**避免 Agent 本身 OOM**。
5. 此时重新运行 `python server.py` 启动服务器。
6. 观察 Agent，它在下一次重试成功后，会自动消化队列中的积压日志，迅速恢复正常采集速度，且**日志零丢失**。

### 验证 2：滑动窗口告警判定与收敛 (Alert Suppression)
1. 默认情况下，数据库中自带了告警规则：“High Error Rate (ERROR >= 5 in 10s)”——10秒内出现超过5次 ERROR 日志即报警。
2. 运行模拟器 `generator.py`，因为 ERROR 日志占比约 5%，随着时间推移，当 10 秒内误差数量满足条件时，网页看板会**立刻闪烁红光，并弹出红色的告警卡片**。
3. 若您需要测试真实的飞书/钉钉推送，可在运行 `server.py` 时传入环境变量 `ALERT_WEBHOOK_URL`，即可在群聊中收到实时推送！
