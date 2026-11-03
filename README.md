> 可靠采集升级：新增持久化位点、本地磁盘 outbox、确认后删除和服务端幂等入库。首次启动从头采集。运行、保证边界、验证与性能数据见 [可靠投递说明](docs/reliable-delivery.md)。历史介绍中的“不丢失”等表述以该说明中的条件和限制为准。

# LogSentinel: 轻量级分布式日志采集与实时告警系统

这是一个高效、高可靠的分布式日志分析与实时告警系统，旨在解决生产环境中**多节点日志汇聚慢、故障感知滞后、并发写入可能引发系统崩溃 (OOM)** 等核心痛点。

本项目专为开发实习岗位简历量身定制，核心实现了 **Agent 采集端背压控制 (Backpressure)、内存批合并上报 (Batching)、网络故障指数退避重试 (Exponential Backoff with Jitter)、滑动窗口规则判断** 等企业级高可靠特性。

---

## 🚀 项目核心亮点 (写在简历上的加分项)

- **有界队列背压机制 (Backpressure)**：Agent 采集端内部设计了有界环形缓冲区（Bounded Queue）。当服务器不可用、网络抖动或返回 HTTP 429 (Too Many Requests) 时，Agent 会自动降低或暂停日志读取速率，直至服务器恢复，物理性隔离并防范 Agent 内存溢出（OOM）。
- **重试容灾设计 (Exponential Backoff with Jitter)**：网络异常时，Agent 使用带有随机抖动的指数退避算法进行重试，避免重试流量瞬间冲垮服务器（“惊群效应”）；对 4xx 这类**不可恢复错误**则直接丢弃该批次并继续尾随，避免无效重试堵塞队列。
- **滑动窗口告警引擎 (Sliding Window Engine)**：服务端基于内存双端队列设计了滑动窗口判定算法，支持按指定时间区间、特定级别或关键字进行流式频次判定（如：“10秒内出现超过5次 ERROR 则告警”）。
- **告警风暴收敛 (Alert Suppression)**：系统对同一个告警规则实施冷却机制（Cooldown，默认 10 秒，可用 `ALERT_COOLDOWN_SECONDS` 调整），配合“触发→自动恢复”状态机，避免指标在阈值附近抖动时反复触发告警、冲垮告警通道。
- **现代化极客风看板 (Glassmorphic Web Dashboard)**：前端使用 WebSocket 建立实时双向数据流通道，结合 Canvas 绘制动态 EPS（每秒事件数）曲线，并支持终端控制台过滤、限制最大渲染行数以保护浏览器内存。
- **服务端可靠性工程**：所有 SQLite 读写通过 `asyncio.to_thread` 卸载到线程池执行（事件循环永不被磁盘 I/O 阻塞），数据库启用 WAL 模式并为高频查询建索引；规则评估使用异步锁串行化，杜绝并发双触发。

---

## 🛠️ 技术栈

- **采集端 (Agent)**：Python 3.9+ / Asyncio (异步非阻塞 I/O) / Psutil (系统资源指标上报)
- **服务端 (Log Server)**：FastAPI (异步 Web 框架) / Uvicorn (高性能 ASGI 服务器) / SQLite (轻量级本地化持久存储)
- **前端看板 (Web UI)**：HTML5 / Vanilla CSS (极客暗黑玻璃拟物风) / Vanilla JS / WebSockets / Chart.js (实时数据可视化)

---

## 📂 项目结构

```
log-sentinel/
├── static/                     # 前端静态资源
│   ├── index.html              # 可视化监控大屏
│   ├── style.css               # 玻璃拟物暗黑风样式
│   └── script.js               # WebSocket通信与图表更新逻辑
├── scripts/
│   └── verify_core_fixes.py    # 核心机制端到端回归测试 (20 项断言)
├── requirements.txt            # 依赖列表
├── database.py                 # SQLite 初始化、迁移与 CRUD 逻辑 (WAL 模式)
├── server.py                   # FastAPI 服务端与滑动窗口规则引擎
├── agent.py                    # 异步日志采集代理 (带背压、退避重试与 4xx 丢批)
├── generator.py                # 业务系统日志流模拟发生器
└── README.md                   # 项目使用说明书
```

---

## 🚦 运行与调试步骤

为了方便地在本地查看完整数据流和容灾效果，请按以下步骤开启多个终端运行：

### 1. 初始化依赖环境
在项目根目录下创建虚拟环境并安装依赖：
```bash
# 1. 进入项目根目录
cd log-sentinel

# 2. 创建并激活虚拟环境 (已有 .venv 可直接激活)
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. 安装依赖包
pip install -r requirements.txt
```

### 2. 启动核心日志服务器 (Server)
服务器默认监听 `8001` 端口（可用环境变量 `PORT` 覆盖），自动初始化 SQLite 数据库：
```bash
python server.py
```
*启动后，在浏览器访问 `http://localhost:8001` 即可打开可视化大屏。*

> 可选环境变量：`PORT`（监听端口）、`LOG_SENTINEL_TOKEN`（启用写接口鉴权）、`ALERT_COOLDOWN_SECONDS`（告警冷却时长，默认 10）、`LOG_SENTINEL_DB_PATH`（数据库文件路径）。

### 3. 启动日志发生模拟器 (Generator)
模拟业务系统向 `app.log` 持续输出日志（每秒 10 条）：
```bash
python generator.py --rate 10 --file app.log
```

### 4. 启动日志采集端 (Agent)
Agent 将实时监听 `app.log` 文件（从文件末尾开始尾随），批量合并日志并上报至服务端：
```bash
python agent.py --file app.log --server http://localhost:8001 --source my-mac-app
```
*此时，回到网页控制台即可看到源源不断的实时日志和吞吐折线图！*

> 若服务端启用了 `LOG_SENTINEL_TOKEN` 鉴权，Agent 需追加 `--token <令牌>`（或同样设置该环境变量）。

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
2. 运行模拟器 `generator.py`，因为 ERROR 日志占比约 5%，随着时间推移，当 10 秒内误差数量满足条件时，网页看板会**立刻闪烁红光，并弹出红色的告警卡片**；条件恢复后告警卡片自动变绿进入“已恢复”状态。
3. 告警恢复后若立刻再次满足触发条件，会被 **Cooldown 冷却机制**抑制，不会重复推送。
4. 若您需要测试真实的飞书/钉钉推送，在网页看板下方的“实时告警通道路由”中填入群机器人 Webhook 地址并保存即可（或调用 `POST /api/settings/webhook`），告警触发与恢复都会同步推送到群聊。

---

## 🔐 可选鉴权

默认所有接口开放（本地演示模式）。启动服务端前设置环境变量 `LOG_SENTINEL_TOKEN=<自定义密钥>`，所有**写接口**（日志上报、规则增删、Webhook 配置、告警处理）即要求请求头 `X-API-Token: <密钥>`；只读查询接口与 WebSocket 看板保持开放，看板功能不受影响：

- **Agent**：追加 `--token <密钥>` 参数（或同样设置 `LOG_SENTINEL_TOKEN` 环境变量）；
- **看板**：在“访问令牌 (服务端安全设置)”卡片中填入密钥，令牌保存在浏览器 localStorage 并自动随写请求携带。

---

## ✅ 回归测试

```bash
python scripts/verify_core_fixes.py
```

脚本会以临时数据库和随机端口启动真实 `server.py`，覆盖 20 项断言：可选 Token 鉴权（401/放行）、告警触发与自动恢复、冷却期抑制与冷却解除、离线告警按节点精确隔离（`source` 结构化匹配，杜绝文本 LIKE 误匹配）、历史日志查询，全部通过后自动清理。
