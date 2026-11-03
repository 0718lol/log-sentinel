# 可靠采集与投递

## 运行

先部署新版服务端，再启动新版 Agent；新版 Agent 必须收到 `acknowledged_ids` 才删除本地事件。

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python server.py
.venv/bin/python agent.py --file app.log --source demo --state .agent-state/demo.db --spool-limit 10000
```

首次启动从文件开头采集；已有状态从已落盘的字节位点恢复。默认状态路径由文件绝对路径和 source 生成，重启时应保持路径和 source 不变。每个状态数据库只允许一个进程持有；目前使用 POSIX 文件锁，支持 macOS/Linux。

## 数据流与保证

1. 只采集换行结束的完整记录，位点使用字节偏移，支持 UTF-8。未完成的半行等待后续写入。
2. 每条记录首次采集时生成随机 event_id，固定其时间戳和内容。SQLite FULL 同步事务同时写入事件和 checkpoint；写入失败不会推进位点。
3. 本地 outbox 达到事件数量上限时暂停读取源文件。发送器按批读取已持久化事件，网络错误、429、5xx 指数退避重试，事件 ID 保持不变。
4. 服务端在 SQLite 事务中提交，唯一索引阻止相同 event_id 重复入库。仅新入库事件参与当前进程的窗口统计和 WebSocket 广播。
5. 服务端返回整批确认 ID，Agent 仅在确认集合匹配后事务删除 outbox。提交成功但响应丢失时可以安全重传。
6. 401/403/422 等错误保留整批数据并停止 Agent，修复配置或升级协议后使用原状态重启。

旧客户端没有 event_id 仍可接入，但无法去重。已有服务端数据库启动时自动添加 event_id 列和唯一索引，历史数据保留。

## 轮转和恢复边界

- rename 轮转先读完仍打开的旧文件，再从头读取新文件；重启时在原目录查找 checkpoint 对应的设备号和 inode。
- copytruncate 通过文件缩小或位点前最多 128 字节内容变化检测；同样大小但内容改变的重写也可检测。
- copytruncate 在两次观察之间删掉的未读内容无法恢复；若重写后位点附近内容完全相同，也无法可靠识别。优先使用 rename 轮转。
- 被删除、移出原目录或在发现前多次轮转的未读文件不保证恢复。需要源日志保留策略配合。
- 轮转旧文件若存在未换行的尾部，Agent 停止并保留状态，要求补齐换行后重启。
- 当前只保证数据库日志行幂等，不保证告警通知恰好一次。提交数据库后进程崩溃可能导致对应实时通知未发送，告警窗口也不跨服务重启恢复。
- outbox 限制的是事件数量，不是字节数；单条超大日志、源文件保留和磁盘容量需要部署时单独规划。同步持久化会影响吞吐，当前适合单机轻量采集场景。
- 不要删除状态数据库或更改 source 来恢复积压，否则会重新采集并生成新 ID；状态数据库和锁文件应使用专属可写目录。

## 验证

```sh
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python scripts/verify_core_fixes.py
.venv/bin/python scripts/verify_delivery.py --events 1000 --output docs/delivery-results.json
```

真实进程实验依次覆盖：无服务端采集、SIGKILL 后恢复、rename 轮转、同大小截断重写、提交后重复投递。对所有输入消息核对总数和唯一数，要求无缺失、无重复，最终 outbox 清空。性能数据见 `delivery-results.json`，是一次本机运行的结果，不代表生产 SLA；延迟包含 50ms 轮询观测粒度。复现结果可能受机器、磁盘与负载影响。

简历描述建议：实现日志采集位点与本地 outbox 原子持久化、服务端唯一索引去重及提交后确认，覆盖断网、进程强制退出和文件轮转场景；通过真实进程实验验证数据完整性并测量吞吐与端到端延迟。
