// Global state
let socket = null;
let epsChart = null;
let logsCountThisSecond = 0;
const epsHistory = Array(60).fill(0);
const epsLabels = Array(60).fill("");
let reconnectTimeout = null;

// DOM Elements
const wsStatus = document.getElementById("ws-status");
const ruleCountVal = document.getElementById("rule-count");
const alertCountVal = document.getElementById("alert-count");
const epsVal = document.getElementById("eps-value");
const consoleOutput = document.getElementById("console-output");
const logSearchInput = document.getElementById("log-search");
const autoScrollCheckbox = document.getElementById("auto-scroll");
const clearConsoleBtn = document.getElementById("clear-console");
const alertsList = document.getElementById("alerts-list");
const rulesList = document.getElementById("rules-list");
const ruleForm = document.getElementById("rule-form");

const webhookUrlInput = document.getElementById("webhook-url");
const btnSaveWebhook = document.getElementById("btn-save-webhook");
const btnTestWebhook = document.getElementById("btn-test-webhook");
const webhookStatus = document.getElementById("webhook-status");

// Initialize application
document.addEventListener("DOMContentLoaded", () => {
    initChart();
    connectWebSocket();
    fetchRules();
    fetchAlerts();
    fetchLogs();
    fetchWebhookConfig();

    // Event listeners
    clearConsoleBtn.addEventListener("click", () => {
        consoleOutput.innerHTML = "";
    });

    ruleForm.addEventListener("submit", handleCreateRule);

    // Dynamic filtering of logs already in DOM on search input
    logSearchInput.addEventListener("input", filterLogs);

    // Setup periodic EPS calculation (every 1 second)
    setInterval(updateEPS, 1000);

    // Webhook actions
    btnSaveWebhook.addEventListener("click", saveWebhookConfig);
    btnTestWebhook.addEventListener("click", testWebhookConfig);

    // Navigation Active State Highlight & Smooth Scroll
    const navItems = document.querySelectorAll(".nav-menu .nav-item");
    navItems.forEach(item => {
        item.addEventListener("click", (e) => {
            navItems.forEach(n => n.classList.remove("active"));
            item.classList.add("active");
            
            const targetId = item.getAttribute("href");
            const targetEl = document.querySelector(targetId);
            if (targetEl) {
                e.preventDefault();
                if (targetId === "#dashboard") {
                    // Since #dashboard is the scroll container itself, scroll its content to the top
                    targetEl.scrollTo({ top: 0, behavior: "smooth" });
                } else {
                    targetEl.scrollIntoView({ behavior: "smooth" });
                }
            }
        });
    });

    // Scroll Spy: Automatically update active nav menu item on scroll
    const mainContent = document.querySelector(".main-content");
    const rulesManager = document.getElementById("rules-manager");
    const navDashboard = document.querySelector('.nav-menu a[href="#dashboard"]');
    const navRules = document.querySelector('.nav-menu a[href="#rules-manager"]');

    if (mainContent && rulesManager && navDashboard && navRules) {
        mainContent.addEventListener("scroll", () => {
            const rect = rulesManager.getBoundingClientRect();
            // If the top of the rules-manager section is in the top 60% of the viewport, highlight Rules Manager
            if (rect.top < window.innerHeight * 0.6) {
                navDashboard.classList.remove("active");
                navRules.classList.add("active");
            } else {
                navRules.classList.remove("active");
                navDashboard.classList.add("active");
            }
        });
    }
});

// ----------------- WebSocket Connection -----------------
function connectWebSocket() {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}/ws/logs`;

    console.log(`Connecting to WebSocket: ${wsUrl}`);
    socket = new WebSocket(wsUrl);

    socket.onopen = () => {
        console.log("WebSocket connected.");
        wsStatus.className = "status-badge connected";
        wsStatus.querySelector(".text").innerText = "在线";
        if (reconnectTimeout) {
            clearTimeout(reconnectTimeout);
            reconnectTimeout = null;
        }
    };

    socket.onmessage = (event) => {
        try {
            const message = JSON.parse(event.data);
            if (message.type === "logs") {
                handleIncomingLogs(message.data);
            } else if (message.type === "alert") {
                handleIncomingAlert(message.data);
            } else if (message.type === "alert_resolved") {
                handleAlertResolved(message.data);
            }
        } catch (err) {
            console.error("Failed to parse socket message:", err);
        }
    };

    socket.onclose = () => {
        console.warn("WebSocket disconnected. Attempting reconnect in 3s...");
        wsStatus.className = "status-badge disconnected";
        wsStatus.querySelector(".text").innerText = "断开";
        
        // Auto-reconnect
        if (!reconnectTimeout) {
            reconnectTimeout = setTimeout(connectWebSocket, 3000);
        }
    };

    socket.onerror = (error) => {
        console.error("WebSocket error:", error);
    };
}

// ----------------- Log Handling -----------------
function handleIncomingLogs(logEntries) {
    logsCountThisSecond += logEntries.length;
    
    logEntries.forEach(log => {
        appendLogRow(log);
    });

    limitConsoleRows(100);
    if (autoScrollCheckbox.checked) {
        consoleOutput.scrollTop = consoleOutput.scrollHeight;
    }
}

function appendLogRow(log) {
    const row = document.createElement("div");
    row.className = `log-row`;
    row.dataset.level = log.level;
    row.dataset.message = log.message;

    // Format level badge class
    const levelLower = log.level.toLowerCase();
    const badgeClass = `log-badge level-${levelLower}`;
    
    // Highlight error messages in red text
    const textClass = levelLower === "error" ? "log-text highlight-error" : "log-text";

    // Clean timestamp for display (e.g. 11:30:45.123)
    let timeStr = log.timestamp;
    try {
        if (timeStr.includes("T")) {
            timeStr = timeStr.split("T")[1].replace("Z", "");
        } else if (timeStr.includes(" ")) {
            timeStr = timeStr.split(" ")[1];
        }
    } catch (_) {}

    row.innerHTML = `
        <span class="log-time">${escapeHTML(timeStr)}</span>
        <span class="log-source">[${escapeHTML(log.source)}]</span>
        <span class="${escapeHTML(badgeClass)}">${escapeHTML(log.level)}</span>
        <span class="${escapeHTML(textClass)}">${escapeHTML(log.message)}</span>
    `;

    // Apply active filter instantly
    const query = logSearchInput.value.toLowerCase().trim();
    if (query) {
        const match = log.level.toLowerCase().includes(query) || log.message.toLowerCase().includes(query);
        row.style.display = match ? "flex" : "none";
    }

    consoleOutput.appendChild(row);
}

function limitConsoleRows(maxRows) {
    const rows = consoleOutput.getElementsByClassName("log-row");
    while (rows.length > maxRows) {
        consoleOutput.removeChild(rows[0]);
    }
}

function filterLogs() {
    const query = logSearchInput.value.toLowerCase().trim();
    const rows = consoleOutput.getElementsByClassName("log-row");
    
    for (let row of rows) {
        const level = row.dataset.level.toLowerCase();
        const msg = row.dataset.message.toLowerCase();
        
        if (!query || level.includes(query) || msg.includes(query)) {
            row.style.display = "flex";
        } else {
            row.style.display = "none";
        }
    }
}

// ----------------- Alert Handling -----------------
function handleIncomingAlert(alert) {
    // Increment active alerts count
    const countSpan = document.getElementById("alert-count");
    let count = parseInt(countSpan.innerText) || 0;
    countSpan.innerText = count + 1;

    // Prepend to alerts list
    addAlertCard(alert, true);
}

function addAlertCard(alert, isNew = false) {
    const noAlerts = alertsList.querySelector(".no-alerts");
    if (noAlerts) {
        noAlerts.remove();
    }

    const card = document.createElement("div");
    card.className = `alert-item ${isNew ? "new-trigger" : ""}`;
    card.id = `alert-card-${alert.id || 'temp-' + Date.now()}`;
    card.dataset.ruleId = alert.rule_id;
    card.dataset.alertId = alert.id || "";

    // Clean timestamp for alert display
    let localTime = "";
    try {
        const dt = new Date(alert.timestamp);
        localTime = dt.toLocaleTimeString();
    } catch (_) {
        localTime = alert.timestamp;
    }

    card.innerHTML = `
        <div class="alert-content">
            <div class="alert-header-row">
                <span class="alert-title"><i class="fa-solid fa-triangle-exclamation"></i> ${escapeHTML(alert.rule_name)}</span>
                <span class="alert-time">${escapeHTML(localTime)}</span>
            </div>
            <p class="alert-desc">${escapeHTML(alert.message)}</p>
        </div>
        ${alert.id ? `<button class="btn-dismiss" onclick="dismissAlert(${alert.id})">解决</button>` : ''}
    `;

    alertsList.insertBefore(card, alertsList.firstChild);

    // Remove glow after 10s
    if (isNew) {
        setTimeout(() => {
            card.classList.remove("new-trigger");
        }, 10000);
    }
}

function handleAlertResolved(data) {
    // Decrease active alerts count
    const countSpan = document.getElementById("alert-count");
    let count = parseInt(countSpan.innerText) || 0;
    countSpan.innerText = Math.max(0, count - 1);

    // Find the alert card corresponding to this rule_id
    const card = alertsList.querySelector(`[data-rule-id="${data.rule_id}"]`);
    if (card) {
        card.classList.add("resolved");
        card.classList.remove("new-trigger");
        
        // Update alert title to green checkmark
        const titleSpan = card.querySelector(".alert-title");
        if (titleSpan) {
            titleSpan.innerHTML = `<i class="fa-solid fa-circle-check"></i> ${escapeHTML(titleSpan.innerText)} (已恢复)`;
        }

        // Update alert description
        const descP = card.querySelector(".alert-desc");
        if (descP) {
            descP.innerText = data.message;
        }

        // Remove the dismiss button since it's already resolved
        const dismissBtn = card.querySelector(".btn-dismiss");
        if (dismissBtn) {
            dismissBtn.remove();
        }

        // Slowly fade out and remove the alert card after 5 seconds
        setTimeout(() => {
            card.classList.add("fade-out");
            setTimeout(() => {
                card.remove();
                
                // If alerts list is now empty, add the default text back
                if (alertsList.querySelectorAll(".alert-item").length === 0) {
                    alertsList.innerHTML = `
                        <div class="no-alerts">
                            <i class="fa-solid fa-circle-check"></i>
                            <p>暂无活跃告警，系统运行平稳</p>
                        </div>`;
                }
            }, 800); // match transition duration
        }, 5000);
    }
}

// ----------------- API Requests -----------------
async function fetchLogs() {
    try {
        const res = await fetch("/api/logs?limit=50");
        const logs = await res.json();
        // Since we fetch recent (descending ID), reverse it to append in chronological order
        logs.reverse().forEach(log => appendLogRow(log));
        if (autoScrollCheckbox.checked) {
            consoleOutput.scrollTop = consoleOutput.scrollHeight;
        }
    } catch (err) {
        console.error("Failed to fetch logs history:", err);
    }
}

async function fetchRules() {
    try {
        const res = await fetch("/api/rules");
        const rules = await res.json();
        ruleCountVal.innerText = rules.length;
        
        rulesList.innerHTML = "";
        if (rules.length === 0) {
            rulesList.innerHTML = `<p class="no-alerts">暂无规则，可使用表单添加</p>`;
            return;
        }

        rules.forEach(rule => {
            const item = document.createElement("div");
            item.className = "rule-item";
            
            let metaHtml = "";
            if (rule.condition_type === "count") {
                metaHtml += `<span><i class="fa-solid fa-clock"></i> 窗口: ${rule.window_seconds}s</span>`;
                metaHtml += `<span><i class="fa-solid fa-arrow-up-9-1"></i> 频次: &ge;${rule.threshold}次</span>`;
                if (rule.level_filter) {
                    metaHtml += `<span><i class="fa-solid fa-filter"></i> 级别: ${escapeHTML(rule.level_filter)}</span>`;
                }
            } else if (rule.condition_type === "keyword") {
                metaHtml += `<span><i class="fa-solid fa-key"></i> 关键字: "${escapeHTML(rule.keyword_filter)}"</span>`;
                metaHtml += `<span><i class="fa-solid fa-clock"></i> 窗口: ${rule.window_seconds}s</span>`;
            } else if (rule.condition_type === "cpu_threshold") {
                metaHtml += `<span><i class="fa-solid fa-microchip"></i> CPU &ge; ${rule.threshold}%</span>`;
                metaHtml += `<span><i class="fa-solid fa-clock"></i> 窗口: ${rule.window_seconds}s</span>`;
            } else if (rule.condition_type === "ram_threshold") {
                metaHtml += `<span><i class="fa-solid fa-memory"></i> 内存 &ge; ${rule.threshold}MB</span>`;
                metaHtml += `<span><i class="fa-solid fa-clock"></i> 窗口: ${rule.window_seconds}s</span>`;
            } else if (rule.condition_type === "agent_offline") {
                metaHtml += `<span><i class="fa-solid fa-plug-circle-xmark"></i> 离线超时: ${rule.threshold}s</span>`;
            }

            item.innerHTML = `
                <div class="rule-info">
                    <h4>${escapeHTML(rule.name)}</h4>
                    <div class="rule-meta">${metaHtml}</div>
                </div>
                <button class="btn-delete" onclick="deleteRule(${rule.id})">
                    <i class="fa-regular fa-trash-can"></i>
                </button>
            `;
            rulesList.appendChild(item);
        });
    } catch (err) {
        console.error("Failed to fetch rules:", err);
    }
}

async function fetchAlerts() {
    try {
        const res = await fetch("/api/alerts?limit=30");
        const alerts = await res.json();
        
        const activeAlerts = alerts.filter(a => a.status === "active");
        alertCountVal.innerText = activeAlerts.length;

        alertsList.innerHTML = "";
        if (activeAlerts.length === 0) {
            alertsList.innerHTML = `
                <div class="no-alerts">
                    <i class="fa-solid fa-circle-check"></i>
                    <p>暂无活跃告警，系统运行平稳</p>
                </div>`;
            return;
        }

        activeAlerts.forEach(alert => {
            addAlertCard(alert, false);
        });
    } catch (err) {
        console.error("Failed to fetch alerts:", err);
    }
}

async function handleCreateRule(e) {
    e.preventDefault();
    
    const name = document.getElementById("rule-name").value.trim();
    const type = document.getElementById("rule-type").value;
    const windowSec = parseInt(document.getElementById("rule-window").value);
    const threshold = parseInt(document.getElementById("rule-threshold").value);
    const level = document.getElementById("rule-level").value;
    const keyword = document.getElementById("rule-keyword").value.trim();

    const payload = {
        name: name,
        condition_type: type,
        threshold: threshold,
        window_seconds: windowSec,
        level_filter: level || null,
        keyword_filter: keyword || null
    };

    try {
        const res = await fetch("/api/rules", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });
        
        if (res.ok) {
            ruleForm.reset();
            // Default select adjustments
            document.getElementById("rule-window").value = 10;
            document.getElementById("rule-threshold").value = 5;
            fetchRules();
        } else {
            const err = await res.json();
            alert(`创建失败: ${err.detail}`);
        }
    } catch (err) {
        console.error("Error creating rule:", err);
    }
}

async function deleteRule(id) {
    if (!confirm("确定要删除此告警规则吗？")) return;
    try {
        const res = await fetch(`/api/rules/${id}`, { method: "DELETE" });
        if (res.ok) {
            fetchRules();
        }
    } catch (err) {
        console.error("Error deleting rule:", err);
    }
}

async function dismissAlert(id) {
    try {
        const res = await fetch(`/api/alerts/${id}/dismiss`, { method: "POST" });
        if (res.ok) {
            fetchAlerts();
        }
    } catch (err) {
        console.error("Error dismissing alert:", err);
    }
}

// ----------------- Chart.js Dashboard -----------------
function initChart() {
    const ctx = document.getElementById("epsChart").getContext("2d");
    
    // Create time labels (e.g. -59s, -58s, ... 0s)
    for (let i = 59; i >= 0; i--) {
        epsLabels[59 - i] = i === 0 ? "现在" : `-${i}秒`;
    }

    epsChart = new Chart(ctx, {
        type: "line",
        data: {
            labels: epsLabels,
            datasets: [{
                label: "每秒事件数 (EPS)",
                data: epsHistory,
                borderColor: "#38bdf8",
                borderWidth: 2,
                backgroundColor: "rgba(56, 189, 248, 0.05)",
                fill: true,
                tension: 0.3,
                pointRadius: 0,
                pointHoverRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: {
                    grid: { color: "rgba(255, 255, 255, 0.03)" },
                    ticks: {
                        color: "#8b949e",
                        font: { size: 9 },
                        maxTicksLimit: 10
                    }
                },
                y: {
                    grid: { color: "rgba(255, 255, 255, 0.03)" },
                    ticks: {
                        color: "#8b949e",
                        font: { size: 9 },
                        stepSize: 5
                    },
                    min: 0
                }
            }
        }
    });
}

function updateEPS() {
    // Current second EPS value
    const eps = logsCountThisSecond;
    logsCountThisSecond = 0; // Reset counter for next second

    // Update numeric display
    epsVal.innerText = eps;

    // Scroll chart history
    epsHistory.shift();
    epsHistory.push(eps);

    // Update Chart
    epsChart.update("none"); // Update without animations for fast rendering
}

// ----------------- Utilities -----------------
function escapeHTML(str) {
    if (!str) return "";
    return str.replace(/[&<>'"]/g, 
        tag => ({
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            "'": '&#39;',
            '"': '&quot;'
        }[tag] || tag)
    );
}

// ----------------- Webhook Config Functions -----------------
async function fetchWebhookConfig() {
    try {
        const res = await fetch("/api/settings/webhook");
        const data = await res.json();
        if (data.webhook_url) {
            webhookUrlInput.value = data.webhook_url;
        }
    } catch (err) {
        console.error("Failed to fetch webhook config:", err);
    }
}

async function saveWebhookConfig() {
    const url = webhookUrlInput.value.trim();
    showWebhookStatus("正在保存...", "info");
    try {
        const res = await fetch("/api/settings/webhook", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ webhook_url: url })
        });
        if (res.ok) {
            showWebhookStatus("配置保存成功！", "success");
        } else {
            showWebhookStatus("保存失败，请重试。", "error");
        }
    } catch (err) {
        showWebhookStatus("保存发生异常，请检查网络。", "error");
    }
}

async function testWebhookConfig() {
    const url = webhookUrlInput.value.trim();
    if (!url) {
        showWebhookStatus("请先输入 Webhook 地址！", "error");
        return;
    }
    showWebhookStatus("正在发送测试推送...", "info");
    try {
        const res = await fetch("/api/settings/webhook/test", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ webhook_url: url })
        });
        const data = await res.json();
        if (res.ok) {
            showWebhookStatus("测试消息发送成功！请检查您的飞书/钉钉群组。", "success");
        } else {
            showWebhookStatus(`发送失败: ${data.detail || '未知错误'}`, "error");
        }
    } catch (err) {
        showWebhookStatus("发送发生异常，请检查网络。", "error");
    }
}

function showWebhookStatus(msg, type) {
    webhookStatus.innerText = msg;
    webhookStatus.className = `webhook-status-msg ${type}`;
    setTimeout(() => {
        if (webhookStatus.innerText === msg) {
            webhookStatus.innerText = "";
            webhookStatus.className = "webhook-status-msg";
        }
    }, 5000);
}
