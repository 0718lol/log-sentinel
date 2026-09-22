import sqlite3
import os
from datetime import datetime, timezone
from typing import Optional

# Location of the SQLite file; overridable via env (used by tests and multi-instance setups)
DB_PATH = os.environ.get("LOG_SENTINEL_DB_PATH") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "log_system.db")

def get_db_connection():
    """Get a connection to the SQLite database with row factory enabled."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize the database schema and insert default alerting rules."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create logs table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        level TEXT NOT NULL,
        message TEXT NOT NULL,
        source TEXT NOT NULL
    )
    """)

    # Create rules table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        condition_type TEXT NOT NULL, /* 'count' or 'keyword' */
        threshold INTEGER NOT NULL,
        window_seconds INTEGER NOT NULL,
        level_filter TEXT,
        keyword_filter TEXT,
        is_active INTEGER DEFAULT 1
    )
    """)

    # Create alerts table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        rule_name TEXT NOT NULL,
        rule_id INTEGER NOT NULL,
        message TEXT NOT NULL,
        status TEXT DEFAULT 'active', /* 'active', 'resolved' or 'dismissed' */
        source TEXT,                  /* agent node this alert is about (offline alerts only) */
        FOREIGN KEY (rule_id) REFERENCES rules(id)
    )
    """)

    # Migration for databases created before the source column existed
    cursor.execute("PRAGMA table_info(alerts)")
    columns = {row[1] for row in cursor.fetchall()}
    if "source" not in columns:
        cursor.execute("ALTER TABLE alerts ADD COLUMN source TEXT")

    # WAL journal mode: concurrent reads no longer block log writes (and vice versa)
    cursor.execute("PRAGMA journal_mode=WAL")

    # Indexes backing the hot rule-evaluation queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_alerts_rule_status ON alerts(rule_id, status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_alerts_source ON alerts(source)")

    # Create settings table for dynamic webhook storage
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)
    # Insert default empty webhook if not exists
    cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('webhook_url', '')")

    # Insert default rules if they don't exist
    default_rules = [
        ("High Error Rate (ERROR >= 5 in 10s)", "count", 5, 10, "ERROR", None),
        ("Critical Security Warning (WARN >= 3 in 15s)", "count", 3, 15, "WARN", None),
        ("Database Exception Keyword", "keyword", 1, 5, None, "Exception"),
        ("Out Of Memory Warning", "keyword", 1, 10, None, "OOM"),
        ("Host CPU Usage Overload (CPU >= 80%)", "cpu_threshold", 80, 10, None, None),
        ("Host Memory Leak Warning (RAM >= 100MB)", "ram_threshold", 100, 10, None, None),
        ("Agent Node Offline Alert", "agent_offline", 30, 30, None, None)
    ]

    for name, cond_type, threshold, window, lvl, kw in default_rules:
        try:
            cursor.execute("""
            INSERT INTO rules (name, condition_type, threshold, window_seconds, level_filter, keyword_filter, is_active)
            VALUES (?, ?, ?, ?, ?, ?, 1)
            """, (name, cond_type, threshold, window, lvl, kw))
        except sqlite3.IntegrityError:
            pass # Rule already exists

    conn.commit()
    conn.close()

def save_logs(log_entries):
    """Save batch of log entries to database.
    log_entries: list of dicts with keys: timestamp, level, message, source
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.executemany("""
    INSERT INTO logs (timestamp, level, message, source)
    VALUES (:timestamp, :level, :message, :source)
    """, log_entries)
    conn.commit()
    conn.close()

def get_recent_logs(limit=200):
    """Fetch recent logs from database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM logs ORDER BY id DESC LIMIT ?", (limit,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_rules():
    """Retrieve all alerting rules."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM rules")
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def add_rule(name, condition_type, threshold, window_seconds, level_filter=None, keyword_filter=None):
    """Create a new alerting rule."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
        INSERT INTO rules (name, condition_type, threshold, window_seconds, level_filter, keyword_filter)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (name, condition_type, threshold, window_seconds, level_filter, keyword_filter))
        conn.commit()
        success = True
    except sqlite3.IntegrityError:
        success = False
    conn.close()
    return success

def delete_rule(rule_id):
    """Delete a rule by ID."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM rules WHERE id = ?", (rule_id,))
    conn.commit()
    conn.close()

def save_alert(rule_id, rule_name, message, source: Optional[str] = None):
    """Save a triggered alert to the database. `source` identifies the affected agent node."""
    conn = get_db_connection()
    cursor = conn.cursor()
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    cursor.execute("""
    INSERT INTO alerts (timestamp, rule_name, rule_id, message, status, source)
    VALUES (?, ?, ?, ?, 'active', ?)
    """, (timestamp, rule_name, rule_id, message, source))
    conn.commit()
    alert_id = cursor.lastrowid
    conn.close()
    return alert_id

def get_recent_alerts(limit=50):
    """Fetch recent alerts."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT ?", (limit,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def dismiss_alert(alert_id):
    """Mark an alert as dismissed."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE alerts SET status = 'dismissed' WHERE id = ?", (alert_id,))
    conn.commit()
    conn.close()

def get_active_alert_by_rule(rule_id):
    """Fetch the active alert for a specific rule."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM alerts WHERE rule_id = ? AND status = 'active' ORDER BY id DESC LIMIT 1", (rule_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def resolve_alert_by_rule(rule_id):
    """Mark all active alerts for a rule as resolved."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE alerts SET status = 'resolved' WHERE rule_id = ? AND status = 'active'", (rule_id,))
    conn.commit()
    conn.close()

def get_active_agent_offline_alert(rule_id, source):
    """Fetch active offline alert for a specific agent node (exact source match)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM alerts
        WHERE rule_id = ? AND status = 'active' AND source = ?
        ORDER BY id DESC LIMIT 1
    """, (rule_id, source))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def resolve_agent_offline_alert(rule_id, source):
    """Mark active offline alert for a specific agent node as resolved (exact source match)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE alerts
        SET status = 'resolved'
        WHERE rule_id = ? AND status = 'active' AND source = ?
    """, (rule_id, source))
    conn.commit()
    conn.close()

def get_setting(key):
    """Retrieve configuration setting from settings table."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else ""

def save_setting(key, value):
    """Save/Update configuration setting in settings table."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
    conn.commit()
    conn.close()

# Initialize DB on load
if __name__ == "__main__":
    init_db()
    print("Database initialized successfully at:", DB_PATH)
