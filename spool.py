"""Durable file checkpoints and outbox; one agent owns each state database."""
import fcntl
import json
import os
import sqlite3
import uuid


class Spool:
    def __init__(self, path, limit=10000):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.lock = open(str(path) + ".lock", "a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close()
            raise RuntimeError("State database is already owned by another agent")
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS checkpoints (
                path TEXT PRIMARY KEY, identity TEXT, offset INTEGER, anchor BLOB);
            CREATE TABLE IF NOT EXISTS outbox (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE, payload TEXT);
        ''')
        self.limit = limit

    def checkpoint(self, path):
        return self.db.execute('SELECT identity, offset, anchor FROM checkpoints WHERE path=?', (path,)).fetchone()

    def pending_count(self):
        return self.db.execute('SELECT count(*) FROM outbox').fetchone()[0]

    def append(self, payload, checkpoint=None):
        if self.pending_count() >= self.limit:
            return False
        payload = dict(payload, event_id=uuid.uuid4().hex)
        with self.db:
            self.db.execute('INSERT INTO outbox(event_id,payload) VALUES (?,?)',
                            (payload['event_id'], json.dumps(payload)))
            if checkpoint:
                self.db.execute('INSERT OR REPLACE INTO checkpoints VALUES (?,?,?,?)', checkpoint)
        return True

    def batch(self, size):
        return [json.loads(row[0]) for row in self.db.execute(
            'SELECT payload FROM outbox ORDER BY sequence LIMIT ?', (size,))]

    def acknowledge(self, ids):
        with self.db:
            self.db.executemany('DELETE FROM outbox WHERE event_id=?', [(x,) for x in ids])

    def close(self):
        self.db.close()
        self.lock.close()
