import asyncio
import os
import sys
import time
import argparse
import random
import hashlib
from spool import Spool
from datetime import datetime, timezone
import httpx
import psutil


def utc_now_iso() -> str:
    """Current UTC time as an ISO-8601 string with Z suffix."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

# Level parser helper
def detect_level(message: str) -> str:
    msg_upper = message.upper()
    if "[ERROR]" in msg_upper or "ERROR" in msg_upper or "EXCEPTION" in msg_upper or "FAILED" in msg_upper:
        return "ERROR"
    elif "[WARN]" in msg_upper or "WARN" in msg_upper or "WARNING" in msg_upper:
        return "WARN"
    else:
        return "INFO"

class LogAgent:
    def __init__(self, filepath: str, server_url: str, source: str, batch_size: int, batch_interval: float, token: str = "", state_path: str = None, spool_limit: int = 10000):
        self.filepath = os.path.abspath(filepath)
        self.ingest_url = f"{server_url.rstrip('/')}/api/logs/ingest"
        self.source = source
        self.batch_size = batch_size
        self.batch_interval = batch_interval

        # Sent with every request when the server enables LOG_SENTINEL_TOKEN auth
        self.headers = {"X-API-Token": token} if token else {}

        state_path = state_path or os.path.join(".agent-state", hashlib.sha256(
            (self.filepath + "\0" + source).encode()).hexdigest() + ".db")
        self.spool = Spool(state_path, spool_limit)
        self.client = httpx.AsyncClient(trust_env=False)
        self.running = False

    async def start(self):
        self.running = True
        print(f"[*] Starting Log Agent. Monitoring: {self.filepath}")
        print(f"[*] Target Log Server: {self.ingest_url}")
        print(f"[*] Durable outbox limit: {self.spool.limit} events (backpressure enabled)")

        # Create tasks
        self.tailer_task = asyncio.create_task(self.tail_file())
        self.batcher_task = asyncio.create_task(self.batch_and_send())
        self.stats_task = asyncio.create_task(self.report_agent_metrics())

        try:
            await asyncio.gather(self.tailer_task, self.batcher_task, self.stats_task)
        except asyncio.CancelledError:
            pass
        finally:
            self.running = False
            for task in (self.tailer_task, self.batcher_task, self.stats_task):
                task.cancel()
            await asyncio.gather(self.tailer_task, self.batcher_task, self.stats_task, return_exceptions=True)
            await self.client.aclose()
            self.spool.close()
            print("[*] Log Agent stopped.")

    async def tail_file(self):
        """Resume byte offsets, drain renamed files, and persist complete lines only."""
        handle = None
        identity = None
        anchor = b""
        try:
            while self.running:
                if handle is None:
                    checkpoint = self.spool.checkpoint(self.filepath)
                    candidates = [self.filepath]
                    # Recover an unread renamed file in the same directory after restart.
                    if checkpoint:
                        candidates += [os.path.join(os.path.dirname(self.filepath), name)
                                       for name in sorted(os.listdir(os.path.dirname(self.filepath)))]
                    chosen = None
                    for candidate in candidates:
                        try:
                            st = os.stat(candidate)
                            candidate_id = f"{st.st_dev}:{st.st_ino}"
                            if checkpoint and candidate_id == checkpoint[0]:
                                chosen = candidate
                                break
                        except FileNotFoundError:
                            continue
                    if chosen is None:
                        chosen = self.filepath
                    try:
                        handle = open(chosen, "rb")
                    except FileNotFoundError:
                        await asyncio.sleep(0.1)
                        continue
                    st = os.fstat(handle.fileno())
                    identity = f"{st.st_dev}:{st.st_ino}"
                    offset = checkpoint[1] if checkpoint and checkpoint[0] == identity else 0
                    anchor = checkpoint[2] if offset else b""
                    handle.seek(max(0, offset - len(anchor)))
                    if st.st_size < offset or handle.read(len(anchor)) != anchor:
                        offset, anchor = 0, b""
                    handle.seek(offset)
                offset = handle.tell()
                # Detect copytruncate even if the replacement has already grown.
                handle.seek(max(0, offset - len(anchor)))
                changed = handle.read(len(anchor)) != anchor
                handle.seek(offset)
                if os.fstat(handle.fileno()).st_size < offset or changed:
                    handle.seek(0)
                    anchor = b""
                    offset = 0
                line = handle.readline()
                if line.endswith(b"\n"):
                    end = handle.tell()
                    handle.seek(max(0, end - 128))
                    next_anchor = handle.read(end - max(0, end - 128))
                    handle.seek(end)
                    message = line.rstrip(b"\r\n").decode("utf-8", errors="replace")
                    payload = {"timestamp": utc_now_iso(), "level": detect_level(message),
                               "message": message, "source": self.source}
                    if self.spool.append(payload, (self.filepath, identity, end, next_anchor)):
                        anchor = next_anchor
                    else:
                        handle.seek(offset)
                        await asyncio.sleep(0.1)
                    continue
                handle.seek(offset)
                try:
                    st = os.stat(self.filepath)
                    rotated = f"{st.st_dev}:{st.st_ino}" != identity
                except FileNotFoundError:
                    rotated = False
                if rotated:
                    if line:
                        raise RuntimeError("Rotated file ends in an incomplete line; preserve it and repair before resuming")
                    handle.close()
                    handle = None
                    # Retire the drained file so restart recovery selects the new file.
                    with self.spool.db:
                        self.spool.db.execute('DELETE FROM checkpoints WHERE path=?', (self.filepath,))
                await asyncio.sleep(0.1)
        finally:
            if handle:
                handle.close()

    async def batch_and_send(self):
        while self.running:
            batch = self.spool.batch(self.batch_size)
            if batch:
                await self.send_batch_with_retry(batch)
            else:
                await asyncio.sleep(self.batch_interval)

    async def send_batch_with_retry(self, log_lines):
        """Sends a log batch to the server. Retries transient failures with exponential backoff."""
        payload = log_lines

        attempt = 0
        base_delay = 1.0
        max_delay = 15.0

        while self.running:
            try:
                response = await self.client.post(self.ingest_url, json=payload, timeout=5.0, headers=self.headers)
                if response.status_code == 200:
                    expected = {entry["event_id"] for entry in payload}
                    try:
                        acknowledged = set(response.json()["acknowledged_ids"])
                    except (ValueError, KeyError, TypeError):
                        raise RuntimeError("Server did not return durable event acknowledgements")
                    if acknowledged != expected:
                        raise RuntimeError("Server acknowledgement does not match submitted events")
                    self.spool.acknowledge(acknowledged)
                    # Ingestion successful
                    if attempt > 0:
                        print(f"[*] Ingestion recovered after {attempt} retries.")
                    return
                elif 400 <= response.status_code < 500 and response.status_code != 429:
                    raise RuntimeError(f"HTTP {response.status_code}: batch retained in durable spool; fix configuration and restart")
                elif response.status_code == 429:
                    # Server is rate limiting (Backpressure triggered by server)
                    print(f"[!] Server returned 429 Too Many Requests. Backing off...")
                else:
                    print(f"[!] Server returned error code {response.status_code}. Retrying...")
            except httpx.RequestError as e:
                print(f"[!] Connection failed to log server: {e}. Retrying...")

            # Calculate Exponential Backoff with Jitter (random variation)
            attempt += 1
            delay = min(max_delay, base_delay * (2 ** min(attempt, 10)))
            jitter = random.uniform(0.0, 0.5 * delay)
            sleep_time = delay + jitter
            
            print(f"[i] Backoff Active. Queue fill: {self.spool.pending_count()}/{self.spool.limit}. Retrying in {sleep_time:.2f}s...")
            await asyncio.sleep(sleep_time)

    async def report_agent_metrics(self):
        """Persist heartbeat events when outbox capacity is available."""
        while self.running:
            await asyncio.sleep(10.0)
            try:
                cpu = psutil.cpu_percent(interval=None)
                mem = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024 # MB
                metric_log = f"[INFO] Agent heartbeat metrics: CPU {cpu:.1f}%, RAM {mem:.2f}MB, QueueSize {self.spool.pending_count()}"
                
                self.spool.append({"timestamp": utc_now_iso(), "level": "INFO",
                                   "message": metric_log, "source": self.source})
            except Exception as e:
                print(f"[!] Error gathering agent metrics: {e}", file=sys.stderr)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lightweight Log Agent")
    parser.add_argument("--file", default="app.log", help="Path to log file to monitor")
    parser.add_argument("--server", default="http://localhost:8001", help="Log Server URL")
    parser.add_argument("--source", default="mac-local-agent", help="Source metadata identifier")
    parser.add_argument("--batch-size", type=int, default=50, help="Max batch size before sending")
    parser.add_argument("--batch-interval", type=float, default=0.5, help="Batch send timeout interval (seconds)")
    parser.add_argument("--token", default=os.environ.get("LOG_SENTINEL_TOKEN", ""), help="API token if the server enables auth (or set LOG_SENTINEL_TOKEN)")
    parser.add_argument("--state", default=None, help="Durable state database; one process per database")
    parser.add_argument("--spool-limit", type=int, default=10000)
    args = parser.parse_args()
    if args.batch_size < 1 or args.batch_interval <= 0 or args.spool_limit < 1:
        parser.error("batch-size, batch-interval and spool-limit must be positive")

    agent = LogAgent(
        filepath=args.file,
        server_url=args.server,
        source=args.source,
        batch_size=args.batch_size,
        batch_interval=args.batch_interval,
        state_path=args.state,
        spool_limit=args.spool_limit,
        token=args.token
    )

    # stop() is folded into start()'s finally block: cancelling from a fresh event
    # loop here would touch tasks/sockets bound to the loop asyncio.run just closed.
    try:
        asyncio.run(agent.start())
    except KeyboardInterrupt:
        print("\n[i] Shutting down agent...")
