import asyncio
import os
import sys
import time
import argparse
import random
from datetime import datetime
import httpx
import psutil

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
    def __init__(self, filepath: str, server_url: str, source: str, batch_size: int, batch_interval: float):
        self.filepath = os.path.abspath(filepath)
        self.ingest_url = f"{server_url.rstrip('/')}/api/logs/ingest"
        self.source = source
        self.batch_size = batch_size
        self.batch_interval = batch_interval
        
        self.queue = None
        self.client = httpx.AsyncClient(trust_env=False)
        self.running = False

    async def start(self):
        self.running = True
        # Bounded Queue - core of backpressure. Stops tailing when full (1000 items limit).
        # Created inside start() to bind to the active asyncio event loop.
        self.queue = asyncio.Queue(maxsize=1000)
        print(f"[*] Starting Log Agent. Monitoring: {self.filepath}")
        print(f"[*] Target Log Server: {self.ingest_url}")
        print(f"[*] Bounded Queue size limit: 1000 lines (Backpressure enabled)")

        # Create tasks
        self.tailer_task = asyncio.create_task(self.tail_file())
        self.batcher_task = asyncio.create_task(self.batch_and_send())
        self.stats_task = asyncio.create_task(self.report_agent_metrics())

        await asyncio.gather(self.tailer_task, self.batcher_task, self.stats_task)

    async def stop(self):
        self.running = False
        self.tailer_task.cancel()
        self.batcher_task.cancel()
        self.stats_task.cancel()
        await self.client.aclose()
        print("[*] Log Agent stopped.")

    async def tail_file(self):
        """Tails the target file asynchronously, pushing lines to the bounded queue."""
        last_ino = None
        file_handle = None

        while self.running:
            try:
                if not os.path.exists(self.filepath):
                    # Wait for file to be created
                    if file_handle:
                        file_handle.close()
                        file_handle = None
                    await asyncio.sleep(1.0)
                    continue

                stat = os.stat(self.filepath)
                # Check for file rotation (inode change or size shrink)
                if last_ino != stat.st_ino or (file_handle and os.path.getsize(self.filepath) < file_handle.tell()):
                    print(f"[*] Log file rotated or recreated: {self.filepath}")
                    if file_handle:
                        file_handle.close()
                    file_handle = open(self.filepath, "r", encoding="utf-8")
                    last_ino = stat.st_ino
                    # If it's a new file, start from start. If not, seek to end
                    # Let's seek to end to avoid double-processing massive files, but start at beginning if empty
                    file_handle.seek(0, os.SEEK_END)

                if not file_handle:
                    file_handle = open(self.filepath, "r", encoding="utf-8")
                    last_ino = stat.st_ino
                    file_handle.seek(0, os.SEEK_END)

                # Read new lines
                line = file_handle.readline()
                if not line:
                    # No new content, wait a bit
                    await asyncio.sleep(0.1)
                    continue

                line_stripped = line.strip()
                if line_stripped:
                    # Put line in the bounded queue. If queue is full, this will yield control and block (Backpressure!)
                    # It will only resume when the batcher successfully sends batches to the server.
                    await self.queue.put(line_stripped)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[!] Error in file tailer: {e}", file=sys.stderr)
                await asyncio.sleep(2.0)

        if file_handle:
            file_handle.close()

    async def batch_and_send(self):
        """Gathers logs from the queue and sends them in batches with Exponential Backoff."""
        batch = []
        last_send_time = time.time()

        while self.running:
            try:
                # Wait for an item from the queue
                try:
                    timeout = max(0.01, self.batch_interval - (time.time() - last_send_time))
                    item = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                    batch.append(item)
                    self.queue.task_done()
                except asyncio.TimeoutError:
                    # Trigger send on interval timeout
                    pass

                # If batch size reached or timeout exceeded, send the batch
                if batch and (len(batch) >= self.batch_size or (time.time() - last_send_time) >= self.batch_interval):
                    await self.send_batch_with_retry(batch)
                    batch = []
                    last_send_time = time.time()

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[!] Error in batcher loop: {e}", file=sys.stderr)
                await asyncio.sleep(1.0)

    async def send_batch_with_retry(self, log_lines):
        """Sends a log batch to the server. Retries on failure using exponential backoff."""
        # Convert lines to ingest payloads
        payload = []
        for line in log_lines:
            payload.append({
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "level": detect_level(line),
                "message": line,
                "source": self.source
            })

        attempt = 0
        base_delay = 1.0
        max_delay = 15.0

        while self.running:
            try:
                response = await self.client.post(self.ingest_url, json=payload, timeout=5.0)
                if response.status_code == 200:
                    # Ingestion successful
                    if attempt > 0:
                        print(f"[*] Ingestion recovered after {attempt} retries.")
                    return
                elif response.status_code == 429:
                    # Server is rate limiting (Backpressure triggered by server)
                    print(f"[!] Server returned 429 Too Many Requests. Backing off...")
                else:
                    print(f"[!] Server returned error code {response.status_code}. Retrying...")
            except httpx.RequestError as e:
                print(f"[!] Connection failed to log server: {e}. Retrying...")

            # Calculate Exponential Backoff with Jitter (random variation)
            attempt += 1
            delay = min(max_delay, base_delay * (2 ** attempt))
            jitter = random.uniform(0.0, 0.5 * delay)
            sleep_time = delay + jitter
            
            print(f"[i] Backoff Active. Queue fill: {self.queue.qsize()}/1000. Retrying in {sleep_time:.2f}s...")
            await asyncio.sleep(sleep_time)

    async def report_agent_metrics(self):
        """Periodically puts agent system resource metrics into the queue as a log entry."""
        while self.running:
            await asyncio.sleep(10.0)
            try:
                cpu = psutil.cpu_percent(interval=None)
                mem = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024 # MB
                metric_log = f"[INFO] Agent heartbeat metrics: CPU {cpu:.1f}%, RAM {mem:.2f}MB, QueueSize {self.queue.qsize()}"
                
                # Try putting in queue without blocking, skip if full to avoid blocking heartbeat
                try:
                    self.queue.put_nowait(metric_log)
                except asyncio.QueueFull:
                    pass
            except Exception as e:
                print(f"[!] Error gathering agent metrics: {e}", file=sys.stderr)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lightweight Log Agent")
    parser.add_argument("--file", default="app.log", help="Path to log file to monitor")
    parser.add_argument("--server", default="http://localhost:8001", help="Log Server URL")
    parser.add_argument("--source", default="mac-local-agent", help="Source metadata identifier")
    parser.add_argument("--batch-size", type=int, default=50, help="Max batch size before sending")
    parser.add_argument("--batch-interval", type=float, default=0.5, help="Batch send timeout interval (seconds)")
    args = parser.parse_args()

    agent = LogAgent(
        filepath=args.file,
        server_url=args.server,
        source=args.source,
        batch_size=args.batch_size,
        batch_interval=args.batch_interval
    )

    try:
        asyncio.run(agent.start())
    except KeyboardInterrupt:
        print("\n[i] Shutting down agent...")
        asyncio.run(agent.stop())
