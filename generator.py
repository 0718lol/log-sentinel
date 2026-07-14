import time
import random
import argparse
from datetime import datetime

# Sample log message templates
INFO_TEMPLATES = [
    "User login successful for user_id={user_id}",
    "GET /api/v1/items/{item_id} status=200 duration={duration}ms",
    "POST /api/v1/cart/add status=201 duration={duration}ms",
    "Database connection pool active connections: {conn_count}",
    "Cache hit ratio: {cache_pct}% for key=namespace:{item_id}",
    "Job scheduler processed job_id={job_id} successfully",
    "Worker node-{node_id} heart beat acknowledged",
    "File uploaded successfully: doc_{item_id}.pdf, size={duration}KB",
]

WARN_TEMPLATES = [
    "Database query execution time elevated: {duration}ms (Threshold: 100ms)",
    "Slow response from authentication microservice: {duration}ms",
    "Disk utilization on node-{node_id} is at {cache_pct}%",
    "API rate limit approaching for client_ip=192.168.1.{conn_count}",
    "Connection replication lag: {duration}s on replica-{node_id}",
    "Session expiration warning for session_id={job_id}",
]

ERROR_TEMPLATES = [
    "Database connection pool exhausted! Cannot acquire connection.",
    "Exception: Database connection timeout on pool-{node_id}",
    "Out Of Memory (OOM) error on worker-thread-{job_id}",
    "Failed to charge credit card for user_id={user_id}. Error: INSIGNIFICANT_FUNDS",
    "CRITICAL: Service registration failed with Eureka/Consul server",
    "NullPointerException in controller.UserController.getUserProfile(UserController.java:142)",
    "Failed to write to write-ahead log (WAL) on disk space error",
]

def generate_log_line():
    """Generate a random, realistic-looking log line with level and timestamp."""
    rand = random.random()
    
    # Placeholders
    user_id = random.randint(1000, 9999)
    item_id = random.randint(100, 500)
    job_id = random.randint(50000, 99999)
    node_id = random.randint(1, 8)
    duration = random.randint(120, 2500) if rand > 0.8 else random.randint(5, 45)
    conn_count = random.randint(20, 95)
    cache_pct = random.randint(85, 99) if rand > 0.8 else random.randint(30, 80)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

    if rand < 0.80:
        level = "[INFO]"
        msg = random.choice(INFO_TEMPLATES).format(
            user_id=user_id, item_id=item_id, job_id=job_id, node_id=node_id, duration=duration, conn_count=conn_count, cache_pct=cache_pct
        )
    elif rand < 0.95:
        level = "[WARN]"
        msg = random.choice(WARN_TEMPLATES).format(
            user_id=user_id, item_id=item_id, job_id=job_id, node_id=node_id, duration=duration, conn_count=conn_count, cache_pct=cache_pct
        )
    else:
        level = "[ERROR]"
        msg = random.choice(ERROR_TEMPLATES).format(
            user_id=user_id, item_id=item_id, job_id=job_id, node_id=node_id, duration=duration, conn_count=conn_count, cache_pct=cache_pct
        )

    return f"{timestamp} {level} {msg}"

def main():
    parser = argparse.ArgumentParser(description="Log Stream Generator")
    parser.add_argument("--file", default="app.log", help="Path to write log file")
    parser.add_argument("--rate", type=float, default=10.0, help="Logs generated per second")
    args = parser.parse_args()

    print(f"[*] Starting Log Generator. Target file: {args.file}")
    print(f"[*] Generation Rate: {args.rate} logs/second (dynamic sleep: {1.0 / args.rate:.4f}s)")
    print("[*] Press Ctrl+C to stop.")

    log_count = 0
    start_time = time.time()

    with open(args.file, "a", encoding="utf-8") as f:
        try:
            while True:
                log_line = generate_log_line()
                f.write(log_line + "\n")
                f.flush() # Force write to disk immediately for tailer
                
                log_count += 1
                if log_count % 50 == 0:
                    elapsed = time.time() - start_time
                    current_rate = log_count / elapsed
                    print(f"[i] Generated {log_count} logs. Running rate: {current_rate:.2f} logs/sec")

                # Sleep to maintain requested rate
                time.sleep(1.0 / args.rate)
        except KeyboardInterrupt:
            print(f"\n[i] Stopping generator. Total logs generated: {log_count}")

if __name__ == "__main__":
    main()
