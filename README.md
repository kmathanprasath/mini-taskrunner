# Mini Async Task Runner -> Go + Redis

A task queue system built from scratch in Go using Redis as the message broker. No libraries like Celery or Sidekiq just raw Redis commands to understand how async task runners actually work under the hood.

---

## What Does This Do?

It processes jobs in the background. You submit a task (like "scan this AWS account"), and a pool of workers picks it up, executes it, and handles failures automatically.

Think of it like a restaurant kitchen:
- **Producer** = waiter taking orders
- **Queue** = the order tickets hanging on the rail
- **Workers** = chefs cooking in parallel
- **Dead Letter Queue** = orders that failed 3 times and need manager attention

---

## How It Works (Step by Step)

```
1. Producer creates a task → pushes it to a Redis list (the queue)
2. Worker atomically pops the task from queue → moves it to a "processing" list
3. Worker executes the task handler
4. On SUCCESS → removes task from processing list (acknowledged)
5. On FAILURE → retries with exponential backoff (2s, 4s, 8s...)
6. After max retries → moves task to Dead Letter Queue for investigation
```

---

## The 5 Files Explained

### `task.go` -> What is a task?

A task is a unit of work with:
- An ID (unique identifier)
- A type (which handler to run -> "compliance_scan", "send_email", etc.)
- A queue (which line to stand in -> "scans" or "default")
- A payload (the data the handler needs -> like `{"provider": "aws"}`)
- Retry state (how many times it's been attempted, when to retry next)
- Backoff calculator (each retry waits longer: 2s → 4s → 8s → 16s → max 5min)

### `broker.go` -> The Redis message transport

The broker is the middleman between producers and workers. It uses Redis commands:

| Operation | Redis Command | What It Does |
|-----------|--------------|--------------|
| Enqueue | `LPUSH` | Adds task to the left of the queue |
| Dequeue | `RPOPLPUSH` | Atomically moves task from queue → processing list |
| Ack | `LREM` | Removes task from processing list (done!) |
| Nack | `LREM` + `LPUSH` | Moves task back to queue for retry |
| DLQ | `LPUSH` to dlq list | Permanently failed task stored for debugging |
| Lock | `SET NX` | Prevents same task from running twice simultaneously |

**Why RPOPLPUSH?** It's atomic. If the worker crashes between popping and processing, the task is still in the processing list it's never lost. This gives us **at-least-once delivery**.

### `worker.go` -> The worker pool

The pool manages concurrency and task execution:

1. **Polling loop** -> continuously checks queues for new tasks (scans queue first, then default)
2. **Semaphore** -> a buffered Go channel that limits to 4 concurrent tasks max
3. **Rate limiter** -> a ticker that caps how fast tasks are dequeued (prevents thundering herd)
4. **Timeout** -> each task gets a context with deadline. If it takes too long, it's cancelled
5. **Retry logic** -> on failure, decides whether to retry or move to DLQ
6. **Graceful shutdown** -> on Ctrl+C, stops accepting new tasks and waits for in-flight tasks to finish

### `handlers.go` -> The actual work

These are simulated task handlers (in production, they'd call real APIs):

- `HandleComplianceScan` -> simulates scanning a cloud account (3-8 seconds). GCP intentionally fails to demonstrate retry + DLQ behavior
- `HandleSendEmail` -> simulates sending a notification (500ms)
- `HandleGenerateReport` -> simulates PDF generation (2 seconds)

### `main.go` -> Ties everything together

1. Connects to Redis
2. Registers handlers in the registry
3. Starts the worker pool (4 goroutines)
4. Produces 5 demo tasks with staggered delays
5. Runs a DLQ monitor that reports dead tasks every 15 seconds
6. Waits for Ctrl+C, then shuts down gracefully

---

## Key Concepts Demonstrated

### 1. Delivery Guarantees

**At-Least-Once** (what we use):
- Task is only removed from the system after successful processing
- If worker crashes mid-task, the task survives in the processing list
- Tradeoff: a task might run more than once (so handlers should be idempotent)

**At-Most-Once** (not used):
- Acknowledge immediately, process after. If crash → task lost forever

**Exactly-Once** (impossible in distributed systems):
- You can only approximate it with at-least-once + idempotent handlers
- The Two Generals Problem proves true exactly-once is impossible

### 2. Retry with Exponential Backoff

When a task fails:
```
Attempt 1 fails → wait 2 seconds → retry
Attempt 2 fails → wait 4 seconds → retry  
Attempt 3 fails → wait 8 seconds → retry
...max retries exceeded → move to Dead Letter Queue
```

Why exponential? If a downstream service is overloaded, hammering it with immediate retries makes things worse. Backing off gives it time to recover.

### 3. Dead Letter Queue (DLQ)

Tasks that fail permanently (after all retries exhausted) go to a separate queue. They sit there until a human:
- Investigates the root cause
- Fixes the bug
- Replays the tasks manually

Without a DLQ, poison messages (tasks that always fail) would retry forever and block the queue.

### 4. Concurrency Control

A buffered channel of size 4 acts as a semaphore:
```go
sem := make(chan struct{}, 4)  // max 4 tasks at once

sem <- struct{}{}  // acquire slot (blocks if 4 already running)
go processTask()
<-sem              // release slot when done
```

### 5. Idempotency Guard

Before processing, we set a Redis lock (`SET key NX` = set only if not exists):
- If lock acquired → safe to proceed
- If lock already exists → another worker is handling this task, skip it

Prevents duplicate execution when the same task accidentally gets dequeued twice.

### 6. Priority Queues

Workers check the `scans` queue before the `default` queue on every poll cycle. Compliance scans are more important than emails or reports, so they get processed first when both queues have work.

### 7. Staggered Dispatch

When enqueueing 100 tasks at once (like a cron triggering scans for all cloud accounts), each task gets a `NotBefore` timestamp:
```
Task 1: start immediately
Task 2: start after 2 seconds
Task 3: start after 4 seconds
...
```

This prevents all tasks from hitting downstream APIs simultaneously (thundering herd problem).

---

## Running It

```bash
# Start Redis (required)
docker run -d -p 6379:6379 redis:7-alpine

# Run the task runner
go run .

# Stop with Ctrl+C (graceful shutdown)
```

### Expected Output

```
Connected to Redis at localhost:6379
Worker pool started: concurrency=4, queues=[scans default]

[producer] Enqueuing task_1 (type=compliance_scan) immediately
[producer] Enqueuing task_2 (type=compliance_scan) with 2s delay
[producer] Enqueuing task_3 (type=send_email) with 4s delay
[producer] Enqueuing task_4 (type=generate_report) with 6s delay
[producer] Enqueuing task_5 (type=compliance_scan) with 8s delay

[worker] Processing task_1 (type=compliance_scan, attempt=1/3)
[scan] ✓ Scan complete for aws: 73 findings (20 FAIL)
[worker] ✓ Task_1 completed in 5s

[worker] Processing task_2 (type=compliance_scan, attempt=1/3)
[scan] ✓ Scan complete for azure: 91 findings (8 FAIL)
[worker] ✓ Task_2 completed in 3s

[worker] Processing task_5 (type=compliance_scan, attempt=1/3)
[worker] ✗ Task_5 failed (attempt 1/3), retrying in 2s: GCP API error
[worker] ✗ Task_5 failed (attempt 2/3), retrying in 4s: GCP API error
[worker] ✗ Task_5 PERMANENTLY FAILED after 3 attempts
[dlq] Task_5 moved to DLQ

[dlq-monitor] Queue 'scans' DLQ has 1 failed task
```

### Clean Redis Between Runs

```bash
redis-cli FLUSHDB
```

---

## Project Structure

```
async/
├── main.go        -> Entry point, producer, DLQ monitor, shutdown
├── task.go        -> Task struct, status types, backoff math
├── broker.go      -> Redis operations (enqueue, dequeue, ack, DLQ, locks)
├── worker.go      -> Worker pool, concurrency, timeout, retry logic
├── handlers.go    -> Simulated task handlers (scan, email, report)
├── go.mod         -> Go module + Redis dependency
├── go.sum         -> Dependency checksums
└── README.md      -> This file
```

---

## How This Maps to Real Systems

| This Project | Celery (Python) | Sidekiq (Ruby) | Bull (Node.js) |
|---|---|---|---|
| Redis LPUSH/RPOP | Redis broker | Redis queues | Redis + Bull protocol |
| Worker goroutines | Prefork workers | Thread pool | Node event loop + workers |
| Semaphore channel | `--concurrency=N` | `-c N` threads | `concurrency` option |
| RPOPLPUSH + Ack | `acks_late=True` | `super_fetch` | Lock-based processing |
| Exponential backoff | `retry(countdown=)` | `sidekiq_retry_in` | `backoff` option |
| DLQ list | Flower dead tasks | Dead set | Failed job set |
| `SET NX` lock | `unique_on` plugins | `unique_for` | Bull unique jobs |

---

## Scaling This to Production

1. **More workers** -> Run multiple instances of this binary (horizontal scaling)
2. **Redis Streams** -> Replace Lists with Streams for consumer groups (multiple consumers, message replay)
3. **Delayed queue** -> Use Redis Sorted Sets (`ZADD` with score=timestamp) for proper scheduled tasks
4. **Metrics** -> Export queue depth, processing time, error rate to Prometheus
5. **Health check** -> Add HTTP `/health` endpoint for Kubernetes liveness probes
6. **Circuit breaker** -> Stop processing if downstream keeps failing (prevent cascade)
7. **Sharding** -> Partition queues by tenant or region for multi-tenant systems
