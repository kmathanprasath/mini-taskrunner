# Interview Prep — Async Task Runners & Distributed Systems

Every question below is answered with reference to this repo's code where applicable.

---

## SECTION 1: ASYNC TASK RUNNERS (Core of the Role)

---

### Q: How do task queues work? Explain the components.

**Answer:**

A task queue has 4 core components:

1. **Producer** — creates tasks and pushes them to the queue. In this repo: `main.go produceDemoTasks()` calls `broker.Enqueue()`. In production: your API endpoint that triggers a scan.

2. **Broker** — the message transport that holds tasks until workers pick them up. In this repo: Redis. Tasks are stored in Redis Lists (`queue:scans`, `queue:default`). The broker decouples producers from consumers — they don't need to know about each other.

3. **Queue** — a FIFO data structure holding pending tasks. In this repo: Redis List with LPUSH (add to left) and RPOP (take from right). Multiple named queues allow priority and isolation (`scans` queue is checked before `default`).

4. **Worker** — a process/goroutine that dequeues tasks and executes them. In this repo: `worker.go WorkerPool` runs 4 goroutines, each pulling tasks and calling the registered handler.

**How they interact:**
```
Producer → LPUSH → Redis List (queue) → RPOPLPUSH → Worker → Execute Handler
```

---

### Q: What is a Dead Letter Queue (DLQ)? Why do you need one?

**Answer:**

A DLQ is a separate queue where tasks go after they've permanently failed (exhausted all retries).

**Why it exists:**
- Without a DLQ, a "poison message" (a task that always fails) would retry forever, blocking the queue
- DLQ lets you isolate failures for human investigation
- After fixing the root cause, you can replay DLQ tasks

**In this repo:** `broker.go MoveToDLQ()` pushes the failed task to `queue:{name}:dlq`. The `main.go runDLQMonitor()` checks DLQ size every 15 seconds (like a K8s CronJob).

**Real-world example:** In our compliance engine, `cleanup_stuck_scans` runs every 15 minutes. If a scan has been stuck in PENDING/RUNNING for >45 minutes (worker crashed), it marks it FAILED. Same concept as DLQ — isolate the failure, don't let it block new scans.

---

### Q: Explain retry logic and backoff strategies.

**Answer:**

When a task fails, you have three choices:
1. **No retry** — task is lost (at-most-once)
2. **Immediate retry** — hammers the failing service, makes things worse
3. **Exponential backoff** — wait longer between each retry (correct approach)

**Exponential backoff formula in this repo** (`task.go BackoffDuration()`):
```
Retry 1: wait 2 seconds
Retry 2: wait 4 seconds
Retry 3: wait 8 seconds
Retry 4: wait 16 seconds
...capped at 5 minutes max
```

**Why exponential?** If a downstream service is overloaded, immediate retries add load. Backing off gives it time to recover. The exponential curve means early retries are fast (transient errors resolve quickly) but persistent failures don't create a retry storm.

**In this repo:** `worker.go handleFailure()` checks `task.RetryCount <= task.MaxRetry`. If yes → calculate backoff → set `task.NextRetryAt` → re-enqueue. If no → move to DLQ.

**Jitter (production enhancement):** Add random jitter to backoff to prevent synchronized retries from multiple workers hitting the same service at the same moment.

---

### Q: Explain delivery guarantees. What's the difference between at-least-once, at-most-once, and exactly-once?

**Answer:**

**At-Most-Once:**
- Acknowledge the task immediately when dequeued, before processing
- If worker crashes during processing → task is lost forever
- Simple but unacceptable for important work (compliance scans, payments)
- Example: Fire-and-forget logging, analytics events where loss is tolerable

**At-Least-Once (what this repo implements):**
- Acknowledge the task only AFTER successful processing
- If worker crashes during processing → task stays in the processing list → can be recovered and re-executed
- Tradeoff: task might execute more than once (if worker crashes after completing but before acking)
- Solution: make handlers idempotent (running twice produces same result)
- Example: This repo uses RPOPLPUSH (atomic move to processing list) + Ack only after handler returns success

**Exactly-Once (impossible in distributed systems):**
- The Two Generals Problem proves you cannot guarantee exactly-once delivery over an unreliable network
- What you CAN do: at-least-once delivery + idempotent handlers = "effectively once"
- Why it's hard: between "task completed" and "ack sent", the network can fail. The broker doesn't know if the task completed or not, so it must redeliver (at-least-once) or accept loss (at-most-once)

**In this repo:**
- `broker.go Dequeue()` uses RPOPLPUSH — atomically moves task from queue to processing list
- `broker.go Ack()` removes from processing list only after handler succeeds
- `broker.go AcquireLock()` uses SETNX for idempotency — prevents duplicate concurrent execution

---

### Q: What is idempotency? Why does it matter for task queues?

**Answer:**

An operation is idempotent if executing it multiple times produces the same result as executing it once.

**Why it matters:** With at-least-once delivery, a task might run twice (worker crashes after completing but before acking). If the handler isn't idempotent, you get duplicate side effects (double charges, duplicate emails, corrupted data).

**How to achieve idempotency:**
1. **Unique constraint** — database rejects duplicate inserts (e.g., unique scan_id)
2. **Idempotency key** — check if this exact operation already completed before doing it again
3. **Distributed lock** — prevent concurrent execution of the same task

**In this repo:** `broker.go AcquireLock()` uses Redis `SET NX` (set if not exists) with a TTL. Before processing, the worker tries to acquire the lock. If another worker already has it → skip. This is the same pattern as the compliance engine's `if already_running: skip` check.

---

### Q: Explain scheduling patterns — cron-style vs event-driven.

**Answer:**

**Cron-style (time-based):**
- Tasks fire at fixed intervals: "every Sunday at 3 AM", "every 15 minutes"
- Good for: periodic scans, cleanup jobs, report generation
- In this repo's backend: `cron_scan.py` triggered by OS cron / K8s CronJob
- In K8s: `cronjob-scan.yaml` with schedule `"30 21 * * 6"` (weekly)

**Event-driven (trigger-based):**
- Tasks fire when something happens: "user clicked scan", "file uploaded", "webhook received"
- Good for: on-demand scans, real-time processing, reactive workflows
- In this repo's backend: API endpoint creates a scan → calls `run_compliance_scan.delay()`

**Priority queues:**
- Different queues for different importance levels
- In this repo: `scans` queue is polled before `default` queue in `worker.go pollLoop()`
- Compliance scans are more important than emails or reports

**Rate limiting:**
- Cap how fast tasks are processed to protect downstream services
- In this repo: `worker.go rateTick` ticker limits dequeue rate to 10/sec
- In the backend: `rate_limit="10/m"` on the Celery task decorator

**Fan-out pattern:**
- One trigger dispatches many tasks (e.g., cron triggers scans for ALL 100 providers)
- Problem: 100 tasks starting simultaneously overwhelms cloud APIs
- Solution: staggered dispatch — each task gets an increasing delay
- In this repo: `main.go` sets `task.NotBefore = time.Now().Add(i * 2s)`
- In the backend: `cron_scan.py` uses `countdown=delay` incrementing by 30s per provider

---

## SECTION 2: DISTRIBUTED SYSTEMS THEORY

---

### Q: Explain the CAP theorem.

**Answer:**

In a distributed system, you can only guarantee 2 out of 3:
- **C**onsistency — every read returns the most recent write
- **A**vailability — every request gets a response (even if stale)
- **P**artition tolerance — system works despite network splits between nodes

**You must always choose P** (networks WILL partition), so the real choice is:
- **CP** (Consistency + Partition tolerance) — reject requests during partition rather than serve stale data
  - Examples: PostgreSQL, etcd, ZooKeeper, Redis (single-node)
  - Use when: financial transactions, leader election, task deduplication
- **AP** (Availability + Partition tolerance) — serve potentially stale data rather than reject requests
  - Examples: Cassandra, DynamoDB, DNS, CDNs
  - Use when: shopping carts, social media feeds, analytics

**How this applies to our task runner:**
- Our Redis broker is CP (single-node Redis is consistent but unavailable if it goes down)
- Task state must be consistent — we can't have two workers thinking they own the same task
- The SETNX lock in `broker.go` is a CP operation — it either succeeds or fails, never returns "maybe"

---

### Q: Explain consistency models.

**Answer:**

From strongest to weakest:

**Linearizability (strongest):**
- Operations appear to happen instantaneously at some point between invocation and response
- Like a single-threaded system — everyone sees the same order
- Expensive (requires coordination between all nodes)
- Example: Our Redis SETNX lock — either you got the lock or you didn't, no ambiguity

**Strong consistency:**
- After a write completes, all subsequent reads return that write
- Example: PostgreSQL with synchronous replication

**Causal consistency:**
- If operation A caused operation B, everyone sees A before B
- But concurrent operations can be seen in different orders by different nodes
- Example: "I posted a comment, then edited it" — everyone sees post before edit

**Read-your-writes:**
- After you write, YOUR subsequent reads see your write (but others might not yet)
- Example: After updating your profile, you see the new name immediately

**Eventual consistency (weakest):**
- Given enough time with no new writes, all replicas converge to the same value
- No guarantee about WHEN — could be milliseconds or minutes
- Example: DNS propagation, Cassandra with quorum=1

**In this repo:**
- Task state in Redis is strongly consistent (single-node Redis)
- The SETNX lock provides linearizable mutual exclusion
- If we scaled to Redis Cluster, we'd need to think about consistency during failover

---

### Q: Explain the Raft consensus algorithm and leader election.

**Answer:**

Raft solves: "How do N nodes agree on a single value (or sequence of operations) even if some nodes crash?"

**Three roles:**
1. **Leader** — handles all client requests, replicates to followers
2. **Follower** — passively replicates what leader sends
3. **Candidate** — a follower that hasn't heard from leader and starts an election

**Leader election process:**
1. Follower's election timeout expires (hasn't heard from leader)
2. Follower becomes Candidate, increments term, votes for itself
3. Candidate requests votes from all other nodes
4. If majority votes yes → becomes Leader
5. Leader sends heartbeats to prevent new elections

**Split-brain problem:**
- Network partition splits cluster into two groups
- Each group might elect its own leader → two leaders (split brain)
- Raft prevents this: you need MAJORITY votes. With 5 nodes split 3/2, only the group of 3 can elect a leader. The group of 2 is stuck (no majority).

**Where Raft is used:**
- **etcd** — Kubernetes' brain. Stores all cluster state. Uses Raft for replication.
- **Kubernetes leader election** — controllers use etcd leases to elect one active instance
- **CockroachDB, TiKV** — distributed databases using Raft per partition

**Relevance to task queues:**
- If we ran multiple Redis instances for HA, we'd need consensus on "who owns this task"
- Redis Sentinel uses a simpler leader election (not Raft) for failover
- In production: Redis Cluster or etcd-backed coordination for distributed task ownership

---

### Q: What are the key chapters of DDIA and what do they teach?

**Answer:**

**Chapter 5 — Replication:**
- Single-leader, multi-leader, leaderless replication
- Replication lag and its consequences (read-your-writes, monotonic reads)
- Conflict resolution in multi-leader setups

**Chapter 6 — Partitioning (Sharding):**
- Key-range vs hash partitioning
- How to rebalance partitions when adding/removing nodes
- Secondary indexes in partitioned systems

**Chapter 7 — Transactions:**
- ACID guarantees and what they actually mean
- Isolation levels (read committed, snapshot isolation, serializable)
- Distributed transactions and 2PC (two-phase commit)

**Chapter 8 — The Trouble with Distributed Systems:**
- Unreliable networks (packets lost, delayed, duplicated)
- Unreliable clocks (NTP drift, leap seconds)
- Process pauses (GC, VM migration)
- Why you can't trust any single source of truth without consensus

**Chapter 9 — Consistency and Consensus:**
- Linearizability and its cost
- Ordering guarantees (total order broadcast)
- Consensus algorithms (Raft, Paxos)
- Why exactly-once is hard (Two Generals Problem, FLP impossibility)

---

### Q: How would you scale a task queue to 100 million users?

**Answer:**

**Level 1 — Vertical scaling:**
- Bigger machine, more RAM, faster Redis
- Limit: single machine has a ceiling

**Level 2 — Horizontal scaling (what our K8s backend does):**
- Run multiple worker pods (HPA scales 2→8 based on CPU/memory)
- All workers consume from the same Redis queue
- RPOPLPUSH ensures no two workers grab the same task
- This handles ~10K-100K tasks/day easily

**Level 3 — Queue sharding (100M users):**
- Single Redis instance becomes bottleneck at ~100K ops/sec
- Shard queues by tenant, region, or hash: `queue:scans:us-east`, `queue:scans:eu-west`
- Each shard has its own Redis instance + worker pool
- Routing layer decides which shard handles which task

**Level 4 — Replace Redis Lists with Redis Streams or Kafka:**
- Redis Streams: consumer groups, message acknowledgment, replay from offset
- Kafka: persistent log, millions of messages/sec, multi-consumer replay
- Both support partitioning natively

**Level 5 — Multi-region:**
- Deploy queue infrastructure in each region
- Tasks stay local to their region (data locality)
- Cross-region only for coordination (leader election, global state)

**Backpressure mechanisms:**
- Queue depth monitoring — alert when queue grows faster than workers drain it
- Rate limiting at ingestion — reject/throttle new tasks when system is saturated
- Priority queues — critical tasks get processed first, bulk tasks wait
- Circuit breaker — stop enqueueing if downstream is dead

---

### Q: What are the common failure modes in distributed systems?

**Answer:**

**Network partitions:**
- Nodes can't communicate but are still running
- Split-brain: each partition thinks it's the leader
- In our task runner: if Redis becomes unreachable, workers can't dequeue. They retry connection with backoff.

**Clock skew and NTP:**
- Different machines have different clock times (even with NTP, drift of 10-100ms is normal)
- Dangerous for: TTL-based locks, event ordering, timeout calculations
- In our task runner: `task.NotBefore` uses local clock. If clocks differ between producer and worker, task might start early/late.
- Solution: use logical clocks (Lamport timestamps) or centralized time source for ordering

**Partial failures:**
- Some nodes fail while others continue
- You can't tell if a node is dead or just slow (network timeout ≠ node death)
- In our task runner: if a worker crashes mid-task, the task stays in the processing list. The DLQ monitor (or a reaper) must detect and recover it.

**Cascading failures:**
- Service A fails → Service B (which depends on A) gets timeouts → B's thread pool exhausts → B fails → C fails...
- Prevention: circuit breakers, bulkheads, timeouts, rate limiting
- In our task runner: if all tasks fail (downstream is dead), exponential backoff prevents retry storms. A circuit breaker would stop processing entirely until downstream recovers.

**Byzantine faults:**
- A node sends WRONG data (not just no data) — lies, corruption, malicious behavior
- Most systems assume non-Byzantine (crash-fault tolerance only)
- Relevant for: blockchain, multi-party computation. NOT relevant for internal task queues.

---

## SECTION 3: PRODUCTION OPERATIONS & DEBUGGING

---

### Q: Explain SLIs, SLOs, and SLAs.

**Answer:**

**SLI (Service Level Indicator)** — a metric that measures service health
- Examples: request latency P99, error rate, task completion rate, queue depth
- For our task runner: "percentage of tasks completed within 30 seconds"

**SLO (Service Level Objective)** — a target value for an SLI
- Example: "99.9% of tasks complete within 30 seconds"
- Internal commitment — the team agrees to maintain this
- Defines the "error budget": if SLO is 99.9%, you can tolerate 0.1% failures per month

**SLA (Service Level Agreement)** — a contract with consequences
- Example: "If uptime drops below 99.9%, customer gets credits"
- External commitment — legal/financial penalties for violation
- SLA is always looser than SLO (you want buffer)

**Error budget math:**
- 99.9% uptime per month = 43.2 minutes of allowed downtime
- 99.99% = 4.3 minutes
- 99.95% = 21.6 minutes

**For a task queue SLO:**
- "99.5% of enqueued tasks are processed within 60 seconds"
- "DLQ size never exceeds 100 tasks for more than 15 minutes"
- "Worker pool recovers from failure within 2 minutes (MTTR)"

---

### Q: Walk me through an incident response process.

**Answer:**

**1. Alert** — monitoring detects anomaly
- Queue depth growing (tasks piling up)
- Error rate spike (tasks failing)
- Worker pods crashing (OOMKilled, CrashLoopBackOff)

**2. Triage** — assess severity
- Is it affecting users? (P1 vs P3)
- Is it getting worse? (growing queue = yes)
- What's the blast radius? (one queue? all queues?)

**3. Mitigate** — stop the bleeding (don't fix root cause yet)
- Scale up workers (increase HPA replicas)
- Pause the failing queue (stop dequeuing)
- Rollback recent deploy
- Enable circuit breaker on failing downstream

**4. Root Cause Analysis (RCA)**
- Check logs: what error are tasks failing with?
- Check metrics: when did it start? What changed?
- Check deploys: was there a release in the last hour?
- Check dependencies: is Redis/DB/downstream healthy?

**5. Postmortem** — document and prevent recurrence
- Timeline of events
- What went wrong (root cause)
- What went right (detection, mitigation)
- Action items with owners and deadlines
- No blame — focus on systemic improvements

**MTTR reduction strategies:**
- Better alerting (catch issues in minutes, not hours)
- Runbooks (step-by-step mitigation for known failure modes)
- Automated remediation (auto-scale, auto-restart, auto-rollback)
- Chaos engineering (break things intentionally to practice response)

---

### Q: How do you debug latency issues in a task queue?

**Answer:**

**RED Method (Rate, Errors, Duration):**
- **Rate** — how many tasks/sec are being processed? Is it dropping?
- **Errors** — what percentage of tasks are failing? Which types?
- **Duration** — how long are tasks taking? P50 vs P99?

**P50 vs P99:**
- P50 (median): half of tasks complete faster than this. Shows "normal" performance.
- P99: 99% of tasks complete faster than this. Shows worst-case for most users.
- If P50=2s but P99=30s → you have a long-tail latency problem (some tasks are extremely slow)

**Where latency hides in a task queue:**
1. **Enqueue latency** — time to push to Redis (should be <1ms)
2. **Queue wait time** — time task sits in queue before a worker picks it up (indicates worker saturation)
3. **Processing time** — actual handler execution time
4. **Ack latency** — time to acknowledge completion

**Debugging tools:**
- **Distributed tracing** (Tempo/Jaeger): trace a task from enqueue → dequeue → handler → ack
- **Flame graphs**: identify which function inside the handler is slow
- **Queue depth metrics**: if queue keeps growing, workers can't keep up
- **Redis SLOWLOG**: check if Redis itself is the bottleneck

**In this repo:**
- `worker.go processTask()` records `task.StartedAt` and `task.DoneAt`
- Duration is logged: `"Task completed in 5.001s"`
- In production: export these as Prometheus histograms for P50/P99 dashboards

---

### Q: Explain resilience patterns.

**Answer:**

**Circuit Breaker:**
- If downstream fails N times in a row → "open" the circuit (stop calling it)
- After a cooldown period → "half-open" (try one request)
- If it succeeds → "close" (resume normal traffic)
- Prevents cascading failures and gives downstream time to recover
- Example: if cloud API returns 503 for 5 consecutive scans, stop scanning for 60 seconds

**Bulkhead:**
- Isolate components so one failure doesn't take down everything
- In this repo: separate queues (`scans` vs `default`). If scan handlers are all failing, email tasks still process normally.
- In K8s: separate worker deployments for different task types

**Timeouts:**
- Never wait forever. Always set a deadline.
- In this repo: `context.WithTimeout(ctx, 30s)` for soft timeout, `time.After(35s)` for hard kill
- Without timeouts: one stuck task holds a worker slot forever → eventually all workers are stuck

**Retries with exponential backoff:**
- Already covered above. Key point: retries without backoff = DDoS-ing yourself.

**Rate limiting as self-defense:**
- Limit how fast YOU consume, not just how fast others send to you
- In this repo: `rateTick` limits dequeue to 10 tasks/sec
- Protects downstream services from being overwhelmed by your workers
- In the backend: `rate_limit="10/m"` on the Celery scan task

**Graceful degradation:**
- When overloaded, do less rather than crash
- Example: skip non-critical tasks, reduce scan depth, serve cached results
- In this repo: priority queues ensure critical tasks (scans) run before nice-to-haves (reports)

---

## SECTION 4: CONFIGURATION MANAGEMENT & GO

---

### Q: How do feature flags and config systems work?

**Answer:**

**What they are:** A way to change application behavior without deploying new code.

**How they work:**
1. Application checks flag value at runtime: `if featureFlags.isEnabled("new-scan-engine") { ... }`
2. Flag values are stored in a central service (LaunchDarkly, Flipt, ConfigMap)
3. Changes propagate to all instances without restart

**Targeting strategies:**
- **Boolean** — on/off for everyone
- **Percentage rollout** — enable for 10% of users, then 50%, then 100% (canary)
- **User targeting** — enable for specific accounts (beta testers)
- **Environment targeting** — enabled in staging, disabled in production

**Config validation patterns:**
- Schema validation before applying (reject invalid config)
- Dry-run mode (show what would change without applying)
- Rollback capability (revert to previous known-good config)
- Audit trail (who changed what, when, why)

**In the compliance engine backend:**
- `k8s/configmap.yaml` holds runtime config (concurrency, timeouts, scan schedule)
- Changes to ConfigMap → pod restart picks up new values
- For zero-downtime config changes: use a sidecar that watches ConfigMap and signals the app

---

### Q: Explain safe config rollout strategies.

**Answer:**

**Config drift prevention:**
- Store config in version control (GitOps)
- ArgoCD/Flux syncs desired state from Git → K8s
- If someone manually edits a ConfigMap, ArgoCD reverts it

**Rollback strategies:**
- Git revert → ArgoCD auto-syncs → previous config restored
- K8s: `kubectl rollout undo deployment/compliance-worker`
- Feature flags: flip the flag off instantly (no deploy needed)

**Canary rollouts for config:**
1. Deploy new config to 1 pod (canary)
2. Monitor error rate and latency for 10 minutes
3. If healthy → roll to all pods
4. If degraded → rollback canary immediately

**In the backend:**
- `CELERY_WORKER_CONCURRENCY` in ConfigMap — changing from 4→8 requires pod restart
- `SCAN_STAGGER_SECONDS` — changing from 30→60 takes effect on next cron run
- Secrets (credentials) are in K8s Secrets, rotated separately from config

---

### Q: Explain Go concurrency patterns relevant to infrastructure engineering.

**Answer:**

**Goroutines:**
- Lightweight threads (~2KB stack vs 1MB for OS threads)
- In this repo: each worker is a goroutine, the poll loop is a goroutine, DLQ monitor is a goroutine
- `go processTask(ctx, task)` — spawns concurrent execution

**Channels:**
- Communication between goroutines (CSP model: "share memory by communicating")
- In this repo: `sem chan struct{}` — buffered channel as concurrency semaphore
- `stopCh chan struct{}` — signal channel for shutdown
- `errCh chan error` — collect handler result from goroutine

**Context propagation:**
- `context.Context` carries deadlines, cancellation signals, and values through call chains
- In this repo: `context.WithTimeout(ctx, 30s)` creates a child context with deadline
- When parent context is cancelled (Ctrl+C), all child contexts are cancelled too
- Handlers check `ctx.Done()` for graceful cancellation

**sync.WaitGroup:**
- Wait for multiple goroutines to finish
- In this repo: `pool.wg.Add(1)` before spawning, `defer pool.wg.Done()` in goroutine
- `pool.wg.Wait()` in Stop() blocks until all in-flight tasks complete

**Error wrapping (Go 1.13+):**
```go
return fmt.Errorf("dequeue from %s: %w", queue, err)
// Caller can unwrap: errors.Is(err, redis.Nil)
```

**Select statement:**
- Multiplexes multiple channel operations
- In this repo: `select { case <-ctx.Done(): return; case <-stopCh: return; case <-rateTick.C: ... }`
- Non-blocking choice between "shutdown signal", "rate limit tick", or "context cancelled"

---

### Q: Compare secrets management approaches.

**Answer:**

**Kubernetes Secrets:**
- Base64 encoded (NOT encrypted at rest by default)
- Mounted as files or env vars in pods
- In the backend: `k8s/secret.yaml` holds DB passwords, JWT secret, encryption key
- Limitation: anyone with kubectl access can read them. No rotation. No audit trail.

**HashiCorp Vault:**
- Secrets encrypted at rest and in transit
- Dynamic secrets (generate short-lived DB credentials on demand)
- Lease expiry (secrets auto-revoke after TTL)
- Audit log (who accessed what secret, when)
- Secret rotation without app restart (app re-fetches on lease expiry)

**AWS Secrets Manager:**
- Managed service (no infrastructure to run)
- Automatic rotation for RDS passwords
- Cross-account access via IAM policies
- Versioning (rollback to previous secret value)

**Zero-trust secret access:**
- No long-lived credentials stored anywhere
- Use IAM roles (pods assume role via IRSA/Pod Identity)
- Short-lived tokens refreshed automatically
- Principle of least privilege (each service only accesses its own secrets)

**For the compliance engine:**
- Cloud credentials are encrypted with Fernet (`COMPLIANCE_ENCRYPTION_KEY`)
- Stored in PostgreSQL, decrypted at scan time only
- In production: should migrate to Vault with dynamic credentials and auto-rotation

---

## SECTION 5: QUESTIONS ABOUT THIS SPECIFIC REPO

---

### Q: Walk me through what happens when a task is enqueued and processed.

**Answer (trace through the code):**

```
1. main.go: broker.Enqueue(ctx, &task)
   → task.go: task.Status = "pending", task.CreatedAt = now
   → broker.go: json.Marshal(task) → Redis SET task:{id} (store state)
   → broker.go: Redis LPUSH queue:scans (add to queue)

2. worker.go: pollLoop() ticks → tryDequeue()
   → broker.go: Redis RPOPLPUSH queue:scans → queue:scans:processing
   → Returns task to worker

3. worker.go: processTask()
   → broker.go: AcquireLock() → Redis SET task:{id}:lock NX (idempotency)
   → task.Status = "running", task.StartedAt = now
   → broker.go: UpdateTaskState() → Redis SET task:{id} (persist state)
   → handlers.go: HandleComplianceScan(ctx, task) — executes business logic

4a. SUCCESS:
   → task.Status = "completed", task.DoneAt = now
   → broker.go: Ack() → Redis LREM queue:scans:processing (remove from processing)
   → broker.go: ReleaseLock() → Redis DEL task:{id}:lock

4b. FAILURE (retries remaining):
   → task.RetryCount++, task.NextRetryAt = now + backoff
   → broker.go: Nack() → Redis LREM processing + LPUSH queue:scans (re-enqueue)
   → Back to step 2 after backoff expires

4c. FAILURE (max retries exhausted):
   → task.Status = "dead"
   → broker.go: MoveToDLQ() → Redis LREM processing + LPUSH queue:scans:dlq
   → Task is permanently parked for human investigation
```

---

### Q: What would you change to make this production-ready?

**Answer:**

1. **Redis Streams instead of Lists** — consumer groups, message IDs, proper acknowledgment, replay from offset
2. **Delayed queue with Sorted Sets** — `ZADD delayed_queue score=timestamp` + periodic ZRANGEBYSCORE to move ready tasks to main queue
3. **Prometheus metrics** — queue depth, processing duration histogram, error rate counter, DLQ size gauge
4. **HTTP health endpoint** — `/health` for K8s liveness probe, `/ready` for readiness
5. **Structured JSON logging** — for Grafana/Loki ingestion instead of plain text
6. **Configuration from environment** — all WorkerConfig values from env vars
7. **Processing list reaper** — goroutine that checks processing list for stale tasks (worker crashed) and moves them back to queue
8. **Graceful dequeue error handling** — suppress "context canceled" errors during shutdown
9. **Task result storage** — store handler return values for the producer to query
10. **Multi-instance coordination** — if running multiple binaries, use Redis Streams consumer groups so tasks aren't double-processed

---

### Q: How does this compare to Celery? What does Celery do that this doesn't?

**Answer:**

**What Celery adds:**
- Task routing with complex rules (headers, content type matching)
- Task chains, groups, chords (workflow composition)
- Canvas (complex task pipelines: A → B → C, or A + B → C)
- Result backends (store return values in DB/Redis/S3)
- Celery Beat (built-in periodic task scheduler)
- Flower (real-time monitoring dashboard)
- Serialization formats (JSON, pickle, msgpack)
- Worker autoscaling (autoscale based on queue depth)
- Task revocation (cancel a running task remotely)
- ETA/countdown with proper delayed queue implementation

**What this repo demonstrates that Celery hides:**
- The actual Redis commands (LPUSH, RPOPLPUSH, LREM)
- How at-least-once delivery works at the protocol level
- How concurrency limiting works (semaphore pattern)
- How retry backoff is calculated and applied
- How DLQ decisions are made
- How graceful shutdown drains in-flight work
- How idempotency locks prevent duplicate execution

**The point:** Celery is a 50K+ line library. This repo is 400 lines that expose the core mechanics. Understanding these mechanics makes you better at debugging Celery in production.

---

## SECTION 6: SYSTEM DESIGN QUESTIONS

---

### Q: Design an async task runner for a team of 50 engineers with 100M tasks/day.

**Answer:**

**Requirements:**
- 100M tasks/day = ~1,150 tasks/sec average, ~5,000 tasks/sec peak
- Multiple task types with different priorities and SLOs
- At-least-once delivery with idempotent handlers
- Observability (metrics, tracing, logging)
- Horizontal scalability

**Architecture:**

```
┌─────────────┐      ┌──────────────────┐     ┌─────────────────┐
│  API Layer  │────▶│  Message Broker   │────▶│  Worker Fleet  │
│  (enqueue)  │      │  (Kafka/Redis    │     │  (K8s pods,     │
│             │      │   Streams)       │     │   auto-scaled)  │
└─────────────┘      └──────────────────┘     └─────────────────┘
                           │                         │
                           ▼                         ▼
                    ┌──────────────┐         ┌──────────────┐
                    │  Dead Letter │         │  Results DB  │
                    │  Queue       │         │  (Postgres)  │
                    └──────────────┘         └──────────────┘
                                                     │
                                                     ▼
                                             ┌──────────────┐
                                             │  Monitoring  │
                                             │  (Prometheus │
                                             │   + Grafana) │
                                             └──────────────┘
```

**Broker choice at this scale:**
- Redis Streams: good up to ~500K tasks/day per instance. Shard for more.
- Kafka: built for this scale. Partitioned topics, consumer groups, replay. Handles 100M/day easily.
- SQS: managed, auto-scales, but higher latency (~20-50ms vs ~1ms for Redis)

**Worker fleet:**
- K8s Deployment with HPA (scale on queue depth metric via KEDA)
- Each pod runs N goroutines (concurrency per pod)
- Separate deployments per task type (isolation / bulkhead)
- Resource limits prevent one task type from starving others

**Reliability:**
- At-least-once: consumer offset committed after processing
- Idempotency: deduplication table in Postgres (task_id + status)
- DLQ: separate topic/queue for failed tasks
- Circuit breaker: per-downstream, trip after 5 consecutive failures
- Timeout: per-task-type configurable (scan=30min, email=10s)

**Observability:**
- Metrics: enqueue rate, dequeue rate, processing duration (P50/P99), error rate, queue depth, DLQ size
- Tracing: trace ID propagated from API → broker → worker → downstream
- Alerting: queue depth > threshold for > 5 min, DLQ growing, error rate > 1%

---

### Q: A task is stuck. How do you debug it?

**Answer:**

**Step 1: Identify the stuck task**
```bash
# Check processing list (tasks that were dequeued but never acked)
redis-cli LRANGE queue:scans:processing 0 -1

# Check task state
redis-cli GET task:{id}
```

**Step 2: Determine why it's stuck**
- Is the worker still running? (check pod status, logs)
- Did the handler hang? (no timeout configured, or waiting on unresponsive downstream)
- Did the worker crash? (OOMKilled, panic, segfault)
- Is there a lock that was never released? (`redis-cli GET task:{id}:lock`)

**Step 3: Recover**
- If worker crashed: move task from processing list back to queue (or let reaper do it)
- If handler is hanging: kill the pod (K8s will restart it), task stays in processing list
- If lock is stale: `redis-cli DEL task:{id}:lock` (manual intervention)

**Step 4: Prevent recurrence**
- Add hard timeout (this repo has it: `time.After(HardTimeout)`)
- Add processing list reaper (check age of tasks in processing, move stale ones back)
- Add alerting on processing list size (if it grows, workers are dying)

**In the compliance engine backend:**
- `cleanup_stuck_scans` runs every 15 minutes
- Finds scans stuck in PENDING/RUNNING for >45 minutes
- Marks them FAILED so new scans can be triggered
- Same concept as a processing list reaper

---

### Q: What happens if Redis goes down?

**Answer:**

**Immediate impact:**
- Workers can't dequeue (RPOPLPUSH fails)
- Producers can't enqueue (LPUSH fails)
- All in-flight tasks continue executing (they're already in memory)
- New tasks are rejected

**Mitigation strategies:**

1. **Redis Sentinel** — automatic failover to replica (seconds of downtime)
2. **Redis Cluster** — sharded, if one shard dies others continue
3. **Local retry buffer** — producer holds tasks in memory/disk, retries enqueue with backoff
4. **Circuit breaker on broker** — stop trying to enqueue, return error to caller immediately
5. **Graceful degradation** — API returns "scan queued" optimistically, retries enqueue in background

**Data loss risk:**
- Redis with AOF (appendonly): lose at most 1 second of data
- Redis with RDB only: lose data since last snapshot (could be minutes)
- For zero data loss: use Kafka (persistent, replicated log) as primary broker

**In this repo:**
- Workers log the error and keep polling (they'll reconnect when Redis comes back)
- Tasks in the processing list survive Redis restart (if AOF is enabled)
- Tasks only in memory (between RPOPLPUSH and handler start) are lost on worker crash

---

## QUICK REFERENCE: One-Line Answers

| Question | Answer |
|----------|--------|
| What's a broker? | Message transport between producers and consumers (Redis, Kafka, RabbitMQ) |
| What's a DLQ? | Queue for permanently failed tasks that need human investigation |
| Why exponential backoff? | Prevents retry storms that overwhelm failing services |
| Why not exactly-once? | Two Generals Problem — can't confirm receipt over unreliable network |
| What's idempotency? | Running an operation twice produces the same result as once |
| What's a circuit breaker? | Stops calling a failing service, gives it time to recover |
| What's a bulkhead? | Isolates failures so one component can't take down everything |
| What's acks_late? | Acknowledge task only after completion, not on receipt |
| What's prefetch=1? | Worker takes one task at a time, doesn't hoard from queue |
| What's RPOPLPUSH? | Atomic pop-and-push — task is never "in limbo" between queues |
| P50 vs P99? | P50=median (normal case), P99=worst case for 99% of requests |
| CAP — which to pick? | CP for task queues (consistency matters), AP for read-heavy caches |
| Error budget? | 99.9% SLO = 43 minutes/month of allowed failures |
| MTTR? | Mean Time To Recovery — how fast you fix incidents |

---

*Last updated: May 2026*
*Repo: github.com/yourusername/mini-taskrunner*
