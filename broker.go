package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// ─── Redis Broker ────────────────────────────────────────────────────────────
// The broker is the message transport layer — equivalent to Redis as your
// CELERY_BROKER_URL. It handles:
//   - Enqueueing tasks (LPUSH to queue list)
//   - Dequeueing tasks (BRPOPLPUSH for at-least-once delivery)
//   - Acknowledging completed tasks (remove from processing list)
//   - Moving failed tasks to DLQ
//   - Storing task state/results
//
// Delivery guarantee: AT-LEAST-ONCE
//   - Task is moved from queue → processing list atomically (BRPOPLPUSH)
//   - If worker crashes, task stays in processing list
//   - A reaper goroutine can move stale processing tasks back to queue
//   - Task is only removed from processing list after handler completes
//   - This mirrors your acks_late=True setting

type RedisBroker struct {
	client *redis.Client
}

// Redis key naming convention:
//   queue:{name}           — pending tasks (Redis List)
//   queue:{name}:processing — tasks currently being worked on (Redis List)
//   queue:{name}:dlq       — dead letter queue (Redis List)
//   task:{id}              — task state/metadata (Redis Hash)
//   task:{id}:lock         — idempotency lock (Redis String with TTL)

func queueKey(queue string) string      { return fmt.Sprintf("queue:%s", queue) }
func processingKey(queue string) string  { return fmt.Sprintf("queue:%s:processing", queue) }
func dlqKey(queue string) string         { return fmt.Sprintf("queue:%s:dlq", queue) }
func taskKey(id string) string           { return fmt.Sprintf("task:%s", id) }
func lockKey(id string) string           { return fmt.Sprintf("task:%s:lock", id) }

func NewRedisBroker(ctx context.Context, addr string) (*RedisBroker, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		DB:           0,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     20,
	})

	// Verify connection
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &RedisBroker{client: client}, nil
}

func (b *RedisBroker) Close() error {
	return b.client.Close()
}

// ─── Enqueue ─────────────────────────────────────────────────────────────────
// Pushes a task to the left of the queue list.
// Equivalent to: run_compliance_scan.delay(scan_id=str(scan.id))
// Or with countdown: run_compliance_scan.apply_async(kwargs={...}, countdown=delay)
func (b *RedisBroker) Enqueue(ctx context.Context, task *Task) error {
	task.Status = StatusPending
	task.CreatedAt = time.Now()

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}

	// Store task metadata
	if err := b.client.Set(ctx, taskKey(task.ID), data, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("store task state: %w", err)
	}

	// Push to queue (LPUSH — new tasks go to the left, RPOP takes from right = FIFO)
	if err := b.client.LPush(ctx, queueKey(task.Queue), data).Err(); err != nil {
		return fmt.Errorf("enqueue task: %w", err)
	}

	return nil
}

// ─── Dequeue ─────────────────────────────────────────────────────────────────
// Atomically moves a task from the queue to the processing list.
// This is the AT-LEAST-ONCE guarantee:
//   - If worker crashes after dequeue but before ack, task stays in processing
//   - A reaper can move it back to the queue
//
// Equivalent to: Celery worker fetching a task with acks_late=True
// The task is only "acknowledged" when we call Ack() after successful processing.
func (b *RedisBroker) Dequeue(ctx context.Context, queue string) (*Task, error) {
	// RPOPLPUSH: pop from right of queue, push to left of processing list
	// This is atomic — no task loss even if we crash between operations
	data, err := b.client.RPopLPush(ctx, queueKey(queue), processingKey(queue)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // queue empty
		}
		return nil, fmt.Errorf("dequeue: %w", err)
	}

	var task Task
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("unmarshal task: %w", err)
	}

	return &task, nil
}

// ─── Ack (Acknowledge) ──────────────────────────────────────────────────────
// Removes the task from the processing list after successful completion.
// This is the "acknowledgment" — equivalent to Celery's acks_late behavior.
// Only called AFTER the handler returns successfully.
func (b *RedisBroker) Ack(ctx context.Context, task *Task) error {
	data, _ := json.Marshal(task)
	// Remove one occurrence from processing list
	return b.client.LRem(ctx, processingKey(task.Queue), 1, data).Err()
}

// ─── Nack (Negative Acknowledge / Requeue) ──────────────────────────────────
// Moves a failed task back to the queue for retry.
// Equivalent to: self.retry(exc=exc, countdown=backoff)
func (b *RedisBroker) Nack(ctx context.Context, task *Task) error {
	// Remove from processing
	oldData, _ := json.Marshal(task)
	b.client.LRem(ctx, processingKey(task.Queue), 1, oldData)

	// Update state and re-enqueue
	task.Status = StatusRetrying
	task.NextRetryAt = time.Now().Add(task.BackoffDuration())

	newData, _ := json.Marshal(task)
	b.client.Set(ctx, taskKey(task.ID), newData, 24*time.Hour)

	// Push back to queue
	return b.client.LPush(ctx, queueKey(task.Queue), newData).Err()
}

// ─── MoveToDLQ ───────────────────────────────────────────────────────────────
// Moves a permanently failed task to the Dead Letter Queue.
// Equivalent to: your cleanup_stuck_scans marking scans as FAILED after max retries.
//
// DLQ tasks can be:
//   - Inspected for debugging (redis-cli LRANGE queue:scans:dlq 0 -1)
//   - Replayed manually after fixing the root cause
//   - Purged after investigation
func (b *RedisBroker) MoveToDLQ(ctx context.Context, task *Task) error {
	// Remove from processing
	oldData, _ := json.Marshal(task)
	b.client.LRem(ctx, processingKey(task.Queue), 1, oldData)

	// Mark as dead
	task.Status = StatusDead
	now := time.Now()
	task.DoneAt = &now

	newData, _ := json.Marshal(task)
	b.client.Set(ctx, taskKey(task.ID), newData, 72*time.Hour) // keep DLQ state for 3 days

	// Push to DLQ
	log.Printf("[dlq] Task %s (type=%s) moved to DLQ after %d retries: %s",
		task.ID, task.Type, task.RetryCount, task.Error)

	return b.client.LPush(ctx, dlqKey(task.Queue), newData).Err()
}

// ─── DLQSize ─────────────────────────────────────────────────────────────────
func (b *RedisBroker) DLQSize(ctx context.Context, queue string) (int64, error) {
	return b.client.LLen(ctx, dlqKey(queue)).Result()
}

// ─── AcquireLock (Idempotency Guard) ────────────────────────────────────────
// Prevents duplicate execution of the same task.
// Equivalent to your check:
//   already_running = ComplianceScan.objects.filter(provider=provider,
//       status__in=[PENDING, RUNNING]).exists()
//
// Uses Redis SET NX (set if not exists) with a TTL.
// Returns true if lock acquired (safe to proceed), false if already running.
func (b *RedisBroker) AcquireLock(ctx context.Context, taskID string, ttl time.Duration) (bool, error) {
	return b.client.SetNX(ctx, lockKey(taskID), "locked", ttl).Result()
}

// ReleaseLock releases the idempotency lock after task completion.
func (b *RedisBroker) ReleaseLock(ctx context.Context, taskID string) error {
	return b.client.Del(ctx, lockKey(taskID)).Err()
}

// ─── UpdateTaskState ─────────────────────────────────────────────────────────
// Persists the current task state to Redis (for monitoring/inspection).
// Equivalent to: scan.status = RUNNING; scan.save(update_fields=["status"])
func (b *RedisBroker) UpdateTaskState(ctx context.Context, task *Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return b.client.Set(ctx, taskKey(task.ID), data, 24*time.Hour).Err()
}

// ─── QueueSize ───────────────────────────────────────────────────────────────
func (b *RedisBroker) QueueSize(ctx context.Context, queue string) (int64, error) {
	return b.client.LLen(ctx, queueKey(queue)).Result()
}

// ─── ProcessingSize ──────────────────────────────────────────────────────────
func (b *RedisBroker) ProcessingSize(ctx context.Context, queue string) (int64, error) {
	return b.client.LLen(ctx, processingKey(queue)).Result()
}
