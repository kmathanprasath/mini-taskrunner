package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// ─── Worker Configuration ────────────────────────────────────────────────────
// Maps directly to your Celery settings:
//   Concurrency     → CELERY_WORKER_CONCURRENCY (4)
//   Queues          → --queues="scans,default" (priority order)
//   PollInterval    → how often to check for new tasks
//   TaskTimeout     → CELERY_TASK_SOFT_TIME_LIMIT (1800s)
//   HardTimeout     → CELERY_TASK_TIME_LIMIT (2100s)
//   RateLimitPerSec → rate_limit="10/m" on task decorator

type WorkerConfig struct {
	Concurrency     int
	Queues          []string      // checked in priority order
	PollInterval    time.Duration
	TaskTimeout     time.Duration // soft timeout — handler gets context cancellation
	HardTimeout     time.Duration // hard timeout — goroutine is abandoned
	RateLimitPerSec int           // max tasks dequeued per second
}

// ─── Handler Registry ────────────────────────────────────────────────────────
// Maps task type names to handler functions.
// Equivalent to Celery's autodiscover_tasks() + @shared_task decorators.

type TaskHandler func(ctx context.Context, task *Task) error

type HandlerRegistry struct {
	handlers map[string]TaskHandler
}

func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{handlers: make(map[string]TaskHandler)}
}

func (r *HandlerRegistry) Register(taskType string, handler TaskHandler) {
	r.handlers[taskType] = handler
}

func (r *HandlerRegistry) Get(taskType string) (TaskHandler, bool) {
	h, ok := r.handlers[taskType]
	return h, ok
}

// ─── Worker Pool ─────────────────────────────────────────────────────────────
// A pool of worker goroutines that consume tasks from Redis queues.
//
// Architecture mirrors your Celery worker:
//   - Semaphore channel limits concurrency (like --concurrency=4)
//   - Each worker goroutine processes one task at a time (prefetch=1)
//   - Tasks are polled from queues in priority order (scans before default)
//   - Rate limiter prevents thundering herd on queue
//   - Graceful shutdown drains in-flight tasks before exiting

type WorkerPool struct {
	broker    *RedisBroker
	registry  *HandlerRegistry
	config    WorkerConfig
	sem       chan struct{}    // concurrency semaphore
	wg        sync.WaitGroup  // tracks in-flight tasks
	stopCh    chan struct{}    // signals workers to stop polling
	rateTick  *time.Ticker    // rate limiter
}

func NewWorkerPool(broker *RedisBroker, registry *HandlerRegistry, cfg WorkerConfig) *WorkerPool {
	interval := time.Second / time.Duration(cfg.RateLimitPerSec)
	return &WorkerPool{
		broker:   broker,
		registry: registry,
		config:   cfg,
		sem:      make(chan struct{}, cfg.Concurrency),
		stopCh:   make(chan struct{}),
		rateTick: time.NewTicker(interval),
	}
}

// Start launches the polling loop. It runs until ctx is cancelled.
func (p *WorkerPool) Start(ctx context.Context) {
	p.wg.Add(1)
	go p.pollLoop(ctx)
}

// Stop signals the pool to stop and waits for in-flight tasks to complete.
// Equivalent to: Celery worker warm shutdown (finish current tasks, stop accepting new ones).
func (p *WorkerPool) Stop() {
	close(p.stopCh)
	p.rateTick.Stop()
	p.wg.Wait()
}

// pollLoop continuously checks queues for available tasks.
// Queues are checked in priority order — "scans" is checked before "default".
// This mirrors your CELERY_TASK_ROUTES where scan tasks get dedicated queue priority.
func (p *WorkerPool) pollLoop(ctx context.Context) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-p.rateTick.C:
			// Rate limiter tick — try to dequeue one task
			task := p.tryDequeue(ctx)
			if task == nil {
				// No task available, sleep briefly to avoid busy-wait
				time.Sleep(p.config.PollInterval)
				continue
			}

			// Check if task is ready (supports countdown/stagger)
			if !task.IsReady() {
				// Not ready yet — put it back and continue
				// In production you'd use a sorted set (ZRANGEBYSCORE) for delayed tasks
				p.broker.Enqueue(ctx, task)
				continue
			}

			// Acquire concurrency slot (blocks if all workers busy)
			// This is the equivalent of prefetch_multiplier=1:
			// worker won't grab more tasks than it can process
			select {
			case p.sem <- struct{}{}:
				p.wg.Add(1)
				go p.processTask(ctx, task)
			case <-ctx.Done():
				// Shutting down — requeue the task
				p.broker.Nack(ctx, task)
				return
			}
		}
	}
}

// tryDequeue attempts to dequeue a task from queues in priority order.
func (p *WorkerPool) tryDequeue(ctx context.Context) *Task {
	for _, queue := range p.config.Queues {
		task, err := p.broker.Dequeue(ctx, queue)
		if err != nil {
			log.Printf("[worker] Error dequeuing from %s: %v", queue, err)
			continue
		}
		if task != nil {
			return task
		}
	}
	return nil
}

// processTask executes a single task with timeout, retry, and error handling.
//
// Flow (mirrors your run_compliance_scan task):
//   1. Acquire idempotency lock (skip if already running)
//   2. Mark task as RUNNING
//   3. Execute handler with timeout context
//   4. On success: mark COMPLETED, ack, release lock
//   5. On failure: retry with backoff OR move to DLQ
func (p *WorkerPool) processTask(ctx context.Context, task *Task) {
	defer p.wg.Done()
	defer func() { <-p.sem }() // release concurrency slot

	// ── Idempotency guard ────────────────────────────────────────────────
	// Equivalent to: if ComplianceScan.objects.filter(status__in=[PENDING, RUNNING]).exists(): skip
	acquired, err := p.broker.AcquireLock(ctx, task.ID, p.config.HardTimeout)
	if err != nil {
		log.Printf("[worker] Lock error for task %s: %v", task.ID, err)
		p.broker.Nack(ctx, task)
		return
	}
	if !acquired {
		log.Printf("[worker] Task %s already running (idempotency guard), skipping", task.ID)
		// Remove from processing without requeue
		p.broker.Ack(ctx, task)
		return
	}
	defer p.broker.ReleaseLock(ctx, task.ID)

	// ── Mark as running ──────────────────────────────────────────────────
	task.Status = StatusRunning
	now := time.Now()
	task.StartedAt = &now
	p.broker.UpdateTaskState(ctx, task)

	log.Printf("[worker] Processing task %s (type=%s, attempt=%d/%d)",
		task.ID, task.Type, task.RetryCount+1, task.MaxRetry+1)

	// ── Find handler ─────────────────────────────────────────────────────
	handler, ok := p.registry.Get(task.Type)
	if !ok {
		task.Error = fmt.Sprintf("no handler registered for task type: %s", task.Type)
		log.Printf("[worker] %s", task.Error)
		p.broker.MoveToDLQ(ctx, task)
		return
	}

	// ── Execute with timeout ─────────────────────────────────────────────
	// Soft timeout: context cancelled → handler should check ctx.Done() and clean up
	// Hard timeout: we abandon the goroutine (like CELERY_TASK_TIME_LIMIT)
	taskCtx, taskCancel := context.WithTimeout(ctx, p.config.TaskTimeout)
	defer taskCancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- handler(taskCtx, task)
	}()

	// Wait for handler to complete or hard timeout
	select {
	case handlerErr := <-errCh:
		if handlerErr != nil {
			p.handleFailure(ctx, task, handlerErr)
		} else {
			p.handleSuccess(ctx, task)
		}

	case <-time.After(p.config.HardTimeout):
		// Hard timeout — equivalent to SoftTimeLimitExceeded in your tasks.py
		task.Error = "task exceeded hard timeout limit"
		log.Printf("[worker] Task %s HARD TIMEOUT after %v", task.ID, p.config.HardTimeout)
		p.handleFailure(ctx, task, fmt.Errorf(task.Error))
	}
}

// handleSuccess marks the task as completed and acknowledges it.
// Equivalent to:
//   scan.status = COMPLETED
//   scan.completed_at = now()
//   scan.save()
func (p *WorkerPool) handleSuccess(ctx context.Context, task *Task) {
	task.Status = StatusCompleted
	now := time.Now()
	task.DoneAt = &now
	task.Error = ""

	p.broker.UpdateTaskState(ctx, task)
	p.broker.Ack(ctx, task)

	duration := now.Sub(*task.StartedAt)
	log.Printf("[worker] ✓ Task %s completed in %v", task.ID, duration)
}

// handleFailure implements retry logic with exponential backoff.
// If retries exhausted → move to DLQ.
//
// Equivalent to:
//   except Exception as exc:
//       scan.status = FAILED
//       raise self.retry(exc=exc, countdown=0)  ← retry
//   OR
//       cleanup_stuck_scans marks as FAILED     ← DLQ
func (p *WorkerPool) handleFailure(ctx context.Context, task *Task, err error) {
	task.Error = err.Error()
	task.RetryCount++

	if task.RetryCount <= task.MaxRetry {
		// ── Retry with exponential backoff ────────────────────────────────
		backoff := task.BackoffDuration()
		log.Printf("[worker] ✗ Task %s failed (attempt %d/%d), retrying in %v: %s",
			task.ID, task.RetryCount, task.MaxRetry+1, backoff, err)

		task.Status = StatusRetrying
		task.NextRetryAt = time.Now().Add(backoff)
		p.broker.Nack(ctx, task)
	} else {
		// ── Max retries exhausted → Dead Letter Queue ────────────────────
		log.Printf("[worker] ✗ Task %s PERMANENTLY FAILED after %d attempts: %s",
			task.ID, task.RetryCount, err)
		p.broker.MoveToDLQ(ctx, task)
	}
}
