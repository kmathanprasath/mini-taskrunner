/*
Mini Async Task Runner — Go + Redis
====================================
A production-style task queue demonstrating:
  - Jobs, Workers, Broker (Redis), Queues
  - Dead Letter Queue (DLQ) for permanently failed tasks
  - Retry logic with exponential backoff
  - At-least-once delivery (ack after completion)
  - Concurrency limits (worker pool)
  - Rate limiting (token bucket per queue)
  - Multiple named queues with priority
  - Task timeout (soft + hard)
  - Staggered dispatch (countdown delay)
  - Idempotency guard (skip if already running)
  - Graceful shutdown

Architecture (mirrors your Celery backend):
  ┌──────────┐     ┌───────────┐     ┌──────────────┐
  │ Producer │────▶│   Redis   │────▶│   Workers    │
  │ (API)    │     │  (Broker) │     │  (Goroutines)│
  └──────────┘     └───────────┘     └──────────────┘
                         │                    │
                         ▼                    ▼
                   ┌───────────┐     ┌──────────────┐
                   │   DLQ     │     │  Result DB   │
                   │  (Redis)  │     │   (Redis)    │
                   └───────────┘     └──────────────┘

Usage:
  # Start Redis
  docker run -d -p 6379:6379 redis:7-alpine

  # Run the task runner (starts workers + produces demo tasks)
  go run .

  # Or build and run
  go build -o taskrunner . && ./taskrunner
*/
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown on SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// ── Configuration ────────────────────────────────────────────────────────
	redisAddr := getEnv("REDIS_URL", "localhost:6379")

	cfg := WorkerConfig{
		Concurrency:    4,              // max 4 tasks in parallel (like your CELERY_WORKER_CONCURRENCY=4)
		Queues:         []string{"scans", "default"}, // priority order (scans first)
		PollInterval:   500 * time.Millisecond,
		TaskTimeout:    30 * time.Second,  // soft timeout per task (scaled down for demo)
		HardTimeout:    35 * time.Second,  // hard kill timeout
		RateLimitPerSec: 10,              // max 10 tasks/sec dequeued (like rate_limit="10/m" scaled)
	}

	// ── Initialize broker ────────────────────────────────────────────────────
	broker, err := NewRedisBroker(ctx, redisAddr)
	if err != nil {
		log.Fatalf("Failed to connect to Redis at %s: %v", redisAddr, err)
	}
	defer broker.Close()

	log.Printf("Connected to Redis at %s", redisAddr)

	// ── Register task handlers ───────────────────────────────────────────────
	registry := NewHandlerRegistry()
	registry.Register("compliance_scan", HandleComplianceScan)
	registry.Register("send_email", HandleSendEmail)
	registry.Register("generate_report", HandleGenerateReport)

	// ── Start worker pool ────────────────────────────────────────────────────
	pool := NewWorkerPool(broker, registry, cfg)
	pool.Start(ctx)

	log.Printf("Worker pool started: concurrency=%d, queues=%v", cfg.Concurrency, cfg.Queues)

	// ── Produce demo tasks ───────────────────────────────────────────────────
	go produceDemoTasks(ctx, broker)

	// ── Start DLQ cleanup (like your cleanup_stuck_scans CronJob) ────────────
	go runDLQMonitor(ctx, broker)

	// ── Wait for shutdown signal ─────────────────────────────────────────────
	<-sigCh
	log.Println("Shutdown signal received, draining workers...")
	cancel()
	pool.Stop()
	log.Println("All workers stopped. Goodbye.")
}

// produceDemoTasks enqueues sample tasks to demonstrate the system.
func produceDemoTasks(ctx context.Context, broker *RedisBroker) {
	time.Sleep(1 * time.Second) // let workers start first

	tasks := []Task{
		{
			ID:       generateID(),
			Type:     "compliance_scan",
			Queue:    "scans",
			Payload:  map[string]interface{}{"provider": "aws", "account_id": "123456789012"},
			MaxRetry: 2,
		},
		{
			ID:       generateID(),
			Type:     "compliance_scan",
			Queue:    "scans",
			Payload:  map[string]interface{}{"provider": "azure", "subscription": "sub-001"},
			MaxRetry: 2,
		},
		{
			ID:       generateID(),
			Type:     "send_email",
			Queue:    "default",
			Payload:  map[string]interface{}{"to": "admin@example.com", "subject": "Scan Complete"},
			MaxRetry: 3,
		},
		{
			ID:       generateID(),
			Type:     "generate_report",
			Queue:    "default",
			Payload:  map[string]interface{}{"format": "pdf", "scan_id": "scan-42"},
			MaxRetry: 1,
		},
		// This one will fail to demonstrate retry + DLQ
		{
			ID:       generateID(),
			Type:     "compliance_scan",
			Queue:    "scans",
			Payload:  map[string]interface{}{"provider": "gcp", "project": "fail-on-purpose"},
			MaxRetry: 2,
		},
	}

	for i, task := range tasks {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Staggered dispatch — like your cron_scan.py countdown logic
		delay := time.Duration(i) * 2 * time.Second
		if delay > 0 {
			task.NotBefore = time.Now().Add(delay)
			log.Printf("[producer] Enqueuing task %s (type=%s) with %v delay", task.ID, task.Type, delay)
		} else {
			log.Printf("[producer] Enqueuing task %s (type=%s) immediately", task.ID, task.Type)
		}

		if err := broker.Enqueue(ctx, &task); err != nil {
			log.Printf("[producer] ERROR enqueuing task %s: %v", task.ID, err)
		}
	}

	log.Println("[producer] All demo tasks enqueued")
}

// runDLQMonitor periodically logs DLQ contents (like your cleanup_stuck_scans CronJob)
func runDLQMonitor(ctx context.Context, broker *RedisBroker) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, queue := range []string{"scans", "default"} {
				count, _ := broker.DLQSize(ctx, queue)
				if count > 0 {
					log.Printf("[dlq-monitor] Queue '%s' DLQ has %d failed tasks", queue, count)
				}
			}
		}
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

var idCounter int64

func generateID() string {
	idCounter++
	return fmt.Sprintf("task_%d_%d", time.Now().UnixNano(), idCounter)
}
