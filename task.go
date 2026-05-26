package main

import (
	"time"
)

// ─── Task ────────────────────────────────────────────────────────────────────
// Represents a unit of work — equivalent to a Celery task message.
//
// Maps to your backend:
//   Task.ID       → scan.id / scan.celery_task_id
//   Task.Type     → task name ("django_integration.tasks.run_compliance_scan")
//   Task.Queue    → "scans" or "default" (CELERY_TASK_ROUTES)
//   Task.MaxRetry → max_retries=1 in @shared_task decorator
//   Task.Payload  → kwargs passed to .delay() or .apply_async()

type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusRunning   TaskStatus = "running"
	StatusCompleted TaskStatus = "completed"
	StatusFailed    TaskStatus = "failed"
	StatusRetrying  TaskStatus = "retrying"
	StatusDead      TaskStatus = "dead" // moved to DLQ
)

// Task is the job definition that gets serialized to Redis.
type Task struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`       // handler name
	Queue     string                 `json:"queue"`      // target queue
	Payload   map[string]interface{} `json:"payload"`    // task arguments
	Status    TaskStatus             `json:"status"`
	MaxRetry  int                    `json:"max_retry"`  // max retry attempts
	RetryCount int                   `json:"retry_count"`
	Error     string                 `json:"error,omitempty"`
	CreatedAt time.Time              `json:"created_at"`
	StartedAt *time.Time             `json:"started_at,omitempty"`
	DoneAt    *time.Time             `json:"done_at,omitempty"`
	NotBefore time.Time              `json:"not_before,omitempty"` // countdown/stagger support

	// Backoff state — exponential backoff for retries
	// Like your Celery countdown=0 but we make it smarter
	NextRetryAt time.Time `json:"next_retry_at,omitempty"`
}

// IsReady returns true if the task is eligible to run now.
// Supports staggered dispatch (NotBefore) and retry backoff (NextRetryAt).
func (t *Task) IsReady() bool {
	now := time.Now()
	if !t.NotBefore.IsZero() && now.Before(t.NotBefore) {
		return false
	}
	if !t.NextRetryAt.IsZero() && now.Before(t.NextRetryAt) {
		return false
	}
	return true
}

// BackoffDuration calculates exponential backoff for the current retry.
// Formula: base * 2^(retryCount-1) — capped at 5 minutes.
//
// Retry 1: 2s
// Retry 2: 4s
// Retry 3: 8s
// ...
// Max: 5 minutes
func (t *Task) BackoffDuration() time.Duration {
	base := 2 * time.Second
	backoff := base * (1 << (t.RetryCount - 1))
	maxBackoff := 5 * time.Minute
	if backoff > maxBackoff {
		return maxBackoff
	}
	return backoff
}
