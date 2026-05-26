package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"
)

// ─── Task Handlers ───────────────────────────────────────────────────────────
// These are the actual "business logic" functions — equivalent to your
// @shared_task decorated functions in tasks.py.
//
// Each handler:
//   - Receives a context (for timeout/cancellation)
//   - Receives the task (for payload access)
//   - Returns nil on success, error on failure
//   - Should check ctx.Done() for graceful timeout handling

// HandleComplianceScan simulates your run_compliance_scan task.
// In production this would call cloud APIs, run compliance checks, etc.
func HandleComplianceScan(ctx context.Context, task *Task) error {
	provider, _ := task.Payload["provider"].(string)
	log.Printf("[scan] Starting compliance scan for provider=%s", provider)

	// Simulate the scan taking 3-8 seconds (your real scans take 5-30 minutes)
	scanDuration := time.Duration(3+rand.Intn(5)) * time.Second

	// Simulate failure for GCP provider (demonstrates retry + DLQ)
	if provider == "gcp" {
		// Simulate partial work before failure
		select {
		case <-time.After(1 * time.Second):
		case <-ctx.Done():
			log.Printf("[scan] Scan for %s cancelled (timeout)", provider)
			return ctx.Err()
		}
		return fmt.Errorf("GCP API error: permission denied for project %v", task.Payload["project"])
	}

	// Simulate scan work with context awareness (graceful timeout)
	select {
	case <-time.After(scanDuration):
		// Scan completed successfully
		findings := 10 + rand.Intn(90)
		failed := rand.Intn(findings / 2)
		log.Printf("[scan] ✓ Scan complete for %s: %d findings (%d FAIL)", provider, findings, failed)
		return nil

	case <-ctx.Done():
		// Context cancelled — equivalent to SoftTimeLimitExceeded
		log.Printf("[scan] Scan for %s interrupted: %v", provider, ctx.Err())
		return ctx.Err()
	}
}

// HandleSendEmail simulates sending a notification email.
// Equivalent to a lightweight task on the "default" queue.
func HandleSendEmail(ctx context.Context, task *Task) error {
	to, _ := task.Payload["to"].(string)
	subject, _ := task.Payload["subject"].(string)

	log.Printf("[email] Sending email to=%s subject=%q", to, subject)

	// Simulate email send (fast task)
	select {
	case <-time.After(500 * time.Millisecond):
		log.Printf("[email] ✓ Email sent to %s", to)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// HandleGenerateReport simulates generating a PDF report.
func HandleGenerateReport(ctx context.Context, task *Task) error {
	format, _ := task.Payload["format"].(string)
	scanID, _ := task.Payload["scan_id"].(string)

	log.Printf("[report] Generating %s report for scan %s", format, scanID)

	// Simulate report generation (medium task)
	select {
	case <-time.After(2 * time.Second):
		log.Printf("[report] ✓ Report generated: %s_%s.pdf", scanID, format)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
