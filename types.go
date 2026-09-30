package godataredaction

import "time"

// TaskStatus is the terminal-aware lifecycle state of a redaction task.
type TaskStatus string

const (
	// TaskRunning means the task accepts shard claiming and receipts.
	TaskRunning TaskStatus = "running"
	// TaskCancelled is a terminal state: no new claims are accepted and every
	// staged output of the task has been removed.
	TaskCancelled TaskStatus = "cancelled"
	// TaskPublished is a terminal state: an immutable manifest exists and the
	// results became downloadable at PublishedAt.
	TaskPublished TaskStatus = "published"
)

// ShardStatus is the per-shard processing state.
type ShardStatus string

const (
	ShardPending   ShardStatus = "pending"
	ShardLeased    ShardStatus = "leased"
	ShardSucceeded ShardStatus = "succeeded"
	ShardFailed    ShardStatus = "failed"
)

// CreateInput freezes everything a task execution depends on: the source data
// version, the redaction-rule version and the set of shards to process.
//
// A canonical fingerprint is computed from ExternalID together with these
// inputs; resubmitting the same external ID with the same inputs returns the
// original task, while a changed data range or rule set is rejected as a
// conflict.
type CreateInput struct {
	// ExternalID is the caller-assigned idempotency key.
	ExternalID string
	// SourceVersion freezes the source data snapshot version.
	SourceVersion string
	// RuleVersion freezes the redaction-rule set version.
	RuleVersion string
	// ShardIDs is the frozen set of shards to process; order is significant
	// only for deterministic presentation, the fingerprint is order
	// independent.
	ShardIDs []string
}

// Task is the frozen definition plus the mutable lifecycle state.
type Task struct {
	ID            string     `json:"id"`
	ExternalID    string     `json:"external_id"`
	SourceVersion string     `json:"source_version"`
	RuleVersion   string     `json:"rule_version"`
	Fingerprint   string     `json:"fingerprint"`
	ShardIDs      []string   `json:"shard_ids"`
	Shards        []*Shard   `json:"shards"`
	Status        TaskStatus `json:"status"`
	// Attempt is the task-level fencing/version counter. It starts at 0 and
	// never changes: shard retries always run against the original frozen
	// task version. Receipts carrying another version are rejected.
	Attempt int `json:"attempt"`
	// ManifestID is set exactly once, when the task enters TaskPublished.
	ManifestID  string     `json:"manifest_id,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
	CancelledAt *time.Time `json:"cancelled_at,omitempty"`
}

// Shard is one frozen unit of work together with its lease/result state.
type Shard struct {
	ID     string      `json:"id"`
	Status ShardStatus `json:"status"`

	// Attempt is the fencing token of the current lease. Every new lease
	// (including takeover of an expired lease and explicit retries) gets a
	// strictly greater value. A receipt whose attempt is smaller than the
	// shard's current attempt is a late receipt from an old worker and is
	// rejected without touching the new result.
	Attempt        int        `json:"attempt"`
	Worker         string     `json:"worker,omitempty"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`

	// Output only becomes meaningful in ShardSucceeded. Digest is the
	// content digest the worker claims; the coordinator verifies it against
	// the delivered payload before accepting the receipt.
	Digest     string `json:"digest,omitempty"`
	Count      int64  `json:"count,omitempty"`
	OutputRef  string `json:"output_ref,omitempty"`
	FailReason string `json:"fail_reason,omitempty"`
}

// Claim describes one leased shard handed to a worker.
type Claim struct {
	TaskID    string
	Attempt   int // task version, always the frozen original version
	Shard     *Shard
	LeaseID   int // shard fencing token to echo back in the receipt
	ExpiresAt time.Time
}

// Receipt is a worker's processing result for one shard.
type Receipt struct {
	// TaskID identifies the task. ExternalID is accepted as an alias only
	// when it is the only reference supplied.
	TaskID     string
	ExternalID string
	// ShardID identifies the shard.
	ShardID string
	// Attempt must be the task version returned with the lease.
	Attempt int
	// LeaseID is the fencing token from the claim. It is the authority for
	// rejecting late receipts after another worker took over.
	LeaseID int
	// Worker is the claiming worker identity.
	Worker string
	// Succeeded distinguishes success from a retryable failure receipt.
	Succeeded bool
	// Digest is the output content digest; Count is the processed record
	// count. Both are required on success.
	Digest string
	Count  int64
	// Payload is the redacted shard output. It is stored content-addressed
	// only after the digest is verified. It may be nil when re-acknowledging
	// an already accepted result (idempotent duplicate receipt). An empty
	// payload is a legitimate zero-record result and is accepted when Digest
	// equals the SHA-256 of the empty string.
	Payload []byte
	// FailReason describes a retryable failure.
	FailReason string
}

// Ack is the receipt acceptance result. Replayed reports whether the receipt
// was an idempotent replay of an already accepted success.
type Ack struct {
	ShardID  string
	Status   ShardStatus
	Replayed bool
}

// Progress is the externally visible task progress.
type Progress struct {
	TaskID        string
	ExternalID    string
	Status        TaskStatus
	SourceVersion string
	RuleVersion   string
	Total         int
	Succeeded     int
	Failed        int
	Leased        int
	Pending       int
	// Published is the gate for data visibility: output is downloadable only
	// when this is true. While false no half-processed data is readable, no
	// matter how many shards have succeeded.
	Published  bool
	ManifestID string
}

// Manifest is the immutable result list generated in a single publication.
type Manifest struct {
	ID            string `json:"id"`
	TaskID        string `json:"task_id"`
	TaskAttempt   int    `json:"task_attempt"`
	SourceVersion string `json:"source_version"`
	RuleVersion   string `json:"rule_version"`
	// Entries reference exactly the successful shard outputs; their digests
	// were re-verified against stored bytes at publication time.
	Entries []ManifestEntry `json:"entries"`
	// Digest covers the ordered (task version, source, rules, shard digests)
	// and is what ManifestID is derived from.
	Digest    string    `json:"digest"`
	CreatedAt time.Time `json:"created_at"`
}

// ManifestEntry binds one shard to its immutable output object.
type ManifestEntry struct {
	ShardID   string `json:"shard_id"`
	Digest    string `json:"digest"`
	Count     int64  `json:"count"`
	OutputRef string `json:"output_ref"`
}
