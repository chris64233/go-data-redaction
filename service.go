package godataredaction

import (
	"fmt"
	"time"
)

// Clock is the time source used for leases and timestamps; tests inject a
// controllable one.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Option configures a Service.
type Option func(*Service)

// WithClock injects a custom Clock (mainly for tests).
func WithClock(c Clock) Option {
	return func(s *Service) { s.clock = c }
}

// WithLeaseTTL sets how long a claimed shard stays owned by a worker before
// another worker may take it over.
func WithLeaseTTL(d time.Duration) Option {
	return func(s *Service) {
		if d > 0 {
			s.leaseTTL = d
		}
	}
}

// Service is the coordinator for resumable, once-only-publishing redaction
// tasks. All state is persisted under the storage directory; a Service
// reopened on the same directory resumes every task exactly where it stopped.
type Service struct {
	st       *store
	clock    Clock
	leaseTTL time.Duration
}

// DefaultLeaseTTL is used when WithLeaseTTL is not supplied.
const DefaultLeaseTTL = 30 * time.Second

// NewService opens (or creates) the persistent coordinator rooted at dir.
func NewService(dir string, opts ...Option) (*Service, error) {
	st, err := newStore(dir)
	if err != nil {
		return nil, err
	}
	s := &Service{st: st, clock: realClock{}, leaseTTL: DefaultLeaseTTL}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// resolveRef finds a task by internal ID or external ID. Internal IDs take
// precedence when both could match.
func (s *Service) resolveRef(id, externalID string) (*Task, error) {
	if id != "" {
		if t, ok := s.st.tasks[id]; ok {
			return t, nil
		}
	}
	if externalID != "" {
		if tid, ok := s.st.extIndex[externalID]; ok {
			return s.st.tasks[tid], nil
		}
	}
	return nil, ErrNotFound
}

// requireShard returns the shard for an ID.
func requireShard(t *Task, shardID string) (*Shard, error) {
	for _, sh := range t.Shards {
		if sh.ID == shardID {
			return sh, nil
		}
	}
	return nil, fmt.Errorf("%w: shard %s", ErrNotFound, shardID)
}

// CreateTask freezes the source data version, rule version and shard set.
//
// The same external ID with the identical frozen inputs returns the original
// task (idempotent resubmission). The same external ID with a changed data
// range or rule version is rejected with ErrConflict; it never silently
// creates a second task.
func (s *Service) CreateTask(in CreateInput) (*Task, error) {
	fp, err := inputFingerprint(in)
	if err != nil {
		return nil, err
	}
	id := taskID(fp)

	s.st.mu.Lock()
	defer s.st.mu.Unlock()

	if existing, ok := s.st.extIndex[in.ExternalID]; ok {
		t := s.st.tasks[existing]
		if t.Fingerprint != fp {
			return nil, fmt.Errorf("%w: external id %q reused with different source/rule versions or shard set",
				ErrConflict, in.ExternalID)
		}
		return t, nil // identical resubmission -> original task
	}
	if _, collision := s.st.tasks[id]; collision {
		// Extremely unlikely hash collision with a different external id.
		return nil, fmt.Errorf("%w: derived task id collision", ErrConflict)
	}

	now := s.clock.Now()
	shards := make([]*Shard, 0, len(in.ShardIDs))
	for _, sid := range in.ShardIDs {
		shards = append(shards, &Shard{ID: sid, Status: ShardPending, Attempt: 0})
	}
	t := &Task{
		ID:            id,
		ExternalID:    in.ExternalID,
		SourceVersion: in.SourceVersion,
		RuleVersion:   in.RuleVersion,
		Fingerprint:   fp,
		ShardIDs:      append([]string(nil), in.ShardIDs...),
		Shards:        shards,
		Status:        TaskRunning,
		Attempt:       0,
		CreatedAt:     now,
	}
	s.st.tasks[id] = t
	s.st.extIndex[in.ExternalID] = id
	if err := s.st.saveLocked(); err != nil {
		delete(s.st.tasks, id)
		delete(s.st.extIndex, in.ExternalID)
		return nil, err
	}
	return t, nil
}

// GetTask returns a task by internal or external ID.
func (s *Service) GetTask(id, externalID string) (*Task, error) {
	s.st.mu.RLock()
	defer s.st.mu.RUnlock()
	return s.resolveRef(id, externalID)
}

// ClaimShard leases one claimable shard to worker for the lease TTL. Only
// pending shards, and leased shards whose lease has expired, are claimable.
// After cancellation no new claims are granted.
func (s *Service) ClaimShard(id, externalID, worker string) (*Claim, error) {
	if worker == "" {
		return nil, fmt.Errorf("%w: worker id is required", ErrInvalidInput)
	}
	s.st.mu.Lock()
	defer s.st.mu.Unlock()

	t, err := s.resolveRef(id, externalID)
	if err != nil {
		return nil, err
	}
	if t.Status != TaskRunning {
		return nil, terminalErr(t.Status)
	}

	now := s.clock.Now()
	var sh *Shard
	for _, candidate := range t.Shards {
		switch candidate.Status {
		case ShardPending:
			sh = candidate
		case ShardLeased:
			if candidate.LeaseExpiresAt != nil && !now.Before(*candidate.LeaseExpiresAt) {
				sh = candidate // expired lease: takeover allowed
			}
		}
		if sh != nil {
			break
		}
	}
	if sh == nil {
		return nil, ErrNoAvailableShard
	}

	sh.Attempt++ // new fencing token; any in-flight old receipt is now stale
	sh.Status = ShardLeased
	sh.Worker = worker
	exp := now.Add(s.leaseTTL)
	sh.LeaseExpiresAt = &exp
	sh.FailReason = ""
	// A (re)claimed shard never inherits a previous attempt's output; a
	// succeeded shard is never claimable in the first place.
	sh.Digest, sh.Count, sh.OutputRef = "", 0, ""

	if err := s.st.saveLocked(); err != nil {
		return nil, err
	}
	return &Claim{
		TaskID:    t.ID,
		Attempt:   t.Attempt,
		Shard:     sh,
		LeaseID:   sh.Attempt,
		ExpiresAt: exp,
	}, nil
}

// SubmitReceipt accepts a worker result.
//
// Validation order:
//  1. task exists and is still running (cancel/publish wins terminal races),
//  2. shard exists,
//  3. receipt task version equals the frozen task version,
//  4. the lease fencing token matches the current lease (late receipts after
//     takeover are rejected with ErrLeaseFenced and cannot overwrite),
//  5. on success the claimed digest matches the payload bytes, and the
//     payload re-hash matches (ErrDigestMismatch otherwise).
//
// A successful receipt for an already succeeded shard is idempotent only when
// the digest is identical; a different digest for the same shard is a conflict
// (ErrDigestMismatch), never treated as an ordinary retry.
func (s *Service) SubmitReceipt(r *Receipt) (*Ack, bool, error) {
	if r == nil || r.ShardID == "" {
		return nil, false, fmt.Errorf("%w: receipt and shard id are required", ErrInvalidInput)
	}
	s.st.mu.Lock()
	defer s.st.mu.Unlock()

	t, err := s.resolveRef(r.TaskID, r.ExternalID)
	if err != nil {
		return nil, false, err
	}
	sh, err := requireShard(t, r.ShardID)
	if err != nil {
		return nil, false, err
	}

	// Idempotent replay of an accepted success. This is checked before the
	// terminal-state guard so a worker re-delivering the same acknowledged
	// result after publication gets a benign replay instead of an error. A
	// different digest for an already succeeded shard is always a conflict,
	// regardless of task state.
	if sh.Status == ShardSucceeded {
		switch {
		case r.Succeeded && sh.Digest == r.Digest:
			published, err := s.publishIfCompleteLocked(t)
			if err != nil {
				return &Ack{ShardID: sh.ID, Status: ShardSucceeded, Replayed: true}, false, err
			}
			return &Ack{ShardID: sh.ID, Status: ShardSucceeded, Replayed: true}, published, nil
		case r.Succeeded && r.Digest != "" && sh.Digest != r.Digest:
			return nil, false, fmt.Errorf("%w: shard %s already has digest %s, refusing %s",
				ErrDigestMismatch, sh.ID, sh.Digest, r.Digest)
		default:
			return nil, false, fmt.Errorf("shard %s already succeeded", sh.ID)
		}
	}

	if t.Status != TaskRunning {
		return nil, false, terminalErr(t.Status)
	}

	// Fencing: version then lease token.
	if r.Attempt != t.Attempt {
		return nil, false, fmt.Errorf("%w: receipt version %d, task version %d",
			ErrVersionMismatch, r.Attempt, t.Attempt)
	}
	if sh.Status != ShardLeased {
		return nil, false, fmt.Errorf("shard %s is %s, not leased", sh.ID, sh.Status)
	}
	if r.LeaseID != sh.Attempt {
		return nil, false, fmt.Errorf("%w: shard %s receipt lease %d, current lease %d",
			ErrLeaseFenced, sh.ID, r.LeaseID, sh.Attempt)
	}
	if r.Worker != "" && sh.Worker != r.Worker {
		return nil, false, fmt.Errorf("%w: receipt worker %s, lease worker %s",
			ErrNotLeaser, r.Worker, sh.Worker)
	}

	if !r.Succeeded {
		sh.Status = ShardFailed
		sh.FailReason = r.FailReason
		sh.Worker = r.Worker
		if err := s.st.saveLocked(); err != nil {
			return nil, false, err
		}
		return &Ack{ShardID: sh.ID, Status: ShardFailed}, false, nil
	}

	// Success: verify claimed digest against delivered payload.
	if !validDigest(r.Digest) {
		return nil, false, fmt.Errorf("%w: digest must be sha256 hex", ErrInvalidInput)
	}
	if len(r.Payload) == 0 {
		return nil, false, fmt.Errorf("%w: successful receipt requires payload", ErrInvalidInput)
	}
	if got := hexDigest(r.Payload); got != r.Digest {
		return nil, false, fmt.Errorf("%w: claimed %s, payload hashes to %s",
			ErrDigestMismatch, r.Digest, got)
	}
	if err := s.st.putOutputLocked(t.ID, r.Digest, r.Payload); err != nil {
		return nil, false, err
	}

	prevDigest := sh.Digest
	sh.Status = ShardSucceeded
	sh.Digest = r.Digest
	sh.Count = r.Count
	sh.OutputRef = r.Digest
	sh.FailReason = ""
	sh.LeaseExpiresAt = nil
	sh.Worker = r.Worker

	if err := s.st.saveLocked(); err != nil {
		return nil, false, err
	}
	// Remove any stale object left by a superseded attempt on this shard.
	if prevDigest != "" && prevDigest != r.Digest {
		_ = s.st.removeOutputLocked(t.ID, prevDigest)
	}

	published, perr := s.publishIfCompleteLocked(t)
	if perr != nil {
		return &Ack{ShardID: sh.ID, Status: ShardSucceeded}, false, perr
	}
	return &Ack{ShardID: sh.ID, Status: ShardSucceeded}, published, nil
}

// RetryShard resets a failed shard so it can be reclaimed, strictly at the
// original frozen task version. Succeeded/leased/pending shards are not
// retryable here; retries only restart failed work and never change the task
// version.
func (s *Service) RetryShard(id, externalID, shardID string) error {
	s.st.mu.Lock()
	defer s.st.mu.Unlock()

	t, err := s.resolveRef(id, externalID)
	if err != nil {
		return err
	}
	if t.Status != TaskRunning {
		return terminalErr(t.Status)
	}
	sh, err := requireShard(t, shardID)
	if err != nil {
		return err
	}
	if sh.Status != ShardFailed {
		return fmt.Errorf("%w: shard %s is %s", ErrShardNotFailed, shardID, sh.Status)
	}

	sh.Status = ShardPending
	sh.Worker = ""
	sh.LeaseExpiresAt = nil
	sh.FailReason = ""
	// Bumping the attempt fences any late receipt from the failed attempt.
	sh.Attempt++
	return s.st.saveLocked()
}

// Cancel moves a running task to the terminal cancelled state, stops new
// claims and removes every staged output. Cancelling an already terminal task
// is a no-op that reports its existing state.
func (s *Service) Cancel(id, externalID string) (*Task, error) {
	s.st.mu.Lock()
	defer s.st.mu.Unlock()

	t, err := s.resolveRef(id, externalID)
	if err != nil {
		return nil, err
	}
	if t.Status != TaskRunning {
		return t, nil
	}
	now := s.clock.Now()
	t.Status = TaskCancelled
	t.CancelledAt = &now
	if err := s.st.saveLocked(); err != nil {
		t.Status = TaskRunning
		t.CancelledAt = nil
		return nil, err
	}
	// Safe cleanup: no manifest referenced any output of this task.
	if err := s.st.removeAllOutputsLocked(t.ID); err != nil {
		return t, fmt.Errorf("task cancelled but output cleanup failed: %w", err)
	}
	return t, nil
}

// publishIfCompleteLocked performs the one-shot publication. It MUST be called
// with the write lock held, which makes "the last shards complete
// concurrently" serialize: exactly one caller can observe all-succeeded and
// perform the transition.
func (s *Service) publishIfCompleteLocked(t *Task) (bool, error) {
	// Only the caller that performs the running -> published transition
	// reports true. Concurrent callers that find the task already published
	// get false, which guarantees exactly one publication observation even
	// when the last receipts land simultaneously.
	if t.Status != TaskRunning {
		return false, nil
	}
	for _, sh := range t.Shards {
		if sh.Status != ShardSucceeded {
			return false, nil
		}
	}

	// Re-verify every stored output digest before sealing the manifest.
	entries := make([]ManifestEntry, 0, len(t.Shards))
	for _, sh := range t.Shards {
		b, err := s.st.readOutputLocked(t.ID, sh.Digest)
		if err != nil {
			return false, fmt.Errorf("%w: shard %s output unreadable: %v",
				ErrVerificationFailed, sh.ID, err)
		}
		if got := hexDigest(b); got != sh.Digest {
			return false, fmt.Errorf("%w: shard %s stored digest corrupted",
				ErrVerificationFailed, sh.ID)
		}
		entries = append(entries, ManifestEntry{
			ShardID:   sh.ID,
			Digest:    sh.Digest,
			Count:     sh.Count,
			OutputRef: sh.OutputRef,
		})
	}

	digest, err := manifestDigest(t, entries)
	if err != nil {
		return false, err
	}
	m := &Manifest{
		ID:            "manifest_" + digest[:16],
		TaskID:        t.ID,
		TaskAttempt:   t.Attempt,
		SourceVersion: t.SourceVersion,
		RuleVersion:   t.RuleVersion,
		Entries:       entries,
		Digest:        digest,
		CreatedAt:     s.clock.Now(),
	}
	if err := s.st.putManifestLocked(t.ID, m); err != nil {
		return false, err
	}

	now := m.CreatedAt
	t.Status = TaskPublished
	t.ManifestID = m.ID
	t.PublishedAt = &now
	if err := s.st.saveLocked(); err != nil {
		return false, err
	}

	// GC: delete staged objects not referenced by the sealed manifest. Under
	// the normal flow none exist; this also heals orphaned bytes from
	// crashed/interrupted attempts.
	if err := s.gcOutputsLocked(t); err != nil {
		return true, err
	}
	return true, nil
}

// Publish asks the coordinator to publish if the task is complete; it returns
// whether the task is published. This exists for callers that want an explicit
// publication check; normally publication happens automatically on the last
// receipt.
func (s *Service) Publish(id, externalID string) (*Manifest, bool, error) {
	s.st.mu.Lock()
	defer s.st.mu.Unlock()
	t, err := s.resolveRef(id, externalID)
	if err != nil {
		return nil, false, err
	}
	if t.Status == TaskPublished {
		m, err := s.st.readManifestLocked(t.ID, t.ManifestID)
		return m, true, err
	}
	if t.Status == TaskCancelled {
		return nil, false, ErrTaskCancelled
	}
	published, err := s.publishIfCompleteLocked(t)
	if err != nil || !published {
		return nil, published, err
	}
	m, err := s.st.readManifestLocked(t.ID, t.ManifestID)
	return m, true, err
}

// gcOutputsLocked removes stored outputs not referenced by the task's current
// authoritative state:
//   - published: only manifest-referenced digests are retained;
//   - cancelled: every output is removed;
//   - running: digests referenced by succeeded shards are retained.
func (s *Service) gcOutputsLocked(t *Task) error {
	stored, err := s.st.listOutputDigestsLocked(t.ID)
	if err != nil {
		return err
	}
	live := map[string]struct{}{}
	switch t.Status {
	case TaskPublished:
		m, err := s.st.readManifestLocked(t.ID, t.ManifestID)
		if err != nil {
			return err
		}
		for _, e := range m.Entries {
			live[e.Digest] = struct{}{}
		}
	case TaskCancelled:
		// retain nothing
	default:
		for _, sh := range t.Shards {
			if sh.Status == ShardSucceeded && sh.Digest != "" {
				live[sh.Digest] = struct{}{}
			}
		}
	}
	for _, d := range stored {
		if _, ok := live[d]; ok {
			continue
		}
		if err := s.st.removeOutputLocked(t.ID, d); err != nil {
			return err
		}
	}
	return nil
}

// GC removes outputs no longer referenced by any task state. It is safe to run
// repeatedly and is invoked automatically at cancellation and publication.
func (s *Service) GC(id, externalID string) error {
	s.st.mu.Lock()
	defer s.st.mu.Unlock()
	t, err := s.resolveRef(id, externalID)
	if err != nil {
		return err
	}
	return s.gcOutputsLocked(t)
}

// Progress reports counts and the publication gate without exposing any
// partial output.
func (s *Service) Progress(id, externalID string) (*Progress, error) {
	s.st.mu.RLock()
	defer s.st.mu.RUnlock()
	t, err := s.resolveRef(id, externalID)
	if err != nil {
		return nil, err
	}
	p := &Progress{
		TaskID:        t.ID,
		ExternalID:    t.ExternalID,
		Status:        t.Status,
		SourceVersion: t.SourceVersion,
		RuleVersion:   t.RuleVersion,
		Total:         len(t.Shards),
		Published:     t.Status == TaskPublished,
		ManifestID:    t.ManifestID,
	}
	for _, sh := range t.Shards {
		switch sh.Status {
		case ShardSucceeded:
			p.Succeeded++
		case ShardFailed:
			p.Failed++
		case ShardLeased:
			p.Leased++
		case ShardPending:
			p.Pending++
		}
	}
	return p, nil
}

// GetManifest returns the immutable result manifest. It is available only
// after publication; before that callers receive ErrNotPublished so partial
// results can never be read externally.
func (s *Service) GetManifest(id, externalID string) (*Manifest, error) {
	s.st.mu.RLock()
	defer s.st.mu.RUnlock()
	t, err := s.resolveRef(id, externalID)
	if err != nil {
		return nil, err
	}
	if t.Status != TaskPublished || t.ManifestID == "" {
		return nil, ErrNotPublished
	}
	return s.st.readManifestLocked(t.ID, t.ManifestID)
}

// DownloadOutput returns one shard's redacted output bytes. It is the sole
// external read path for result data and is gated by publication: with
// un-published or cancelled tasks it fails with ErrNotPublished, so a
// half-processed data set is never externally visible.
func (s *Service) DownloadOutput(id, externalID, shardID string) ([]byte, error) {
	s.st.mu.RLock()
	defer s.st.mu.RUnlock()
	t, err := s.resolveRef(id, externalID)
	if err != nil {
		return nil, err
	}
	if t.Status != TaskPublished || t.ManifestID == "" {
		return nil, ErrNotPublished
	}
	m, err := s.st.readManifestLocked(t.ID, t.ManifestID)
	if err != nil {
		return nil, err
	}
	var ref string
	for _, e := range m.Entries {
		if e.ShardID == shardID {
			ref = e.OutputRef
			break
		}
	}
	if ref == "" {
		return nil, fmt.Errorf("%w: shard %s not in manifest", ErrNotFound, shardID)
	}
	b, err := s.st.readOutputLocked(t.ID, ref)
	if err != nil {
		return nil, err
	}
	if got := hexDigest(b); got != ref {
		return nil, fmt.Errorf("%w: shard %s stored digest corrupted", ErrVerificationFailed, shardID)
	}
	return b, nil
}
