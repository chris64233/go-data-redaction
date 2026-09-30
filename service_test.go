package godataredaction

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ---- test helpers ----

type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock               { return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} }
func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func digestOf(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func testService(t *testing.T, ttl time.Duration) (*Service, *fakeClock, string) {
	t.Helper()
	dir := t.TempDir()
	clk := newFakeClock()
	svc, err := NewService(dir, WithClock(clk), WithLeaseTTL(ttl))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc, clk, dir
}

func newTestTask(t *testing.T, svc *Service, ext string, shardIDs ...string) *Task {
	t.Helper()
	task, err := svc.CreateTask(CreateInput{
		ExternalID:    ext,
		SourceVersion: "src-v1",
		RuleVersion:   "rule-v1",
		ShardIDs:      shardIDs,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	return task
}

func claim(t *testing.T, svc *Service, taskID, worker string) *Claim {
	t.Helper()
	c, err := svc.ClaimShard(taskID, "", worker)
	if err != nil {
		t.Fatalf("ClaimShard: %v", err)
	}
	return c
}

func successReceipt(c *Claim, worker string, payload []byte, count int64) *Receipt {
	return &Receipt{
		TaskID:    c.TaskID,
		ShardID:   c.Shard.ID,
		Attempt:   c.Attempt,
		LeaseID:   c.LeaseID,
		Worker:    worker,
		Succeeded: true,
		Digest:    digestOf(payload),
		Count:     count,
		Payload:   payload,
	}
}

// ---- 1. creation: freeze, idempotency, conflict ----

func TestCreateTask_FreezesInputs(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a", "b", "c")
	if task.SourceVersion != "src-v1" || task.RuleVersion != "rule-v1" {
		t.Fatalf("versions not frozen: %+v", task)
	}
	if len(task.Shards) != 3 || task.Shards[0].Status != ShardPending {
		t.Fatalf("shards not initialized pending: %+v", task.Shards)
	}
	if task.Attempt != 0 || task.Status != TaskRunning {
		t.Fatalf("new task should run at version 0: %+v", task)
	}
}

func TestCreateTask_IdempotentSameInput(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	first := newTestTask(t, svc, "ext-1", "a", "b")

	// Same inputs, shards in a different order: same fingerprint.
	again, err := svc.CreateTask(CreateInput{
		ExternalID: "ext-1", SourceVersion: "src-v1", RuleVersion: "rule-v1",
		ShardIDs: []string{"b", "a"},
	})
	if err != nil {
		t.Fatalf("identical resubmission should succeed: %v", err)
	}
	if again.ID != first.ID {
		t.Fatalf("expected original task %s, got %s", first.ID, again.ID)
	}
}

func TestCreateTask_ConflictOnChangedInput(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	newTestTask(t, svc, "ext-1", "a", "b")

	cases := []CreateInput{
		{ExternalID: "ext-1", SourceVersion: "src-v2", RuleVersion: "rule-v1", ShardIDs: []string{"a", "b"}},
		{ExternalID: "ext-1", SourceVersion: "src-v1", RuleVersion: "rule-v2", ShardIDs: []string{"a", "b"}},
		{ExternalID: "ext-1", SourceVersion: "src-v1", RuleVersion: "rule-v1", ShardIDs: []string{"a", "c"}},
		{ExternalID: "ext-1", SourceVersion: "src-v1", RuleVersion: "rule-v1", ShardIDs: []string{"a"}},
		{ExternalID: "ext-1", SourceVersion: "src-v1", RuleVersion: "rule-v1", ShardIDs: []string{"a", "b", "c"}},
	}
	for i, in := range cases {
		_, err := svc.CreateTask(in)
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("case %d: expected ErrConflict, got %v", i, err)
		}
	}
}

func TestCreateTask_InvalidInput(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	bad := []CreateInput{
		{ExternalID: "", SourceVersion: "s", RuleVersion: "r", ShardIDs: []string{"a"}},
		{ExternalID: "x", SourceVersion: "", RuleVersion: "r", ShardIDs: []string{"a"}},
		{ExternalID: "x", SourceVersion: "s", RuleVersion: "", ShardIDs: []string{"a"}},
		{ExternalID: "x", SourceVersion: "s", RuleVersion: "r", ShardIDs: nil},
		{ExternalID: "x", SourceVersion: "s", RuleVersion: "r", ShardIDs: []string{"a", "a"}},
	}
	for i, in := range bad {
		if _, err := svc.CreateTask(in); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("case %d: expected ErrInvalidInput, got %v", i, err)
		}
	}
}

// ---- 2. leases, takeover, fencing, digest conflicts ----

func TestClaimAndSucceed_Lifecycle(t *testing.T) {
	svc, _, _ := testService(t, 10*time.Minute)
	task := newTestTask(t, svc, "ext-1", "a", "b")

	c1 := claim(t, svc, task.ID, "w1")
	if c1.Shard.ID != "a" || c1.LeaseID != 1 || c1.Attempt != task.Attempt {
		t.Fatalf("unexpected first claim: %+v", c1)
	}
	// Second claim gets the other shard.
	c2 := claim(t, svc, task.ID, "w2")
	if c2.Shard.ID != "b" {
		t.Fatalf("expected shard b, got %s", c2.Shard.ID)
	}
	if _, err := svc.ClaimShard(task.ID, "", "w3"); !errors.Is(err, ErrNoAvailableShard) {
		t.Fatalf("expected ErrNoAvailableShard, got %v", err)
	}

	payload := []byte("redacted-a")
	ack, published, err := svc.SubmitReceipt(successReceipt(c1, "w1", payload, 3))
	if err != nil || ack.Status != ShardSucceeded || published {
		t.Fatalf("unexpected receipt result: %+v pub=%v err=%v", ack, published, err)
	}

	// Partial state: no manifest / download yet.
	if _, err := svc.GetManifest(task.ID, ""); !errors.Is(err, ErrNotPublished) {
		t.Fatalf("partial task must not expose a manifest: %v", err)
	}
	if _, err := svc.DownloadOutput(task.ID, "", "a"); !errors.Is(err, ErrNotPublished) {
		t.Fatalf("partial task must not allow download: %v", err)
	}

	// Replaying the same receipt is idempotent and still does not publish
	// while another shard is incomplete.
	ack, published, err = svc.SubmitReceipt(successReceipt(c1, "w1", payload, 3))
	if err != nil || !ack.Replayed || published {
		t.Fatalf("replay: ack=%+v pub=%v err=%v", ack, published, err)
	}
}

func TestLeaseExpiry_TakeoverAndFencedLateReceipt(t *testing.T) {
	svc, clk, _ := testService(t, 10*time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")

	c1 := claim(t, svc, task.ID, "w1")
	if c1.LeaseID != 1 {
		t.Fatalf("first lease id = %d", c1.LeaseID)
	}

	// Lease not expired: another worker cannot take the shard.
	if _, err := svc.ClaimShard(task.ID, "", "w2"); !errors.Is(err, ErrNoAvailableShard) {
		t.Fatalf("non-expired lease must not be claimable: %v", err)
	}

	clk.advance(11 * time.Minute) // expire w1's lease
	c2 := claim(t, svc, task.ID, "w2")
	if c2.LeaseID != 2 || c2.Shard.Worker != "w2" {
		t.Fatalf("takeover claim wrong: %+v", c2)
	}

	// w1's late receipt must be fenced and cannot overwrite anything.
	late := successReceipt(c1, "w1", []byte("w1-output"), 1)
	if _, _, err := svc.SubmitReceipt(late); !errors.Is(err, ErrLeaseFenced) {
		t.Fatalf("expected ErrLeaseFenced, got %v", err)
	}

	// Wrong worker on the current lease is also rejected.
	badWorker := successReceipt(c2, "w1", []byte("x"), 1)
	if _, _, err := svc.SubmitReceipt(badWorker); !errors.Is(err, ErrNotLeaser) {
		t.Fatalf("expected ErrNotLeaser, got %v", err)
	}

	// w2 completes with its own output. w1's bytes must not be on disk.
	p2 := []byte("w2-output")
	_, published, err := svc.SubmitReceipt(successReceipt(c2, "w2", p2, 2))
	if err != nil || !published {
		t.Fatalf("w2 completion should publish: published=%v err=%v", published, err)
	}
	got, err := svc.DownloadOutput(task.ID, "", "a")
	if err != nil || string(got) != string(p2) {
		t.Fatalf("download = %q, err=%v; want w2 output", got, err)
	}
}

func TestReceipt_VersionMismatch(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")
	c := claim(t, svc, task.ID, "w1")
	r := successReceipt(c, "w1", []byte("x"), 1)
	r.Attempt = 9 // receipts must carry the frozen task version
	if _, _, err := svc.SubmitReceipt(r); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}
}

func TestReceipt_DigestVerification(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")
	c := claim(t, svc, task.ID, "w1")

	// Claimed digest does not match payload.
	r := successReceipt(c, "w1", []byte("actual-bytes"), 1)
	r.Digest = digestOf([]byte("different-bytes"))
	if _, _, err := svc.SubmitReceipt(r); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("expected ErrDigestMismatch, got %v", err)
	}
}

func TestReceipt_EmptyShardOutputIsValid(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a", "b")

	// A shard may legitimately redact to zero bytes (no records in range).
	// Digest verification still applies: the digest of empty output is the
	// well-known sha256 of nothing.
	c1 := claim(t, svc, task.ID, "w1")
	empty := []byte{}
	if _, pub, err := svc.SubmitReceipt(successReceipt(c1, "w1", empty, 0)); err != nil || pub {
		t.Fatalf("empty output receipt: pub=%v err=%v", pub, err)
	}

	// A nil payload re-ack of the same empty result is an idempotent replay.
	replay := successReceipt(c1, "w1", empty, 0)
	replay.Payload = nil
	if ack, _, err := svc.SubmitReceipt(replay); err != nil || !ack.Replayed {
		t.Fatalf("nil-payload replay of empty output: ack=%+v err=%v", ack, err)
	}

	// Negative processed counts are rejected up front.
	c2 := claim(t, svc, task.ID, "w2")
	bad := successReceipt(c2, "w2", []byte("B"), -1)
	if _, _, err := svc.SubmitReceipt(bad); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("negative count should be ErrInvalidInput, got %v", err)
	}

	// Finish properly; the published manifest keeps the empty entry and the
	// empty bytes download as empty.
	if _, pub, err := svc.SubmitReceipt(successReceipt(c2, "w2", []byte("B"), 2)); err != nil || !pub {
		t.Fatalf("final receipt should publish: pub=%v err=%v", pub, err)
	}
	if b, err := svc.DownloadOutput(task.ID, "", "a"); err != nil || len(b) != 0 {
		t.Fatalf("empty output download = %d bytes, err=%v", len(b), err)
	}
	m, err := svc.GetManifest(task.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	var emptyCount int64 = -1
	for _, e := range m.Entries {
		if e.ShardID == "a" {
			emptyCount = e.Count
		}
	}
	if emptyCount != 0 {
		t.Fatalf("empty shard manifest count = %d, want 0", emptyCount)
	}
}

// TestAPI_ReturnsDetachedSnapshots ensures caller-held Task/Shard values are
// copies: mutating them cannot corrupt coordinator state, and concurrent
// readers of the returned values never race with internal transitions.
func TestAPI_ReturnsDetachedSnapshots(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")
	task.Status = TaskCancelled // tamper with the returned value...

	got, err := svc.GetTask(task.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != TaskRunning {
		t.Fatalf("mutating the create result changed internal state: %s", got.Status)
	}
	got.Status = TaskPublished
	got.ManifestID = "fake"
	got.Shards[0].Status = ShardSucceeded
	got.Shards[0].Digest = "fake"

	again, err := svc.GetTask(task.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != TaskRunning || again.ManifestID != "" ||
		again.Shards[0].Status != ShardPending || again.Shards[0].Digest != "" {
		t.Fatalf("mutating GetTask result leaked into internal state: %+v", again)
	}

	// The Claim hands back a shard snapshot; advancing the claim's state must
	// not change the lease the coordinator fenced.
	c := claim(t, svc, task.ID, "w1")
	c.Shard.Worker = "attacker"
	c.Shard.Attempt = 999
	if _, _, err := svc.SubmitReceipt(&Receipt{
		TaskID: task.ID, ShardID: "a", Attempt: task.Attempt,
		LeaseID: 1, Worker: "w1", Succeeded: false, FailReason: "x",
	}); err != nil {
		t.Fatalf("coordinator lease altered through claim snapshot: %v", err)
	}

	// Concurrent reads of returned snapshots vs. state transitions under
	// -race must not report data races.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			t, err := svc.GetTask(task.ID, "")
			if err == nil {
				_ = t.Status
				for _, sh := range t.Shards {
					_ = sh.ID + sh.Worker
				}
			}
		}
	}()
	for i := 0; i < 100; i++ {
		svc.Progress(task.ID, "")
	}
	<-done
}

func TestCancel_ReturnedTaskIsSnapshot(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")
	cancelled, err := svc.Cancel(task.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	cancelled.Status = TaskRunning
	cancelled.CancelledAt = nil
	again, err := svc.GetTask(task.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != TaskCancelled || again.CancelledAt == nil {
		t.Fatalf("mutating cancel result leaked into internal state: %+v", again)
	}
}

func TestSucceededShard_DifferentDigestIsConflictNotRetry(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a", "b")
	c := claim(t, svc, task.ID, "w1")
	p1 := []byte("first-output")
	if _, _, err := svc.SubmitReceipt(successReceipt(c, "w1", p1, 1)); err != nil {
		t.Fatalf("first receipt: %v", err)
	}
	// Shard b remains pending, so the task is still running and the only
	// claimable shard is b: succeeded shard a cannot be re-claimed.
	next, err := svc.ClaimShard(task.ID, "", "w2")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if next.Shard.ID != "b" {
		t.Fatalf("expected to claim pending shard b, got %s", next.Shard.ID)
	}
	if _, err := svc.ClaimShard(task.ID, "", "w3"); !errors.Is(err, ErrNoAvailableShard) {
		t.Fatalf("succeeded shard must not be claimable: %v", err)
	}

	// A receipt claiming a different digest for the same shard is a digest
	// conflict even if it forges the recorded lease token; it must never
	// overwrite the accepted result.
	got, _ := svc.GetTask(task.ID, "")
	var sh *Shard
	for _, s := range got.Shards {
		if s.ID == "a" {
			sh = s
		}
	}
	r := &Receipt{
		TaskID: task.ID, ShardID: "a", Attempt: task.Attempt,
		LeaseID: sh.Attempt, Worker: "w1", Succeeded: true,
		Digest: digestOf([]byte("second-output")), Count: 9, Payload: []byte("second-output"),
	}
	if _, _, err := svc.SubmitReceipt(r); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("expected ErrDigestMismatch for different digest, got %v", err)
	}

	// Task cannot complete/publish while conflict unresolved; the accepted
	// digest is untouched.
	got, _ = svc.GetTask(task.ID, "")
	for _, s := range got.Shards {
		if s.ID == "a" && s.Digest != digestOf(p1) {
			t.Fatalf("accepted digest was overwritten: %s", s.Digest)
		}
	}
}

// ---- 3. publication: once, verified, atomic visibility ----

func TestPublish_OnlyAfterAllSucceededAndVerified(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a", "b")

	c1 := claim(t, svc, task.ID, "w1")
	if _, _, err := svc.SubmitReceipt(successReceipt(c1, "w1", []byte("A"), 1)); err != nil {
		t.Fatal(err)
	}
	// Explicit publish while incomplete: no error, not published.
	if _, pub, err := svc.Publish(task.ID, ""); err != nil || pub {
		t.Fatalf("incomplete publish: pub=%v err=%v", pub, err)
	}

	c2 := claim(t, svc, task.ID, "w2")
	if _, pub, err := svc.SubmitReceipt(successReceipt(c2, "w2", []byte("B"), 2)); err != nil || !pub {
		t.Fatalf("last receipt should publish: pub=%v err=%v", pub, err)
	}

	m, err := svc.GetManifest(task.ID, "")
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if len(m.Entries) != 2 || m.TaskAttempt != task.Attempt {
		t.Fatalf("manifest entries wrong: %+v", m)
	}
	if m.ID != "manifest_"+m.Digest[:16] {
		t.Fatalf("manifest id not content-derived")
	}
	// Manifest file is immutable on disk (0444).
	info, err := os.Stat(filepath.Join(svcPath(svc), "tasks", task.ID, "manifests", m.ID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o444 {
		t.Fatalf("manifest perm = %o, want 0444", perm)
	}

	// Downloads of every entry work and verify bytes.
	if b, err := svc.DownloadOutput(task.ID, "", "a"); err != nil || string(b) != "A" {
		t.Fatalf("download a = %q, %v", b, err)
	}
	if b, err := svc.DownloadOutput(task.ID, "", "b"); err != nil || string(b) != "B" {
		t.Fatalf("download b = %q, %v", b, err)
	}
	if _, err := svc.DownloadOutput(task.ID, "", "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown shard download: %v", err)
	}

	// Repeated publish is idempotent: same manifest.
	m2, pub, err := svc.Publish(task.ID, "")
	if err != nil || !pub || m2.ID != m.ID {
		t.Fatalf("republish: %+v pub=%v err=%v", m2, pub, err)
	}
	// Only one manifest file exists.
	files, _ := os.ReadDir(filepath.Join(svcPath(svc), "tasks", task.ID, "manifests"))
	if len(files) != 1 {
		t.Fatalf("expected exactly 1 manifest file, got %d", len(files))
	}
}

func TestPublish_ConcurrentLastShards_PublishesExactlyOnce(t *testing.T) {
	const n = 12
	for iter := 0; iter < 5; iter++ {
		svc, _, _ := testService(t, time.Minute)
		ids := make([]string, n)
		for i := range ids {
			ids[i] = "s" + string(rune('a'+i))
		}
		task := newTestTask(t, svc, "ext", ids...)

		claims := make([]*Claim, n)
		for i := range claims {
			claims[i] = claim(t, svc, task.ID, "w")
		}
		// Complete n-1 first, then race the final receipts (all receipts are
		// valid candidates to trigger publication checks).
		for i := 1; i < n; i++ {
			if _, _, err := svc.SubmitReceipt(successReceipt(claims[i], "w", []byte(claims[i].Shard.ID), 1)); err != nil {
				t.Fatal(err)
			}
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		results := make(chan bool, n)
		// All n workers race submitting; first n-1 are replays of their own
		// succeeded shards, last one completes the final shard. Multiple
		// replays also invoke the publication check.
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				payload := []byte(claims[i].Shard.ID)
				_, pub, err := svc.SubmitReceipt(successReceipt(claims[i], "w", payload, 1))
				// Replays of an already accepted digest stay idempotent even
				// after another goroutine published; the one claim that
				// actually switches state is counted below.
				if err != nil {
					t.Errorf("iter %d shard %d: unexpected receipt err: %v", iter, i, err)
					return
				}
				results <- pub
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)
		var got int32
		for p := range results {
			if p {
				got++
			}
		}
		if got != 1 {
			t.Fatalf("iter %d: %d callers observed publication, want exactly 1", iter, got)
		}

		m, err := svc.GetManifest(task.ID, "")
		if err != nil {
			t.Fatalf("manifest missing after race: %v", err)
		}
		if len(m.Entries) != n {
			t.Fatalf("manifest has %d entries, want %d", len(m.Entries), n)
		}
		files, _ := os.ReadDir(filepath.Join(svcPath(svc), "tasks", task.ID, "manifests"))
		if len(files) != 1 {
			t.Fatalf("expected one manifest file, got %d", len(files))
		}
	}
}

func TestProgress_CountsAndGate(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a", "b", "c")

	c1 := claim(t, svc, task.ID, "w1")
	if _, _, err := svc.SubmitReceipt(successReceipt(c1, "w1", []byte("A"), 1)); err != nil {
		t.Fatal(err)
	}
	c2 := claim(t, svc, task.ID, "w2")
	if _, _, err := svc.SubmitReceipt(&Receipt{
		TaskID: task.ID, ShardID: c2.Shard.ID, Attempt: c2.Attempt, LeaseID: c2.LeaseID,
		Worker: "w2", Succeeded: false, FailReason: "boom",
	}); err != nil {
		t.Fatal(err)
	}

	p, err := svc.Progress(task.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	want := Progress{Total: 3, Succeeded: 1, Failed: 1, Pending: 1}
	if p.Succeeded != want.Succeeded || p.Failed != want.Failed ||
		p.Pending != want.Pending || p.Leased != 0 || p.Published {
		t.Fatalf("progress = %+v, want %+v (leased 0)", p, want)
	}
}

// ---- 4. retry, cancel, terminal races, GC ----

func TestRetry_FailedShardSameVersion(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")
	c := claim(t, svc, task.ID, "w1")

	if _, _, err := svc.SubmitReceipt(&Receipt{
		TaskID: task.ID, ShardID: "a", Attempt: c.Attempt, LeaseID: c.LeaseID,
		Worker: "w1", Succeeded: false, FailReason: "transient",
	}); err != nil {
		t.Fatal(err)
	}

	// Retrying non-failed shards is rejected.
	if err := svc.RetryShard(task.ID, "", "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing shard: %v", err)
	}
	// Only the failed shard can be reset.
	if err := svc.RetryShard(task.ID, "", "a"); err != nil {
		t.Fatalf("RetryShard: %v", err)
	}

	got, _ := svc.GetTask(task.ID, "")
	var sh *Shard
	for _, s := range got.Shards {
		if s.ID == "a" {
			sh = s
		}
	}
	if sh.Status != ShardPending || sh.Attempt != 2 {
		t.Fatalf("after retry shard = %+v", sh)
	}
	if got.Attempt != 0 {
		t.Fatalf("task version must stay frozen at 0, got %d", got.Attempt)
	}

	// New lease, new fencing token (failed lease 1 -> retry bump 2 -> new
	// lease 3); a receipt on the old failed lease is fenced.
	c2 := claim(t, svc, task.ID, "w2")
	if c2.LeaseID != 3 {
		t.Fatalf("new lease = %d, want 3", c2.LeaseID)
	}
	old := &Receipt{TaskID: task.ID, ShardID: "a", Attempt: 0, LeaseID: c.LeaseID,
		Worker: "w1", Succeeded: true, Digest: digestOf([]byte("late")), Payload: []byte("late"), Count: 1}
	if _, _, err := svc.SubmitReceipt(old); !errors.Is(err, ErrLeaseFenced) {
		t.Fatalf("late receipt after retry should be fenced: %v", err)
	}

	if _, pub, err := svc.SubmitReceipt(successReceipt(c2, "w2", []byte("done"), 1)); err != nil || !pub {
		t.Fatalf("retried shard completion should publish: pub=%v err=%v", pub, err)
	}
}

func TestCancel_StopsClaimsAndCleansOutputs(t *testing.T) {
	svc, _, dir := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a", "b")
	c1 := claim(t, svc, task.ID, "w1")
	if _, _, err := svc.SubmitReceipt(successReceipt(c1, "w1", []byte("A"), 1)); err != nil {
		t.Fatal(err)
	}
	c2 := claim(t, svc, task.ID, "w2") // leased, outstanding

	cancelled, err := svc.Cancel(task.ID, "")
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if cancelled.Status != TaskCancelled || cancelled.CancelledAt == nil {
		t.Fatalf("cancel state wrong: %+v", cancelled)
	}

	// No new claims.
	if _, err := svc.ClaimShard(task.ID, "", "w3"); !errors.Is(err, ErrTaskCancelled) {
		t.Fatalf("claim after cancel: %v", err)
	}
	// Outstanding receipt is rejected by the terminal state.
	if _, _, err := svc.SubmitReceipt(successReceipt(c2, "w2", []byte("B"), 1)); !errors.Is(err, ErrTaskCancelled) {
		t.Fatalf("receipt after cancel: %v", err)
	}
	// No result access.
	if _, err := svc.GetManifest(task.ID, ""); !errors.Is(err, ErrNotPublished) {
		t.Fatalf("manifest after cancel: %v", err)
	}
	if _, err := svc.DownloadOutput(task.ID, "", "a"); !errors.Is(err, ErrNotPublished) {
		t.Fatalf("download after cancel: %v", err)
	}
	// Retry rejected too.
	if err := svc.RetryShard(task.ID, "", "a"); !errors.Is(err, ErrTaskCancelled) {
		t.Fatalf("retry after cancel: %v", err)
	}
	// All staged output bytes removed.
	stored, err := os.ReadDir(filepath.Join(dir, "tasks", task.ID, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	if len(stored) != 0 {
		t.Fatalf("expected no staged outputs after cancel, found %d", len(stored))
	}
	// Cancel idempotent.
	again, err := svc.Cancel(task.ID, "")
	if err != nil || again.Status != TaskCancelled {
		t.Fatalf("second cancel: %+v %v", again, err)
	}
}

func TestCancelAfterPublish_OnlyOneTerminalState(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")
	c := claim(t, svc, task.ID, "w1")
	if _, _, err := svc.SubmitReceipt(successReceipt(c, "w1", []byte("A"), 1)); err != nil {
		t.Fatal(err)
	}
	// Task is now published. Cancel must not flip it.
	cancelled, err := svc.Cancel(task.ID, "")
	if err != nil || cancelled.Status != TaskPublished {
		t.Fatalf("cancel of published task must keep published: %+v err=%v", cancelled, err)
	}
	if b, err := svc.DownloadOutput(task.ID, "", "a"); err != nil || string(b) != "A" {
		t.Fatalf("published output must survive cancel attempt: %q %v", b, err)
	}
	// Claims/receipts on a published task are refused.
	if _, err := svc.ClaimShard(task.ID, "", "w2"); !errors.Is(err, ErrTaskPublished) {
		t.Fatalf("claim after publish: %v", err)
	}
}

func TestConcurrent_CancelVsReceipts_OneTerminalState(t *testing.T) {
	for iter := 0; iter < 10; iter++ {
		svc, _, _ := testService(t, 5*time.Minute)
		task := newTestTask(t, svc, "ext", "a")
		c := claim(t, svc, task.ID, "w1")

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var cancelState TaskStatus
		var receiptErr error
		go func() {
			defer wg.Done()
			<-start
			ct, _ := svc.Cancel(task.ID, "")
			cancelState = ct.Status
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _, receiptErr = svc.SubmitReceipt(successReceipt(c, "w1", []byte("A"), 1))
		}()
		close(start)
		wg.Wait()

		got, _ := svc.GetTask(task.ID, "")
		if got.Status != TaskCancelled && got.Status != TaskPublished {
			t.Fatalf("iter %d: task ended non-terminal: %s", iter, got.Status)
		}
		if got.Status == TaskCancelled {
			if receiptErr == nil || !errors.Is(receiptErr, ErrTaskCancelled) {
				t.Fatalf("iter %d: cancelled but receipt accepted: %v", iter, receiptErr)
			}
			if _, err := svc.GetManifest(task.ID, ""); !errors.Is(err, ErrNotPublished) {
				t.Fatalf("iter %d: cancelled task has manifest", iter)
			}
		} else {
			if receiptErr != nil {
				t.Fatalf("iter %d: published but receipt errored: %v", iter, receiptErr)
			}
			if b, err := svc.DownloadOutput(task.ID, "", "a"); err != nil || string(b) != "A" {
				t.Fatalf("iter %d: published output wrong: %q %v", iter, b, err)
			}
		}
		if cancelState != got.Status {
			t.Fatalf("iter %d: cancel observed %s, final %s", iter, cancelState, got.Status)
		}
	}
}

// TestConcurrent_CancelVsRetryReceipt_OneTerminalState races a cancel against
// the receipt that completes a retried shard: exactly one terminal state may
// survive, and the losing side must observe the matching terminal error.
func TestConcurrent_CancelVsRetryReceipt_OneTerminalState(t *testing.T) {
	for iter := 0; iter < 25; iter++ {
		svc, _, _ := testService(t, 5*time.Minute)
		task := newTestTask(t, svc, "ext", "a")
		c1 := claim(t, svc, task.ID, "w1")
		if _, _, err := svc.SubmitReceipt(&Receipt{
			TaskID: task.ID, ShardID: "a", Attempt: c1.Attempt, LeaseID: c1.LeaseID,
			Worker: "w1", Succeeded: false, FailReason: "transient",
		}); err != nil {
			t.Fatal(err)
		}
		if err := svc.RetryShard(task.ID, "", "a"); err != nil {
			t.Fatal(err)
		}
		c2 := claim(t, svc, task.ID, "w2")

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var receiptErr error
		var receiptPublished bool
		go func() {
			defer wg.Done()
			<-start
			_, receiptPublished, receiptErr = svc.SubmitReceipt(
				successReceipt(c2, "w2", []byte("A"), 1))
		}()
		go func() {
			defer wg.Done()
			<-start
			svc.Cancel(task.ID, "")
		}()
		close(start)
		wg.Wait()

		got, _ := svc.GetTask(task.ID, "")
		switch got.Status {
		case TaskPublished:
			if receiptErr != nil || !receiptPublished {
				t.Fatalf("iter %d: published but receipt saw err=%v pub=%v",
					iter, receiptErr, receiptPublished)
			}
			if b, err := svc.DownloadOutput(task.ID, "", "a"); err != nil || string(b) != "A" {
				t.Fatalf("iter %d: published output wrong: %q %v", iter, b, err)
			}
		case TaskCancelled:
			if receiptErr == nil || !errors.Is(receiptErr, ErrTaskCancelled) {
				t.Fatalf("iter %d: cancelled but retried receipt accepted: %v", iter, receiptErr)
			}
			if _, err := svc.GetManifest(task.ID, ""); !errors.Is(err, ErrNotPublished) {
				t.Fatalf("iter %d: cancelled task exposes manifest", iter)
			}
		default:
			t.Fatalf("iter %d: non-terminal final state %s", iter, got.Status)
		}
	}
}

// ---- 5. persistence across restart ----

func TestPersistence_AcrossRestart(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock()

	open := func() *Service {
		svc, err := NewService(dir, WithClock(clk), WithLeaseTTL(time.Minute))
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		return svc
	}

	svc := open()
	in := CreateInput{ExternalID: "ext-1", SourceVersion: "src-v1", RuleVersion: "rule-v1", ShardIDs: []string{"a", "b"}}
	task, err := svc.CreateTask(in)
	if err != nil {
		t.Fatal(err)
	}
	c := claim(t, svc, task.ID, "w1")
	if _, _, err := svc.SubmitReceipt(successReceipt(c, "w1", []byte("A"), 1)); err != nil {
		t.Fatal(err)
	}

	// Restart mid-processing: state resumes; same external ID returns same
	// task, not a conflict.
	svc = open()
	task2, err := svc.CreateTask(in)
	if err != nil {
		t.Fatalf("resubmit after restart: %v", err)
	}
	if task2.ID != task.ID {
		t.Fatalf("task id changed across restart")
	}
	p, err := svc.Progress(task.ID, "")
	if err != nil || p.Succeeded != 1 || p.Pending != 1 || p.Published {
		t.Fatalf("progress after restart: %+v err=%v", p, err)
	}
	if _, err := svc.DownloadOutput(task.ID, "", "a"); !errors.Is(err, ErrNotPublished) {
		t.Fatalf("partial data still gated after restart: %v", err)
	}

	// Finish on the restarted service.
	c2 := claim(t, svc, task.ID, "w2")
	if _, _, err := svc.SubmitReceipt(successReceipt(c2, "w2", []byte("B"), 1)); err != nil {
		t.Fatal(err)
	}

	// Restart after publication: manifest and outputs remain readable.
	svc = open()
	m, err := svc.GetManifest(task.ID, "")
	if err != nil {
		t.Fatalf("manifest after restart: %v", err)
	}
	if len(m.Entries) != 2 {
		t.Fatalf("entries after restart: %d", len(m.Entries))
	}
	if b, err := svc.DownloadOutput(task.ID, "", "b"); err != nil || string(b) != "B" {
		t.Fatalf("download after restart: %q %v", b, err)
	}
	got, _ := svc.GetTask("", "ext-1")
	if got.Status != TaskPublished {
		t.Fatalf("external lookup after restart: %s", got.Status)
	}
}

// ---- 6. GC of unreferenced outputs ----

func TestReopen_ReconcilesOrphanedOutputs(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock()
	svc, err := NewService(dir, WithClock(clk), WithLeaseTTL(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	task := newTestTask(t, svc, "ext-1", "a")
	c := claim(t, svc, task.ID, "w1")
	live := []byte("A")
	if _, _, err := svc.SubmitReceipt(successReceipt(c, "w1", live, 1)); err != nil {
		t.Fatal(err)
	}

	// Orphan bytes (e.g. an attempt that crashed after staging but before its
	// receipt committed) are collected when the store reopens.
	orphan := digestOf([]byte("orphan-bytes"))
	svc.st.mu.Lock()
	if err := svc.st.putOutputLocked(task.ID, orphan, []byte("orphan-bytes")); err != nil {
		t.Fatal(err)
	}
	svc.st.mu.Unlock()

	svc2, err := NewService(dir, WithClock(clk), WithLeaseTTL(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	blobs, _ := os.ReadDir(filepath.Join(dir, "tasks", task.ID, "blobs"))
	if len(blobs) != 1 || blobs[0].Name() != digestOf(live) {
		names := make([]string, len(blobs))
		for i, b := range blobs {
			names[i] = b.Name()
		}
		t.Fatalf("reopen reconciliation left blobs %v, want only live output", names)
	}
	// Published task resumes readable.
	if b, err := svc2.DownloadOutput(task.ID, "", "a"); err != nil || string(b) != "A" {
		t.Fatalf("download after reopen: %q %v", b, err)
	}
}

func TestReopen_ManifestWrittenBeforeStateFlush_RepublishesIdempotently(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock()
	svc, err := NewService(dir, WithClock(clk), WithLeaseTTL(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	task := newTestTask(t, svc, "ext-1", "a")
	c := claim(t, svc, task.ID, "w1")
	if _, pub, err := svc.SubmitReceipt(successReceipt(c, "w1", []byte("A"), 1)); err != nil || !pub {
		t.Fatalf("initial publish: pub=%v err=%v", pub, err)
	}

	// Simulate a crash after the manifest file was written but before the
	// state snapshot recorded publication: rewrite state.json back to running.
	svc.st.mu.Lock()
	internal := svc.st.tasks[task.ID]
	manifestID := internal.ManifestID
	internal.Status = TaskRunning
	internal.ManifestID = ""
	internal.PublishedAt = nil
	if err := svc.st.saveLocked(); err != nil {
		t.Fatal(err)
	}
	svc.st.mu.Unlock()

	svc2, err := NewService(dir, WithClock(clk), WithLeaseTTL(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	m, ok, err := svc2.Publish(task.ID, "")
	if err != nil || !ok {
		t.Fatalf("publish after crash window: ok=%v err=%v", ok, err)
	}
	if m.ID != manifestID {
		t.Fatalf("republish must reuse the content-derived manifest: %s != %s", m.ID, manifestID)
	}
	files, _ := os.ReadDir(filepath.Join(dir, "tasks", task.ID, "manifests"))
	if len(files) != 1 {
		t.Fatalf("expected one manifest file, got %d", len(files))
	}
}

func TestReopen_ReconcilesCancelledOutputs(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock()
	svc, err := NewService(dir, WithClock(clk), WithLeaseTTL(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	task := newTestTask(t, svc, "ext-1", "a", "b")
	c := claim(t, svc, task.ID, "w1")
	if _, _, err := svc.SubmitReceipt(successReceipt(c, "w1", []byte("A"), 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Cancel(task.ID, ""); err != nil {
		t.Fatal(err)
	}
	// Simulate leftover bytes on disk that cleanup did not reach (crash
	// window); reopening must honor the cancelled terminal state and remove
	// them.
	svc.st.mu.Lock()
	if err := svc.st.putOutputLocked(task.ID, digestOf([]byte("A")), []byte("A")); err != nil {
		t.Fatal(err)
	}
	svc.st.mu.Unlock()

	if _, err := NewService(dir, WithClock(clk), WithLeaseTTL(time.Minute)); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "tasks", task.ID, "blobs"))
	if len(entries) != 0 {
		t.Fatalf("cancelled task must keep no outputs after reopen, got %d", len(entries))
	}
}

func TestGC_RemovesUnreferencedOutputs(t *testing.T) {
	svc, _, dir := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a", "b")
	c1 := claim(t, svc, task.ID, "w1")
	if _, _, err := svc.SubmitReceipt(successReceipt(c1, "w1", []byte("A"), 1)); err != nil {
		t.Fatal(err)
	}

	// Drop an orphaned object that nothing references (simulates a crashed
	// attempt that staged bytes but never committed a receipt).
	orphan := digestOf([]byte("orphan"))
	svc.st.mu.Lock()
	if err := svc.st.putOutputLocked(task.ID, orphan, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	svc.st.mu.Unlock()

	if err := svc.GC(task.ID, ""); err != nil {
		t.Fatalf("GC: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "tasks", task.ID, "blobs", orphan)); !os.IsNotExist(err) {
		t.Fatalf("orphan output should have been collected, err=%v", err)
	}
	// Referenced (succeeded) output survives while running.
	if _, err := os.Stat(filepath.Join(dir, "tasks", task.ID, "blobs", digestOf([]byte("A")))); err != nil {
		t.Fatalf("live output removed by GC: %v", err)
	}

	// After publication GC retains only manifest entries (exercised by
	// finishing and re-running GC).
	c2 := claim(t, svc, task.ID, "w2")
	if _, _, err := svc.SubmitReceipt(successReceipt(c2, "w2", []byte("B"), 1)); err != nil {
		t.Fatal(err)
	}
	if err := svc.GC(task.ID, ""); err != nil {
		t.Fatal(err)
	}
	blobs, _ := os.ReadDir(filepath.Join(dir, "tasks", task.ID, "blobs"))
	if len(blobs) != 2 {
		t.Fatalf("published task should retain exactly its 2 outputs, got %d", len(blobs))
	}
}

func TestPublish_VerifiesStoredBytesBeforeSealing(t *testing.T) {
	svc, _, dir := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a", "b")
	c1 := claim(t, svc, task.ID, "w1")
	payloadA := []byte("A")
	if _, _, err := svc.SubmitReceipt(successReceipt(c1, "w1", payloadA, 1)); err != nil {
		t.Fatal(err)
	}

	// Tamper with the staged bytes of the already succeeded shard (simulates
	// disk corruption). Its content-addressed filename keeps the old digest.
	digestA := digestOf(payloadA)
	blob := filepath.Join(dir, "tasks", task.ID, "blobs", digestA)
	if err := os.WriteFile(blob, []byte("CORRUPTED"), 0o644); err != nil {
		t.Fatal(err)
	}

	c2 := claim(t, svc, task.ID, "w2")
	_, published, err := svc.SubmitReceipt(successReceipt(c2, "w2", []byte("B"), 1))
	if !errors.Is(err, ErrVerificationFailed) || published {
		t.Fatalf("corrupt output must block publication: pub=%v err=%v", published, err)
	}
	got, _ := svc.GetTask(task.ID, "")
	if got.Status != TaskRunning || got.ManifestID != "" {
		t.Fatalf("failed verification must not publish: status=%s manifest=%s", got.Status, got.ManifestID)
	}
	if _, err := svc.GetManifest(task.ID, ""); !errors.Is(err, ErrNotPublished) {
		t.Fatalf("no manifest may exist after failed verification: %v", err)
	}
	files, _ := os.ReadDir(filepath.Join(dir, "tasks", task.ID, "manifests"))
	if len(files) != 0 {
		t.Fatalf("no manifest file may exist, found %d", len(files))
	}

	// Repair the bytes; explicit Publish now re-verifies and seals.
	svc.st.mu.Lock()
	err = atomicWriteFile(blob, payloadA, 0o644)
	svc.st.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	m, ok, err := svc.Publish(task.ID, "")
	if err != nil || !ok {
		t.Fatalf("publish after repair: ok=%v err=%v", ok, err)
	}
	if len(m.Entries) != 2 {
		t.Fatalf("manifest entries = %d", len(m.Entries))
	}
	if b, err := svc.DownloadOutput(task.ID, "", "a"); err != nil || string(b) != "A" {
		t.Fatalf("download after repaired publish: %q %v", b, err)
	}
}

// svcPath exposes the storage directory for filesystem assertions.
func svcPath(s *Service) string { return s.st.dir }
