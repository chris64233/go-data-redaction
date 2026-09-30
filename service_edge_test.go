package godataredaction

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ---- claim edge cases ----

func TestClaim_RequiresWorker(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	newTestTask(t, svc, "ext-1", "a")
	if _, err := svc.ClaimShard("", "ext-1", ""); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty worker: expected ErrInvalidInput, got %v", err)
	}
}

func TestClaim_FailedShardIsNotAutoClaimable(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")
	c := claim(t, svc, task.ID, "w1")

	if _, _, err := svc.SubmitReceipt(&Receipt{
		TaskID: task.ID, ShardID: "a", Attempt: c.Attempt, LeaseID: c.LeaseID,
		Worker: "w1", Succeeded: false, FailReason: "transient",
	}); err != nil {
		t.Fatal(err)
	}

	// A failed shard must not be silently handed out again: workers must go
	// through the explicit (same frozen task version) retry path.
	if _, err := svc.ClaimShard(task.ID, "", "w2"); !errors.Is(err, ErrNoAvailableShard) {
		t.Fatalf("failed shard auto-claimed: %v", err)
	}
	if err := svc.RetryShard(task.ID, "", "a"); err != nil {
		t.Fatalf("RetryShard: %v", err)
	}
	c2 := claim(t, svc, task.ID, "w2")
	if c2.Shard.ID != "a" || c2.Attempt != task.Attempt {
		t.Fatalf("retried claim wrong: %+v", c2)
	}
}

func TestClaim_LeaseExpiryBoundary(t *testing.T) {
	svc, clk, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")
	claim(t, svc, task.ID, "w1")

	// One nanosecond before expiry the lease is still held.
	clk.advance(time.Minute - time.Nanosecond)
	if _, err := svc.ClaimShard(task.ID, "", "w2"); !errors.Is(err, ErrNoAvailableShard) {
		t.Fatalf("lease just inside TTL must not be takeable: %v", err)
	}
	// Exactly at (and after) expiry another worker may take over.
	clk.advance(time.Nanosecond)
	c2 := claim(t, svc, task.ID, "w2")
	if c2.LeaseID != 2 || c2.Shard.Worker != "w2" {
		t.Fatalf("takeover at exact expiry wrong: %+v", c2)
	}
}

// ---- receipt validation edge cases ----

func TestReceipt_InvalidInput(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")
	c := claim(t, svc, task.ID, "w1")

	if _, _, err := svc.SubmitReceipt(nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nil receipt: %v", err)
	}
	emptyShard := successReceipt(c, "w1", []byte("x"), 1)
	emptyShard.ShardID = ""
	if _, _, err := svc.SubmitReceipt(emptyShard); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty shard id: %v", err)
	}

	// Success requires a well-formed digest.
	badDigest := successReceipt(c, "w1", []byte("x"), 1)
	badDigest.Digest = "not-a-sha256"
	if _, _, err := svc.SubmitReceipt(badDigest); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("malformed digest: %v", err)
	}

	// Success requires actual payload bytes to verify against.
	noPayload := successReceipt(c, "w1", []byte("x"), 1)
	noPayload.Payload = nil
	if _, _, err := svc.SubmitReceipt(noPayload); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("missing payload: %v", err)
	}

	// Unknown task / shard references.
	if _, _, err := svc.SubmitReceipt(&Receipt{TaskID: "task_missing", ShardID: "a"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown task: %v", err)
	}
	missingShard := successReceipt(c, "w1", []byte("x"), 1)
	missingShard.ShardID = "nope"
	if _, _, err := svc.SubmitReceipt(missingShard); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown shard: %v", err)
	}
}

func TestReceipt_NeverLeasedShardRejected(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")

	// A receipt arriving for a shard that was never leased is rejected even
	// if it forges a plausible fencing token.
	r := &Receipt{
		TaskID: task.ID, ShardID: "a", Attempt: task.Attempt, LeaseID: 1,
		Worker: "w1", Succeeded: false, FailReason: "never claimed",
	}
	if _, _, err := svc.SubmitReceipt(r); err == nil {
		t.Fatalf("receipt for a never-leased shard must be rejected")
	}
}

func TestReceipt_FailureCannotDowngradeSucceededShard(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a", "b")
	c := claim(t, svc, task.ID, "w1")
	if _, _, err := svc.SubmitReceipt(successReceipt(c, "w1", []byte("A"), 1)); err != nil {
		t.Fatal(err)
	}
	// Task is still running (b pending); a late failure receipt for the
	// succeeded shard cannot downgrade it.
	fail := &Receipt{
		TaskID: task.ID, ShardID: "a", Attempt: task.Attempt, LeaseID: c.LeaseID,
		Worker: "w1", Succeeded: false, FailReason: "late failure",
	}
	if _, _, err := svc.SubmitReceipt(fail); err == nil {
		t.Fatalf("failure receipt must not downgrade a succeeded shard")
	}
	got, _ := svc.GetTask(task.ID, "")
	for _, sh := range got.Shards {
		if sh.ID == "a" && sh.Status != ShardSucceeded {
			t.Fatalf("succeeded shard downgraded to %s", sh.Status)
		}
	}
}

// ---- external id alias ----

func TestExternalIDAlias_FullFlow(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-42", "a")

	// Every operation works with the external id alone.
	c, err := svc.ClaimShard("", "ext-42", "w1")
	if err != nil {
		t.Fatalf("claim by external id: %v", err)
	}
	ack, published, err := svc.SubmitReceipt(&Receipt{
		ExternalID: "ext-42", ShardID: "a", Attempt: c.Attempt, LeaseID: c.LeaseID,
		Worker: "w1", Succeeded: true,
		Digest: digestOf([]byte("A")), Count: 1, Payload: []byte("A"),
	})
	if err != nil || !published || ack.Status != ShardSucceeded {
		t.Fatalf("receipt by external id: %+v pub=%v err=%v", ack, published, err)
	}
	if p, err := svc.Progress("", "ext-42"); err != nil || !p.Published || p.ExternalID != "ext-42" {
		t.Fatalf("progress by external id: %+v err=%v", p, err)
	}
	if m, err := svc.GetManifest("", "ext-42"); err != nil || m.TaskID != task.ID {
		t.Fatalf("manifest by external id: %+v err=%v", m, err)
	}
	if b, err := svc.DownloadOutput("", "ext-42", "a"); err != nil || string(b) != "A" {
		t.Fatalf("download by external id: %q err=%v", b, err)
	}
}

// ---- post-publication semantics ----

func TestReplayAfterPublished_IsBenignReplay(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")
	c := claim(t, svc, task.ID, "w1")
	payload := []byte("A")
	ack, published, err := svc.SubmitReceipt(successReceipt(c, "w1", payload, 1))
	if err != nil || !published {
		t.Fatalf("publish: pub=%v err=%v", published, err)
	}
	// Redelivering the same acknowledged result after publication is a
	// benign replay, not a terminal-state error.
	ack, published, err = svc.SubmitReceipt(successReceipt(c, "w1", payload, 1))
	if err != nil || !ack.Replayed || published {
		t.Fatalf("post-publish replay: ack=%+v pub=%v err=%v", ack, published, err)
	}
}

func TestRetry_RejectedAfterPublication(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")
	c := claim(t, svc, task.ID, "w1")
	if _, _, err := svc.SubmitReceipt(successReceipt(c, "w1", []byte("A"), 1)); err != nil {
		t.Fatal(err)
	}
	if err := svc.RetryShard(task.ID, "", "a"); !errors.Is(err, ErrTaskPublished) {
		t.Fatalf("retry on published task: %v", err)
	}
}

func TestProgress_AfterPublish(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a", "b")
	c1 := claim(t, svc, task.ID, "w1")
	if _, _, err := svc.SubmitReceipt(successReceipt(c1, "w1", []byte("A"), 1)); err != nil {
		t.Fatal(err)
	}
	c2 := claim(t, svc, task.ID, "w2")
	if _, _, err := svc.SubmitReceipt(successReceipt(c2, "w2", []byte("B"), 2)); err != nil {
		t.Fatal(err)
	}
	p, err := svc.Progress(task.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if !p.Published || p.Status != TaskPublished || p.ManifestID == "" {
		t.Fatalf("published progress wrong: %+v", p)
	}
	if p.Total != 2 || p.Succeeded != 2 || p.Failed != 0 || p.Leased != 0 || p.Pending != 0 {
		t.Fatalf("published counters wrong: %+v", p)
	}
}

func TestDownload_DetectsPostPublishTampering(t *testing.T) {
	svc, _, dir := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")
	c := claim(t, svc, task.ID, "w1")
	payload := []byte("A")
	if _, _, err := svc.SubmitReceipt(successReceipt(c, "w1", payload, 1)); err != nil {
		t.Fatal(err)
	}
	// Tamper with the published blob; the download path re-hashes and must
	// refuse to serve corrupt bytes.
	blob := filepath.Join(dir, "tasks", task.ID, "blobs", digestOf(payload))
	if err := os.WriteFile(blob, []byte("CORRUPTED"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.DownloadOutput(task.ID, "", "a"); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("corrupt published output: %v", err)
	}
}

// ---- missing tasks / terminal idempotency ----

func TestOperations_MissingTask(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)

	if _, err := svc.GetTask("task_nope", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetTask: %v", err)
	}
	if _, err := svc.ClaimShard("task_nope", "", "w1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ClaimShard: %v", err)
	}
	if _, _, err := svc.Publish("task_nope", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := svc.Cancel("task_nope", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Cancel: %v", err)
	}
	if err := svc.RetryShard("task_nope", "", "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RetryShard: %v", err)
	}
	if _, err := svc.Progress("task_nope", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Progress: %v", err)
	}
	if _, err := svc.GetManifest("task_nope", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetManifest: %v", err)
	}
	if err := svc.GC("task_nope", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GC: %v", err)
	}
}

func TestResubmitAfterCancel_ReturnsOriginalCancelledTask(t *testing.T) {
	svc, _, _ := testService(t, time.Minute)
	task := newTestTask(t, svc, "ext-1", "a")
	if _, err := svc.Cancel(task.ID, ""); err != nil {
		t.Fatal(err)
	}
	// Identical resubmission still returns the original task; cancellation
	// is terminal, so no fresh runnable task is created under the same
	// external id.
	again, err := svc.CreateTask(CreateInput{
		ExternalID: "ext-1", SourceVersion: "src-v1", RuleVersion: "rule-v1",
		ShardIDs: []string{"a"},
	})
	if err != nil {
		t.Fatalf("identical resubmission after cancel: %v", err)
	}
	if again.ID != task.ID || again.Status != TaskCancelled {
		t.Fatalf("expected the original cancelled task, got %+v", again)
	}
	if _, err := svc.ClaimShard(task.ID, "", "w1"); !errors.Is(err, ErrTaskCancelled) {
		t.Fatalf("cancelled task must never accept claims again: %v", err)
	}
}
