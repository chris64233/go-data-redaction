package godataredaction

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- 测试基础设施 -----------------------------------------------------------

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestManager(t *testing.T, opts ...Option) (*Manager, *fakeClock, string) {
	t.Helper()
	clock := newFakeClock()
	dir := t.TempDir()
	finalOpts := append([]Option{WithClock(clock), WithLeaseTTL(time.Minute), WithGCGrace(0)}, opts...)
	m, err := NewManager(dir, finalOpts...)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m, clock, dir
}

func reopenManager(t *testing.T, dir string, clock Clock) *Manager {
	t.Helper()
	m, err := NewManager(dir, WithClock(clock), WithLeaseTTL(time.Minute), WithGCGrace(0))
	if err != nil {
		t.Fatalf("reopen NewManager: %v", err)
	}
	return m
}

func sampleInput(ext string) CreateInput {
	return CreateInput{
		ExternalID:    ext,
		Version:       7,
		SourceVersion: "src-v1",
		RuleVersion:   "rule-v3",
		Shards: []ShardSpec{
			{ID: "s1", SourceRange: "0-100"},
			{ID: "s2", SourceRange: "100-200"},
			{ID: "s3", SourceRange: "200-300"},
		},
	}
}

func outputFor(shardID string) []byte {
	return []byte("redacted-output-of-" + shardID)
}

func mustPut(t *testing.T, m *Manager, data []byte) (string, string) {
	t.Helper()
	ref, digest, err := m.PutOutput(context.Background(), data)
	if err != nil {
		t.Fatalf("PutOutput: %v", err)
	}
	return ref, digest
}

func mustClaim(t *testing.T, m *Manager, taskID, worker string) *Claim {
	t.Helper()
	c, err := m.ClaimShard(context.Background(), taskID, worker)
	if err != nil {
		t.Fatalf("ClaimShard: %v", err)
	}
	return c
}

func successReceipt(c *Claim, digest string, count int64) Receipt {
	return Receipt{
		TaskID:      c.TaskID,
		ShardID:     c.ShardID,
		TaskVersion: c.TaskVersion,
		Token:       c.Token,
		WorkerID:    c.WorkerID,
		Success:     true,
		Digest:      digest,
		Ref:         digest,
		Count:       count,
	}
}

func mustSucceed(t *testing.T, m *Manager, c *Claim, data []byte) {
	t.Helper()
	_, digest := mustPut(t, m, data)
	if _, err := m.SubmitReceipt(context.Background(), successReceipt(c, digest, int64(len(data)))); err != nil {
		t.Fatalf("SubmitReceipt: %v", err)
	}
}

func completeAll(t *testing.T, m *Manager, taskID string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		c := mustClaim(t, m, taskID, fmt.Sprintf("worker-%d", i))
		mustSucceed(t, m, c, outputFor(c.ShardID))
	}
}

// ---- 创建：冻结、幂等与冲突 --------------------------------------------------

func TestCreateFreezesVersionsAndShards(t *testing.T) {
	m, _, _ := newTestManager(t)
	task, err := m.CreateTask(context.Background(), sampleInput("job-1"))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if task.Version != 7 || task.SourceVersion != "src-v1" || task.RuleVersion != "rule-v3" {
		t.Fatalf("versions not frozen: %+v", task)
	}
	if len(task.Shards) != 3 || task.Status != TaskActive || task.InputFingerprint == "" {
		t.Fatalf("unexpected initial task: %+v", task)
	}
}

func TestCreateIdempotentSameInput(t *testing.T) {
	m, _, _ := newTestManager(t)
	ctx := context.Background()
	first, err := m.CreateTask(ctx, sampleInput("job-1"))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	// 分片提交顺序不同不影响指纹。
	in := sampleInput("job-1")
	in.Shards[0], in.Shards[2] = in.Shards[2], in.Shards[0]
	again, err := m.CreateTask(ctx, in)
	if err != nil {
		t.Fatalf("duplicate create should be idempotent: %v", err)
	}
	if again.ID != first.ID {
		t.Fatalf("expected same task id, got %s vs %s", first.ID, again.ID)
	}
}

func TestCreateConflictOnChangedInput(t *testing.T) {
	m, _, _ := newTestManager(t)
	ctx := context.Background()
	if _, err := m.CreateTask(ctx, sampleInput("job-1")); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	cases := map[string]func(CreateInput) CreateInput{
		"rule version changed":   func(in CreateInput) CreateInput { in.RuleVersion = "rule-v4"; return in },
		"source version changed": func(in CreateInput) CreateInput { in.SourceVersion = "src-v2"; return in },
		"task version changed":   func(in CreateInput) CreateInput { in.Version = 8; return in },
		"shard range changed":    func(in CreateInput) CreateInput { in.Shards[0].SourceRange = "0-999"; return in },
		"shard set changed": func(in CreateInput) CreateInput {
			in.Shards = append(in.Shards, ShardSpec{ID: "s4", SourceRange: "300-400"})
			return in
		},
	}
	for name, mutate := range cases {
		if _, err := m.CreateTask(ctx, mutate(sampleInput("job-1"))); !errors.Is(err, ErrConflict) {
			t.Fatalf("%s: expected ErrConflict, got %v", name, err)
		}
	}
	// 不同外部任务号互不影响。
	if _, err := m.CreateTask(ctx, sampleInput("job-2")); err != nil {
		t.Fatalf("different external id should succeed: %v", err)
	}
}

func TestCreateInvalidInput(t *testing.T) {
	m, _, _ := newTestManager(t)
	ctx := context.Background()
	empty := sampleInput("")
	if _, err := m.CreateTask(ctx, empty); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty external id: %v", err)
	}
	noShards := sampleInput("x")
	noShards.Shards = nil
	if _, err := m.CreateTask(ctx, noShards); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("no shards: %v", err)
	}
	dup := sampleInput("x")
	dup.Shards = append(dup.Shards, ShardSpec{ID: "s1", SourceRange: "dup"})
	if _, err := m.CreateTask(ctx, dup); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("duplicate shard: %v", err)
	}
}

// ---- 租约：过期接管与迟到回执 ------------------------------------------------

func TestLeaseExpiryTakeoverRejectsStaleReceipt(t *testing.T) {
	m, clock, _ := newTestManager(t)
	ctx := context.Background()
	task, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-lease", Version: 1, SourceVersion: "s", RuleVersion: "r",
		Shards: []ShardSpec{{ID: "s1", SourceRange: "r1"}},
	})

	w1 := mustClaim(t, m, task.ID, "worker-1")
	// 租约未过期，没有可领取的分片。
	if _, err := m.ClaimShard(ctx, task.ID, "worker-2"); !errors.Is(err, ErrNoEligibleShard) {
		t.Fatalf("expected ErrNoEligibleShard before expiry, got %v", err)
	}
	clock.advance(time.Minute + time.Second)
	w2 := mustClaim(t, m, task.ID, "worker-2")
	if w2.Token != w1.Token+1 {
		t.Fatalf("takeover token should increment: %d -> %d", w1.Token, w2.Token)
	}
	if w2.ShardID != "s1" {
		t.Fatalf("takeover should claim same shard, got %s", w2.ShardID)
	}

	// 旧工作者迟到回执：不能覆盖新结果。
	_, oldDigest := mustPut(t, m, []byte("stale-worker-1-output"))
	_, err := m.SubmitReceipt(ctx, successReceipt(w1, oldDigest, 1))
	if !errors.Is(err, ErrStaleLease) {
		t.Fatalf("late receipt with old token: expected ErrStaleLease, got %v", err)
	}
	// 旧分片仍由 worker-2 有效持有，输出没有被污染。
	p, _ := m.GetProgress(ctx, task.ID)
	if p.Succeeded != 0 || p.Leased != 1 || p.ExpiredTakeovers != 1 {
		t.Fatalf("unexpected progress after stale receipt: %+v", p)
	}

	// 新工作者正常完成。
	mustSucceed(t, m, w2, outputFor("s1"))
	p, _ = m.GetProgress(ctx, task.ID)
	if p.Status != TaskPublished || p.Succeeded != 1 {
		t.Fatalf("expected published, got %+v", p)
	}
}

func TestReceiptVersionMismatch(t *testing.T) {
	m, _, _ := newTestManager(t)
	ctx := context.Background()
	task, _ := m.CreateTask(ctx, sampleInput("job-v"))
	c := mustClaim(t, m, task.ID, "w")
	_, digest := mustPut(t, m, outputFor(c.ShardID))
	r := successReceipt(c, digest, 1)
	r.TaskVersion = task.Version + 1
	if _, err := m.SubmitReceipt(ctx, r); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}
}

func TestDifferentDigestIsConflictNotRetry(t *testing.T) {
	m, clock, _ := newTestManager(t)
	ctx := context.Background()
	task, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-digest", Version: 1, SourceVersion: "s", RuleVersion: "r",
		Shards: []ShardSpec{{ID: "s1", SourceRange: "r1"}, {ID: "s2", SourceRange: "r2"}},
	})
	c := mustClaim(t, m, task.ID, "worker-1")
	mustSucceed(t, m, c, outputFor("s1"))
	got, _ := m.GetTask(ctx, task.ID)
	original := got.Shards[0].Result

	// 租约在成功后已释放；另一个工作者持过期/新 token 提交不同摘要，必须被当作冲突。
	clock.advance(time.Minute + time.Second)
	_, otherDigest := mustPut(t, m, []byte("totally-different-output"))
	r := Receipt{
		TaskID: task.ID, ShardID: "s1", TaskVersion: task.Version, Token: 999,
		WorkerID: "worker-late", Success: true, Digest: otherDigest, Ref: otherDigest, Count: 42,
	}
	if _, err := m.SubmitReceipt(ctx, r); !errors.Is(err, ErrDigestConflict) {
		t.Fatalf("expected ErrDigestConflict, got %v", err)
	}
	got, _ = m.GetTask(ctx, task.ID)
	if got.Shards[0].Status != ShardSucceeded || got.Shards[0].Result.Digest != original.Digest {
		t.Fatalf("succeeded shard must not be overwritten or reset: %+v", got.Shards[0])
	}
	// 相同摘要/引用/条数的重复回执是幂等成功，不是冲突。
	same := Receipt{
		TaskID: task.ID, ShardID: "s1", TaskVersion: task.Version, Token: 999,
		Success: true, Digest: original.Digest, Ref: original.Ref, Count: original.Count,
	}
	res, err := m.SubmitReceipt(ctx, same)
	if err != nil || res.Digest != original.Digest {
		t.Fatalf("idempotent re-submission: %v %+v", err, res)
	}
}

// ---- 发布原子性：处理中不可见、只发布一次 -------------------------------------

func TestResultsInvisibleUntilPublication(t *testing.T) {
	m, _, _ := newTestManager(t)
	ctx := context.Background()
	task, _ := m.CreateTask(ctx, sampleInput("job-visibility"))
	c1 := mustClaim(t, m, task.ID, "w")
	mustSucceed(t, m, c1, outputFor("s1"))

	// 处理期间：清单不可取、任何分片输出都不可下载，哪怕该分片已经完成。
	if _, err := m.GetManifest(ctx, task.ID); !errors.Is(err, ErrNotPublished) {
		t.Fatalf("GetManifest during processing: %v", err)
	}
	if _, _, err := m.DownloadResult(ctx, task.ID, "s1"); !errors.Is(err, ErrNotPublished) {
		t.Fatalf("DownloadResult during processing: %v", err)
	}
	p, _ := m.GetProgress(ctx, task.ID)
	if p.Status != TaskActive || p.ManifestID != "" || p.Succeeded != 1 {
		t.Fatalf("unexpected mid-progress: %+v", p)
	}

	completeAll(t, m, task.ID, 2) // s2, s3
	mf, err := m.GetManifest(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetManifest after completion: %v", err)
	}
	if len(mf.Entries) != 3 || mf.TotalCount != int64(len("redacted-output-of-s1")+len("redacted-output-of-s2")+len("redacted-output-of-s3")) {
		t.Fatalf("manifest mismatch: %+v", mf)
	}
	rc, entry, err := m.DownloadResult(ctx, task.ID, "s2")
	if err != nil {
		t.Fatalf("DownloadResult after publish: %v", err)
	}
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	if string(data) != "redacted-output-of-s2" || entry.Digest != mf.Entries[1].Digest {
		t.Fatalf("downloaded data/entry mismatch: %q %+v", data, entry)
	}
}

func TestPublishOnlyOnceUnderConcurrentCompletion(t *testing.T) {
	m, _, dir := newTestManager(t)
	ctx := context.Background()
	const n = 20
	specs := make([]ShardSpec, n)
	for i := range specs {
		specs[i] = ShardSpec{ID: fmt.Sprintf("s%02d", i), SourceRange: fmt.Sprintf("range-%d", i)}
	}
	task, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-concurrent", Version: 3, SourceVersion: "s", RuleVersion: "r", Shards: specs,
	})
	// 先顺序领取，再并发提交最后回执（发布竞争点）。
	claims := make([]*Claim, n)
	for i := 0; i < n; i++ {
		claims[i] = mustClaim(t, m, task.ID, fmt.Sprintf("worker-%02d", i))
	}
	digests := make([]string, n)
	for i, c := range claims {
		_, digests[i] = mustPut(t, m, outputFor(c.ShardID))
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, c := range claims {
		wg.Add(1)
		go func(i int, c *Claim) {
			defer wg.Done()
			<-start
			if _, err := m.SubmitReceipt(ctx, successReceipt(c, digests[i], int64(len(outputFor(c.ShardID))))); err != nil {
				t.Errorf("receipt: %v", err)
			}
		}(i, c)
	}
	close(start)
	wg.Wait()

	got, err := m.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Status != TaskPublished {
		t.Fatalf("expected published, got %s", got.Status)
	}
	mfs, err := os.ReadDir(filepath.Join(dir, "manifests"))
	if err != nil {
		t.Fatalf("read manifests dir: %v", err)
	}
	if len(mfs) != 1 {
		t.Fatalf("exactly one immutable manifest expected, got %d", len(mfs))
	}
	mf, _ := m.GetManifest(ctx, task.ID)
	if mf.ID != got.ManifestID || len(mf.Entries) != n {
		t.Fatalf("manifest mismatch: %+v", mf)
	}
	// 显式发布是幂等的，返回同一份不可变清单。
	again, err := m.PublishResult(ctx, task.ID)
	if err != nil || again.ID != mf.ID {
		t.Fatalf("republish: %v id=%s vs %s", err, again.ID, mf.ID)
	}
}

func TestPublishRejectedWhileIncompleteOrCanceled(t *testing.T) {
	m, _, _ := newTestManager(t)
	ctx := context.Background()
	task, _ := m.CreateTask(ctx, sampleInput("job-pub"))
	if _, err := m.PublishResult(ctx, task.ID); !errors.Is(err, ErrNotComplete) {
		t.Fatalf("premature publish: %v", err)
	}
	c := mustClaim(t, m, task.ID, "w")
	mustSucceed(t, m, c, outputFor(c.ShardID))
	if _, err := m.CancelTask(ctx, task.ID); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	if _, err := m.PublishResult(ctx, task.ID); !errors.Is(err, ErrTerminal) {
		t.Fatalf("publish after cancel: %v", err)
	}
}

func TestPrePublishDigestVerification(t *testing.T) {
	_, clock, dir := newTestManager(t)
	ctx := context.Background()
	m := reopenManager(t, dir, clock)
	task, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-verify", Version: 1, SourceVersion: "s", RuleVersion: "r",
		Shards: []ShardSpec{{ID: "s1", SourceRange: "r1"}},
	})
	c := mustClaim(t, m, task.ID, "w")
	_, digest := mustPut(t, m, outputFor("s1"))
	if _, err := m.SubmitReceipt(ctx, successReceipt(c, digest, 10)); err != nil {
		t.Fatalf("SubmitReceipt: %v", err)
	}
	mf, err := m.GetManifest(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	// 已发布。随后模拟磁盘上的输出被损坏（位腐烂/外部误改），
	// 并把任务状态文件回退为“全成功但尚未翻转发布、尚未记下清单”（崩溃窗口等价场景）。
	if err := os.WriteFile(filepath.Join(dir, "outputs", digest), []byte("tampered"), 0o644); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	taskFile := filepath.Join(dir, "tasks", task.ID+".json")
	raw, err := os.ReadFile(taskFile)
	if err != nil {
		t.Fatalf("read task file: %v", err)
	}
	s := string(raw)
	s = strings.ReplaceAll(s, `"Status": "published"`, `"Status": "active"`)
	s = strings.ReplaceAll(s, `"ManifestID": "`+mf.ID+`"`, `"ManifestID": ""`)
	if err := os.WriteFile(taskFile, []byte(s), 0o644); err != nil {
		t.Fatalf("rewrite task file: %v", err)
	}
	// 重启时恢复逻辑必须重新执行发布前摘要校验并拒绝发布损坏输出。
	if _, err := NewManager(dir, WithClock(clock)); !errors.Is(err, ErrOutputMismatch) {
		t.Fatalf("expected ErrOutputMismatch on recovery publish, got %v", err)
	}
}

func TestReceiptWithMissingOrBadOutputFailsShard(t *testing.T) {
	m, _, _ := newTestManager(t)
	ctx := context.Background()
	task, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-badout", Version: 1, SourceVersion: "s", RuleVersion: "r",
		Shards: []ShardSpec{{ID: "s1", SourceRange: "r1"}},
	})
	c := mustClaim(t, m, task.ID, "w")
	bogusDigest := strings.Repeat("a", 64)
	r := successReceipt(c, bogusDigest, 5)
	if _, err := m.SubmitReceipt(ctx, r); !errors.Is(err, ErrOutputMismatch) {
		t.Fatalf("missing output: expected ErrOutputMismatch, got %v", err)
	}
	got, _ := m.GetTask(ctx, task.ID)
	if got.Shards[0].Status != ShardFailed {
		t.Fatalf("shard should be failed after unverifiable receipt, got %s", got.Shards[0].Status)
	}
}

// ---- 失败、重试与取消 ---------------------------------------------------------

func TestFailureRetryAtSameVersion(t *testing.T) {
	m, _, _ := newTestManager(t)
	ctx := context.Background()
	task, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-retry", Version: 9, SourceVersion: "s", RuleVersion: "r",
		Shards: []ShardSpec{{ID: "s1", SourceRange: "r1"}, {ID: "s2", SourceRange: "r2"}},
	})
	c1 := mustClaim(t, m, task.ID, "w")
	if _, err := m.SubmitReceipt(ctx, Receipt{
		TaskID: task.ID, ShardID: c1.ShardID, TaskVersion: 9, Token: c1.Token,
		WorkerID: "w", Success: false, Error: "boom",
	}); err != nil {
		t.Fatalf("failure receipt: %v", err)
	}
	p, _ := m.GetProgress(ctx, task.ID)
	if p.Failed != 1 || p.Leased != 0 {
		t.Fatalf("progress after failure: %+v", p)
	}
	// 失败分片不被自动领取：另一个 Claim 只能拿到 s2。
	c2 := mustClaim(t, m, task.ID, "w")
	if c2.ShardID != "s2" {
		t.Fatalf("failed shard must not be auto-claimed, got %s", c2.ShardID)
	}
	// 错误版本重试被拒。
	if err := m.RetryShard(ctx, task.ID, "s1", 10); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("retry wrong version: %v", err)
	}
	if err := m.RetryShard(ctx, task.ID, "s1", 9); err != nil {
		t.Fatalf("RetryShard: %v", err)
	}
	got, _ := m.GetTask(ctx, task.ID)
	if got.Shards[0].Status != ShardPending {
		t.Fatalf("expected pending after retry, got %s", got.Shards[0].Status)
	}
	if err := m.RetryShard(ctx, task.ID, "s2", 9); !errors.Is(err, ErrShardNotFailed) {
		t.Fatalf("retry non-failed: %v", err)
	}
	// 重试后重新领取并完成；s2 先完成也不会提前发布。
	c1b := mustClaim(t, m, task.ID, "w-retry")
	mustSucceed(t, m, c1b, outputFor("s1"))
	mustSucceed(t, m, c2, outputFor("s2"))
	got, _ = m.GetTask(ctx, task.ID)
	if got.Status != TaskPublished {
		t.Fatalf("expected published after retry success, got %s", got.Status)
	}
}

func TestCancelStopsClaimsAndRacesLeaveOneTerminal(t *testing.T) {
	m, _, _ := newTestManager(t)
	ctx := context.Background()
	task, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-cancel", Version: 1, SourceVersion: "s", RuleVersion: "r",
		Shards: []ShardSpec{{ID: "s1", SourceRange: "r1"}},
	})
	c := mustClaim(t, m, task.ID, "w")
	canceled, err := m.CancelTask(ctx, task.ID)
	if err != nil || canceled.Status != TaskCanceled || canceled.CanceledAt == nil {
		t.Fatalf("cancel: %v %+v", err, canceled)
	}
	// 取消幂等。
	again, err := m.CancelTask(ctx, task.ID)
	if err != nil || again.Status != TaskCanceled {
		t.Fatalf("idempotent cancel: %v", err)
	}
	// 停止新领取；在途回执进入终态校验被拒；不能再发布。
	if _, err := m.ClaimShard(ctx, task.ID, "w2"); !errors.Is(err, ErrTerminal) {
		t.Fatalf("claim after cancel: %v", err)
	}
	_, digest := mustPut(t, m, outputFor("s1"))
	if _, err := m.SubmitReceipt(ctx, successReceipt(c, digest, 1)); !errors.Is(err, ErrTerminal) {
		t.Fatalf("receipt after cancel: %v", err)
	}
	if _, err := m.PublishResult(ctx, task.ID); !errors.Is(err, ErrTerminal) {
		t.Fatalf("publish after cancel: %v", err)
	}
	if _, _, err := m.DownloadResult(ctx, task.ID, "s1"); !errors.Is(err, ErrNotPublished) {
		t.Fatalf("canceled task never published: %v", err)
	}
}

func TestPublishAndCancelMutuallyExclusive(t *testing.T) {
	m, _, _ := newTestManager(t)
	ctx := context.Background()
	task, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-race", Version: 1, SourceVersion: "s", RuleVersion: "r",
		Shards: []ShardSpec{{ID: "s1", SourceRange: "r1"}},
	})
	c := mustClaim(t, m, task.ID, "w")

	// 顺序一：回执（自动发布）先到，取消必败。
	mustSucceed(t, m, c, outputFor("s1"))
	if _, err := m.CancelTask(ctx, task.ID); !errors.Is(err, ErrTerminal) {
		t.Fatalf("cancel after publish: %v", err)
	}
	got, _ := m.GetTask(ctx, task.ID)
	if got.Status != TaskPublished {
		t.Fatalf("final state should be published, got %s", got.Status)
	}
}

func TestFinalReceiptRacingCancelLeavesExactlyOneTerminal(t *testing.T) {
	m, _, _ := newTestManager(t)
	ctx := context.Background()

	// 大量并发“最终成功回执（触发发布）”与“取消”，无论调度如何只能留下一个终态。
	const iterations = 100
	published, canceled := 0, 0
	for i := 0; i < iterations; i++ {
		in := CreateInput{
			ExternalID: fmt.Sprintf("job-final-race-%d", i), Version: 1, SourceVersion: "s", RuleVersion: "r",
			Shards: []ShardSpec{{ID: "s1", SourceRange: "r1"}},
		}
		tk, _ := m.CreateTask(ctx, in)
		cl := mustClaim(t, m, tk.ID, "w")
		_, dg := mustPut(t, m, outputFor("s1"))
		rc := successReceipt(cl, dg, 9)

		var wg sync.WaitGroup
		var mu sync.Mutex
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := m.SubmitReceipt(ctx, rc); err == nil {
				mu.Lock()
				published++
				mu.Unlock()
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := m.CancelTask(ctx, tk.ID); err == nil {
				mu.Lock()
				canceled++
				mu.Unlock()
			}
		}()
		wg.Wait()

		got, err := m.GetTask(ctx, tk.ID)
		if err != nil {
			t.Fatalf("get task: %v", err)
		}
		if got.Status != TaskPublished && got.Status != TaskCanceled {
			t.Fatalf("iteration %d: non-terminal status %s", i, got.Status)
		}
		// 取消后再收到同一成功回执必须被拒绝（不能把终态翻成发布）。
		if got.Status == TaskCanceled {
			if _, err := m.SubmitReceipt(ctx, rc); !errors.Is(err, ErrTerminal) {
				t.Fatalf("receipt after canceled: %v", err)
			}
		}
		// 发布后再取消必须被拒绝。
		if got.Status == TaskPublished {
			if _, err := m.CancelTask(ctx, tk.ID); !errors.Is(err, ErrTerminal) {
				t.Fatalf("cancel after published: %v", err)
			}
		}
	}
	if published+canceled != iterations {
		t.Fatalf("expected %d terminal wins, got published=%d canceled=%d", iterations, published, canceled)
	}
}

func TestRestartClearsOrphanManifestID(t *testing.T) {
	m, clock, dir := newTestManager(t)
	ctx := context.Background()
	task, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-orphan", Version: 1, SourceVersion: "s", RuleVersion: "r",
		Shards: []ShardSpec{{ID: "s1", SourceRange: "r1"}},
	})
	c := mustClaim(t, m, task.ID, "w")
	mustSucceed(t, m, c, outputFor("s1"))
	// 已发布。模拟“状态翻转前清单文件丢失”的崩溃窗口：状态回退 active 且 ManifestID 悬挂。
	taskFile := filepath.Join(dir, "tasks", task.ID+".json")
	raw, err := os.ReadFile(taskFile)
	if err != nil {
		t.Fatalf("read task: %v", err)
	}
	s := strings.ReplaceAll(string(raw), `"Status": "published"`, `"Status": "active"`)
	if err := os.WriteFile(taskFile, []byte(s), 0o644); err != nil {
		t.Fatalf("rewrite task: %v", err)
	}
	m2 := reopenManager(t, dir, clock)
	got, err := m2.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got.Status != TaskPublished {
		t.Fatalf("orphan manifest id should be retried and republished, got %s", got.Status)
	}
	mf, err := m2.GetManifest(ctx, task.ID)
	if err != nil || len(mf.Entries) != 1 {
		t.Fatalf("manifest after orphan recovery: %v %+v", err, mf)
	}
}

func TestRetryFailedShardsBatch(t *testing.T) {
	m, _, _ := newTestManager(t)
	ctx := context.Background()
	task, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-batch", Version: 2, SourceVersion: "s", RuleVersion: "r",
		Shards: []ShardSpec{{ID: "s1", SourceRange: "r1"}, {ID: "s2", SourceRange: "r2"}},
	})
	c1 := mustClaim(t, m, task.ID, "w")
	if _, err := m.SubmitReceipt(ctx, Receipt{
		TaskID: task.ID, ShardID: "s1", TaskVersion: 2, Token: c1.Token, Success: false, Error: "x",
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}
	retried, err := m.RetryFailedShards(ctx, task.ID, 2)
	if err != nil || len(retried) != 1 || retried[0] != "s1" {
		t.Fatalf("batch retry: %v %v", err, retried)
	}
	if _, err := m.RetryFailedShards(ctx, task.ID, 3); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("batch retry wrong version: %v", err)
	}
	completeAll(t, m, task.ID, 2)
	got, _ := m.GetTask(ctx, task.ID)
	if got.Status != TaskPublished {
		t.Fatalf("expected published, got %s", got.Status)
	}
}

// ---- GC：只清理未被成功清单引用的输出 -----------------------------------------

func TestGarbageCollectUnreferencedOutputs(t *testing.T) {
	m, _, _ := newTestManager(t)
	ctx := context.Background()

	// 任务 A 完整发布：其输出受清单永久保护。
	taskA, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-a", Version: 1, SourceVersion: "s", RuleVersion: "r",
		Shards: []ShardSpec{{ID: "a1", SourceRange: "ra"}},
	})
	cA := mustClaim(t, m, taskA.ID, "w")
	dataA := outputFor("a1")
	_, digestA := mustPut(t, m, dataA)
	mustSucceed(t, m, cA, dataA)

	// 任务 B 取消：其成功分片输出不再受保护。
	taskB, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-b", Version: 1, SourceVersion: "s", RuleVersion: "r",
		Shards: []ShardSpec{{ID: "b1", SourceRange: "rb"}, {ID: "b2", SourceRange: "rc"}},
	})
	cB := mustClaim(t, m, taskB.ID, "w")
	_, digestB := mustPut(t, m, outputFor("b1"))
	if _, err := m.SubmitReceipt(ctx, successReceipt(cB, digestB, 3)); err != nil {
		t.Fatalf("B receipt: %v", err)
	}
	if _, err := m.CancelTask(ctx, taskB.ID); err != nil {
		t.Fatalf("cancel B: %v", err)
	}

	// 游离输出：没有任何任务/清单引用。
	_, orphan := mustPut(t, m, []byte("nobody-references-me"))

	// 活动任务 C 的在途成功输出：即便尚无清单也必须受保护。
	taskC, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-c", Version: 1, SourceVersion: "s", RuleVersion: "r",
		Shards: []ShardSpec{{ID: "c1", SourceRange: "rd"}, {ID: "c2", SourceRange: "re"}},
	})
	cC := mustClaim(t, m, taskC.ID, "w")
	_, digestC := mustPut(t, m, outputFor("c1"))
	if _, err := m.SubmitReceipt(ctx, successReceipt(cC, digestC, 4)); err != nil {
		t.Fatalf("C receipt: %v", err)
	}

	removed, err := m.GarbageCollect(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if removed != 2 {
		t.Fatalf("expected 2 unreferenced outputs removed (canceled + orphan), got %d", removed)
	}
	if !m.outputs.exists(digestA) {
		t.Fatal("published manifest output was deleted")
	}
	if m.outputs.exists(digestB) {
		t.Fatal("canceled task output should have been collected")
	}
	if m.outputs.exists(orphan) {
		t.Fatal("orphan output should have been collected")
	}
	if !m.outputs.exists(digestC) {
		t.Fatal("in-flight succeeded output of active task must be preserved")
	}
	// GC 幂等。
	n, err := m.GarbageCollect(ctx)
	if err != nil || n != 0 {
		t.Fatalf("second GC: n=%d err=%v", n, err)
	}
}

func TestGCGracePeriodProtectsFreshOutputs(t *testing.T) {
	m, clock, _ := newTestManager(t, WithGCGrace(time.Hour))
	ctx := context.Background()
	_, orphan := mustPut(t, m, []byte("fresh-orphan"))
	removed, err := m.GarbageCollect(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if removed != 0 || !m.outputs.exists(orphan) {
		t.Fatalf("fresh output must be kept by grace period: removed=%d exists=%v", removed, m.outputs.exists(orphan))
	}
	clock.advance(time.Hour + time.Second)
	removed, _ = m.GarbageCollect(ctx)
	if removed != 1 || m.outputs.exists(orphan) {
		t.Fatalf("orphan should be collected after grace: removed=%d", removed)
	}
}

// ---- 持久化与崩溃恢复 ---------------------------------------------------------

func TestRestartRecoversLeaseStateAndPublishes(t *testing.T) {
	_, clock, dir := newTestManager(t)
	ctx := context.Background()
	m := reopenManager(t, dir, clock)

	task, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-restart", Version: 1, SourceVersion: "s", RuleVersion: "r",
		Shards: []ShardSpec{{ID: "s1", SourceRange: "r1"}},
	})
	w1 := mustClaim(t, m, task.ID, "worker-1")

	// 重启：租约与 fencing token 序列必须恢复。
	m2 := reopenManager(t, dir, clock)
	clock.advance(time.Minute + time.Second)
	w2 := mustClaim(t, m2, task.ID, "worker-2")
	if w2.Token != w1.Token+1 {
		t.Fatalf("token sequence not recovered: %d -> %d", w1.Token, w2.Token)
	}
	_, stale := mustPut(t, m2, []byte("stale"))
	if _, err := m2.SubmitReceipt(ctx, successReceipt(w1, stale, 1)); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale receipt after restart: %v", err)
	}
	mustSucceed(t, m2, w2, outputFor("s1"))
	got, _ := m2.GetTask(ctx, task.ID)
	if got.Status != TaskPublished {
		t.Fatalf("expected published after recovery completion, got %s", got.Status)
	}

	// 再次重启：发布态、不可变清单与下载能力全部保留。
	m3 := reopenManager(t, dir, clock)
	mf, err := m3.GetManifest(ctx, task.ID)
	if err != nil {
		t.Fatalf("manifest after restart: %v", err)
	}
	if len(mf.Entries) != 1 || mf.TaskVersion != 1 || mf.SourceVersion != "s" || mf.RuleVersion != "r" {
		t.Fatalf("manifest content lost: %+v", mf)
	}
	rc, _, err := m3.DownloadResult(ctx, task.ID, "s1")
	if err != nil {
		t.Fatalf("download after restart: %v", err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != string(outputFor("s1")) {
		t.Fatalf("output content changed across restart: %q", data)
	}
}

func TestRestartReconcilesManifestWrittenBeforeStatusFlip(t *testing.T) {
	_, clock, dir := newTestManager(t)
	ctx := context.Background()
	m := reopenManager(t, dir, clock)
	task, _ := m.CreateTask(ctx, CreateInput{
		ExternalID: "job-crash", Version: 1, SourceVersion: "s", RuleVersion: "r",
		Shards: []ShardSpec{{ID: "s1", SourceRange: "r1"}},
	})
	c := mustClaim(t, m, task.ID, "w")
	mustSucceed(t, m, c, outputFor("s1"))
	if _, err := m.GetManifest(ctx, task.ID); err != nil {
		t.Fatalf("should be published: %v", err)
	}
	// 模拟崩溃窗口：清单文件已存在，ManifestID 已记下，但任务状态停在 active。
	taskFile := filepath.Join(dir, "tasks", task.ID+".json")
	raw, err := os.ReadFile(taskFile)
	if err != nil {
		t.Fatalf("read task file: %v", err)
	}
	s := strings.ReplaceAll(string(raw), `"Status": "published"`, `"Status": "active"`)
	if err := os.WriteFile(taskFile, []byte(s), 0o644); err != nil {
		t.Fatalf("rewrite task file: %v", err)
	}

	m2 := reopenManager(t, dir, clock)
	got, err := m2.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask after reconciliation: %v", err)
	}
	if got.Status != TaskPublished || got.ManifestID == "" {
		t.Fatalf("reconciliation should finalize publication: %+v", got)
	}
}

// ---- 进度查询 ----------------------------------------------------------------

func TestProgressCounters(t *testing.T) {
	m, _, _ := newTestManager(t)
	ctx := context.Background()
	task, _ := m.CreateTask(ctx, sampleInput("job-progress"))
	p0, _ := m.GetProgress(ctx, task.ID)
	if p0.Total != 3 || p0.Pending != 3 || p0.Status != TaskActive {
		t.Fatalf("initial progress: %+v", p0)
	}
	c := mustClaim(t, m, task.ID, "w")
	p1, _ := m.GetProgress(ctx, task.ID)
	if p1.Pending != 2 || p1.Leased != 1 {
		t.Fatalf("claimed progress: %+v", p1)
	}
	mustSucceed(t, m, c, outputFor(c.ShardID))
	p2, _ := m.GetProgress(ctx, task.ID)
	if p2.Succeeded != 1 || p2.ProcessedCount != int64(len(outputFor("s1"))) {
		t.Fatalf("succeeded progress: %+v", p2)
	}
	if _, err := m.GetProgress(ctx, "does-not-exist"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing task: %v", err)
	}
}
