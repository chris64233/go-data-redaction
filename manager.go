// Package-level documentation lives in doc.go.
package godataredaction

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

// 默认租约期限与 GC 宽限期，可用 Option 覆盖。
const (
	defaultLeaseTTL = 5 * time.Minute
	defaultGCGrace  = 10 * time.Minute
)

// Clock 抽象时间来源，生产用墙钟，测试可注入可控时钟。
type Clock interface {
	Now() time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// Manager 是任务编排器。所有公开方法均可被多 goroutine 并发调用，
// 内部以单一互斥锁串行化状态变更，并在每次变更后整体原子持久化。
type Manager struct {
	mu       sync.Mutex
	store    stateStore
	outputs  *outputStore
	clock    Clock
	leaseTTL time.Duration
	gcGrace  time.Duration

	tasks      map[string]*taskRecord
	byExternal map[string]string // ExternalID -> TaskID
	manifests  map[string]*Manifest
}

type taskRecord struct {
	task     *Task
	tokenSeq map[string]int64 // shardID -> 当前已发放的最大租约 token
}

// Option 配置 Manager。
type Option func(*Manager)

// WithClock 注入自定义时钟（测试用）。
func WithClock(c Clock) Option {
	return func(m *Manager) { m.clock = c }
}

// WithLeaseTTL 设置分片租约期限。
func WithLeaseTTL(d time.Duration) Option {
	return func(m *Manager) { m.leaseTTL = d }
}

// WithGCGrace 设置未引用输出的 GC 宽限期：落盘早于 now-grace 且未被
// 任何成功清单（或活动任务的成功分片）引用的输出才会被清理。
func WithGCGrace(d time.Duration) Option {
	return func(m *Manager) { m.gcGrace = d }
}

// NewManager 打开（或初始化）root 目录下的管理器，并从磁盘恢复任务状态：
// 若清单已写而任务状态未翻转（崩溃于发布中途），会自动完成发布对账。
func NewManager(root string, opts ...Option) (*Manager, error) {
	m := &Manager{
		clock:      wallClock{},
		leaseTTL:   defaultLeaseTTL,
		gcGrace:    defaultGCGrace,
		tasks:      map[string]*taskRecord{},
		byExternal: map[string]string{},
		manifests:  map[string]*Manifest{},
	}
	for _, o := range opts {
		o(m)
	}
	m.outputs = newOutputStore(root, m.clock)
	if m.leaseTTL <= 0 {
		return nil, fmt.Errorf("%w: lease ttl must be positive", ErrInvalidArgument)
	}
	if m.gcGrace < 0 {
		return nil, fmt.Errorf("%w: gc grace must be non-negative", ErrInvalidArgument)
	}
	store := newFileStore(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	tasks, err := store.loadTasks()
	if err != nil {
		return nil, err
	}
	manifests, err := store.loadManifests()
	if err != nil {
		return nil, err
	}
	m.store = store
	for _, mf := range manifests {
		m.manifests[mf.ID] = mf
	}
	for _, t := range tasks {
		rec := &taskRecord{task: t, tokenSeq: map[string]int64{}}
		for i := range t.Shards {
			if t.Shards[i].Lease != nil {
				rec.tokenSeq[t.Shards[i].Spec.ID] = t.Shards[i].Lease.Token
			}
		}
		m.tasks[t.ID] = rec
		m.byExternal[t.ExternalID] = t.ID
		// 发布对账：
		// 1) 清单文件已生成但任务状态未及翻转 —— 补翻转为 published；
		// 2) 最后一个成功回执已持久化但发布未完成 —— 重新执行一次性发布。
		switch t.Status {
		case TaskPublished:
			if _, ok := m.manifests[t.ManifestID]; !ok {
				return nil, fmt.Errorf("corrupt state: published task %s missing manifest %s", t.ID, t.ManifestID)
			}
		case TaskActive:
			if t.ManifestID != "" {
				if _, ok := m.manifests[t.ManifestID]; ok {
					t.Status = TaskPublished
					if err := store.saveTask(t); err != nil {
						return nil, err
					}
				} else {
					t.ManifestID = ""
					if err := store.saveTask(t); err != nil {
						return nil, err
					}
				}
			}
			if t.Status == TaskActive && allSucceeded(t) {
				if _, err := m.publishLocked(rec); err != nil {
					return nil, err
				}
			}
		}
	}
	return m, nil
}

// CreateTask 创建（或幂等取回）一个脱敏任务。创建时冻结全部输入维度。
func (m *Manager) CreateTask(ctx context.Context, in CreateInput) (*Task, error) {
	if err := validateCreateInput(in); err != nil {
		return nil, err
	}
	fp, err := inputFingerprint(in)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if existingID, ok := m.byExternal[in.ExternalID]; ok {
		existing := m.tasks[existingID].task
		if existing.InputFingerprint == fp {
			return cloneTask(existing), nil // 相同外部任务号 + 相同输入：返回原任务
		}
		return nil, fmt.Errorf("%w: external id %q already used by task %s", ErrConflict, in.ExternalID, existingID)
	}

	now := m.clock.Now()
	id := deriveTaskID(in.ExternalID, fp)
	t := &Task{
		ID:               id,
		ExternalID:       in.ExternalID,
		Version:          in.Version,
		SourceVersion:    in.SourceVersion,
		RuleVersion:      in.RuleVersion,
		Status:           TaskActive,
		InputFingerprint: fp,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	t.Shards = make([]Shard, 0, len(in.Shards))
	for _, spec := range in.Shards {
		t.Shards = append(t.Shards, Shard{Spec: spec, Status: ShardPending})
	}
	rec := &taskRecord{task: t, tokenSeq: map[string]int64{}}
	if err := m.store.saveTask(t); err != nil {
		return nil, err
	}
	m.tasks[t.ID] = rec
	m.byExternal[t.ExternalID] = t.ID
	return cloneTask(t), nil
}

// ClaimShard 领取一个可处理分片：优先待处理分片，其次租约已过期的分片。
// 已失败的分片不会被自动重新领取，必须显式按原版本 RetryShard。
func (m *Manager) ClaimShard(ctx context.Context, taskID, workerID string) (*Claim, error) {
	if workerID == "" {
		return nil, fmt.Errorf("%w: worker id is required", ErrInvalidArgument)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, err := m.requireActiveTask(taskID)
	if err != nil {
		return nil, err
	}
	t := rec.task
	now := m.clock.Now()

	idx := -1
	for i := range t.Shards {
		if t.Shards[i].Status == ShardPending {
			idx = i
			break
		}
	}
	if idx == -1 {
		earliest := time.Time{}
		for i := range t.Shards {
			s := &t.Shards[i]
			if s.Status != ShardLeased || s.Lease == nil || now.Before(s.Lease.ExpiresAt) {
				continue
			}
			if earliest.IsZero() || s.Lease.ExpiresAt.Before(earliest) {
				earliest, idx = s.Lease.ExpiresAt, i
			}
		}
	}
	if idx == -1 {
		return nil, ErrNoEligibleShard
	}

	shard := &t.Shards[idx]
	rec.tokenSeq[shard.Spec.ID]++
	token := rec.tokenSeq[shard.Spec.ID]
	if shard.Status == ShardLeased {
		// 过期接管：旧 token 即刻失效。
		shard.ExpiredTakeovers++
	}
	shard.Attempts++
	shard.Status = ShardLeased
	shard.Lease = &Lease{
		WorkerID:  workerID,
		Token:     token,
		ExpiresAt: now.Add(m.leaseTTL),
		ClaimedAt: now,
	}
	shard.LastError = ""
	t.UpdatedAt = now
	if err := m.store.saveTask(t); err != nil {
		return nil, err
	}
	return &Claim{
		TaskID:         t.ID,
		TaskVersion:    t.Version,
		ShardID:        shard.Spec.ID,
		Token:          token,
		WorkerID:       workerID,
		LeaseExpiresAt: shard.Lease.ExpiresAt,
	}, nil
}

// SubmitReceipt 提交分片处理回执。
//
// 成功回执必须携带当前租约 token、冻结任务版本，以及 PutOutput 得到的
// 输出引用与摘要；引用的输出必须存在。返回冻结的分片结果。
// 失败回执（Success=false）把分片标记为 failed，可随后按原版本重试。
// 对“同一分片已成功但摘要不同”的回执返回 ErrDigestConflict 且不改动状态。
func (m *Manager) SubmitReceipt(ctx context.Context, r Receipt) (*ShardResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.tasks[r.TaskID]
	if !ok {
		return nil, fmt.Errorf("%w: task %s", ErrNotFound, r.TaskID)
	}
	t := rec.task
	shard := findShard(t, r.ShardID)
	if shard == nil {
		return nil, fmt.Errorf("%w: shard %s", ErrNotFound, r.ShardID)
	}
	now := m.clock.Now()

	// 终态任务的幂等/拒绝处理。
	if t.Status == TaskPublished {
		// 发布后的迟到成功回执：与清单一致则幂等返回，其余一律拒绝，终态不可变。
		if r.Success && r.TaskVersion == t.Version && shard.Result != nil &&
			r.Digest == shard.Result.Digest && r.Ref == shard.Result.Ref && r.Count == shard.Result.Count {
			res := *shard.Result
			return &res, nil
		}
		return nil, fmt.Errorf("%w: task %s is published", ErrTerminal, t.ID)
	}
	if t.Status == TaskCanceled {
		return nil, fmt.Errorf("%w: task %s is canceled", ErrTerminal, t.ID)
	}

	if r.TaskVersion != t.Version {
		return nil, fmt.Errorf("%w: receipt version %d, frozen version %d", ErrVersionMismatch, r.TaskVersion, t.Version)
	}
	// 已成功分片（任务尚未发布）：先于 fencing 判定——
	// 相同内容是幂等重投；不同摘要不是普通重试，既不覆盖既有结果也不重置分片。
	if shard.Status == ShardSucceeded && shard.Result != nil {
		if r.Success && r.Digest == shard.Result.Digest && r.Ref == shard.Result.Ref && r.Count == shard.Result.Count {
			res := *shard.Result
			return &res, nil
		}
		return nil, fmt.Errorf("%w: shard %s succeeded with digest %s, got %q",
			ErrDigestConflict, r.ShardID, shard.Result.Digest, r.Digest)
	}
	// Fencing：只接受当前租约持有人的回执。
	if shard.Lease == nil || r.Token != shard.Lease.Token {
		return nil, fmt.Errorf("%w: shard %s", ErrStaleLease, r.ShardID)
	}

	if !r.Success {
		shard.Status = ShardFailed
		shard.Failures++
		shard.LastError = r.Error
		shard.Lease = nil
		t.UpdatedAt = now
		if err := m.store.saveTask(t); err != nil {
			return nil, err
		}
		return nil, nil
	}

	if r.Count < 0 {
		return nil, fmt.Errorf("%w: count must be non-negative", ErrInvalidArgument)
	}
	if !isHexDigest(r.Digest) || r.Ref != r.Digest {
		return m.rejectOutputReceipt(rec, shard, r, "digest/ref is not a content-addressed output reference", now)
	}
	if !m.outputs.exists(r.Digest) {
		return m.rejectOutputReceipt(rec, shard, r, "referenced output does not exist in output store", now)
	}

	res := &ShardResult{
		Digest:   r.Digest,
		Ref:      r.Ref,
		Count:    r.Count,
		WorkerID: shard.Lease.WorkerID,
		At:       now,
	}
	shard.Status = ShardSucceeded
	shard.Result = res
	shard.Lease = nil
	shard.LastError = ""
	t.UpdatedAt = now

	// 先持久化成功回执，再尝试发布；发布本身也先写不可变清单后翻转状态。
	if err := m.store.saveTask(t); err != nil {
		return nil, err
	}
	if _, err := m.publishIfCompleteLocked(rec); err != nil {
		return nil, err
	}
	out := *res
	return &out, nil
}

// rejectOutputReceipt 处理“回执声明成功但其输出无法通过校验”的情况：
// 输出引用无效意味着该分片不可能参与发布，按失败落账，允许按原版本重试。
func (m *Manager) rejectOutputReceipt(rec *taskRecord, shard *Shard, r Receipt, reason string, now time.Time) (*ShardResult, error) {
	shard.Status = ShardFailed
	shard.Failures++
	shard.Lease = nil
	shard.LastError = reason + ": " + r.Digest
	rec.task.UpdatedAt = now
	if err := m.store.saveTask(rec.task); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("%w: shard %s: %s", ErrOutputMismatch, r.ShardID, reason)
}

// RetryShard 按原任务版本重试一个失败分片，将其重新置为待处理。
func (m *Manager) RetryShard(ctx context.Context, taskID, shardID string, version int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, err := m.requireActiveTask(taskID)
	if err != nil {
		return err
	}
	t := rec.task
	if version != t.Version {
		return fmt.Errorf("%w: retry version %d, frozen version %d", ErrVersionMismatch, version, t.Version)
	}
	shard := findShard(t, shardID)
	if shard == nil {
		return fmt.Errorf("%w: shard %s", ErrNotFound, shardID)
	}
	if shard.Status != ShardFailed {
		return fmt.Errorf("%w: shard %s status %s", ErrShardNotFailed, shardID, shard.Status)
	}
	shard.Status = ShardPending
	shard.Lease = nil
	shard.Result = nil
	shard.LastError = ""
	t.UpdatedAt = m.clock.Now()
	return m.store.saveTask(t)
}

// RetryFailedShards 按原版本批量重试任务内全部失败分片。
func (m *Manager) RetryFailedShards(ctx context.Context, taskID string, version int64) (retried []string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, e := m.requireActiveTask(taskID)
	if e != nil {
		return nil, e
	}
	t := rec.task
	if version != t.Version {
		return nil, fmt.Errorf("%w: retry version %d, frozen version %d", ErrVersionMismatch, version, t.Version)
	}
	now := m.clock.Now()
	changed := false
	for i := range t.Shards {
		s := &t.Shards[i]
		if s.Status == ShardFailed {
			s.Status = ShardPending
			s.Lease = nil
			s.Result = nil
			s.LastError = ""
			retried = append(retried, s.Spec.ID)
			changed = true
		}
	}
	if changed {
		t.UpdatedAt = now
		if err := m.store.saveTask(t); err != nil {
			return nil, err
		}
	}
	return retried, nil
}

// CancelTask 取消任务。取消后停止一切新领取与接管，在途回执进入终态校验而被拒绝。
// 已发布任务不可取消；重复取消幂等返回。
func (m *Manager) CancelTask(ctx context.Context, taskID string) (*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.tasks[taskID]
	if !ok {
		return nil, fmt.Errorf("%w: task %s", ErrNotFound, taskID)
	}
	t := rec.task
	switch t.Status {
	case TaskCanceled:
		return cloneTask(t), nil
	case TaskPublished:
		return nil, fmt.Errorf("%w: task %s already published", ErrTerminal, taskID)
	}
	now := m.clock.Now()
	t.Status = TaskCanceled
	t.CanceledAt = &now
	t.UpdatedAt = now
	if err := m.store.saveTask(t); err != nil {
		return nil, err
	}
	return cloneTask(t), nil
}

// PublishResult 显式发布结果。任务已发布时幂等返回既有不可变清单；
// 仍有未成功分片时返回 ErrNotComplete；任一输出摘要复核失败返回 ErrOutputMismatch。
// 正常处理路径下，最后一个成功回执也会自动触发同一发布逻辑。
func (m *Manager) PublishResult(ctx context.Context, taskID string) (*Manifest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.tasks[taskID]
	if !ok {
		return nil, fmt.Errorf("%w: task %s", ErrNotFound, taskID)
	}
	t := rec.task
	if t.Status == TaskPublished {
		return cloneManifest(m.manifests[t.ManifestID]), nil
	}
	if t.Status == TaskCanceled {
		return nil, fmt.Errorf("%w: task %s is canceled", ErrTerminal, taskID)
	}
	return m.publishLocked(rec)
}

// publishIfCompleteLocked 在全部成功时发布，否则不做任何事。
func (m *Manager) publishIfCompleteLocked(rec *taskRecord) (*Manifest, error) {
	if !allSucceeded(rec.task) {
		return nil, nil
	}
	return m.publishLocked(rec)
}

// publishLocked 执行唯一的发布动作：
//  1. 收集成功分片并按 ShardID 排序；
//  2. 逐个复核输出仍存在且内容 sha256 与回执摘要一致（摘要校验）；
//  3. 计算内容寻址清单 ID（不含时间戳，崩溃重放得到同一 ID）；
//  4. 先写不可变清单文件，再把任务原子翻转为 published。
//
// 调用方必须持有 m.mu，因此并发完成最后分片时只有一个 goroutine 进入发布。
func (m *Manager) publishLocked(rec *taskRecord) (*Manifest, error) {
	t := rec.task
	if t.Status == TaskPublished {
		return cloneManifest(m.manifests[t.ManifestID]), nil
	}
	if !allSucceeded(t) {
		return nil, fmt.Errorf("%w: task %s has unfinished shards", ErrNotComplete, t.ID)
	}

	entries := make([]ManifestEntry, 0, len(t.Shards))
	var total int64
	for i := range t.Shards {
		s := &t.Shards[i]
		if err := m.outputs.verify(s.Result.Digest); err != nil {
			return nil, fmt.Errorf("pre-publish verification failed for shard %s: %w", s.Spec.ID, err)
		}
		entries = append(entries, ManifestEntry{
			ShardID:     s.Spec.ID,
			Digest:      s.Result.Digest,
			Ref:         s.Result.Ref,
			Count:       s.Result.Count,
			WorkerID:    s.Result.WorkerID,
			SourceRange: s.Spec.SourceRange,
		})
		total += s.Result.Count
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ShardID < entries[j].ShardID })

	manifest := &Manifest{
		TaskID:        t.ID,
		TaskVersion:   t.Version,
		SourceVersion: t.SourceVersion,
		RuleVersion:   t.RuleVersion,
		Entries:       entries,
		TotalCount:    total,
	}
	manifest.ID = manifestID(manifest)
	if existing, ok := m.manifests[manifest.ID]; ok {
		manifest.CreatedAt = existing.CreatedAt // 崩溃重放：保留首次发布时间
	} else {
		manifest.CreatedAt = m.clock.Now()
	}

	if _, ok := m.manifests[manifest.ID]; !ok {
		if err := m.store.saveManifest(manifest); err != nil {
			return nil, err
		}
		m.manifests[manifest.ID] = manifest
	}
	t.Status = TaskPublished
	t.ManifestID = manifest.ID
	t.UpdatedAt = m.clock.Now()
	if err := m.store.saveTask(t); err != nil {
		return nil, err
	}
	return cloneManifest(manifest), nil
}

// GetTask 查询任务状态快照。
func (m *Manager) GetTask(ctx context.Context, taskID string) (*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.tasks[taskID]
	if !ok {
		return nil, fmt.Errorf("%w: task %s", ErrNotFound, taskID)
	}
	return cloneTask(rec.task), nil
}

// GetTaskByExternalID 按外部任务号查询。
func (m *Manager) GetTaskByExternalID(ctx context.Context, externalID string) (*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.byExternal[externalID]
	if !ok {
		return nil, fmt.Errorf("%w: external id %q", ErrNotFound, externalID)
	}
	return cloneTask(m.tasks[id].task), nil
}

// GetProgress 返回进度。处理中的任务不暴露任何输出内容或清单。
func (m *Manager) GetProgress(ctx context.Context, taskID string) (*Progress, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.tasks[taskID]
	if !ok {
		return nil, fmt.Errorf("%w: task %s", ErrNotFound, taskID)
	}
	t := rec.task
	p := &Progress{
		TaskID:     t.ID,
		ExternalID: t.ExternalID,
		Version:    t.Version,
		Status:     t.Status,
		Total:      len(t.Shards),
		ManifestID: t.ManifestID,
		CreatedAt:  t.CreatedAt,
		UpdatedAt:  t.UpdatedAt,
	}
	for i := range t.Shards {
		s := &t.Shards[i]
		switch s.Status {
		case ShardPending:
			p.Pending++
		case ShardLeased:
			p.Leased++
		case ShardSucceeded:
			p.Succeeded++
			p.ProcessedCount += s.Result.Count
		case ShardFailed:
			p.Failed++
		}
		p.ExpiredTakeovers += s.ExpiredTakeovers
	}
	return p, nil
}

// GetManifest 仅在任务已发布后返回不可变结果清单；处理中返回 ErrNotPublished。
func (m *Manager) GetManifest(ctx context.Context, taskID string) (*Manifest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.tasks[taskID]
	if !ok {
		return nil, fmt.Errorf("%w: task %s", ErrNotFound, taskID)
	}
	t := rec.task
	if t.Status != TaskPublished {
		return nil, fmt.Errorf("%w: task %s status %s", ErrNotPublished, taskID, t.Status)
	}
	return cloneManifest(m.manifests[t.ManifestID]), nil
}

// DownloadResult 读取已发布任务中某个分片的脱敏输出。
// 任务未发布时一律返回 ErrNotPublished —— 处理期间不可能读到半套数据。
func (m *Manager) DownloadResult(ctx context.Context, taskID, shardID string) (io.ReadCloser, *ManifestEntry, error) {
	m.mu.Lock()
	rec, ok := m.tasks[taskID]
	if !ok {
		m.mu.Unlock()
		return nil, nil, fmt.Errorf("%w: task %s", ErrNotFound, taskID)
	}
	t := rec.task
	if t.Status != TaskPublished {
		m.mu.Unlock()
		return nil, nil, fmt.Errorf("%w: task %s status %s", ErrNotPublished, taskID, t.Status)
	}
	mf := m.manifests[t.ManifestID]
	var entry *ManifestEntry
	for i := range mf.Entries {
		if mf.Entries[i].ShardID == shardID {
			e := mf.Entries[i]
			entry = &e
			break
		}
	}
	if entry == nil {
		m.mu.Unlock()
		return nil, nil, fmt.Errorf("%w: shard %s not in published manifest", ErrNotFound, shardID)
	}
	// 输出在锁外打开（可能涉及磁盘 IO）；清单不可变，entry 的 Digest 不会再变化。
	digest := entry.Digest
	m.mu.Unlock()

	f, _, err := m.outputs.open(digest)
	if err != nil {
		return nil, nil, err
	}
	return f, entry, nil
}

// PutOutput 由工作者在提交成功回执前调用：把一段脱敏输出写入内容寻址存储，
// 返回引用（即摘要）。输出此时仅落盘，任务发布前外部无法通过任务接口读到它。
func (m *Manager) PutOutput(ctx context.Context, data []byte) (ref, digest string, err error) {
	ref, digest, _, err = m.outputs.put(data)
	return
}

// GarbageCollect 清理“安全可删”的输出：
// 未被任何已发布成功清单引用，且未被活动任务的成功分片引用，
// 且落盘时间早于 GC 宽限期（避免删掉在途输出）。返回删除条数。
func (m *Manager) GarbageCollect(ctx context.Context) (int, error) {
	m.mu.Lock()
	protected := make(map[string]struct{})
	for _, mf := range m.manifests {
		for _, e := range mf.Entries {
			protected[e.Digest] = struct{}{}
		}
	}
	for _, rec := range m.tasks {
		if rec.task.Status == TaskCanceled {
			continue // 已取消任务的输出不提供任何保护，等待回收
		}
		for i := range rec.task.Shards {
			if rec.task.Shards[i].Result != nil {
				protected[rec.task.Shards[i].Result.Digest] = struct{}{}
			}
		}
	}
	cutoff := m.clock.Now().Add(-m.gcGrace)
	m.mu.Unlock()

	digests, err := m.outputs.list()
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, d := range digests {
		if _, ok := protected[d]; ok {
			continue
		}
		mt, err := m.outputs.modTime(d)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return removed, err
		}
		if mt.After(cutoff) {
			continue // 宽限期内的在途/近期输出保留
		}
		if err := m.outputs.delete(d); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

// requireActiveTask 在锁内取出任务，并拒绝任何非 active 状态。
func (m *Manager) requireActiveTask(taskID string) (*taskRecord, error) {
	rec, ok := m.tasks[taskID]
	if !ok {
		return nil, fmt.Errorf("%w: task %s", ErrNotFound, taskID)
	}
	if rec.task.Status != TaskActive {
		return nil, fmt.Errorf("%w: task %s status %s", ErrTerminal, taskID, rec.task.Status)
	}
	return rec, nil
}

func findShard(t *Task, shardID string) *Shard {
	for i := range t.Shards {
		if t.Shards[i].Spec.ID == shardID {
			return &t.Shards[i]
		}
	}
	return nil
}

func allSucceeded(t *Task) bool {
	if len(t.Shards) == 0 {
		return false
	}
	for i := range t.Shards {
		if t.Shards[i].Status != ShardSucceeded {
			return false
		}
	}
	return true
}

func validateCreateInput(in CreateInput) error {
	if in.ExternalID == "" {
		return fmt.Errorf("%w: external id is required", ErrInvalidArgument)
	}
	if len(in.Shards) == 0 {
		return fmt.Errorf("%w: at least one shard is required", ErrInvalidArgument)
	}
	seen := make(map[string]struct{}, len(in.Shards))
	for _, s := range in.Shards {
		if s.ID == "" {
			return fmt.Errorf("%w: shard id is required", ErrInvalidArgument)
		}
		if _, dup := seen[s.ID]; dup {
			return fmt.Errorf("%w: duplicate shard id %q", ErrInvalidArgument, s.ID)
		}
		seen[s.ID] = struct{}{}
	}
	return nil
}

// fingerprintSpec 是参与输入指纹的规范化分片形状。
type fingerprintSpec struct {
	ID          string `json:"id"`
	SourceRange string `json:"source_range"`
}

type fingerprintPayload struct {
	ExternalID    string            `json:"external_id"`
	Version       int64             `json:"version"`
	SourceVersion string            `json:"source_version"`
	RuleVersion   string            `json:"rule_version"`
	Shards        []fingerprintSpec `json:"shards"`
}

// inputFingerprint 计算冻结输入的指纹：外部任务号、任务版本、源数据版本、
// 规则版本与有序分片集合共同决定。分片按 ID 排序，提交顺序不影响指纹。
func inputFingerprint(in CreateInput) (string, error) {
	p := fingerprintPayload{
		ExternalID:    in.ExternalID,
		Version:       in.Version,
		SourceVersion: in.SourceVersion,
		RuleVersion:   in.RuleVersion,
		Shards:        make([]fingerprintSpec, 0, len(in.Shards)),
	}
	for _, s := range in.Shards {
		p.Shards = append(p.Shards, fingerprintSpec{ID: s.ID, SourceRange: s.SourceRange})
	}
	sort.Slice(p.Shards, func(i, j int) bool { return p.Shards[i].ID < p.Shards[j].ID })
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func deriveTaskID(externalID, fingerprint string) string {
	h := sha256.New()
	h.Write([]byte("task:"))
	h.Write([]byte(externalID))
	h.Write([]byte("|"))
	h.Write([]byte(fingerprint))
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	h.Write(b)
	return "task_" + hex.EncodeToString(h.Sum(nil))[:32]
}

// manifestID 对清单内容（不含时间戳）做内容寻址，保证同一份成功结果
// 在崩溃重放时生成完全相同的清单 ID。
func manifestID(m *Manifest) string {
	type entryShape struct {
		ShardID     string `json:"shard_id"`
		Digest      string `json:"digest"`
		Ref         string `json:"ref"`
		Count       int64  `json:"count"`
		SourceRange string `json:"source_range"`
	}
	type payload struct {
		TaskID        string       `json:"task_id"`
		TaskVersion   int64        `json:"task_version"`
		SourceVersion string       `json:"source_version"`
		RuleVersion   string       `json:"rule_version"`
		Entries       []entryShape `json:"entries"`
		TotalCount    int64        `json:"total_count"`
	}
	p := payload{
		TaskID:        m.TaskID,
		TaskVersion:   m.TaskVersion,
		SourceVersion: m.SourceVersion,
		RuleVersion:   m.RuleVersion,
		Entries:       make([]entryShape, 0, len(m.Entries)),
		TotalCount:    m.TotalCount,
	}
	for _, e := range m.Entries {
		p.Entries = append(p.Entries, entryShape{
			ShardID:     e.ShardID,
			Digest:      e.Digest,
			Ref:         e.Ref,
			Count:       e.Count,
			SourceRange: e.SourceRange,
		})
	}
	b, _ := json.Marshal(p)
	sum := sha256.Sum256(b)
	return "manifest_" + hex.EncodeToString(sum[:])
}

func cloneTask(t *Task) *Task {
	if t == nil {
		return nil
	}
	c := *t
	c.Shards = append([]Shard(nil), t.Shards...)
	for i := range c.Shards {
		if t.Shards[i].Lease != nil {
			l := *t.Shards[i].Lease
			c.Shards[i].Lease = &l
		}
		if t.Shards[i].Result != nil {
			r := *t.Shards[i].Result
			c.Shards[i].Result = &r
		}
	}
	if t.CanceledAt != nil {
		ct := *t.CanceledAt
		c.CanceledAt = &ct
	}
	return &c
}

func cloneManifest(m *Manifest) *Manifest {
	if m == nil {
		return nil
	}
	c := *m
	c.Entries = append([]ManifestEntry(nil), m.Entries...)
	return &c
}
