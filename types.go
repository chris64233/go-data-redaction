package godataredaction

import "time"

// TaskStatus 是任务的生命周期状态。
//
// 状态流转：
//
//	TaskActive ──全部分片成功 + 摘要校验通过 + 一次性发布──▶ TaskPublished（终态）
//	TaskActive ──取消───────────────────────────────────▶ TaskCanceled（终态）
//
// 只有 TaskPublished 的任务才允许通过 DownloadResult 读取输出；
// TaskActive 期间即便部分分片已经写出输出，外部也读不到（见 ErrNotPublished）。
type TaskStatus string

const (
	TaskActive    TaskStatus = "active"    // 处理中：冻结版本、分片可领取/重试，结果不可见
	TaskPublished TaskStatus = "published" // 已一次性发布：不可变清单已生成，可下载
	TaskCanceled  TaskStatus = "canceled"  // 已取消（终态）：停止新领取，未发布输出可清理
)

// ShardStatus 是单个分片的处理状态。
type ShardStatus string

const (
	ShardPending   ShardStatus = "pending"   // 待处理（含按原版本重试后重新排队）
	ShardLeased    ShardStatus = "leased"    // 已被某工作者凭有效租约领取
	ShardSucceeded ShardStatus = "succeeded" // 已成功，输出摘要已与输出存储核对一致
	ShardFailed    ShardStatus = "failed"    // 处理失败，可按原任务版本重试
)

// ShardSpec 是创建任务时冻结的待处理分片定义。
type ShardSpec struct {
	// ID 为任务内唯一的分片标识。
	ID string
	// SourceRange 描述该分片覆盖的源数据范围（如分区/偏移区间），仅作冻结与记录用。
	SourceRange string
}

// Lease 记录一次分片领取。Token 为单调递增的 fencing token：
// 每次领取（含过期接管）都会换发新 token，旧 token 的迟到回执一律拒绝。
type Lease struct {
	WorkerID string
	Token    int64
	// ExpiresAt 为租约到期时刻；过期后该分片可被其他工作者接管。
	ExpiresAt time.Time
	// ClaimedAt 为本次领取时刻。
	ClaimedAt time.Time
}

// ShardResult 是分片成功后冻结下来的输出声明。
type ShardResult struct {
	Digest   string // 输出摘要（内容寻址 key，hex sha256）
	Ref      string // 输出存储引用（digest + 大小可定位内容）
	Count    int64  // 处理条数
	WorkerID string // 成功完成该分片的工作者
	At       time.Time
}

// Shard 是任务内一个分片的持久化状态。
type Shard struct {
	Spec             ShardSpec
	Status           ShardStatus
	Lease            *Lease       // Status == ShardLeased 时非 nil
	Result           *ShardResult // Status == ShardSucceeded 时非 nil
	Attempts         int          // 领取次数（含接管）
	Failures         int          // 回执声明失败的次数
	LastError        string       // 最近一次失败原因
	ExpiredTakeovers int          // 租约过期后被其他工作者接管的次数（进度可观测）
}

// Task 是脱敏任务的完整、可持久化状态。
//
// 创建时即冻结三个版本维度，之后永不变更：
//   - SourceVersion：源数据版本
//   - RuleVersion：脱敏规则版本
//   - Shards：待处理分片集合（以输入指纹参与重复/冲突判定）
//
// Version 为本任务冻结的任务版本号；重试不产生新版本，
// 所有领取与回执都必须带回该版本（见 ErrVersionMismatch）。
type Task struct {
	ID            string
	ExternalID    string // 外部任务号（幂等键）
	Version       int64  // 冻结的任务版本
	SourceVersion string // 冻结的源数据版本
	RuleVersion   string // 冻结的脱敏规则版本
	Shards        []Shard
	Status        TaskStatus
	CreatedAt     time.Time
	UpdatedAt     time.Time
	CanceledAt    *time.Time
	// InputFingerprint 为创建时冻结输入（版本+分片集合）的指纹，用于幂等与冲突判定。
	InputFingerprint string
	// ManifestID 为发布后生成的不可变结果清单 ID；未发布时为空。
	ManifestID string
}

// CreateInput 是创建任务的输入。相同 ExternalID + 相同冻结输入重复提交返回原任务。
type CreateInput struct {
	ExternalID    string
	Version       int64
	SourceVersion string
	RuleVersion   string
	Shards        []ShardSpec
}

// Claim 是一次领取结果。工作者必须在 LeaseExpiresAt 前提交回执，
// 否则分片可被其他工作者接管（Token 随之换发，旧回执失效）。
type Claim struct {
	TaskID         string
	TaskVersion    int64 // 冻结的任务版本，回执必须原样带回
	ShardID        string
	Token          int64
	WorkerID       string
	LeaseExpiresAt time.Time
}

// Receipt 是工作者完成（或确认失败）一个分片后提交的回执。
type Receipt struct {
	TaskID      string
	ShardID     string
	TaskVersion int64 // 必须等于冻结版本，否则 ErrVersionMismatch
	Token       int64 // 必须等于当前有效租约 token，否则 ErrStaleLease
	WorkerID    string
	Success     bool   // true=提交输出；false=声明该分片失败
	Digest      string // Success 时必填：输出摘要
	Ref         string // Success 时必填：PutOutput 返回的输出引用
	Count       int64  // Success 时必填：处理条数（不得为负）
	Error       string // Success=false 时的失败原因
}

// ManifestEntry 是不可变结果清单中的单条分片记录。
type ManifestEntry struct {
	ShardID     string
	Digest      string
	Ref         string
	Count       int64
	WorkerID    string
	SourceRange string
}

// Manifest 是发布点一次性生成的不可变结果清单。
//
// 生成时机：全部分片成功且输出摘要与输出存储逐一校验通过之后。
// 清单本身内容寻址（ID = sha256(规范化条目)），一经写出不可修改；
// 只有清单引用到的输出才对外部可见、且在 GC 时受保护。
type Manifest struct {
	ID            string
	TaskID        string
	TaskVersion   int64
	SourceVersion string
	RuleVersion   string
	Entries       []ManifestEntry // 按 ShardID 排序，保证摘要稳定
	TotalCount    int64
	CreatedAt     time.Time
}

// Progress 是任务进度快照（查询接口返回值）。处理中不暴露任何输出内容。
type Progress struct {
	TaskID           string
	ExternalID       string
	Version          int64
	Status           TaskStatus
	Total            int
	Pending          int
	Leased           int
	Succeeded        int
	Failed           int
	ExpiredTakeovers int
	ProcessedCount   int64 // 已成功分片的处理条数累计
	ManifestID       string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}
