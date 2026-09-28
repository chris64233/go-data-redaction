package godataredaction

import "errors"

// 业务级哨兵错误：调用方可用 errors.Is 判别具体冲突类型。
var (
	// ErrNotFound 表示任务、分片或结果条目不存在。
	ErrNotFound = errors.New("not found")
	// ErrConflict 表示相同外部任务号下输入（数据范围或规则版本）与既有任务不一致。
	ErrConflict = errors.New("conflict: external task id bound to a different frozen input")
	// ErrStaleLease 表示回执携带的租约已失效：
	// 租约过期后被其他工作者接管，旧工作者的迟到回执不得覆盖新结果。
	ErrStaleLease = errors.New("stale lease: token superseded or expired by another worker")
	// ErrVersionMismatch 表示回执/重试携带的任务版本与冻结版本不一致。
	ErrVersionMismatch = errors.New("task version mismatch with frozen version")
	// ErrDigestConflict 表示同一分片提交了与已成功结果不同的输出摘要，
	// 这不是普通重试：既不覆盖既有结果，也不把分片重置为待处理。
	ErrDigestConflict = errors.New("digest conflict: shard already succeeded with a different output digest")
	// ErrNoEligibleShard 表示当前没有可领取的分片（没有待处理分片，且没有过期租约可接管）。
	ErrNoEligibleShard = errors.New("no eligible shard to claim")
	// ErrTerminal 表示任务已处于终态（已发布/已取消），该操作不再被允许。
	ErrTerminal = errors.New("task already in terminal state")
	// ErrNotPublished 表示任务尚未一次性发布，外部不得读取其任何输出（防半套数据）。
	ErrNotPublished = errors.New("result not published yet")
	// ErrNotComplete 表示仍有分片未成功，不满足一次性发布条件。
	ErrNotComplete = errors.New("task has unfinished shards, cannot publish")
	// ErrOutputMismatch 表示回执中的输出摘要/引用与输出存储中登记的内容不一致，校验不通过。
	ErrOutputMismatch = errors.New("receipt output does not match registered output digest")
	// ErrInvalidArgument 表示入参非法。
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrShardNotFailed 表示请求重试的分片当前不是失败状态，无需重试。
	ErrShardNotFailed = errors.New("shard is not in failed state")
)
