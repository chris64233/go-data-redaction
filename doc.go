// Package godataredaction 实现可恢复的数据脱敏任务编排，并保证脱敏结果
// “只在完整后发布”（all-or-nothing publication）。
//
// 核心保证：
//
//   - 创建即冻结：任务创建时冻结源数据版本、脱敏规则版本与待处理分片集合，
//     并生成输入指纹。相同外部任务号 + 相同输入重复提交返回原任务；
//     外部任务号相同但数据范围或规则版本变化，返回 ErrConflict。
//   - 租约与 fencing token：工作者凭有期限租约领取分片，租约过期后可被他人接管；
//     每次接管换发单调递增的新 token，旧工作者的迟到回执（ErrStaleLease）
//     不可能覆盖新结果。
//   - 完整才发布：全部分片成功且输出摘要逐一校验通过后，才一次性生成
//     内容寻址的不可变结果清单并切换为可下载；处理期间外部读取得到 ErrNotPublished。
//   - 单一终态：取消、重试回执与最终发布在同一把锁内竞争，取消/发布互斥，
//     并发完成最后分片只发布一次。未被成功清单引用的输出由 GC 安全清理。
//
// 典型流程：
//
//	mgr, _ := NewManager("/var/lib/redaction")
//	task, _ := mgr.CreateTask(ctx, CreateInput{...})      // 创建并冻结
//	claim, _ := mgr.ClaimShard(ctx, task.ID, "worker-1")  // 凭租约领取
//	ref, digest, _ := mgr.PutOutput(ctx, redactedBytes)   // 输出先落盘（外部不可见）
//	mgr.SubmitReceipt(ctx, Receipt{...})                  // 提交摘要/条数/版本
//	mf, _ := mgr.GetManifest(ctx, task.ID)                // 仅发布后可读
//	rc, entry, _ := mgr.DownloadResult(ctx, task.ID, id)  // 仅发布后可下载
package godataredaction
