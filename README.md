# go-data-redaction

可恢复的数据脱敏任务协调器：工作者按分片（shard）领取脱敏工作，**只有全部分片成功且摘要校验通过后，结果才一次性发布**，在此之前外部读不到任何半成品数据。

## 核心保证

### 1. 创建即冻结，重复提交幂等，输入变化报冲突

创建任务时冻结三项输入：

- 源数据版本 `SourceVersion`
- 脱敏规则版本 `RuleVersion`
- 待处理分片集合 `ShardIDs`

三者与外部任务号一起计算规范化指纹（分片顺序不影响指纹），并派生出内部任务 ID。

- 相同外部任务号 + 相同输入重复提交 → 返回原任务（幂等，进程重启后同样成立）；
- 相同外部任务号但数据范围或规则版本变化 → 返回 `ErrConflict`，不会静默创建第二个任务。

### 2. 有期限租约 +  fencing token，迟到回执无法覆盖新结果

- 工作者通过 `ClaimShard` 领取分片，获得带过期时间的租约；
- 每次发租约（含过期接管、失败重试）都会把分片的 `Attempt` 严格递增，作为 fencing token；
- 租约过期后其他工作者可接管；旧工作者的迟到回执因 `LeaseID` 小于当前租约被拒绝（`ErrLeaseFenced`），**不能覆盖新工作者的结果**；
- 回执必须携带领取时的任务版本，版本不符返回 `ErrVersionMismatch`；
- 成功回执中的输出会重新计算 SHA-256 并与申报摘要比对（`ErrDigestMismatch`）；
- 已成功分片若收到**不同摘要**，一律按冲突拒绝，不会被当作普通重试；相同摘要则幂等确认。

### 3. 结果只在完整后发布，且只发布一次

- 每个成功回执先把输出写入**内容寻址**的暂存区（文件名为摘要），但暂存区不是外部读取入口；
- 当且仅当全部分片为 `succeeded` 时，协调器重新读取每个暂存输出并复算摘要，全部通过后才：
  1. 写入不可变结果清单（manifest，只读文件，内容派生 ID）；
  2. 把任务状态一次性切换为 `published`。
- **处理期间外部查询读不到半套数据**：`GetManifest` / `DownloadOutput` 在发布前一律返回 `ErrNotPublished`，进度查询 `Progress` 只返回计数，不含任何输出字节；
- 最后几个分片并发完成时，所有状态迁移在同一把写锁内串行化——**只有一个调用执行发布切换**，磁盘上也只会出现一个清单文件。

### 4. 失败重试、取消与终态竞争

- 失败分片通过 `RetryShard` 按**原任务版本**重置为待领取，任务版本始终保持创建时的冻结值；重试会递增分片 fencing token，旧失败尝试的迟到回执同样被挡下；
- `Cancel` 后任务进入终态 `cancelled`，停止一切新领取，在途回执被拒绝，并删除全部已暂存输出；
- 取消、迟到回执、最终发布在同一把锁下竞争：任务最终只能停留在 `cancelled` 或 `published` 中的一个；发布后的取消不会回滚已发布结果；
- 未被成功清单引用的输出可安全清理：发布/取消时自动 GC，也可随时调用 `GC`；进程在「状态落盘 ↔ 文件操作」之间崩溃时，重新打开存储会自动对账（删除孤儿输出、清空已取消任务的输出、幂等补全清单）。

## API 概览

| 操作 | 方法 | 说明 |
| --- | --- | --- |
| 创建任务 | `CreateTask(CreateInput)` | 冻结版本与分片，幂等/冲突判定 |
| 领取分片 | `ClaimShard(taskRef, worker)` | 返回租约、任务版本与 fencing token |
| 提交回执 | `SubmitReceipt(*Receipt)` | 校验版本/租约/摘要；自动尝试发布 |
| 失败重试 | `RetryShard(taskRef, shardID)` | 仅失败分片可重试，任务版本不变 |
| 取消 | `Cancel(taskRef)` | 终态、停领、清理暂存输出 |
| 显式发布 | `Publish(taskRef)` | 完成则发布，未完成无副作用 |
| 进度查询 | `Progress(taskRef)` | 仅计数与发布标志，不泄露输出 |
| 读取清单 | `GetManifest(taskRef)` | **发布后才可用** |
| 下载结果 | `DownloadOutput(taskRef, shardID)` | **发布后才可用**，下载时再次校验摘要 |
| 垃圾清理 | `GC(taskRef)` | 删除无引用输出 |

任务可用内部 ID 或外部任务号引用（`taskRef = (id, externalID)`）。

## 快速开始

```go
svc, _ := godataredaction.NewService("./data",
    godataredaction.WithLeaseTTL(30*time.Second))

task, err := svc.CreateTask(godataredaction.CreateInput{
    ExternalID:    "batch-2026-09-28",
    SourceVersion: "src@e3b0c44",
    RuleVersion:   "rules@v12",
    ShardIDs:      []string{"shard-0", "shard-1", "shard-2"},
})

// 工作者循环
claim, err := svc.ClaimShard(task.ID, "", "worker-a")
if errors.Is(err, godataredaction.ErrNoAvailableShard) { /* 暂无工作 */ }

payload := doRedact(claim) // 业务脱敏逻辑
ack, published, err := svc.SubmitReceipt(&godataredaction.Receipt{
    TaskID: claim.TaskID, ShardID: claim.Shard.ID,
    Attempt: claim.Attempt, LeaseID: claim.LeaseID, // 原样回传
    Worker: "worker-a", Succeeded: true,
    Digest: sha256Hex(payload), Count: int64(len(records)), Payload: payload,
})
if errors.Is(err, godataredaction.ErrLeaseFenced) { /* 租约已被接管，放弃本地结果 */ }

// 发布前读取结果 -> ErrNotPublished；发布后才可下载
if data, err := svc.DownloadOutput(task.ID, "", "shard-0"); err == nil {
    use(data)
}
```

## 持久化布局

```
<dir>/
  state.json                       # 原子写（temp+rename）：任务/分片/租约/终态
  tasks/<task-id>/
    blobs/<sha256>                 # 内容寻址的暂存/最终输出（不可变）
    manifests/<manifest-id>.json   # 不可变结果清单（0444，内容派生 ID）
```

用同一目录重新 `NewService` 即可从上次状态恢复，处理中的任务继续可领取，已发布任务的清单与输出保持可读。

## 数据不会在处理中提前对外可见

这是本项目最重要的语义，实现上有四道闸门：

1. **暂存不等于发布**：回执成功只把输出写入内部内容寻址暂存区，外部没有任何 API 读取暂存区；
2. **唯一读取入口有发布闸门**：`GetManifest` 和 `DownloadOutput` 只在任务处于 `published` 且存在清单时工作，其余状态（含 `running`、`cancelled`）一律 `ErrNotPublished`；
3. **发布是一个持锁的原子状态切换**：全部成功 + 逐字节复算摘要通过后，先写不可变清单再切换状态；任何外部调用看到的要么是「未发布、无数据」，要么是「已发布、完整清单」，不存在中间态；
4. **下载时复验**：即使在发布后，`DownloadOutput` 也会对读出的字节重新计算摘要，与清单记录不符则返回 `ErrVerificationFailed`，杜绝返回损坏/被篡改的结果。

## 测试

```sh
go test -race -count=1 ./...
```

测试覆盖：创建冻结与幂等/冲突、租约过期接管与迟到回执 fencing、任务版本校验、摘要不一致与同分片异摘要冲突、失败后按原版本重试、取消停领与清理、取消/回执/发布并发只留一个终态、并发完成最后分片恰好发布一次、发布前字节校验拒绝、重启恢复与崩溃对账、孤儿输出 GC，以及处理期间 `ErrNotPublished` 的不可见性。
