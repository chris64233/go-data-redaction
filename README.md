# go-data-redaction

可恢复、可重入的数据脱敏任务编排库：任务被拆成分片由多个工作者并行处理，
**脱敏结果只在全套分片完整、且摘要校验通过之后才一次性对外发布**，
处理期间任何外部查询都不可能读到半套脱敏数据。

## 核心保证

1. **创建即冻结（版本与范围）**
   创建任务时一次性冻结源数据版本、脱敏规则版本、任务版本与待处理分片集合，
   并据此计算输入指纹：
   - 相同外部任务号 + 相同输入重复提交：返回同一个原任务（幂等）；
   - 外部任务号相同但数据范围、分片集合、源版本或规则版本变化：返回
     `ErrConflict`，不会覆盖既有冻结。

2. **有期限租约 + fencing token**
   工作者通过 `ClaimShard` 领取分片，获得带过期时间的租约。租约过期后该分片
   可被其他工作者接管，接管时换发单调递增的新 token：
   - 旧工作者的迟到回执携带旧 token，一律返回 `ErrStaleLease`，不可能覆盖新结果；
   - 同一分片已经成功后，又收到不同摘要的回执，返回 `ErrDigestConflict`，
     这不是普通重试——既不覆盖既有结果，也不把分片重置为待处理。

3. **只在完整后发布（all-or-nothing publication）**
   全部分片成功且发布前逐一复核输出内容 `sha256` 与回执摘要一致后，
   系统才一次性生成内容寻址的**不可变结果清单**（manifest）并把任务翻转为
   `published`。此前：
   - `GetManifest` 返回 `ErrNotPublished`；
   - `GetProgress` 只返回计数进度，不暴露任何输出内容；
   - `DownloadResult`（哪怕下载的是已经完成的那个分片）返回 `ErrNotPublished`。

   输出文件虽然在回执前就已落盘（内容寻址存储），但在发布前没有任何对外读取
   路径，外部看到的世界里它们不存在。发布顺序是“先写不可变清单文件，再原子
   翻转任务状态”，崩溃于中途可在重启时对账补齐。

4. **单一终态**
   取消、重试回执、最终发布都在同一串行化点竞争：
   - `published` 与 `canceled` 互斥，只可能留下一个终态；
   - 并发完成最后几个分片时，发布逻辑只执行一次，磁盘上只产生一份清单；
   - 取消后停止新领取与过期接管，在途回执进入终态后被拒绝。

5. **失败重试与输出清理**
   - 失败分片保持 `failed`，不会被自动重领，只能用**原任务版本**显式
     `RetryShard` / `RetryFailedShards`，版本不符返回 `ErrVersionMismatch`；
   - 未被任何成功清单（或活动任务成功分片）引用的输出，由 `GarbageCollect`
     安全清理；清单引用的输出永久受保护，近期落盘的在途输出有宽限期保护。

## 存储布局

```
<root>/
├── tasks/<task-id>.json        # 任务完整状态，整体 JSON 原子覆盖写（temp + rename + fsync）
├── manifests/<manifest-id>.json# 不可变结果清单，内容寻址，只写一次
└── outputs/<sha256>            # 内容寻址的分片脱敏输出（发布前外部不可读）
```

进程重启时 `NewManager` 从磁盘恢复全部任务、租约 token 序列，并对崩溃窗口做对账：
清单已写而状态未翻转 → 补翻转；全部分片已成功但发布未完成 → 重新校验并发布。

## API 概览

| 方法 | 作用 |
| --- | --- |
| `NewManager(root, opts...)` | 打开/初始化并恢复状态 |
| `CreateTask` | 创建任务，冻结版本与分片集合；幂等 / 冲突检测 |
| `ClaimShard` | 领取待处理分片或接管过期租约 |
| `PutOutput` | 输出落盘到内容寻址存储（发布前外部不可读） |
| `SubmitReceipt` | 提交摘要、处理条数与任务版本；成功/失败回执 |
| `RetryShard` / `RetryFailedShards` | 按原任务版本重试失败分片 |
| `CancelTask` | 取消任务，停止新领取（终态，幂等） |
| `PublishResult` | 显式发布（最后成功回执也会自动发布，幂等） |
| `GetProgress` | 查询进度计数，不暴露处理中的输出 |
| `GetManifest` | 仅发布后可取不可变结果清单 |
| `DownloadResult` | 仅发布后可按清单下载分片脱敏输出 |
| `GarbageCollect` | 清理未被成功清单引用的输出 |

## 使用示例

```go
mgr, err := godataredaction.NewManager("/var/lib/redaction")
if err != nil { /* ... */ }

task, err := mgr.CreateTask(ctx, godataredaction.CreateInput{
    ExternalID:    "batch-2026-09-28",
    Version:       42,
    SourceVersion: "snapshot-20260928",
    RuleVersion:   "rules-v7",
    Shards: []godataredaction.ShardSpec{
        {ID: "shard-0", SourceRange: "0:100000"},
        {ID: "shard-1", SourceRange: "100000:200000"},
    },
})
if errors.Is(err, godataredaction.ErrConflict) { /* 同号不同输入 */ }

for {
    claim, err := mgr.ClaimShard(ctx, task.ID, "worker-a")
    if errors.Is(err, godataredaction.ErrNoEligibleShard) { break /* 完成或等待重试 */ }

    data := redact(claim) // 业务脱敏逻辑
    ref, digest, err := mgr.PutOutput(ctx, data)
    _, err = mgr.SubmitReceipt(ctx, godataredaction.Receipt{
        TaskID: claim.TaskID, ShardID: claim.ShardID,
        TaskVersion: claim.TaskVersion, Token: claim.Token, WorkerID: claim.WorkerID,
        Success: true, Digest: digest, Ref: ref, Count: int64(len(data)),
    })
    // ErrStaleLease：租约已被接管，放弃本次结果；ErrDigestConflict：同分片已有不同结果
}

// 全套完成后清单自动生成；也可显式调用（幂等）
mf, err := mgr.PublishResult(ctx, task.ID)

rc, entry, err := mgr.DownloadResult(ctx, task.ID, mf.Entries[0].ShardID)
defer rc.Close()
```

## 数据不会在处理中提前对外可见

- 输出虽然先写入内容寻址存储，但**唯一的对外读取入口** `DownloadResult` /
  `GetManifest` 都强制任务处于 `published`，处理中一律 `ErrNotPublished`；
- 发布以“先不可变清单、后状态翻转”的顺序原子完成，外部观察只有
  “尚不可见”和“全套可见”两种状态，不存在中间态；
- 清单条目按分片 ID 排序并整体内容寻址，发布后不可修改；
- 取消的任务永远不会产生清单，其输出不对外可见，随后由 GC 安全回收。

## 测试

```sh
go test -race -count=1 ./...
```

测试覆盖：创建幂等与冲突、租约过期接管与迟到回执、摘要冲突、失败按原版本
重试、取消竞争、并发完成只发布一次、处理中不可见、崩溃恢复与发布对账、
未引用输出 GC（含宽限期）。
