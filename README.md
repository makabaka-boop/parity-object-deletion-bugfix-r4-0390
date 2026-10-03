# xorstore — 三盘 2+1 XOR 纠删码对象存储（Go）

每个对象被拆成**两个等长数据分片**和一个**逐字节异或奇偶分片**，分别落在三个本地目录
（`disk0`、`disk1`、`disk2`）。每个对象有一份 JSON 清单（三盘各一副本），记录：

- 原始长度（奇数长度时第二个数据分片补 1 个零字节，不计入原长度）
- 每个分片的角色（`a`/`b`/`p`）、长度与 SHA-256 摘要
- 对象代际 `gen`（单调递增，从 1 开始）

## 关键不变量

1. **条件代际写入（CAS）**：`Put(key, data, expectGen)` 只在当前代际等于
   `expectGen` 时发布 `expectGen+1`，否则返回 `ErrConflict`。
2. **分片齐备且核验后才发布清单**：三个新分片先写入各自的 `.stage/` 暂存文件
   （`O_EXCL` 随机名 + fsync），逐个读回做摘要核验，然后才 rename 到
   代际作用域名 `<id>-gen<N>-<role>.shard`，最后发布清单。清单发布前的任何
   半成品都不可读，旧代际始终可读。
3. **单坏可读可修，双坏明确不可恢复**：读取时按清单摘要校验每个分片；恰好一个
   缺失/损坏（或整盘目录消失）时用另外两个分片 XOR 重建（`a⊕b=p`，任一分片
   等于其余两个的异或），重建结果再次比对清单摘要，然后原子写回修复。两个异常
   返回 `ErrUnrecoverable`，且不触碰坏盘内容。
4. **修复不能覆盖新代际**：分片与清单都是代际作用域文件；修复安装前在同一把
   键锁内重读已发布清单，发现**代际已前进、对象已删除（墓碑）或同代际的分片摘要
   集合已变化**都放弃暂存修复并返回 `ErrConflict`。因此同代际号被复用（清单证据
   丢失后的混淆）同样无法让旧修复落盘。
5. **删除即墓碑，不是删除清单文件**：`Delete(key, expectGen)` 是条件操作，只在
   当前存活代际等于 `expectGen` 时向三盘原子发布一份 `deleted` 墓碑清单（代际为
   `expectGen+1`，删除返回值）。墓碑是该键当前的权威状态：`Get`/`Repair` 返回
   `ErrNotFound`、后台扫描跳过、缺失/过旧的副本只会自愈成墓碑而不可能复活旧内容；
   被删代际的分片在提交后尽力清除，并由重启 GC 兜底。
6. **同名重建以删除代际为条件**：重建必须 `Put(key, data, delGen)`（删除返回的
   代际），发布 `delGen+1`；`expectGen=0` 或旧代际都得到 `ErrConflict`，新内容
   永远不会复用旧代际。若所有清单副本都丢失但分片还在，`Put(…, 0)` 直接返回
   `ErrGenerationAmbiguous`（包装 `ErrManifestCorrupt`），由重启恢复回收无主分片
   后才允许首次写入。
7. **重启恢复**：`Open` 时
   - 清空三个 `.stage/` 目录中的全部未发布暂存文件；
   - 对象集合同时来自清单副本**和**已发布分片文件名（清单全部丢失的 incarnation
     也能被 GC 看见）；
   - 对每个对象取所有盘上代际最高的合法清单，向缺副本/旧副本/损坏副本的盘
     自愈清单（墓碑键自愈回墓碑）；
   - 存活清单只保留本代际分片，墓碑/无清单对象回收其全部分片（GC），**仍被当前
     清单引用的分片一律保留**。

## 盘上布局

```
diskN/
  .stage/<id>-<name>-<rand>.tmp   # 未发布暂存（重启即清）
  .manifests/<id>.json            # 清单副本（三盘冗余）
  <id>-gen<N>-a.shard             # 已发布分片（代际作用域）
  <id>-gen<N>-b.shard
  <id>-gen<N>-p.shard
```

- 分片发布与清单发布都走「同目录临时文件 → rename → fsync 目录」，发布是原子的。
- `id = sha256(key)` 的十六进制；清单内保留原始 `key`。
- 清单取多副本中**代际最高且结构合法**者，单盘清单损坏/过旧不影响读取并会被自愈。

## API 摘要

```go
s, _ := xorstore.Open(ctx, dir0, dir1, dir2, hooks /* 可为 nil */)

gen, err := s.Put(ctx, key, data, expectGen)          // 条件写入，返回新代际
data, gen, repaired, err := s.Get(ctx, key)           // 读，单坏自动重建+修复
g, err := s.Generation(ctx, key)                      // 当前代际；已删除返回 g+ErrNotFound
gen, err := s.Delete(ctx, key, expectGen)             // 条件删除（发布墓碑），返回删除代际
gen, repaired, err := s.Repair(ctx, key)              // 只修复不返回数据（后台用）
loop := s.StartRepairLoop(ctx, 10*time.Second)        // 后台周期扫描修复
defer loop.Stop()
```

故障注入钩子（`Hooks`）：`BeforeDeleteCommit`（墓碑已暂存、发布前）、
`BeforeManifestCommit`（三分片已暂存核验、发布前）与
`BeforeRepairCommit`（修复分片已暂存、安装代际检查前）。返回
`xorstore.ErrSimulatedCrash` 可模拟“进程崩溃”——保留现场，由下次 `Open` 恢复清理。

## 命令行演示

```bash
go build -o xorctl ./cmd/xorctl
./xorctl -root d put greeting hello          # 代际 1
./xorctl -root d put greeting hello-world    # 条件更新为代际 2
./xorctl -root d delete greeting 2           # 条件删除，返回删除代际 3（墓碑）
./xorctl -root d get greeting                # 退出码 5: not found
./xorctl -root d put greeting hello-again 3  # 以删除代际 3 重建为代际 4
echo GARBAGE > d/disk1/*-gen4-b.shard        # 注入单盘损坏
./xorctl -root d get greeting                # 重建并修复，打印 hello-again
rm d/disk2/*-gen4-p.shard; echo X > d/disk0/*-gen4-a.shard
./xorctl -root d get greeting                # 退出码 4: unrecoverable
```

`put` 不带显式代际时，存活对象用读取代际、已删除对象自动用墓碑代际重建。

退出码：3 代际冲突，4 不可恢复，5 对象不存在。

## 测试

```bash
go test -race -count=1 ./...
```

覆盖：

| 测试 | 注入场景 |
| --- | --- |
| `TestSingleShardCorruption_RebuildAndRepair` | 三个角色 × 损坏/缺失、奇偶长度；再注入双坏断言 `ErrUnrecoverable` |
| `TestCrashBeforeManifestPublish` | 清单发布前钩子返回 `ErrSimulatedCrash`：旧版仍可读、暂存残留 3 个、重启清空、重试成功 |
| `TestCrashMidManifestPublish` | 分片已提交但无任何清单副本：重启 GC 孤儿分片、保留旧代际 |
| `TestConditionalWriteDuringRepair` | 修复暂存就绪、安装前的时刻插入条件写入 gen2：旧代际修复被 `ErrConflict` 拒绝，gen2 完好 |
| `TestRepairLosesRaceToWrite` | 修复准备期间代际已前进的另一交错顺序 |
| `TestDiskLossAndManifestHeal` | 整盘目录消失降级读、两盘消失不可恢复 |
| `TestRecoverySweepsAndHeals` | 暂存垃圾、无清单的 gen99 孤儿分片、清单缺副本的恢复统计 |
| `TestBackgroundRepairLoop` | 后台修复循环自动治好坏分片 |
| `TestConcurrentReadersAndWriter` | `-race` 下读写并发一致性 |
| `TestDeleteMakesObjectUnreadable` | 删除后 Get/Repair/后台扫描/重启均不可读，三盘墓碑、旧分片回收；错误代际冲突、重复删除 NotFound |
| `TestDeleteManifestHealsFromSurvivingCopies` | 删除时一盘清单损坏、一盘缺失：发布墓碑后自愈，旧内容不复活 |
| `TestRecreateAfterDeleteUsesDeleteGeneration` | 同名重建只接受删除代际，发布严格更高代际；旧分片消失、新内容/重启可读 |
| `TestRecreateRefusesToReuseGenerationWhenEvidenceLost` | 清单全失但分片在：`Put(…,0)` 返回 `ErrGenerationAmbiguous`；恢复回收后才允许首写 |
| `TestOldRepairCannotTouchDeletedOrRecreatedObject` | 旧代际修复暂存后交错删除/重建：修复 `ErrConflict` 且零安装 |
| `TestSameNumberGenerationReuseBlockedAtRepair` | 同代际号但分片摘要集合不同（复用混淆）：修复仍被拒，新内容完好 |
| `TestDeleteFailureLeavesNoHalfState` | 删除钩子崩溃/中止：对象保持可读、暂存重启清空、重试成功 |
| `TestFailedWriteLeavesNoReadableHalfProduct` | 写入钩子崩溃后旧代际始终可读 |
| `TestRecoveryPreservesCurrentContentAndSingleShardRepair` | 删除+重建后恢复：保留当前分片、GC 旧代际孤儿，单坏恢复能力不变 |
| `TestBackgroundLoopSkipsDeletedAndHealsRecreated` | 后台循环治愈重建对象的坏分片、从不复活已删内容 |
| `TestDeleteThenRecreateWithLostTombstoneCopy` | 删除后丢盘再重建、盘回归重启：墓碑自愈、新内容保留 |

## 范围与取舍

- 单进程内用每键互斥保证 CAS 原子性；多进程共享目录需额外的文件锁（未实现）。
- 只做单对象 2+1 条带：容忍恰好 1 盘故障；修复按摘要判定，不依赖 mtime。
- 发布期间进程在“首个清单 rename 之后”崩溃时，新代际已在部分盘可见，恢复会以
  最高代际合法清单为准自愈其余副本；“rename 之前”崩溃则新分片成为孤儿被 GC。

## 删除与同名重建（小结）

`Delete(ctx,key,expectGen)` 是条件代际操作：校验当前存活代际后原子发布三盘
墓碑清单并返回删除代际；任何一份清单副本的缺失/损坏都无法让旧内容复活（恢复会把
它自愈成墓碑）。重建同名键必须以删除返回的代际作为条件，生成严格更高代际；当清单
证据全失（仅余分片）时不得当作首次写入——`Put(…,0)` 返回 `ErrGenerationAmbiguous`，
重启恢复回收无主分片后才接受首写。修复在提交时重新核对当前对象的代际 **及三分片
角色/长度/摘要集合**（墓碑直接拒绝）；恢复 GC 以当前清单为准，只清理非当前有效分片。
