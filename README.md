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
   半成品都不可读，旧代际始终可读；清单一份都没能发布时，已 rename 的新分片
   会回滚删除，失败操作不留下任何半成品代际（模拟崩溃留下的暂存/孤儿由重启
   恢复清空）。
3. **单坏可读可修，双坏明确不可恢复**：读取时按清单摘要校验每个分片；恰好一个
   缺失/损坏（或整盘目录消失）时用另外两个分片 XOR 重建（`a⊕b=p`，任一分片
   等于其余两个的异或），重建结果再次比对清单摘要，然后原子写回修复。两个异常
   返回 `ErrUnrecoverable`，且不触碰坏盘内容。
4. **修复不能覆盖新代际**：分片与清单都是代际作用域文件；修复安装前在同一把
   键锁内重读已发布清单，发现代际已前进**或对象已删除**则放弃暂存修复并返回
   `ErrConflict`。
5. **删除即发布墓碑**：`Delete(key, expectGen)` 与条件写入同构——把一份更高
   代际的删除清单（`{"deleted": true}`，无分片信息）走同样的「暂存→rename→
   fsync」原子发布。首个副本落地后对象立刻且持续不可读：旧清单副本、残留旧
   分片、暂存中的旧修复、整盘离线后回归的副本都无法让旧内容复活（清单取最高
   代际合法副本，墓碑优先并自愈旧副本）。删除代际的分片尽力回收，残留由重启
   恢复清理。
6. **同名重建不复用旧代际**：重建必须以 `Delete` 返回的代际为 `expectGen`，
   发布为严格更高的代际。若所有清单副本都不可读、但盘上仍有该键的已发布分片
   （旧版删除残留/外部擦除），首次写入（`expectGen=0`）返回
   `ErrGenerationEvidence` 而不是从 gen1 重来——避免新旧内容在同代际不可区分；
   重启恢复确认分片不被任何清单引用后予以回收，之后才可首次写入。
7. **重启恢复**：`Open` 时
   - 清空三个 `.stage/` 目录中的全部未发布暂存文件；
   - 从清单目录**和已发布分片文件名**两侧汇总对象 id；对每个对象取所有盘上
     代际最高的合法清单，向缺副本/旧副本/损坏副本的盘自愈清单；
   - 回收不被已发布清单引用的旧代际/半成品/被删除代际分片（GC），**仍被当前
     清单引用的分片一律保留**；某盘整盘缺失时不清理（其清单副本可能仍在），
     所有盘均无任何清单副本时才回收其分片；全部清单副本均损坏时一律不动，
     留给运维抢救。

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
gen, err := s.Delete(ctx, key, expectGen)             // 条件删除，返回删除代际（墓碑）
gen, repaired, err := s.Repair(ctx, key)              // 只修复不返回数据（后台用）
loop := s.StartRepairLoop(ctx, 10*time.Second)        // 后台周期扫描修复
defer loop.Stop()
```

故障注入钩子（`Hooks`）：`BeforeManifestCommit`（三分片已暂存核验、发布前）与
`BeforeRepairCommit`（修复分片已暂存、安装代际检查前）。返回
`xorstore.ErrSimulatedCrash` 可模拟“进程崩溃”——保留现场，由下次 `Open` 恢复清理。

## 命令行演示

```bash
go build -o xorctl ./cmd/xorctl
./xorctl -root d put greeting hello          # 代际 1
./xorctl -root d put greeting hello-world    # 条件更新为代际 2
echo GARBAGE > d/disk1/*-gen2-b.shard        # 注入单盘损坏
./xorctl -root d get greeting                # 重建并修复，打印 hello-world
rm d/disk2/*-gen2-p.shard; echo X > d/disk0/*-gen2-a.shard
./xorctl -root d get greeting                # 退出码 4: unrecoverable
```

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
| `TestDeleteIsAtomicAndDurable` | 条件删除：错误代际冲突、删除后 Get/Repair 均 `ErrNotFound`、重启仍不可读、二次删除/删不存在键的语义 |
| `TestDeleteTombstoneHealsStaleCopies` | 删除后把一盘清单回退成旧活清单：重启以高代际墓碑自愈，旧内容不复活 |
| `TestDeleteFailureLeavesObjectReadable` | 删除发布前钩子失败：对象持续可读、无半成品、重启后重试删除成功 |
| `TestRecreateDoesNotReuseGenerationWhenEvidenceRemains` | 清单全失但旧分片残留：首次写返回 `ErrGenerationEvidence`，恢复回收后才允许 gen1 写入 |
| `TestLegacyRemovedObjectRecoveryReclaimsShards` | 旧版「直接 remove」删除的盘上形态：恢复回收全部无主分片 |
| `TestOldRepairCannotResurrectDeleted` | 旧修复暂存就绪、提交前交错删除：修复被 `ErrConflict` 拒绝，不写回旧分片 |
| `TestOldRepairCannotClobberRebuiltContent` | 删除+同名重建与旧修复交错：重建代际完好，重启后当前分片不被 GC 误删 |
| `TestDeleteRebuildWithOfflineDiskStaleCopy` | 一盘跨删除与重建全程离线，回归后清单自愈、旧分片回收、单分片重建能力保持 |
| `TestFailedPutLeavesNoReadableHalfProduct` | 写失败（普通错误与模拟崩溃两种）不留可读半成品，旧代际可读，重试成功 |
| `TestRecoveryPreservesLiveShardsAndSingleShardRepair` | 删除→重建后各注入单分片损坏：恢复保留当前内容且单坏仍可重建修复 |
| `TestBackgroundRepairSkipsDeleted` | 后台扫描跳过墓碑对象（不复活、不报错），同时仍治愈存活对象 |
| `TestConcurrentDeleteRecreateAndRepair` | `-race` 下 删除/重建/修复/读取 并发只可能观察到旧值、新值或不存在 |
| `TestDeleteTombstoneManifestShape` | 三盘墓碑副本结构（`deleted`、代际、无分片元数据）与旧分片回收 |

## 范围与取舍

- 单进程内用每键互斥保证 CAS 原子性；多进程共享目录需额外的文件锁（未实现）。
- 只做单对象 2+1 条带：容忍恰好 1 盘故障；修复按摘要判定，不依赖 mtime。
- 发布期间进程在“首个清单 rename 之后”崩溃时，新代际已在部分盘可见，恢复会以
  最高代际合法清单为准自愈其余副本；“rename 之前”崩溃（或普通写失败）则新分片
  被回滚/成为孤儿被 GC，旧代际保持可读。
- 删除同样是清单发布：墓碑至少一份副本落地后删除即生效；所有磁盘在删除与后续
  操作之间整盘离线的情形与普通写一致，磁盘回归后由清单自愈与单分片重建收敛。

## 删除及同名重建

`Delete(ctx, key, expectGen)` 与条件写入同构：发布一份代际为 `expectGen+1`
的墓碑清单（`deleted: true`，不含分片元数据），返回该删除代际。

- 首个墓碑副本 rename 落地起，`Get`/`Repair` 对该键返回 `ErrNotFound`；
  重复删除返回 `ErrNotFound`。
- 后台修复扫描跳过墓碑对象（既不复活也不当错误）。
- 同名重建只能以删除返回的代际作为 `expectGen`，新内容发布在
  `删除代际+1`，绝不复用被删除代际；墓碑随后成为旧代际被自愈覆盖。
- 盘上只剩旧分片、所有清单副本缺失时，`Put(..., 0)` 返回
  `ErrGenerationEvidence` 拒绝复用旧代际；重新 `Open` 让恢复在确认无清单
  引用后回收这些分片，之后才能按全新对象 gen1 写入。
- 修复提交时在同一把键锁内重读清单：代际前进或已删除一律 `ErrConflict`
  放弃安装，旧修复不可能影响已删除或已重建对象。
- 恢复的 GC 只删除不被当前清单引用的分片；同名重建当前代际的分片一律
  保留，单分片损坏的既有恢复能力不受影响。
