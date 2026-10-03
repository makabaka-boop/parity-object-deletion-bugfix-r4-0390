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
   键锁内重读已发布清单，发现代际已前进则放弃暂存修复并返回 `ErrConflict`。
5. **重启恢复**：`Open` 时
   - 清空三个 `.stage/` 目录中的全部未发布暂存文件；
   - 对每个对象取所有盘上代际最高的合法清单，向缺副本/旧副本/损坏副本的盘
     自愈清单；
   - 回收不被已发布清单引用的旧代际/半成品分片（GC），**仍被引用的分片一律保留**。

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

## 范围与取舍

- 单进程内用每键互斥保证 CAS 原子性；多进程共享目录需额外的文件锁（未实现）。
- 只做单对象 2+1 条带：容忍恰好 1 盘故障；修复按摘要判定，不依赖 mtime。
- 发布期间进程在“首个清单 rename 之后”崩溃时，新代际已在部分盘可见，恢复会以
  最高代际合法清单为准自愈其余副本；“rename 之前”崩溃则新分片成为孤儿被 GC。

## 删除及同名重建
Delete(ctx,key,expectGen)使用当前条件代际删除并返回删除代际。Get及Repair不再提供已删除内容，后台扫描跳过已删除对象。重建同名键以删除返回的代际作条件，生成更高代际；代际证据不足时不得当作首次写入。修复在提交时重新核对当前对象代际，恢复清理仅清理非当前有效分片。
