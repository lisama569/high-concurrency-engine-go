# 高并发事件处理引擎（本地内存 + 本地文件）

一个可在本地直接运行的高并发事件处理引擎：用**内存**模拟事件来源与有界
队列/状态，用**本地文件**（WAL + 快照 + 待处理日志 + 死信文件）保证异常
处置与重启一致性。不依赖任何外部中间件。

- 运行示例：`go run ./cmd/demo`
- 运行测试：`go test ./...`
- 并发检测：`go test ./... -race`

---

## 1. 并发模型

```
              有界分片队列                  每业务键串行协调器
 事件来源 ──► shard0 chan ──► worker0 ──► keyCoordinator(actor) ──► 状态存储
(文件+offset) shard1 chan ──► worker1 ──► keyCoordinator(actor)
   │          ...                    ▲
   └─ Submit() 先写 pending.log 再入队 ┘
```

- **分片有界队列**：队列按业务键哈希分成若干分片 channel，每个分片由
  worker 消费。不同业务键天然并行。
- **每业务键 actor（强顺序）**：每个业务键有唯一的串行协调协程。同键事件
  全部进入它的 mailbox，由它按 `Seq` 排序生效：
  - 序号衔接（= 当前版本+1）：应用；
  - 序号超前（缺口）：放入该键的最小序号堆等待前驱，**投递 worker 立即
    释放、不阻塞线程**；前驱应用后在同一协程内级联处理堆中所有连续后继；
  - 序号落后：交由存储层判定为**重复**或**冲突**。

  因此“多个 goroutine 以任意顺序向同一键投递”最终都严格按 `Seq` 生效，
  不会乱序、不会重复累加。

- **在途信号量**：限制“已接收但尚未有最终结论”的事件总量。处理能力不足
  时，事件在有界队列排队；超出 `SubmitTimeout` 即触发明确背压。

## 2. 背压与可观测性

- 处理能力不足时，事件**不会被静默丢弃**。`Submit` 在截止时间内拿不到队列
  空位会返回带明确类别的归一化错误：
  - `queue_full`：容量背压，等待超时（**可重试**）；
  - `stopping`：引擎停机中，事件已在持久日志里（安全重排）；
  - 以及 `invalid` / `storage` / `canceled` / `timeout`。
- `MetricsSnapshot` 暴露吞吐与积压：`received/accepted/rejected/applied/
  duplicates/conflicts/failed/retried`，以及 `QueueDepth/QueueMax` 和按
  原因分类的 `RejectReason`。

## 3. 幂等与状态冲突

- 事件用全局唯一 `ID` 去重：相同 ID 的重复投递/失败重试/重启重放都只会
  生效一次（`duplicates` 指标计数）。
- 每个业务键维护单调 `Version` 与近期已应用 ID 窗口：`Seq < 当前版本`的
  事件被显式标记为 **conflict**，状态不被覆盖（`conflicts` 指标计数），
  而不是静默吞掉或错误覆盖。

## 4. 异常链路归一化

所有来自下游/上下文/存储的错误都经 `normalize` 映射为 `engine.Error`
（带 `Kind`、操作名 `Op`、可读 `Message` 与 `Retryable()`），不直接透出
原始错误类型：

| 来源 | 归一化 Kind | 可重试 |
| --- | --- | --- |
| `context.DeadlineExceeded`（含被包装） | `timeout` | 是 |
| `context.Canceled` | `canceled` | 否 |
| Handler/存储返回普通错误 | `internal` / `storage` | 视情况 |

可重试错误按 `MaxAttempts` 与退避 `Backoff` 重试；超过次数仍失败的事件写入
**死信文件** `dead-letter.log`（含事件与归一化原因），并 Ack 以避免毒消息
永久阻塞同键后续事件。

## 5. 优雅停机与重启一致性

文件布局（位于 `DataDir`）：

| 文件 | 作用 |
| --- | --- |
| `events.log` / `events.offset` | 事件源（追加）与已确认投递偏移 |
| `pending.log` | 待处理事件日志（add/ack），Close 时压缩 |
| `state.wal` / `state.snapshot.json` | 业务状态 WAL 与周期快照 |
| `dead-letter.log` | 最终放弃事件及其归一化原因 |

停机两阶段：

1. 关闭接收（Submit 返回 `stopping`）、停止事件源；
2. 在 `ShutdownTimeout` 内等待排空（队列深度、在途信号量、每键 mailbox 与
   缺口堆全部归零）；随后关闭队列与协调协程并统一 fsync 落盘。
   - 超时未完成的事件**不 Ack**，保留在 `pending.log` 中；
   - 下次 `Start` 重放 `pending.log`，配合存储层幂等，做到**已有状态不
     丢失、事件不重复应用**。

事件源只在 sink（进入引擎）成功后推进 offset；下游拒绝/崩溃不推进，重启
从断点重读，配合幂等实现至少一次投递 + 精确一次生效。

## 6. 持久化级别（配置项 `SyncEachWrite`）

- `true`（默认）：每次确认通过组提交 `fsync`，操作系统崩溃也不丢已确认
  状态（沙箱磁盘 fsync 较昂贵，绝对吞吐受限）。
- `false`：WAL/日志采用微小时长（亚毫秒~毫秒）的组提交，吞吐显著更高；
  OS 崩溃可能丢失最近未落盘窗口内的数据，但事件源 offset 未推进，重启会
  重新投递，存储幂等保证结果一致；优雅停机会完整落盘。

## 7. 配置项（`engine.Config`）

| 字段 | 含义 | 默认 |
| --- | --- | --- |
| `DataDir` | 本地文件目录 | `data` |
| `Workers` | 队列分片/worker 数（向上取 2 的幂） | CPU 核数 |
| `QueuePerShard` | 每个分片有界队列容量 | 1024 |
| `MailboxPerKey` | 每业务键协调器 mailbox 容量 | 256 |
| `SubmitTimeout` | 队列满时 Submit 最长等待 | 50ms |
| `HandleTimeout` | 单事件处理截止 | 1s |
| `MaxAttempts` | 可重试错误最大尝试次数 | 3 |
| `Backoff` | 重试基础退避（第 n 次 n×Backoff） | 5ms |
| `ShutdownTimeout` | 优雅停机最长等待 | 5s |
| `SyncEachWrite` | 是否逐次 fsync（组提交） | true |

## 8. 测试覆盖

- 并发竞争下总量精确、不重不漏（独占键 + 同键高并发乱序）；
- 同键事件严格按 `Seq` 生效；
- 重复投递幂等、乱序事件显式冲突不覆盖；
- 容量背压以可区分原因拒绝并计入指标；停机期间拒绝为 `stopping`；
- 超时/取消/普通错误归一化为可分类别；可重试错误重试后进死信；
- 优雅停机后重启：状态不丢、pending 重放幂等、事件源 offset 断点续读。
- 全部用例在 `go test ./... -race` 下稳定通过，单测日志打印关键输入与
  判定依据（版本、值、指标分类）。
