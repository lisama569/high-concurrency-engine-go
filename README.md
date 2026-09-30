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
              统一公平调度器（优先级环 + 业务键 round-robin）
 事件来源 ──► 高优环[非空业务键环形链表] ─┐
(文件+offset) 普通环[非空业务键环形链表] ─┼─► worker ×N ──► keyCoordinator(actor) ──► 状态存储
   │          （有界总容量、单航班交接）  │      （每业务键一个，按 Seq 串行生效）
   └─ Submit() 先写 pending.log 再入队 ───┘
```

- **统一公平调度器**：所有 worker 共享一个按“两级优先级 × 业务键轮转”的
  有界内存调度器（详见 1.1）。不同业务键天然并行，同一业务键严格串行，
  热点键无法长期霸占消费能力。
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

## 1.1 优先级调度与业务键公平

在原“有界队列 + 每键 actor”之上新增一层**统一公平调度器** `fairScheduler`，
所有 worker 从它领取事件：

```
                      ┌─────────── 高优先级环（非空业务键环形链表）
Submit ──► 有界总容量 ─┤  ring: [k2]⇄[k5]⇄[k1]   ← 每轮每个键只取队头一件
                      └─────────── 普通优先级环（同上）
                              │ pop：先高后低；环内逐键 round-robin
                              ▼
              worker ──(单航班交接)──► 该业务键 keyCoordinator(actor)
```

**优先级（跨键）**

- 事件通过 `Event.Priority` 指定：`0=PriorityNormal`（默认）、`1=PriorityHigh`。
- 调度器出队时**只要还存在高优先级事件，就先消费高优先级环**，因此拥塞窗口
  内高优事件比普通事件更早被消费。
- **同键顺序不被优先级打乱**：每个业务键在调度器内是一条严格 FIFO 队列，
  同键事件无论优先级高低，一律按到达顺序排列；某键挂在哪条环上只由其**队头**
  事件的优先级决定。最终生效顺序仍由该键 actor 按投递/`Seq` 顺序保证。
  即“**跨键按优先级，同键按顺序**”。

**业务键公平（热点键不霸占）**

- 每条优先级环是一张“非空业务键”环形链表。每次只取环头键的队头一件，随后
  把该键转到环尾。于是单个热点键即便积压成千上万件，也必须与其它键**逐键
  轮转**，冷键的事件不会被长期压在后面。
- 某键的 coordinator mailbox 满（或在途信号量耗尽）时，worker **不阻塞等待**，
  而是把该事件退回该键队头、把该键挂到环尾**本轮让位**，立刻去服务其它键；
  待该键排空后自然重新轮到。慢业务键只拖自己。
- 为保证多 worker 并发下同键不乱序，调度器对每个业务键做**单航班交接**：
  领取（claim）某键队头时把该键摘环，直到这件确认进入该键 mailbox（成功 /
  退回 / 停机丢弃三选一）后才重新挂环。同一时刻一个键最多一件在交接路上，
  mailbox 的插入顺序恒等于出队顺序。

**热点键识别（可观测而非静默拖慢）**

- 业务键的在途积压（调度队列段 + coordinator mailbox/缺口堆/处理中段）达到
  配置阈值 `HotKeyBacklog` 时，在指标中标记为热点键：
  `MetricsSnapshot.HotKeyCount` 与每个键的 `KeyBacklogs[i].Hot`。阈值只影响
  标记，不改变公平轮转行为。

## 2. 背压、优先级让位与可观测性

- 处理能力不足时，事件**不会被静默丢弃**。`Submit` 在截止时间内拿不到队列
  空位会返回带明确类别的归一化错误：
  - `queue_full`：**真正的容量耗尽**（队列满且等待 `SubmitTimeout` 超时），
    可重试；
  - `priority_shed`：**优先级让位**——队列被高优先级事件占满时，普通优先级
    事件**立即**被拒绝（不干等超时），与“真正容量耗尽”明确区分，可重试；
    高优先级事件永远不会因为让位而被拒绝（它只会在容量耗尽时得到
    `queue_full`）；
  - `stopping`：引擎停机中，事件已在持久日志里（安全重排）；
  - 以及 `invalid` / `storage` / `canceled` / `timeout`。
  - 注意：`priority_shed` 只作用于**新提交**的普通事件。`pending.log` 中
    已被持久化接收的事件在重启重放走专用路径（等价于不让位），容量满时等待
    空位，绝不会因让位而无法重放。
- `MetricsSnapshot` 在原有吞吐/积压指标上新增**按优先级与业务键维度**：
  - 原有：`received/accepted/rejected/applied/duplicates/conflicts/failed/
    retried`、`QueueDepth/QueueMax`、`RejectReason`（按原因）；
  - 新增 `AcceptedByPri` / `RejectedByPri`（`normal`/`high`）；
  - 新增 `RejectByKindPri`（原因×优先级，如 `queue_full:high`、
    `priority_shed:normal`），一眼区分容量耗尽与优先级让位；
  - 新增 `QueueDepthByPri`（当前各优先级在调度队列的积压）；
  - 新增 `HotThreshold`、`HotKeyCount` 与 `KeyBacklogs`（逐键 `total /
    sched / coord / normal / high / hot`，按总量降序），合并了“调度队列段”
    与“每键 coordinator 段”两段积压。

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
2. 在 `ShutdownTimeout` 内等待排空（调度队列深度、**调度器→coordinator 的
   交接中事件**、在途信号量、每键 mailbox 与缺口堆全部归零）；随后关闭
   调度器与协调协程并统一 fsync 落盘。
   - 超时未完成的事件**不 Ack**，保留在 `pending.log` 中；
   - 下次 `Start` 重放 `pending.log`，配合存储层幂等，做到**已有状态不
     丢失、事件不重复应用**。

事件优先级是 `Event` 的普通字段，随 `pending.log` / `events.log` 的逐行
JSON 一起持久化；停机未决事件在重启重放后**优先级保持不变**。调度器与环
都是纯内存状态，停机即丢弃（它们只决定消费先后，不承载任何未持久化数据），
因此新增的优先级/公平状态不会破坏既有的排空与重启一致性。

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
| `Workers` | 共享调度器的 worker 数（消费者并发，向上取 2 的幂用于容量推导） | CPU 核数 |
| `QueuePerShard` | 每单位的有界队列容量；调度器总容量 = 2 的幂(`Workers`) × `QueuePerShard` | 1024 |
| `MailboxPerKey` | 每业务键协调器 mailbox 容量 | 256 |
| `HotKeyBacklog` | 热点键识别阈值：单键在途积压（调度队列+coordinator）达到即标记热点 | 128 |
| `SubmitTimeout` | 队列满时 Submit 最长等待（让位拒绝不等待） | 50ms |
| `HandleTimeout` | 单事件处理截止 | 1s |
| `MaxAttempts` | 可重试错误最大尝试次数 | 3 |
| `Backoff` | 重试基础退避（第 n 次 n×Backoff） | 5ms |
| `ShutdownTimeout` | 优雅停机最长等待 | 5s |
| `SyncEachWrite` | 是否逐次 fsync（组提交） | true |

## 8. 测试覆盖

原有能力（不回归）：

- 并发竞争下总量精确、不重不漏（独占键 + 同键高并发乱序）；
- 同键事件严格按 `Seq` 生效；
- 重复投递幂等、乱序事件显式冲突不覆盖；
- 容量背压以可区分原因拒绝并计入指标；停机期间拒绝为 `stopping`；
- 超时/取消/普通错误归一化为可分类别；可重试错误重试后进死信；
- 优雅停机后重启：状态不丢、pending 重放幂等、事件源 offset 断点续读。

新增调度与公平性：

- 调度器白盒：跨键高优先于普通出队、同优先级逐键 round-robin、**同键
  严格 FIFO 不被优先级打乱**、多 worker 并发领取的单航班 FIFO；
- 拒绝原因区分：`priority_shed`（让位，立即返回）与 `queue_full`（容量
  耗尽，超时返回），并校验“全普通拥塞不会被误报为让位”；
- 引擎级：混合优先级不丢不重且终态正确、**同键高低优先级交错仍严格按
  投递/`Seq` 生效**、热点键积压时冷键仍快速被处理且热点被指标识别；
- 指标维度：按优先级的接收/拒绝、按“原因×优先级”的拒绝、按优先级与业务
  键的积压与热点标记；
- 停机/重启：优先级随 `pending.log` 持久化，小容量短停机超时的多轮
  “投递中停机 → 重启重放”压力下，所有事件最终恰好生效一次。
- 全部用例在 `go test ./... -race` 下多轮稳定通过，单测日志打印关键输入
  与判定依据（版本、值、指标分类、出队顺序、热点键积压）。

## 9. 本地验证

```bash
go run ./cmd/demo        # 演示优先级/让位拒绝/热点键/公平与重启幂等
go test ./...            # 完整单测
go test ./... -race      # 并发检测（推荐）
go test ./... -race -count=10   # 多轮稳定性
go test ./engine -race -run 'Scheduler|Priority|HotKey|Shutdown' -v  # 只看新增能力
```

demo 中刻意使用很小的容量，可直接观察到：`rejectReasons` 中的
`priority_shed`（普通事件让位并由生产端重试成功）、冷键高优事件在热点键
突发下毫秒级生效、以及 `hot keys` 段对热点键的逐键积压与 `hot=true` 标记。

