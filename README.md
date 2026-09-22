# 高并发事件处理引擎（本地模拟）

用内存有界队列与本地 JSON 文件模拟事件来源与状态存储，在并发压力、异常处置与重启场景下保证处理结果一致且可解释。

## 架构与并发模型

```
生产者(goroutine N) --Submit--> 有界队列(channel, 容量可配) --worker M 个--> 键分片锁(256) --> FileStore(原子落盘)
```

- **有界队列**：`chan event.Event`，容量由 `Config.QueueCapacity` 决定，是背压的边界。
- **工作池**：`Config.Workers` 个 goroutine 从队列消费；队列关闭后排空剩余事件再退出。
- **键级串行**：业务键经 FNV 哈希落入 256 个分片锁之一。同一键的事件在锁内串行应用，不会乱序生效；不同键（不同分片）并行处理。
- **注意**：多工作者下"处理顺序"不等于"提交顺序"。同键事件若乱序到达存储层，靠序号（Seq）判定：过期事件被标记冲突而非静默覆盖。

## 背压规则

`Submit` 的返回语义（绝不静默丢弃）：

| 情形 | 返回 | 指标 |
|---|---|---|
| 成功入队 | `nil` | `Submitted+1` |
| 队列已满 | `*event.Error{Kind: KindQueueFull}` | `Rejected+1` |
| ctx 取消/超时 | `KindCanceled` / `KindTimeout`（归一化） | — |
| 引擎已停机 | `KindCanceled` | — |

调用方收到 `KindQueueFull` 后可退避重试（延迟）或按业务策略降级（拒绝），演示程序 `cmd/engine` 采用指数退避重试。

## 幂等与冲突规则（存储层）

`FileStore.Apply(key, seq, eventID, value)`：

1. **幂等去重**：`eventID` 已在该键 `Applied` 列表中 → 不重复应用，返回 `applied=false`（指标 `Duplicated+1`）。
2. **冲突标记**：`seq <= 当前已生效 Seq` → 不覆盖状态，键被标记 `Conflict=true` 并记录冲突事件 ID（指标 `Conflicts+1`）。
3. **正常生效**：更新 `Seq/Value`，追加 `Applied`，原子落盘（写临时文件 → fsync → rename，崩溃不会出现半写文件）。

## 异常归一化

所有对外错误统一为 `*event.Error{Kind, Msg, Err}`，原始错误仅作日志原因（`Unwrap`），不向调用方透出语义：

- `context.DeadlineExceeded`（含包装）→ `KindTimeout`
- `context.Canceled`（含包装）→ `KindCanceled`
- 其他未知错误 → `KindInternal`
- 用 `event.IsKind(err, kind)` 判定类别。

## 优雅停机与重启

- `Shutdown(ctx)`：停止接收新事件（之后的 `Submit` 返回 `KindCanceled`），关闭队列并等待 worker 排尽在途任务，最后落盘关闭存储；ctx 超时返回 `KindTimeout`。
- **重启一致性**：状态文件包含每键已应用事件 ID 列表，重启后重复投递命中幂等去重，不会重复应用；已落盘状态不丢失。

## 配置项

| 配置 | 说明 | 默认（非法值时） |
|---|---|---|
| `Config.QueueCapacity` | 有界队列容量，背压阈值 | 1 |
| `Config.Workers` | 并发消费工作者数 | 1 |
| `cmd/engine -state` | 状态文件路径 | `data/state.json` |
| `cmd/engine -events/-producers/-workers/-queue` | 演示参数 | 1000/8/4/64 |

## 运行与验证

```bash
# 演示：并发提交、背压退避、优雅停机；重复运行可观察幂等去重
go run ./cmd/engine -events 2000

# 测试（含竞态检测，重复 5 轮验证稳定性）
go test -race -count=5 ./...

# 查看单测中的关键输入与判定依据日志
go test -race ./internal/engine/ -v
```

测试覆盖：

- `internal/event`：超时/取消/内部错误的归一化映射与已归一化错误的透传。
- `internal/store`：幂等应用、乱序冲突标记、重启后状态恢复、同键并发应用不变量（最终 Seq=最大值，每条事件恰好归类一次）。
- `internal/engine`：并发下同键最终状态可复现、容量 1 时的背压拒绝计数（`Submitted+Rejected=总尝试数`）、停机排尽在途任务、重启后重投零重复应用、取消上下文的归一化错误。
