package engine

import (
	"context"
	"path/filepath"
	"sync"
	"time"
)

// Engine 是高并发事件处理引擎的主体。
type Engine struct {
	cfg     Config
	handler Handler

	mu       sync.Mutex
	started  bool
	stopped  bool
	stopCh   chan struct{} // 停机开始：拒绝新提交、取消事件源
	finishCh chan struct{} // 排空结束或超时：终止协调协程与在途处理
	doneCh   chan struct{} // 关闭表示所有后台协程已退出
	wg       sync.WaitGroup

	store   Store
	journal Journal
	queue   *shardedQueue
	dlq     DeadLetter
	source  EventSource
	metrics *Metrics

	// sourceWG 单独等待事件投递协程，停机时先停源再停 worker。
	sourceWG sync.WaitGroup
	// sourceCancel 取消后台事件流。
	sourceCancel context.CancelFunc

	// sequencer 负责按业务键 Seq 的重排序与按序生效。
	sequencer *sequencer
}

// New 按配置构造引擎；此时不创建任何文件。
func New(cfg Config, handler Handler) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if handler == nil {
		handler = SumHandler
	}
	return &Engine{
		cfg:      cfg,
		handler:  handler,
		stopCh:   make(chan struct{}),
		finishCh: make(chan struct{}),
		doneCh:   make(chan struct{}),
	}, nil
}

// SumHandler 是内置的确定性示例处理器：把事件负载累加到业务键值上。
// 处理器不做任何副作用，因此同一事件重试任意次得到的 next 完全一致。
func SumHandler(ctx context.Context, ev Event, current KeyState) (KeyState, error) {
	return KeyState{
		Key:   ev.Key,
		Value: current.Value + ev.Payload,
	}, nil
}

// Start 打开本地状态/日志、重放未确认事件、启动 worker 与事件源。
func (e *Engine) Start(ctx context.Context) error {
	const op = "engine.start"
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return newError(KindInvalid, op, "engine already started", nil)
	}
	e.started = true
	e.mu.Unlock()

	var err error
	dir := e.cfg.DataDir
	if e.store, err = openStoreWith(dir, e.cfg.SyncEachWrite); err != nil {
		return err
	}
	if e.journal, err = openJournalWith(filepath.Join(dir, "pending.log"), e.cfg.SyncEachWrite); err != nil {
		return err
	}
	if e.dlq, err = openDeadLetter(filepath.Join(dir, "dead-letter.log")); err != nil {
		return err
	}
	e.queue = newShardedQueue(e.cfg.Workers, e.cfg.QueuePerShard)
	e.metrics = newMetrics(e.queue.capacity(), e.queue.depth)
	e.sequencer = newSequencer()

	// 启动 worker（与队列分片一一对应，保证同键串行）。
	for i := 0; i < e.cfg.Workers; i++ {
		e.wg.Add(1)
		go e.runWorker(i)
	}

	// 重放上次未 Ack 的事件：它们已在持久日志中，直接重新入队即可，
	// 不再次 Append；重复应用由存储层幂等判重拦截。
	pending, err := e.journal.Replay(ctx)
	if err != nil {
		return err
	}
	for _, ev := range pending {
		if perr := e.enqueueReplay(ev); perr != nil {
			return perr
		}
	}

	// 打开并启动后台事件源。
	if e.source, err = openFileSource(
		filepath.Join(dir, "events.log"),
		filepath.Join(dir, "events.offset"),
	); err != nil {
		return err
	}
	sctx, cancel := context.WithCancel(context.Background())
	e.sourceCancel = cancel
	e.sourceWG.Add(1)
	go func() {
		defer e.sourceWG.Done()
		_ = e.source.Stream(sctx, func(ev Event) *Error {
			return e.Submit(sctx, ev)
		})
	}()

	return nil
}

// enqueueReplay 把日志中恢复的事件重新放入内存队列。
func (e *Engine) enqueueReplay(ev Event) *Error {
	return e.queue.Submit(context.Background(), queuedItem{ev: ev, attempt: 1})
}

// submitCtx 返回用于 Submit 等待的上下文：受 SubmitTimeout、外部 ctx 与
// 停机信号三者共同约束。返回的 cancel 必须在 Submit 结束时调用。
func (e *Engine) submitCtx(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancelTimeout := context.WithTimeout(parent, e.cfg.SubmitTimeout)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-e.finishCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, func() {
		cancel()
		cancelTimeout()
	}
}

// Submit 供外部直接投递一个事件：先持久化进入待处理日志，再进入有界队列。
//
// 拒绝路径（事件不被静默丢弃，原因可区分）：
//   - KindInvalid   事件字段不合法；
//   - KindStopping  停机中；
//   - KindQueueFull 队列满且在 SubmitTimeout 内未获得空位；
//   - KindStorage   持久日志写入失败（事件未入队，无重复风险）。
func (e *Engine) Submit(ctx context.Context, ev Event) *Error {
	const op = "engine.submit"
	e.metrics.recordReceived()

	if err := ev.validate(); err != nil {
		e.metrics.recordRejected(KindInvalid)
		return normalize(op, err)
	}
	if ev.EnqueueAt.IsZero() {
		ev.EnqueueAt = time.Now()
	}

	e.mu.Lock()
	stopping := e.stopped
	e.mu.Unlock()
	if stopping {
		e.metrics.recordRejected(KindStopping)
		return newError(KindStopping, op,
			"engine is shutting down; reject new event "+ev.ID, nil)
	}

	// 先落盘待处理日志，再入内存队列，保证任何拒绝/崩溃后事件仍可重放。
	if err := e.journal.Append(ctx, ev); err != nil {
		e.metrics.recordRejected(KindStorage)
		return normalize(op, err)
	}

	qctx, cancel := e.submitCtx(ctx)
	defer cancel()
	if perr := e.queue.Submit(qctx, queuedItem{ev: ev, attempt: 1}); perr != nil {
		// 入队失败（背压/停机）：事件已在持久日志里，拒绝原因可区分，
		// 不会丢失；调用方可按 Retryable 决定稍后重投或依赖重启重放。
		e.metrics.recordRejected(perr.Kind)
		return perr
	}
	e.metrics.recordAccepted()
	return nil
}

// runWorker 消费一个分片，把每个事件路由到其业务键的串行协调协程。同键
// 顺序由 keyCoordinator 保证；不同键的协调协程彼此并行。
func (e *Engine) runWorker(idx int) {
	defer e.wg.Done()
	ch := e.queue.shardChan(idx)
	for item := range ch {
		e.metrics.incInFlight()
		// deliver 在该键 mailbox 满时会阻塞等待空位（或停机返回 false）。
		// mailbox 容量按同键突发设计，正常不会长期阻塞；不同键不互相阻塞。
		delivered := e.dispatch(item)
		if !delivered {
			// 停机导致未能交付：不 Ack，事件保留在 pending.log 中重放。
			e.metrics.decInFlight()
			continue
		}
		// 已交给协调协程：在途生命周期由事件完成（Ack）时通过 metrics 反映。
		// 这里的在途计数在事件真正出队即计入，协调协程完成后通过队列深度与
		// outstandingCount 共同判定排空。
		e.metrics.decInFlight()
	}
}

// finalize 落定单事件的最终结论：指标、Ack 与死信。
//   - applied/duplicate/conflict：记录指标并 Ack；
//   - failed 且因停机取消：不 Ack，保留待重放；
//   - failed 且尝试耗尽：写死信（原因可区分）后 Ack，避免毒消息阻塞同键。
func (e *Engine) finalize(item queuedItem, outcome Outcome, perr *Error) {
	ev := item.ev
	switch outcome {
	case OutcomeApplied, OutcomeDuplicate, OutcomeConflict:
		e.metrics.recordOutcome(outcome)
		// Ack 异步落盘：即使丢失，重启重放也会被存储层判重，安全。
		_ = e.journal.AckAsync(ev.ID)
		return
	}
	if perr != nil && perr.Kind == KindCanceled {
		return // 安全重排：不 Ack、不写死信
	}
	e.metrics.recordOutcome(OutcomeFailed)
	if perr == nil {
		perr = newError(KindInternal, "engine.finalize", "event failed without error", nil)
	}
	if werr := e.dlq.Write(ev, perr); werr != nil {
		return // 死信落盘失败：不 Ack，保留重放
	}
	_ = e.journal.AckAsync(ev.ID)
}

// processWithRetry 对一个已轮到序号的事件执行有界退避重试，返回最终结论。
func (e *Engine) processWithRetry(item queuedItem) (Outcome, *Error) {
	const op = "engine.process"
	ev := item.ev
	attempt := item.attempt

	var lastErr *Error
	for attempt <= e.cfg.MaxAttempts {
		if attempt > item.attempt {
			e.metrics.recordRetry()
			if !e.sleepBackoff(attempt - 1) {
				// 停机打断退避：给出 canceled 结论；上层不 Ack，事件重放。
				return OutcomeFailed, newError(KindCanceled, op,
					"retry backoff interrupted by shutdown", nil)
			}
		}

		outcome, perr := e.processOnce(ev)
		switch outcome {
		case OutcomeApplied, OutcomeDuplicate, OutcomeConflict:
			return outcome, nil
		case OutcomeFailed:
			lastErr = perr
			if perr != nil && perr.Kind.Retryable() && attempt < e.cfg.MaxAttempts {
				attempt++
				continue
			}
			if lastErr == nil {
				lastErr = newError(KindInternal, op, "event failed without explicit error", nil)
			}
			return OutcomeFailed, lastErr
		}
	}
	if lastErr == nil {
		lastErr = newError(KindInternal, op, "retry budget exhausted without error", nil)
	}
	return OutcomeFailed, lastErr
}

// sleepBackoff 在退避期间响应停机信号；返回 false 表示被停机打断。
func (e *Engine) sleepBackoff(n int) bool {
	d := time.Duration(n) * e.cfg.Backoff
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-e.stopCh:
		return false
	case <-t.C:
		return true
	}
}

// handleContext 返回单个事件处理的上下文：同时受 HandleTimeout 与停机
// 信号约束，停机时在途 Handler 立即收到取消，保证优雅停机可终止慢任务。
func (e *Engine) handleContext() (context.Context, context.CancelFunc) {
	ctx, cancelTimeout := context.WithTimeout(context.Background(), e.cfg.HandleTimeout)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-e.finishCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, func() {
		cancel()
		cancelTimeout()
	}
}

// processOnce 执行单次处理：在存储层原子地读-改-写。
//
// Handler 在业务键分片锁内被调用并施加 HandleTimeout；Handler 返回的
// 任意错误都被归一化，不直接透出。
func (e *Engine) processOnce(ev Event) (Outcome, *Error) {
	const op = "engine.processOnce"
	ctx, cancel := e.handleContext()
	defer cancel()

	_, verdict, aerr := e.store.Modify(ctx, ev, func(cur KeyState) (KeyState, error) {
		return e.handler(ctx, ev, cur)
	})
	if aerr != nil {
		return OutcomeFailed, normalize(op, aerr)
	}
	switch verdict {
	case ApplyOK:
		return OutcomeApplied, nil
	case ApplyDuplicate:
		return OutcomeDuplicate, nil
	case ApplyConflict:
		return OutcomeConflict, newError(KindConflict, op,
			"stale seq for key "+ev.Key, nil)
	default:
		return OutcomeFailed, newError(KindInternal, op, "unknown apply verdict", nil)
	}
}

// Shutdown 优雅停机：
//  1. 标记停机，停止接收新事件（Submit 立即返回 KindStopping）；
//  2. 取消事件源投递协程；
//  3. 在 ShutdownTimeout 内等待在途任务排空（队列清空、worker 空闲）；
//  4. 超时仍未完成的事件保留在 pending.log 中，关闭队列与存储，
//     下次 Start 时安全重放，已有状态不丢失、不重复。
func (e *Engine) Shutdown(ctx context.Context) error {
	const op = "engine.shutdown"
	e.mu.Lock()
	if !e.started || e.stopped {
		e.mu.Unlock()
		return newError(KindInvalid, op, "engine not running", nil)
	}
	e.stopped = true
	e.mu.Unlock()

	close(e.stopCh)
	e.queue.markStopping()

	// 停止事件源投递，不再有新的持久化事件进入。
	if e.sourceCancel != nil {
		e.sourceCancel()
	}
	e.sourceWG.Wait()
	if e.source != nil {
		_ = e.source.Close()
	}

	// 第一阶段：在保留 worker 与协调协程运行的前提下等待自然排空——队列中
	// 无积压、所有业务键协调协程也没有未完成事件（mailbox + 缺口堆）。
	drainCtx, cancelDrain := context.WithTimeout(ctx, e.cfg.ShutdownTimeout)
	defer cancelDrain()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	drained := false
	for {
		if e.queue.depth() == 0 && e.outstandingCount() == 0 {
			drained = true
			break
		}
		select {
		case <-drainCtx.Done():
			drained = false
		case <-ticker.C:
		}
		if drainCtx.Err() != nil {
			break
		}
	}

	// 第二阶段：先通知所有业务键协调协程停机，解除它们在处理/退避/缺口上
	// 的阻塞；未 Ack 事件保留在 pending.log 由下次启动重放。
	e.stopCoordinators()
	// 再关闭分片通道并等待 worker 退出。此时 deliver 若仍阻塞会因 stopCh
	// 立即返回 false，worker 得以收尾。
	e.queue.close()
	e.wg.Wait()

	var firstErr error
	if e.dlq != nil {
		if err := e.dlq.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if e.journal != nil {
		// Close 会压缩日志：仅保留仍未 Ack 的事件。
		if err := e.journal.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if e.store != nil {
		if err := e.store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	close(e.doneCh)

	if !drained {
		return newError(KindTimeout, op,
			"shutdown timeout reached; in-flight events retained in pending log for replay",
			context.DeadlineExceeded)
	}
	return firstErr
}

// Metrics 返回当前可观测指标快照。
func (e *Engine) Metrics() MetricsSnapshot {
	if e.metrics == nil {
		return MetricsSnapshot{RejectReason: map[string]int64{}}
	}
	return e.metrics.Snapshot()
}

// StateSnapshot 返回全部业务键状态副本。
func (e *Engine) StateSnapshot(ctx context.Context) (map[string]KeyState, error) {
	if e.store == nil {
		return map[string]KeyState{}, nil
	}
	return e.store.Snapshot(ctx)
}
