package engine

import (
	"context"
	"path/filepath"
	"sync"
	"time"
)

// handoffBackoff 是 worker 向某键交接失败（mailbox/在途暂时满）、把事件
// 退回调度器让位后的短暂退避，防止无可服务键时空转，同时给该键协调协程
// 排空 mailbox 的时间片。
const handoffBackoff = 200 * time.Microsecond

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
	sched   *fairScheduler
	dlq     DeadLetter
	source  EventSource
	metrics *Metrics

	// sourceWG 单独等待事件投递协程，停机时先停源再停 worker。
	sourceWG sync.WaitGroup
	// sourceCancel 取消后台事件流。
	sourceCancel context.CancelFunc

	// sequencer 负责按业务键 Seq 的重排序与按序生效。
	sequencer *sequencer
	// inflightSem 限制系统中“已接收待结论”的事件总数，构成处理侧背压。
	inflightSem *countingSem

	// testOnPop 仅测试用：Start 创建调度器后透传为出队观测回调。
	testOnPop func(queuedItem)
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
	e.sched = newFairScheduler(nextPow2(e.cfg.Workers)*e.cfg.QueuePerShard,
		e.cfg.HotKeyBacklog)
	e.sched.onPop = e.testOnPop
	e.metrics = newMetrics(e.sched.depth, e.backlogStats)
	e.sequencer = newSequencer()
	// 在途上限 = 队列总容量 + 每键 mailbox 总量上界，给处理侧留足缓冲，
	// 超出后事件停留在有界队列并按 SubmitTimeout 触发容量背压。
	e.inflightSem = newCountingSem(e.sched.capacity() + e.cfg.Workers*e.cfg.MailboxPerKey)

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

// enqueueReplay 把日志中恢复的事件重新放入内存调度队列。重放事件此前已被
// 持久化接收，因此走不让位、容量满则等待的 SubmitReplay，绝不能因
// priority_shed 被拒绝。
func (e *Engine) enqueueReplay(ev Event) *Error {
	return e.sched.SubmitReplay(context.Background(), queuedItem{ev: ev, attempt: 1})
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
		e.metrics.recordRejected(KindInvalid, ev.Priority)
		return normalize(op, err)
	}
	if ev.EnqueueAt.IsZero() {
		ev.EnqueueAt = time.Now()
	}

	e.mu.Lock()
	stopping := e.stopped
	e.mu.Unlock()
	if stopping {
		e.metrics.recordRejected(KindStopping, ev.Priority)
		return newError(KindStopping, op,
			"engine is shutting down; reject new event "+ev.ID, nil)
	}

	// 先落盘待处理日志，再入内存队列，保证任何拒绝/崩溃后事件仍可重放。
	if err := e.journal.Append(ctx, ev); err != nil {
		e.metrics.recordRejected(KindStorage, ev.Priority)
		return normalize(op, err)
	}

	qctx, cancel := e.submitCtx(ctx)
	defer cancel()
	if perr := e.sched.Submit(qctx, queuedItem{ev: ev, attempt: 1}); perr != nil {
		// 入队失败（容量背压/优先级让位/停机）：事件已在持久日志里，拒绝
		// 原因可区分，不会丢失；调用方可按 Retryable 决定稍后重投或依赖重启
		// 重放。
		e.metrics.recordRejected(perr.Kind, ev.Priority)
		return perr
	}
	e.metrics.recordAccepted(ev.Priority)
	return nil
}

// runWorker 从公平调度器领取一个“键交接”，尝试把队头事件投递到该键协调协程。
// 调度器对每个业务键做单航班交接：claim 时该键离环，因此同一时刻一个键只有
// 一件事件在交接路上，保证进入该键 mailbox 的顺序恒等于出队顺序（同键不乱
// 序），即便多个 worker 并发也如此。
//
// 交接结论三选一：
//   - 投递成功（mailbox 有空位、在途有槽）：confirmDelivered，仍有积压的键
//     被挂到环尾，与其它键逐键轮转；
//   - 该键暂时满（mailbox 满或在途信号量耗尽）：returnHead 把事件放回该键
//     队头并把键挂环尾让位，本 worker 立即转而服务其它键——慢业务键只拖自己，
//     不能霸占消费能力；
//   - 停机：dropHead 放弃交接，事件未 Ack，保留 pending.log 重放。
func (e *Engine) runWorker(_ int) {
	defer e.wg.Done()
	for {
		kq, item, ok := e.sched.pop()
		if !ok {
			return // 调度器已关闭且取空
		}
		k := e.coordinatorFor(item.ev.Key)
		switch k.deliver(item) {
		case 1:
			e.sched.confirmDelivered(kq)
		case 0:
			if e.isFinish() {
				e.sched.dropHead(kq) // 停机：不重投、不 Ack，等待重放
			} else {
				// 让位：本轮先服务其它键。这里短暂退避，避免“无其它可服务
				// 键且该键暂时满”时 worker 在 claim/退回 上空转烧 CPU；把
				// 时间片让给该键协调协程排空 mailbox。
				e.sched.returnHead(kq, item)
				time.Sleep(handoffBackoff)
			}
		default: // -1：停机
			e.sched.dropHead(kq)
		}
	}
}

// isFinish 报告引擎是否已进入最终收尾阶段（finishCh 关闭）。
func (e *Engine) isFinish() bool {
	select {
	case <-e.finishCh:
		return true
	default:
		return false
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
	e.sched.markStopping()

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
		// depth=仍在调度队列；inFlightHandoffs=已取出但尚未进入 coordinator
		// mailbox 的交接中事件；outstandingCount=协调器侧在途。三者皆零才算
		// 真正排空，避免在“claim 后、交接完成前”的窗口误判。
		if e.sched.depth() == 0 && e.sched.inFlightHandoffs() == 0 && e.outstandingCount() == 0 {
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
	// 再关闭调度器：残余事件先被 pop 出（随后因 finishCh 不 Ack），取空后
	// worker 退出；未 Ack 事件保留在 pending.log 由下次启动重放。
	e.sched.close()
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

// backlogStats 合并调度器（按优先级可拆）与每键协调器两段的在途数据，
// 产出按优先级与业务键维度的积压快照，并依据调度器阈值标记热点键。
func (e *Engine) backlogStats() BacklogSnapshot {
	st := e.sched.stats()
	coord := e.outstandingByKey()

	merged := make(map[string]*KeyBacklog, len(st.Keys)+len(coord))
	get := func(k string) *KeyBacklog {
		b, ok := merged[k]
		if !ok {
			b = &KeyBacklog{Key: k}
			merged[k] = b
		}
		return b
	}
	for i := range st.Keys {
		ks := st.Keys[i]
		b := get(ks.Key)
		b.Sched = ks.Total
		b.Normal += ks.ByPri[PriorityNormal]
		b.High += ks.ByPri[PriorityHigh]
		if ks.Hot {
			b.Hot = true
		}
	}
	for k, st := range coord {
		if st.total <= 0 {
			continue
		}
		b := get(k)
		b.Coord = st.total
		b.Normal += st.byPri[PriorityNormal]
		b.High += st.byPri[PriorityHigh]
	}
	out := BacklogSnapshot{
		Capacity:     st.Capacity,
		Depth:        st.Depth,
		Normal:       st.ByPri[PriorityNormal],
		High:         st.ByPri[PriorityHigh],
		HotThreshold: st.HotThreshold,
	}
	for _, b := range merged {
		b.Total = b.Sched + b.Coord
		if b.Total > 0 {
			// 热点键按“该键总积压（调度队列 + 协调器）”是否达到阈值识别，
			// 即使事件已被快速拉进该键 mailbox，也不会无声无息地拖慢全局。
			if b.Total >= st.HotThreshold {
				b.Hot = true
				out.HotKeyCount++
			}
			out.Keys = append(out.Keys, *b)
		}
	}
	sortKeyBacklogs(out.Keys)
	return out
}

// Metrics 返回当前可观测指标快照。
func (e *Engine) Metrics() MetricsSnapshot {
	if e.metrics == nil {
		return MetricsSnapshot{
			RejectReason:    map[string]int64{},
			AcceptedByPri:   map[string]int64{},
			RejectedByPri:   map[string]int64{},
			RejectByKindPri: map[string]int64{},
			QueueDepthByPri: map[string]int{},
		}
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
