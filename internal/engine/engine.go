// Package engine 实现有界队列 + 工作池 + 键级串行的高并发事件处理引擎。
package engine

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/lisama569/high-concurrency-engine-go/internal/event"
	"github.com/lisama569/high-concurrency-engine-go/internal/store"
)

// shardCount 是键锁分片数；同一业务键恒落同一分片，保证同键串行。
const shardCount = 256

// Config 引擎配置。
type Config struct {
	QueueCapacity int // 有界队列容量
	Workers       int // 并发工作者数
}

// Metrics 运行期可观测指标快照。
type Metrics struct {
	Submitted  uint64 // 成功入队
	Rejected   uint64 // 背压拒绝（队列满）
	Applied    uint64 // 实际生效
	Duplicated uint64 // 幂等去重命中
	Conflicts  uint64 // 键冲突
	Failed     uint64 // 处理失败
}

// Engine 是事件处理引擎。
type Engine struct {
	cfg   Config
	store store.Store

	queue   chan event.Event
	keyLock [shardCount]sync.Mutex

	submitMu sync.RWMutex // 保护 closed 与 queue 的关闭时序
	closed   bool

	workersWG sync.WaitGroup

	mSubmitted  atomic.Uint64
	mRejected   atomic.Uint64
	mApplied    atomic.Uint64
	mDuplicated atomic.Uint64
	mConflicts  atomic.Uint64
	mFailed     atomic.Uint64
}

// New 创建并启动引擎（工作者立即开始消费）。
func New(cfg Config, st store.Store) *Engine {
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 1
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	e := &Engine{
		cfg:   cfg,
		store: st,
		queue: make(chan event.Event, cfg.QueueCapacity),
	}
	e.workersWG.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		go e.worker()
	}
	return e
}

// Submit 提交事件。队列已满时立即返回 KindQueueFull 错误（背压拒绝，
// 绝不静默丢弃）；ctx 取消/超时则归一化为 KindCanceled/KindTimeout；
// 引擎已停机返回 KindCanceled。
func (e *Engine) Submit(ctx context.Context, ev event.Event) error {
	e.submitMu.RLock()
	defer e.submitMu.RUnlock()
	if e.closed {
		return event.New(event.KindCanceled, "engine: submit on closed engine")
	}
	// 先检查 ctx，保证取消/超时语义确定性地优先于入队与背压。
	select {
	case <-ctx.Done():
		return event.Normalize(ctx.Err())
	default:
	}
	select {
	case e.queue <- ev:
		e.mSubmitted.Add(1)
		return nil
	default:
		e.mRejected.Add(1)
		return event.New(event.KindQueueFull, "engine: queue full (capacity=%d)", e.cfg.QueueCapacity)
	}
}

// Shutdown 优雅停机：停止接收新事件，排尽队列中在途任务后落盘。
// ctx 超时时返回 KindTimeout，此时未排空的事件仍留在队列中，
// 已由 store 落盘的状态不会丢失。
func (e *Engine) Shutdown(ctx context.Context) error {
	e.submitMu.Lock()
	if e.closed {
		e.submitMu.Unlock()
		return nil
	}
	e.closed = true
	close(e.queue)
	e.submitMu.Unlock()

	done := make(chan struct{})
	go func() {
		e.workersWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return e.store.Close()
	case <-ctx.Done():
		return event.Normalize(ctx.Err())
	}
}

// Metrics 返回当前指标快照。
func (e *Engine) Metrics() Metrics {
	return Metrics{
		Submitted:  e.mSubmitted.Load(),
		Rejected:   e.mRejected.Load(),
		Applied:    e.mApplied.Load(),
		Duplicated: e.mDuplicated.Load(),
		Conflicts:  e.mConflicts.Load(),
		Failed:     e.mFailed.Load(),
	}
}

// worker 从队列消费事件直到队列关闭且排空。
func (e *Engine) worker() {
	defer e.workersWG.Done()
	for ev := range e.queue {
		e.process(ev)
	}
}

// process 对单条事件按键串行地应用，并更新指标。
func (e *Engine) process(ev event.Event) {
	lock := &e.keyLock[shard(ev.Key)]
	lock.Lock()
	defer lock.Unlock()

	applied, conflict, err := e.store.Apply(ev.Key, ev.Seq, ev.ID, ev.Payload)
	switch {
	case err != nil:
		e.mFailed.Add(1)
	case conflict:
		e.mConflicts.Add(1)
	case applied:
		e.mApplied.Add(1)
	default:
		e.mDuplicated.Add(1)
	}
}

// shard 返回键对应的分片下标。
func shard(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % shardCount
}
