package engine

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// reorderPumpInterval 是后台重投 pump 的扫描周期。
const reorderPumpInterval = 2 * time.Millisecond

// pendingHeap 是某个业务键上“因序号缺口而暂缓”的最小序号堆。
type pendingHeap []queuedItem

func (h pendingHeap) Len() int           { return len(h) }
func (h pendingHeap) Less(i, j int) bool { return h[i].ev.Seq < h[j].ev.Seq }
func (h pendingHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *pendingHeap) Push(x any)        { *h = append(*h, x.(queuedItem)) }
func (h *pendingHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// keyState_order 保存某个业务键的暂缓堆与判序信息。
type keyOrder struct {
	mu   sync.Mutex
	heap pendingHeap
}

// sequencer 管理按业务键 Seq 的重排序，且不长期占用 worker。
//
// 核心机制：worker 取到事件后调用 admit：
//   - 序号衔接（== 当前版本+1）：admit 返回该事件本身，交由 worker 处理；
//   - 序号超前（缺口）：事件被放入该键暂缓堆，admit 返回 false，worker
//     立即回去继续消费 channel（不睡眠、不占用线程）；
//   - 序号落后：admit 返回该事件本身，由存储层判重/判冲突。
//
// 每次成功应用（版本推进）后调用 release：把暂缓堆中所有“此刻序号已衔接”
// 的事件重新入回分片 channel。它们会被任意空闲 worker 再次取到并 admit，
// 从而最终按序生效。重新入队使用无阻塞方式，队列满时保留在暂缓堆中，由
// 后续 release 或停机前的统一重投继续推进。
type sequencer struct {
	mu     sync.Mutex
	keys   map[string]*keyOrder
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newSequencer() *sequencer {
	return &sequencer{keys: make(map[string]*keyOrder), stopCh: make(chan struct{})}
}

// startPump 启动后台兜底重投协程：周期性扫描所有存在暂缓事件的业务键，
// 把序号已衔接的事件重新投回队列。它保证“前驱已到但一次 release 因队列
// 满未能重投”的事件不会永久滞留。停机时由 stopPump 结束。
func (sq *sequencer) startPump(requeue func()) {
	sq.wg.Add(1)
	go func() {
		defer sq.wg.Done()
		t := time.NewTicker(reorderPumpInterval)
		defer t.Stop()
		for {
			select {
			case <-sq.stopCh:
				return
			case <-t.C:
				requeue()
			}
		}
	}()
}

func (sq *sequencer) stopPump() {
	select {
	case <-sq.stopCh:
	default:
		close(sq.stopCh)
	}
	sq.wg.Wait()
}

func (sq *sequencer) forKey(key string) *keyOrder {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	k, ok := sq.keys[key]
	if !ok {
		k = &keyOrder{}
		sq.keys[key] = k
	}
	return k
}

// admitResult 描述 admit 的判定。
type admitResult int

const (
	admitProcess  admitResult = iota // 事件应立即处理（item 有效）
	admitDeferred                    // 事件已暂缓，调用方无需处理
)

// admit 判定一个出队事件是否轮到自己生效。
// 返回 process 时 out 为应处理事件（通常就是 item）；返回 deferred 表示
// 事件已进入暂缓堆。
func (e *Engine) admit(item queuedItem) (out queuedItem, res admitResult) {
	k := e.sequencer.forKey(item.ev.Key)
	k.mu.Lock()
	defer k.mu.Unlock()

	cur, ok, _ := e.store.Get(context.Background(), item.ev.Key)
	version := int64(0)
	if ok {
		version = cur.Version
	}

	// 暂缓堆中已有更小序号未处理时，无论本事件序号多大都要等；同理，
	// 仅当本事件恰好是下一个期望序号时才放行。
	if len(k.heap) > 0 && k.heap[0].ev.Seq <= item.ev.Seq {
		if !heapContains(k.heap, item.ev.ID) {
			heap.Push(&k.heap, item)
			e.metrics.incDeferred()
		}
		return item, admitDeferred
	}
	if item.ev.Seq > version+1 {
		// 存在缺口：暂缓。
		if !heapContains(k.heap, item.ev.ID) {
			heap.Push(&k.heap, item)
			e.metrics.incDeferred()
		}
		return item, admitDeferred
	}
	// item.ev.Seq <= version+1：衔接（==）或落后（<=，交存储判重/冲突）。
	// 若它其实在堆中（重投场景），先从堆里移除。
	removeFromHeap(&k.heap, item.ev.ID)
	return item, admitProcess
}

// release 在某个业务键成功推进版本后调用，把暂缓堆中序号已衔接的事件
// 重新投递回分片队列。返回被重投的事件数。
func (e *Engine) release(key string) int {
	k := e.sequencer.forKey(key)
	k.mu.Lock()
	cur, ok, _ := e.store.Get(context.Background(), key)
	version := int64(0)
	if ok {
		version = cur.Version
	}
	var ready []queuedItem
	for k.heap.Len() > 0 && k.heap[0].ev.Seq <= version+1 {
		ready = append(ready, heap.Pop(&k.heap).(queuedItem))
	}
	k.mu.Unlock()

	requeued := 0
	for _, it := range ready {
		if e.requeue(it) {
			e.metrics.decDeferred() // 离开暂缓堆、重新进入队列
			requeued++
			continue
		}
		// 队列暂满：放回暂缓堆（仍计为 deferred），等 pump/下次 release。
		k.mu.Lock()
		if !heapContains(k.heap, it.ev.ID) {
			heap.Push(&k.heap, it)
		}
		k.mu.Unlock()
	}
	return requeued
}

// requeue 非阻塞地把暂缓事件重新放回其分片队列。
//
// 排空阶段（stopCh 已关闭但队列尚未 close）仍允许重投，这是把暂缓事件
// 交给 worker 消化、实现优雅排空的路径；队列真正关闭后 trySend 安全返回
// false，事件保留在暂缓堆中（仍未 Ack，重启随 pending.log 重放）。
func (e *Engine) requeue(it queuedItem) bool {
	idx := e.queue.shardOf(it.ev.Key)
	return e.queue.trySend(idx, it)
}

// requeueAllReady 对所有业务键执行一次 release（仅重投序号已衔接的暂缓
// 事件），供后台 pump 与停机排空使用。
func (e *Engine) requeueAllReady() int {
	e.sequencer.mu.Lock()
	keyList := make([]string, 0, len(e.sequencer.keys))
	for key := range e.sequencer.keys {
		keyList = append(keyList, key)
	}
	e.sequencer.mu.Unlock()

	n := 0
	for _, key := range keyList {
		n += e.release(key)
	}
	return n
}

// requeueAllDeferred 停机时把所有仍在暂缓堆中的事件（含序号尚缺口的）尽力
// 重新投回队列，让 worker 再尝试一轮；投递不进去的保留在暂缓堆（仍未
// Ack，重启时由 pending.log 重放）。
func (e *Engine) requeueAllDeferred() int {
	// 先尽可能推进已衔接的。
	e.requeueAllReady()

	e.sequencer.mu.Lock()
	ks := make([]*keyOrder, 0, len(e.sequencer.keys))
	for _, k := range e.sequencer.keys {
		ks = append(ks, k)
	}
	e.sequencer.mu.Unlock()

	n := 0
	for _, k := range ks {
		k.mu.Lock()
		items := make([]queuedItem, k.heap.Len())
		for i := range items {
			items[i] = heap.Pop(&k.heap).(queuedItem)
		}
		k.mu.Unlock()
		for _, it := range items {
			if e.requeue(it) {
				n++
			} else {
				k.mu.Lock()
				if !heapContains(k.heap, it.ev.ID) {
					heap.Push(&k.heap, it)
				}
				k.mu.Unlock()
			}
		}
	}
	return n
}

func heapContains(h pendingHeap, id string) bool {
	for _, it := range h {
		if it.ev.ID == id {
			return true
		}
	}
	return false
}

func removeFromHeap(h *pendingHeap, id string) {
	for i, it := range *h {
		if it.ev.ID == id {
			heap.Remove(h, i)
			return
		}
	}
}
