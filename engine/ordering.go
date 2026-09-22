package engine

import (
	"container/heap"
	"context"
	"sync"
)

// pendingHeap 是某个业务键上因序号缺口而暂缓的事件最小序号堆。
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

const defaultMailboxCap = 256

// keyCoordinator 是单个业务键的串行处理 actor。同键事件全部进入 mailbox，
// 由唯一协程按 Seq 顺序生效：衔接的处理，缺口的入堆等待前驱，前驱应用后
// 级联处理堆顶连续事件。
//
// 投递是异步的：worker 把事件放入 mailbox（经全局在途信号量限并发）后立即
// 返回，不阻塞等待处理完成，因此“序号缺口”不会占用任何 worker 线程。
type keyCoordinator struct {
	eng     *Engine
	key     string
	mailbox chan queuedItem

	mu       sync.Mutex
	waitHeap pendingHeap
	busy     bool
	wg       sync.WaitGroup
}

func newKeyCoordinator(eng *Engine, key string) *keyCoordinator {
	capN := eng.cfg.MailboxPerKey
	if capN <= 0 {
		capN = defaultMailboxCap
	}
	c := &keyCoordinator{
		eng:     eng,
		key:     key,
		mailbox: make(chan queuedItem, capN),
	}
	c.wg.Add(1)
	go c.run()
	return c
}

// deliver 异步投递：先占用一个全局在途槽位（限制系统中被接收待处理的事件
// 总量，构成背压），再把事件放入该键 mailbox。mailbox 满或停机时释放槽位
// 并返回 false，调用方按队列背压/停机处理（事件保留在持久日志中）。
func (c *keyCoordinator) deliver(item queuedItem) bool {
	if !c.eng.inflightSem.acquire(c.eng.finishCh) {
		return false
	}
	select {
	case c.mailbox <- item:
		return true
	case <-c.eng.finishCh:
		c.eng.inflightSem.release()
		return false
	default:
		// mailbox 满：该键处理能力不足，按容量背压拒绝，事件已在持久日志。
		c.eng.inflightSem.release()
		return false
	}
}

func (c *keyCoordinator) run() {
	defer c.wg.Done()
	for {
		select {
		case <-c.eng.finishCh:
			return
		case item := <-c.mailbox:
			c.setBusy(true)
			c.processOne(item)
			c.setBusy(false)
			// 事件走完最终结论（或确认暂缓）后释放一个在途槽位。
			c.eng.inflightSem.release()
		}
	}
}

func (c *keyCoordinator) setBusy(b bool) {
	c.mu.Lock()
	c.busy = b
	c.mu.Unlock()
}

func (c *keyCoordinator) processOne(item queuedItem) {
	c.mu.Lock()
	if !heapContains(c.waitHeap, item.ev.ID) {
		heap.Push(&c.waitHeap, item)
	}
	c.mu.Unlock()
	c.advance()
}

// advance 连续处理堆顶序号衔接的事件，直到遇到缺口或堆空。
func (c *keyCoordinator) advance() {
	ctx, cancel := context.WithCancel(context.Background())
	stopSub := make(chan struct{})
	go func() {
		select {
		case <-c.eng.finishCh:
			cancel()
		case <-stopSub:
		case <-ctx.Done():
		}
	}()
	defer func() { close(stopSub); cancel() }()

	for {
		c.mu.Lock()
		if c.waitHeap.Len() == 0 {
			c.mu.Unlock()
			return
		}
		top := c.waitHeap[0]
		c.mu.Unlock()

		cur, ok, gerr := c.eng.store.Get(ctx, top.ev.Key)
		if gerr != nil {
			c.mu.Lock()
			heap.Pop(&c.waitHeap)
			c.mu.Unlock()
			c.eng.finalize(top, OutcomeFailed, normalize("engine.order.get", gerr))
			continue
		}
		version := int64(0)
		if ok {
			version = cur.Version
		}
		if top.ev.Seq > version+1 {
			// 缺口：事件留在堆中，投递 worker 早已释放；前驱到达后会再次
			// 进入 advance 并级联处理本事件。
			return
		}
		c.mu.Lock()
		heap.Pop(&c.waitHeap)
		c.mu.Unlock()

		outcome, perr := c.eng.processWithRetry(top)
		c.eng.finalize(top, outcome, perr)
	}
}

// sequencer 管理全部业务键协调协程（懒创建）。
type sequencer struct {
	mu   sync.Mutex
	keys map[string]*keyCoordinator
}

func newSequencer() *sequencer {
	return &sequencer{keys: make(map[string]*keyCoordinator)}
}

func (e *Engine) dispatch(item queuedItem) bool {
	e.sequencer.mu.Lock()
	k, ok := e.sequencer.keys[item.ev.Key]
	if !ok {
		k = newKeyCoordinator(e, item.ev.Key)
		e.sequencer.keys[item.ev.Key] = k
	}
	e.sequencer.mu.Unlock()
	return k.deliver(item)
}

// outstandingCount 返回所有键 mailbox、缺口堆与处理中事件的总数，供停机
// 排空判定。
func (e *Engine) outstandingCount() int {
	e.sequencer.mu.Lock()
	ks := make([]*keyCoordinator, 0, len(e.sequencer.keys))
	for _, k := range e.sequencer.keys {
		ks = append(ks, k)
	}
	e.sequencer.mu.Unlock()
	n := 0
	for _, k := range ks {
		k.mu.Lock()
		n += k.waitHeap.Len()
		n += len(k.mailbox)
		if k.busy {
			n++
		}
		k.mu.Unlock()
	}
	return n
}

func (e *Engine) stopCoordinators() {
	select {
	case <-e.finishCh:
	default:
		close(e.finishCh)
	}
	e.sequencer.mu.Lock()
	ks := make([]*keyCoordinator, 0, len(e.sequencer.keys))
	for _, k := range e.sequencer.keys {
		ks = append(ks, k)
	}
	e.sequencer.mu.Unlock()
	for _, k := range ks {
		k.wg.Wait()
	}
}

// countingSem 是一个有界在途信号量：限制“已被引擎接收但尚未最终结论”的
// 事件总数，从而在处理能力不足时通过有界队列形成明确背压。
type countingSem struct {
	ch chan struct{}
}

func newCountingSem(n int) *countingSem {
	if n <= 0 {
		n = 1
	}
	return &countingSem{ch: make(chan struct{}, n)}
}

// acquire 占用一个槽位，阻塞直到有空位或 stop 关闭（返回 false）。
func (s *countingSem) acquire(stop <-chan struct{}) bool {
	select {
	case s.ch <- struct{}{}:
		return true
	case <-stop:
		return false
	}
}

func (s *countingSem) release() {
	select {
	case <-s.ch:
	default:
	}
}

func (s *countingSem) inflight() int { return len(s.ch) }

func heapContains(h pendingHeap, id string) bool {
	for _, it := range h {
		if it.ev.ID == id {
			return true
		}
	}
	return false
}
