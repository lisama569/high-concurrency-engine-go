package engine

import (
	"container/heap"
	"context"
	"sync"
	"time"
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
// 级联处理堆顶连续事件。投递异步：worker 放入 mailbox 后即返回，序号缺口
// 不占用 worker。
type keyCoordinator struct {
	eng     *Engine
	key     string
	mailbox chan queuedItem

	mu       sync.Mutex
	waitHeap pendingHeap
	busy     bool
	// hasWork 在“mailbox 为空但 waitHeap 中可能存在可推进事件”时作为兜底
	// 唤醒信号：advance 应用前驱后若堆里还有事件，会非阻塞触发一次自调度。
	reschedule chan struct{}
	wg         sync.WaitGroup
}

func newKeyCoordinator(eng *Engine, key string) *keyCoordinator {
	capN := eng.cfg.MailboxPerKey
	if capN <= 0 {
		capN = defaultMailboxCap
	}
	c := &keyCoordinator{
		eng:        eng,
		key:        key,
		mailbox:    make(chan queuedItem, capN),
		reschedule: make(chan struct{}, 1),
	}
	c.wg.Add(1)
	go c.run()
	return c
}

// kick 非阻塞地请求协调协程再跑一次 advance（用于前驱应用后推进堆顶后继）。
func (c *keyCoordinator) kick() {
	select {
	case c.reschedule <- struct{}{}:
	default:
	}
}

// deliver 投递事件到该键 mailbox。mailbox 满时阻塞等待空位（天然对慢业务
// 键形成背压，且只阻塞投递该键的 worker，不影响其它键）；停机则返回
// false。在途槽位在事件 finalize 时释放，恒等于“已接收未结论”数。
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
	}
}

func (c *keyCoordinator) run() {
	defer c.wg.Done()
	// settleTick 是兜底重扫节拍：当堆中留有事件但缺少精确 kick（例如前驱
	// 在 kick 之后才入堆）时，周期性再跑一次 advance，保证最终收敛。正常
	// 情况下由 mailbox 消息与 kick 即时驱动，tick 只增加极小开销。
	settle := time.NewTicker(2 * time.Millisecond)
	defer settle.Stop()
	for {
		select {
		case <-c.eng.finishCh:
			return
		case item := <-c.mailbox:
			c.setBusy(true)
			c.processOne(item)
			c.setBusy(false)
		case <-c.reschedule:
			c.setBusy(true)
			c.advance()
			c.setBusy(false)
		case <-settle.C:
			c.mu.Lock()
			has := c.waitHeap.Len() > 0
			c.mu.Unlock()
			if has {
				c.setBusy(true)
				c.advance()
				c.setBusy(false)
			}
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

// handleOutcome 对一个已弹出堆的事件给出最终结论，并只在此时释放在途槽位。
func (c *keyCoordinator) handleOutcome(item queuedItem) {
	outcome, perr := c.eng.processWithRetry(item)
	c.eng.finalize(item, outcome, perr)
	c.eng.inflightSem.release()
}

// advance 从堆顶连续处理序号衔接的事件，直到遇到缺口或堆空。每次成功推进
// 后 kick 自身一次，保证即便没有新 mailbox 消息，堆中已有的后继也会被
// 继续评估（这是修复“前驱已在堆中但无人触发”的关键）。
func (c *keyCoordinator) advance() {
	for {
		c.mu.Lock()
		if c.waitHeap.Len() == 0 {
			c.mu.Unlock()
			return
		}
		top := c.waitHeap[0]
		c.mu.Unlock()

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

		cur, ok, gerr := c.eng.store.Get(ctx, top.ev.Key)
		if gerr != nil {
			c.mu.Lock()
			heap.Pop(&c.waitHeap)
			c.mu.Unlock()
			c.eng.finalize(top, OutcomeFailed, normalize("engine.order.get", gerr))
			c.eng.inflightSem.release()
			close(stopSub)
			cancel()
			continue
		}
		version := int64(0)
		if ok {
			version = cur.Version
		}
		if top.ev.Seq > version+1 {
			// 缺口：事件留在堆中；不释放槽位（仍未结论）。前驱应用后会
			// kick 触发重新评估。
			close(stopSub)
			cancel()
			return
		}
		c.mu.Lock()
		heap.Pop(&c.waitHeap)
		remaining := c.waitHeap.Len()
		c.mu.Unlock()

		c.handleOutcome(top)
		close(stopSub)
		cancel()

		if remaining > 0 {
			c.kick()
		}
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

// outstandingCount 返回所有键 mailbox、缺口堆与处理中事件的总数。
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

// countingSem 是有界在途信号量，槽位数 = 已接收但尚未 finalize 的事件数。
type countingSem struct{ ch chan struct{} }

func newCountingSem(n int) *countingSem {
	if n <= 0 {
		n = 1
	}
	return &countingSem{ch: make(chan struct{}, n)}
}

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
