package engine

import (
	"container/heap"
	"context"
	"sync"
)

// pendingHeap 是某个业务键上因序号缺口而暂缓的最小序号堆。
type pendingHeap []*workItem

func (h pendingHeap) Len() int           { return len(h) }
func (h pendingHeap) Less(i, j int) bool { return h[i].ev.ev.Seq < h[j].ev.ev.Seq }
func (h pendingHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *pendingHeap) Push(x any)        { *h = append(*h, x.(*workItem)) }
func (h *pendingHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// workItem 是协调协程 mailbox 中的一个工作单元，done 在该事件得到最终结论
// （应用/判重/冲突/死信）或因序号缺口暂缓而释放投递 worker 时关闭。
type workItem struct {
	ev      queuedItem
	done    chan struct{}
	doneOne sync.Once
}

// finish 幂等地关闭 done，保证同一事件在“缺口暂缓”与后续“真正处理”两条
// 路径上不会重复关闭。
func (w *workItem) finish() { w.doneOne.Do(func() { close(w.done) }) }

// defaultMailboxCap 是单个业务键 mailbox 的默认缓冲大小。
const defaultMailboxCap = 256

// keyCoordinator 是单个业务键的串行处理 actor：同键事件由唯一协程按 Seq
// 顺序生效，不同键的协调协程并行。worker 投递后阻塞在 workItem.done 上，
// 因此在途处理总数始终受 worker 数约束，背压由有界队列真实反映。
type keyCoordinator struct {
	eng     *Engine
	key     string
	mailbox chan *workItem

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
		mailbox: make(chan *workItem, capN),
	}
	c.wg.Add(1)
	go c.run()
	return c
}

// deliver 投递事件并同步等待最终结论（或停机放弃）。返回 false 表示停机
// 期间未能/未完成，调用方不 Ack 以保留事件待重放。
func (c *keyCoordinator) deliver(item queuedItem) bool {
	wi := &workItem{ev: item, done: make(chan struct{})}
	select {
	case c.mailbox <- wi:
	case <-c.eng.finishCh:
		return false
	}
	select {
	case <-wi.done:
		return true
	case <-c.eng.finishCh:
		return false
	}
}

func (c *keyCoordinator) run() {
	defer c.wg.Done()
	for {
		select {
		case <-c.eng.finishCh:
			return
		case wi := <-c.mailbox:
			c.setBusy(true)
			c.processOne(wi)
			c.setBusy(false)
		}
	}
}

func (c *keyCoordinator) setBusy(b bool) {
	c.mu.Lock()
	c.busy = b
	c.mu.Unlock()
}

// processOne 把事件纳入堆并驱动级联处理。注意：缺口等待期间，当前事件的
// workItem.done 不能提前关闭，否则投递方误以为已完成；只有事件被真正
// finalize 或判定为缺口暂缓时才给予相应处理。缺口事件的 workItem 保存在
// 堆里，由后续 advance 在其被应用时关闭。
func (c *keyCoordinator) processOne(wi *workItem) {
	c.mu.Lock()
	if !heapContains(c.waitHeap, wi.ev.ev.ID) {
		heap.Push(&c.waitHeap, wi)
	} else {
		wi.finish() // 重复进入：以既有 workItem 为准，立即放行投递方
	}
	c.mu.Unlock()
	c.advance()
}

// advance 连续处理堆顶序号衔接的事件；遇到缺口则等待前驱（事件保留在堆，
// 其投递 worker 阻塞在 done 上——这正是“按键背压”，前驱完成后会解除）。
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
			case <-ctx.Done():
			case <-stopSub:
			}
		}()

		cur, ok, gerr := c.eng.store.Get(ctx, top.ev.ev.Key)
		if gerr != nil {
			c.mu.Lock()
			heap.Pop(&c.waitHeap)
			c.mu.Unlock()
			c.eng.finalize(top.ev, OutcomeFailed, normalize("engine.order.get", gerr))
			top.finish()
			close(stopSub)
			cancel()
			continue
		}
		version := int64(0)
		if ok {
			version = cur.Version
		}
		if top.ev.ev.Seq > version+1 {
			// 缺口：释放投递该事件的 worker（不占用线程），事件留在堆中。
			// 前驱事件到达时会由它自己的 processOne->advance 在同协程内
			// 级联把本事件及后续连续事件一起处理，因此无需该 worker 等待。
			close(stopSub)
			cancel()
			top.finish()
			return
		}
		c.mu.Lock()
		heap.Pop(&c.waitHeap)
		c.mu.Unlock()

		outcome, perr := c.eng.processWithRetry(top.ev)
		c.eng.finalize(top.ev, outcome, perr)
		top.finish()
		close(stopSub)
		cancel()
	}
}

// sequencer 管理全部业务键的协调协程（懒创建）。
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

func heapContains(h pendingHeap, id string) bool {
	for _, wi := range h {
		if wi.ev.ev.ID == id {
			return true
		}
	}
	return false
}
