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
	// stagePri 统计当前处于协调器侧（mailbox + 缺口堆 + 处理中）的各优先级
	// 事件数：deliver 成功时增加，事件 finalize 后减少，全程持有 c.mu。
	stagePri [priorityCount]int
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

// deliver 尝试把事件投递到该键 mailbox。它必须是非阻塞的：调度器对每个
// 业务键采用“单航班”交接（claim 时该键离环），若此处阻塞，该键既不在环
// 上、又占住一个 worker，会拖死轮转。因此：
//   - 在途信号量有槽且 mailbox 有空位：立即投递成功，返回 true；
//   - 信号量耗尽或 mailbox 满：返回 false，调用方把事件退回调度器并让该键
//     本轮让位（挂环尾），worker 转而去服务其它键，稍后该键 coordinator
//     排空后自然重新轮到——既对慢业务键背压，又不牺牲键间公平；
//   - 停机：返回 -1，事件不再重投，保留 pending.log 重放。
//
// 返回值：1=已投递，0=暂时满（应退回让位），-1=停机（应丢弃交接）。
func (c *keyCoordinator) deliver(item queuedItem) int {
	if !c.eng.inflightSem.tryAcquire() {
		return 0
	}
	select {
	case c.mailbox <- item:
		c.mu.Lock()
		c.stagePri[normalizePriority(item.ev.Priority)]++
		c.mu.Unlock()
		return 1
	case <-c.eng.finishCh:
		c.eng.inflightSem.release()
		return -1
	default:
		c.eng.inflightSem.release()
		return 0
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

// handleOutcome 对一个已弹出堆的事件给出最终结论，并只在此时释放在途槽位
// 与协调器侧积压计数。
func (c *keyCoordinator) handleOutcome(item queuedItem) {
	outcome, perr := c.eng.processWithRetry(item)
	c.eng.finalize(item, outcome, perr)
	c.eng.inflightSem.release()
	c.mu.Lock()
	c.stagePri[normalizePriority(item.ev.Priority)]--
	c.mu.Unlock()
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
			c.stagePri[normalizePriority(top.ev.Priority)]--
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

// coordinatorFor 返回（必要时懒创建）业务键的串行协调协程。
func (e *Engine) coordinatorFor(key string) *keyCoordinator {
	e.sequencer.mu.Lock()
	k, ok := e.sequencer.keys[key]
	if !ok {
		k = newKeyCoordinator(e, key)
		e.sequencer.keys[key] = k
	}
	e.sequencer.mu.Unlock()
	return k
}

// outstandingCount 返回所有键 mailbox、缺口堆与处理中事件的总数。
func (e *Engine) outstandingCount() int {
	n := 0
	for _, st := range e.outstandingByKey() {
		n += st.total
	}
	return n
}

// coordKeyStat 是单个业务键在协调器侧（mailbox + 缺口堆 + 处理中）的
// 在途统计，按优先级拆分供可观测使用。
type coordKeyStat struct {
	total int
	byPri [priorityCount]int
}

// outstandingByKey 返回每个业务键在协调器侧的在途统计。
func (e *Engine) outstandingByKey() map[string]coordKeyStat {
	e.sequencer.mu.Lock()
	ks := make([]*keyCoordinator, 0, len(e.sequencer.keys))
	for _, k := range e.sequencer.keys {
		ks = append(ks, k)
	}
	e.sequencer.mu.Unlock()
	out := make(map[string]coordKeyStat, len(ks))
	for _, k := range ks {
		k.mu.Lock()
		st := coordKeyStat{byPri: k.stagePri}
		st.total = st.byPri[PriorityNormal] + st.byPri[PriorityHigh]
		k.mu.Unlock()
		if st.total > 0 {
			out[k.key] = st
		}
	}
	return out
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

// tryAcquire 非阻塞地获取一个在途槽位；满时返回 false。
func (s *countingSem) tryAcquire() bool {
	select {
	case s.ch <- struct{}{}:
		return true
	default:
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
