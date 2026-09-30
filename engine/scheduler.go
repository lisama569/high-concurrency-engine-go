package engine

import (
	"context"
	"sync"
)

// queuedItem 是调度队列中的一个条目。
type queuedItem struct {
	ev      Event
	attempt int
}

// priorityCount 是调度器支持的优先级档位数（普通 / 高）。
const priorityCount = 2

// keyQueue 是单个业务键在调度器内的严格 FIFO 缓冲。同键事件无论优先级
// 都按到达顺序排在同一条队尾，保证“同键先后投递的事件最终生效顺序与投递
// 顺序一致”不会被跨键优先级打乱。
//
// 键当前挂在哪个优先级环上，由其队头事件的优先级决定：若队头是普通事件，
// 即使队尾有高优先级事件，该键也必须等队头先被消费（同键不越级），这正是
// “跨键按优先级、同键按顺序”两条规则的交汇点。
type keyQueue struct {
	key   string
	items []queuedItem
	byPri [priorityCount]int
	node  *keyRingNode // 复用的环节点；脱离环时 node 仍保留
	// flightNode 非 nil 表示该键正有一件事件在“调度器 -> coordinator”的
	// 交接中（节点已离环）；交接结论回传后置回 nil 并按需重新挂环。
	flightNode *keyRingNode
}

// KeyQueueStat 是单个业务键在调度器一侧的可观测积压快照。
type KeyQueueStat struct {
	Key     string
	Total   int
	ByPri   [priorityCount]int
	Hot     bool
	HeadPri int
}

// SchedStats 是调度器的整体可观测快照。
type SchedStats struct {
	Depth        int
	Capacity     int
	ByPri        [priorityCount]int
	Keys         []KeyQueueStat
	HotKeyCount  int
	HotThreshold int
}

// fairScheduler 是“按优先级 + 按业务键公平”的有界内存调度器。
//
//   - 容量有界：所有 keyQueue 中的事件总数不超过 capacity；
//   - 两级优先级：pop 时只要存在高优先级事件，就先消费高优先级环；
//   - 键间公平：每个优先级用一条非空键环形链表做逐键轮转（round-robin），
//     单键无论积压多少，每次只取队头一件并把该键移到环尾，热点键不能长期
//     霸占消费能力，各键推进程度尽量均衡；
//   - 同键顺序：同键事件严格 FIFO，优先级只影响跨键的消费先后；
//   - 区分拒绝：容量真正耗尽（所有在队事件都在等待处理空位）超时返回
//     KindQueueFull；队列已满且仍有高优先级积压时，普通事件立即以
//     KindPriorityShed 拒绝（让位高优先级，原因区别于容量耗尽）。
type fairScheduler struct {
	mu      sync.Mutex
	keys    map[string]*keyQueue
	rings   [priorityCount]*keyRing
	size    int
	flights int // 已 claim 但交接结论未回传的事件数（离环、尚未进 mailbox）
	byPri   [priorityCount]int
	capN    int

	hotThreshold int

	notEmpty *sync.Cond // 唤醒等待事件的 worker
	hasSpace *sync.Cond // 唤醒等待空位的提交方

	stopOnce sync.Once
	stopCh   chan struct{}
	stopped  bool // markStopping：拒绝新提交，但仍可排空
	closed   bool // close：worker 取空后退出

	// onPop 仅用于测试观测：每次成功 pop 后同步回调。生产路径恒为 nil。
	onPop func(queuedItem)
}

// newFairScheduler 构造公平调度器。totalCapacity 为所有业务键共享的总容量；
// hotThreshold 为热点键识别阈值（该键在调度器中的积压达到即标记热点）。
func newFairScheduler(totalCapacity, hotThreshold int) *fairScheduler {
	if totalCapacity < 1 {
		totalCapacity = 1
	}
	if hotThreshold < 1 {
		hotThreshold = 1
	}
	s := &fairScheduler{
		keys:         make(map[string]*keyQueue),
		capN:         totalCapacity,
		hotThreshold: hotThreshold,
		stopCh:       make(chan struct{}),
	}
	s.rings[PriorityNormal] = newKeyRing()
	s.rings[PriorityHigh] = newKeyRing()
	s.notEmpty = sync.NewCond(&s.mu)
	s.hasSpace = sync.NewCond(&s.mu)
	return s
}

// markStopping 进入停机阶段：拒绝新提交并唤醒等待空位的提交方；已在队的
// 事件仍允许被 pop 排空，worker 不在此阶段退出。
func (s *fairScheduler) markStopping() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.mu.Lock()
	s.stopped = true
	s.hasSpace.Broadcast()
	s.mu.Unlock()
}

// close 终止调度：唤醒所有 worker；此后 pop 先把残余事件取完，取空后返回
// ok=false（残余事件由上层因 finishCh 不 Ack，保留在 pending.log 重放）。
func (s *fairScheduler) close() {
	s.markStopping()
	s.mu.Lock()
	s.closed = true
	s.notEmpty.Broadcast()
	s.mu.Unlock()
}

func (s *fairScheduler) isStopping() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

// enqueueLocked 把事件追加到其业务键的 FIFO 队尾；空键按队头（即本事件）
// 的优先级挂到对应环。调用方必须持有 s.mu。
func (s *fairScheduler) enqueueLocked(item queuedItem) {
	pri := normalizePriority(item.ev.Priority)
	kq, ok := s.keys[item.ev.Key]
	if !ok {
		kq = &keyQueue{key: item.ev.Key}
		kq.node = &keyRingNode{kq: kq}
		s.keys[item.ev.Key] = kq
	}
	if len(kq.items) == 0 {
		if kq.flightNode == nil {
			// 键处于空闲：按队头优先级入环（复用既有节点）。
			s.rings[pri].pushBack(kq.node)
		}
		// 键正在交接（flightNode != nil，节点已离环）：此处绝不能把同一节点
		// 再次挂环，否则会与交接结论回传时的 relink 重复链接、损坏环结构。
		// 交接结束时 confirm/return/drop 会依据仍有积压统一重新挂环。
	}
	kq.items = append(kq.items, item)
	kq.byPri[pri]++
	s.size++
	s.byPri[pri]++
	s.notEmpty.Broadcast()
}

// Submit 尝试把事件按业务键入队（同键严格 FIFO）。
//   - 普通优先级：若已满且仍有高优先级积压，立即 KindPriorityShed（让位于
//     高优先级，且不占住等待名额）；若满但没有高优先级事件，则按 ctx 等待
//     空位，超时 KindQueueFull（真正的容量耗尽）；
//   - 高优先级：容量未满即入队；满则等待空位（等待期间若停机则 KindStopping），
//     超时 KindQueueFull。
func (s *fairScheduler) Submit(ctx context.Context, item queuedItem) *Error {
	const op = "scheduler.submit"
	pri := normalizePriority(item.ev.Priority)

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return newError(KindStopping, op,
			"engine is shutting down; event retained in durable journal for replay", nil)
	}
	if s.size < s.capN {
		s.enqueueLocked(item)
		s.mu.Unlock()
		return nil
	}

	// 已满：普通优先级遇到任何高优先级积压都立即让位，区别于容量耗尽。
	if pri == PriorityNormal && s.byPri[PriorityHigh] > 0 {
		s.mu.Unlock()
		return newError(KindPriorityShed, op,
			"queue congested by higher-priority events; normal-priority event shed",
			ctx.Err())
	}

	// 等待空位。watchdog 在 ctx 到期/取消或停机时广播，唤醒后重新判定，
	// 以保证“等到空位期间涌入高优先级事件”也能被重新识别为让位。
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.hasSpace.Broadcast()
			s.mu.Unlock()
		case <-s.stopCh:
		case <-done:
		}
	}()

	for s.size >= s.capN {
		if s.stopped {
			s.mu.Unlock()
			return newError(KindStopping, op,
				"engine is shutting down; event retained in durable journal for replay", nil)
		}
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			if err == context.DeadlineExceeded {
				return newError(KindQueueFull, op,
					"bounded queue full; submit timed out under backpressure", err)
			}
			return newError(KindCanceled, op, "submit canceled while queue full", err)
		}
		if pri == PriorityNormal && s.byPri[PriorityHigh] > 0 {
			s.mu.Unlock()
			return newError(KindPriorityShed, op,
				"queue congested by higher-priority events while waiting; normal-priority event shed",
				ctx.Err())
		}
		s.hasSpace.Wait()
	}
	s.enqueueLocked(item)
	s.mu.Unlock()
	return nil
}

// claim 从指定环摘下队头键并取走其队头事件（不立刻重新挂环）。此时该键
// 处于“交接中（in-flight）”：节点已离环，任何 worker 都不会再取到该键，
// 直到调用方通过 confirmDelivered / returnHead / dropHead 给出结论。
//
// 这是“同键单航班”的关键：同一时刻一个业务键最多只有一件事件在从调度器
// 到该键 coordinator mailbox 的交接路上，从而 mailbox 的插入顺序恒等于
// 调度出队顺序，杜绝多 worker 并发发送同一 channel 导致的同键乱序。
// 调用方必须持有 s.mu，且环非空。
func (s *fairScheduler) claim(r *keyRing) (*keyQueue, queuedItem) {
	node := r.popFront()
	kq := node.kq
	kq.flightNode = node
	s.flights++

	item := kq.items[0]
	kq.items[0] = queuedItem{}
	kq.items = kq.items[1:]
	headPri := normalizePriority(item.ev.Priority)
	s.size--
	s.byPri[headPri]--
	kq.byPri[headPri]--

	s.hasSpace.Broadcast() // 取出一件即腾出一个提交容量
	if s.onPop != nil {
		// 持锁回调（仅测试观测）：保证记录顺序严格等于真实出队顺序。
		s.onPop(item)
	}
	return kq, item
}

// relinkLocked 把交接结束的键按当前队头优先级挂到对应环尾；队列为空则不挂。
// 调用方必须持有 s.mu。每次 claim 恰好对应一次 relink，因此这里把交接计数
// 减一，保证“claim 后、结论回传前”的事件不会在排空判定中被漏算。
func (s *fairScheduler) relinkLocked(kq *keyQueue) {
	kq.flightNode = nil
	if s.flights > 0 {
		s.flights--
	}
	if len(kq.items) == 0 {
		return
	}
	newPri := normalizePriority(kq.items[0].ev.Priority)
	s.rings[newPri].pushBack(kq.node)
	// 该键又可被调度，唤醒等待中的 worker。
	s.notEmpty.Broadcast()
}

// confirmDelivered 事件已成功进入该键 coordinator mailbox：把仍有积压的
// 键挂回环尾（让其它键先被服务，天然实现逐键轮转）。
func (s *fairScheduler) confirmDelivered(kq *keyQueue) {
	s.mu.Lock()
	s.relinkLocked(kq)
	s.mu.Unlock()
}

// returnHead 事件因该键 mailbox 满（或在途信号量不足）未能交接：把它放回
// 该键队头并把键挂到环尾——本键本轮让位，worker 转而服务其它键；稍后 mailbox
// 排空后该键会重新轮到。退回不改变在队计数（claim 时已减、退回时加回）。
func (s *fairScheduler) returnHead(kq *keyQueue, item queuedItem) {
	s.mu.Lock()
	pri := normalizePriority(item.ev.Priority)
	kq.items = append([]queuedItem{item}, kq.items...)
	kq.byPri[pri]++
	s.size++
	s.byPri[pri]++
	s.relinkLocked(kq)
	s.mu.Unlock()
}

// dropHead 放弃交接中的事件（仅发生在停机：调用方不再重投，事件未 Ack，
// 保留在 pending.log 由下次启动重放）。把仍有积压的键重新挂环，由收尾阶段
// 继续取走或随关闭终止。
func (s *fairScheduler) dropHead(kq *keyQueue) {
	s.mu.Lock()
	s.relinkLocked(kq)
	s.mu.Unlock()
}

// SubmitReplay 用于启动阶段把 pending.log 中已持久化接收的事件重新入队。
// 与 Submit 的唯一区别：绝不因优先级让位（KindPriorityShed）而拒绝——这些
// 事件在上个生命周期已被接收，必须重新进入队列；容量满时阻塞等待空位
// （ctx 一般为 context.Background），仅在停机时返回 KindStopping。
func (s *fairScheduler) SubmitReplay(ctx context.Context, item queuedItem) *Error {
	const op = "scheduler.replay"
	s.mu.Lock()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.hasSpace.Broadcast()
			s.mu.Unlock()
		case <-s.stopCh:
		case <-done:
		}
	}()
	for s.size >= s.capN {
		if s.stopped {
			s.mu.Unlock()
			return newError(KindStopping, op,
				"engine is shutting down; replayed event retained in journal", nil)
		}
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			if err == context.DeadlineExceeded {
				return newError(KindQueueFull, op, "replay queue full and deadline exceeded", err)
			}
			return newError(KindCanceled, op, "replay canceled while queue full", err)
		}
		s.hasSpace.Wait()
	}
	s.enqueueLocked(item)
	s.mu.Unlock()
	return nil
}

// pop 取下一个可交接事件：优先消费高优先级环，两环内均按业务键轮转。返回
// 所属键队列，调用方必须在交接结束后调用 confirmDelivered / returnHead /
// dropHead 三选一。队列空时阻塞等待；close 后取空返回 ok=false。
func (s *fairScheduler) pop() (*keyQueue, queuedItem, bool) {
	s.mu.Lock()
	for {
		if r := s.rings[PriorityHigh]; !r.empty() {
			kq, item := s.claim(r)
			s.mu.Unlock()
			return kq, item, true
		}
		if r := s.rings[PriorityNormal]; !r.empty() {
			kq, item := s.claim(r)
			s.mu.Unlock()
			return kq, item, true
		}
		if s.closed {
			s.mu.Unlock()
			return nil, queuedItem{}, false
		}
		s.notEmpty.Wait()
	}
}

// depth 返回当前队列积压总量（不含交接中事件）。
func (s *fairScheduler) depth() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// inFlightHandoffs 返回已 claim 但交接结论尚未回传的事件数。优雅停机的
// 排空判定必须把它与 depth 一起视为“仍未进入 coordinator”的事件，避免在
// 交接窗口误判排空完成。
func (s *fairScheduler) inFlightHandoffs() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flights
}

func (s *fairScheduler) capacity() int { return s.capN }

// stats 返回当前积压的可观测快照：总量、容量、按优先级拆分以及按业务键
// 拆分（含热点键标记）。
func (s *fairScheduler) stats() SchedStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := SchedStats{
		Depth:        s.size,
		Capacity:     s.capN,
		ByPri:        s.byPri,
		HotThreshold: s.hotThreshold,
		Keys:         make([]KeyQueueStat, 0, len(s.keys)),
	}
	for _, kq := range s.keys {
		if len(kq.items) == 0 {
			continue
		}
		stat := KeyQueueStat{
			Key:     kq.key,
			Total:   len(kq.items),
			ByPri:   kq.byPri,
			HeadPri: normalizePriority(kq.items[0].ev.Priority),
		}
		if stat.Total >= s.hotThreshold {
			stat.Hot = true
			st.HotKeyCount++
		}
		st.Keys = append(st.Keys, stat)
	}
	return st
}

// keyRing 是“非空业务键”的双向环形链表（带哨兵），支持 O(1) 队头弹出、
// 队尾追加，实现逐键轮转的公平调度。
type keyRing struct {
	root keyRingNode
}

type keyRingNode struct {
	kq         *keyQueue
	prev, next *keyRingNode
}

func newKeyRing() *keyRing {
	r := &keyRing{}
	r.root.prev = &r.root
	r.root.next = &r.root
	return r
}

// pushBack 把节点追加到环尾（哨兵之前）。节点必须当前不在任何环中。
func (r *keyRing) pushBack(n *keyRingNode) {
	n.prev = r.root.prev
	n.next = &r.root
	r.root.prev.next = n
	r.root.prev = n
}

// popFront 移除并返回环头节点（哨兵之后）；环为空时返回 nil。
func (r *keyRing) popFront() *keyRingNode {
	n := r.root.next
	if n == &r.root {
		return nil
	}
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev, n.next = nil, nil
	return n
}

func (r *keyRing) empty() bool { return r.root.next == &r.root }

// nextPow2 返回不小于 n 的最小 2 的幂（用于由 worker 数推导调度容量）。
func nextPow2(n int) int {
	if n < 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
