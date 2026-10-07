package engine

import (
	"context"
	"hash/fnv"
	"sync"
	"time"
)

// queuedItem 是内存有界队列中的一个条目。
type queuedItem struct {
	ev      Event
	attempt int
}

// keyQueue 是一个业务键在分片内的 FIFO 缓冲。
type keyQueue []queuedItem

// fairShard 是一个“两级优先级 + 键间轮转”的公平队列分片。
//
// 调度规则（popLocked）：
//  1. 高优先级队列先于普通队列：只要高优先级层非空，普通层整体等待，绝不
//     越过高优先级服务普通事件（包括“mailbox 背压退回再调度”的事件）；
//  2. 同一优先级内，各业务键以轮转（round-robin）顺序每次只取一个事件：
//     单个热点键即使涌入海量事件，也只能与其他键交替推进，无法长期霸占
//     该分片的消费能力；
//  3. 同一业务键内部严格 FIFO。
//
// 容量规则（Submit）：两级队列共享 capN 个总槽位。普通优先级在分片“拥塞”
// （队列满且存在高优先级积压）时不等待，直接以 KindPriorityThrottled
// 拒绝；其余容量竞争在 SubmitTimeout 后以 KindQueueFull 拒绝——两种原因
// 可区分，且高优先级在拥塞时仍可排队等待空位。
type fairShard struct {
	mu      sync.Mutex
	stopped bool // markStopping 后置位：不再接受阻塞等待
	closed  bool // close 后置位：排空结束，pop 返回 ok=false

	levels [2]struct {
		// buckets 按业务键保存键内 FIFO。
		buckets map[string]keyQueue
		// onRing 记录该键当前是否已登记在轮转序列 order 中，防止“事件被
		// 弹出时键离环、随后因 mailbox 背压退回却漏登记”而永久隐形。
		onRing map[string]bool
		// order 是活跃键（bucket 非空）的环形轮转序列，head 指向下一个
		// 该被服务的键。每次 pop 从 head 取一个事件并把该键转到环尾。
		order []string
		head  int
	}

	used int // 两级队列已占用槽位总数
	capN int

	// waiters 是等待“空位”的 Submit 调用方各自的唤醒信号；workers 是等待
	// “有可调度事件”的 worker（每分片至多一个）。状态变化、停机或关闭时
	// 统一在锁内通过非阻塞发送通知，无需任何辅助看门狗协程。
	waiters map[chan struct{}]struct{}
	workers map[chan struct{}]struct{}
}

func newFairShard(capN int) *fairShard {
	s := &fairShard{
		capN:    capN,
		waiters: make(map[chan struct{}]struct{}),
		workers: make(map[chan struct{}]struct{}),
	}
	s.levels[PriorityNormal].buckets = make(map[string]keyQueue)
	s.levels[PriorityHigh].buckets = make(map[string]keyQueue)
	s.levels[PriorityNormal].onRing = make(map[string]bool)
	s.levels[PriorityHigh].onRing = make(map[string]bool)
	return s
}

// notifyLocked 非阻塞唤醒所有等待空位的 Submit 与等待事件的 worker。
func (s *fairShard) notifyLocked() {
	for ch := range s.waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	for ch := range s.workers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// compactLocked 在环形序列头部空洞过半时压缩，保证长期运行下 order 长度
// 始终与活跃键数量同阶（内存不随历史键无限增长）。
func (s *fairShard) compactLocked(lv Priority) {
	l := &s.levels[lv]
	if l.head < 64 || l.head < len(l.order)/2 {
		return
	}
	n := len(l.order) - l.head
	copy(l.order, l.order[l.head:])
	l.order = l.order[:n]
	l.head = 0
}

// pushLocked 往某键的 FIFO 尾部追加，并在该键未在环上时登记进轮转序列。
// 调用方必须已经确认 used < capN。
func (s *fairShard) pushLocked(lv Priority, item queuedItem) {
	l := &s.levels[lv]
	k := item.ev.Key
	q := l.buckets[k]
	if len(q) == 0 && !l.onRing[k] {
		// 新活跃键排到环尾，等待轮转服务；已是活跃键（队列非空）的事件
		// 直接追加到该键 FIFO 尾部，不改变轮转位置（同键严格保序）。
		l.order = append(l.order, k)
		l.onRing[k] = true
	}
	l.buckets[k] = append(q, item)
	s.used++
}

// requeueLocked 把因 mailbox 背压退回的事件放回其键 FIFO 的尾部。条目被
// 弹出时该键可能恰好已离开轮转序列，因此这里需要按需重新登记，保证退回
// 的事件不会脱离调度。
func (s *fairShard) requeueLocked(lv Priority, item queuedItem) {
	l := &s.levels[lv]
	k := item.ev.Key
	q := l.buckets[k]
	if len(q) == 0 && !l.onRing[k] {
		l.order = append(l.order, k)
		l.onRing[k] = true
	}
	l.buckets[k] = append(q, item)
	s.used++
}

// popLocked 按“高优先级优先、同级键间轮转、键内 FIFO”取出一个事件。
// 返回 ok=false 表示当前没有可调度事件（空队列，或仅剩普通级而高优先级
// 仍有积压——此时普通级必须让出等待）。
func (s *fairShard) popLocked() (item queuedItem, ok bool) {
	for _, lv := range []Priority{PriorityHigh, PriorityNormal} {
		l := &s.levels[lv]
		for l.head < len(l.order) {
			k := l.order[l.head]
			q := l.buckets[k]
			if len(q) == 0 {
				// 该键无积压（异常残留位置）：从环上摘除并继续前进。
				l.order[l.head] = ""
				l.head++
				delete(l.onRing, k)
				continue
			}
			if lv == PriorityNormal && s.levelLenLocked(PriorityHigh) > 0 {
				// 普通级队头但仍有高优先级积压：整体让出，等待高优先级
				// 先被服务。head 不前移，该普通事件保持队头位置。
				return queuedItem{}, false
			}
			item = q[0]
			q = q[1:]
			s.used--
			// 当前服务位置随 head 前移；若该键仍有积压，把它转到环尾，
			// 下一轮才再服务它，给其他键让位；若已清空则离开环。
			l.order[l.head] = ""
			l.head++
			if len(q) == 0 {
				delete(l.buckets, k)
				delete(l.onRing, k)
			} else {
				l.buckets[k] = q
				l.order = append(l.order, k)
			}
			s.compactLocked(lv)
			return item, true
		}
		// 本层耗尽：复位环，下次有新键入队从头开始。
		l.order = l.order[:0]
		l.head = 0
	}
	return queuedItem{}, false
}

// levelLenLocked 返回某一优先级层的积压总数。
func (s *fairShard) levelLenLocked(lv Priority) int {
	l := &s.levels[lv]
	n := 0
	for _, k := range l.order {
		n += len(l.buckets[k])
	}
	return n
}

// levelCountsLocked 返回高/普通两级当前积压。
func (s *fairShard) levelCountsLocked() (high, normal int) {
	return s.levelLenLocked(PriorityHigh), s.levelLenLocked(PriorityNormal)
}

// shardedQueue 是按业务键哈希分片的有界公平调度队列。
type shardedQueue struct {
	shards []*fairShard
	mask   uint64
	capPer int

	stopCh chan struct{} // markStopping 时关闭
}

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

func newShardedQueue(shardCount, capacityPerShard int) *shardedQueue {
	n := nextPow2(shardCount)
	q := &shardedQueue{
		shards: make([]*fairShard, n),
		mask:   uint64(n - 1),
		capPer: capacityPerShard,
		stopCh: make(chan struct{}),
	}
	for i := range q.shards {
		q.shards[i] = newFairShard(capacityPerShard)
	}
	return q
}

// markStopping 通知所有分片：Submit 不再阻塞等待容量；worker 仍可继续
// 排空已入队事件，直到 close。
func (q *shardedQueue) markStopping() {
	select {
	case <-q.stopCh:
		return
	default:
		close(q.stopCh)
	}
	for _, s := range q.shards {
		s.mu.Lock()
		s.stopped = true
		s.notifyLocked()
		s.mu.Unlock()
	}
}

func (q *shardedQueue) shardOf(key string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() & q.mask)
}

// Submit 将事件放入对应分片。
//
// 拒绝原因：
//   - KindPriorityThrottled：普通优先级事件遇分片拥塞（队列满且存在高优
//     先级积压），不等待，直接拒绝（可重试）；
//   - KindQueueFull：容量竞争在 ctx 截止前未拿到空位（任何优先级都可能，
//     表示真正的容量耗尽）；
//   - KindStopping：停机中；
//   - KindCanceled：外部 ctx 主动取消。
func (q *shardedQueue) Submit(ctx context.Context, item queuedItem) *Error {
	return q.submit(ctx, item, false)
}

// SubmitReplay 是重启恢复专用的入队路径：重放的是已落盘的存量事件，必须
// 全部重新入队，因此不套用正常运行时的优先级限流（KindPriorityThrottled）
// 与 SubmitTimeout 背压拒绝——分片满时阻塞等待 worker 腾出空位，直到
// ctx 取消或停机。优先级仍随事件保留，只影响入队后的消费先后。
func (q *shardedQueue) SubmitReplay(ctx context.Context, item queuedItem) *Error {
	return q.submit(ctx, item, true)
}

// submit 是入队主流程；replay=true 时跳过优先级限流，分片满一律等待空位。
func (q *shardedQueue) submit(ctx context.Context, item queuedItem, replay bool) *Error {
	const op = "queue.submit"
	lv := normalizePriority(item.ev.Priority)
	item.ev.Priority = lv
	s := q.shards[q.shardOf(item.ev.Key)]

	queueFullErr := func(cause error) *Error {
		return newError(KindQueueFull, op,
			"bounded queue full; submit timed out under backpressure", cause)
	}

	// wake 在锁外用于等待“空位 / 停机 / ctx”三者之一。
	wake := make(chan struct{}, 1)
	var timer <-chan time.Time
	var timerC *time.Timer

	s.mu.Lock()
	for {
		if s.closed || s.stopped {
			s.mu.Unlock()
			return newError(KindStopping, op,
				"engine is shutting down; event retained in durable journal for replay", nil)
		}
		if s.used < s.capN {
			s.pushLocked(lv, item)
			s.notifyLocked() // 唤醒等待中的 worker
			s.mu.Unlock()
			return nil
		}
		// 容量已满。
		if !replay && lv == PriorityNormal && s.levelLenLocked(PriorityHigh) > 0 {
			// 拥塞由高优先级积压造成：普通优先级不等待，原因与“真正容量
			// 耗尽”区分开。恢复重放（replay）不走此限流：存量事件必须全部
			// 重新入队，改为阻塞等待空位。
			s.mu.Unlock()
			return newError(KindPriorityThrottled, op,
				"normal-priority event throttled: shard congested by high-priority backlog", nil)
		}
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			if err == context.DeadlineExceeded {
				return queueFullErr(err)
			}
			return newError(KindCanceled, op, "submit canceled while queue full", err)
		}
		// 登记为等待者并释放锁；被空位（pop）、停机或关闭唤醒后重新竞争。
		s.waiters[wake] = struct{}{}
		if timer == nil {
			// 以 SubmitTimeout（若 ctx 带截止）作为兜底唤醒，避免错过信号；
			// ctx.Done() 与 stopCh 同样会解除等待。
			if dl, ok := ctx.Deadline(); ok {
				d := time.Until(dl)
				if d > 0 {
					timerC = time.NewTimer(d)
					timer = timerC.C
				}
			}
		}
		s.mu.Unlock()

		select {
		case <-wake:
		case <-q.stopCh:
		case <-ctx.Done():
		case <-timer:
		}
		if timerC != nil {
			timerC.Stop()
			timerC = nil
			timer = nil
		}

		s.mu.Lock()
		delete(s.waiters, wake)
		// 循环回到顶部重新判定容量/停机/截止/拥塞。
	}
}

// pop 取下一个待处理事件；当前仅有被高优先级压制的普通级或队列空时阻塞。
// 返回 ok=false 表示队列已关闭且排空完毕（worker 应退出）。
func (q *shardedQueue) pop(idx int) (queuedItem, bool) {
	s := q.shards[idx]
	wake := make(chan struct{}, 1)
	s.mu.Lock()
	for {
		if item, ok := s.popLocked(); ok {
			s.notifyLocked() // 有空位，唤醒等待容量的 Submit
			s.mu.Unlock()
			return item, true
		}
		if s.closed {
			s.mu.Unlock()
			return queuedItem{}, false
		}
		// 登记等待（每分片只有一个 worker，重复登记被 map 去重）。
		s.workers[wake] = struct{}{}
		s.mu.Unlock()

		select {
		case <-wake:
		case <-q.stopCh:
			// 停机后仍需排空：回到循环继续尝试；close 时才真正退出。
		}
		s.mu.Lock()
		delete(s.workers, wake)
	}
}

// requeue 非阻塞地把事件退回其分片（worker 因目标键 mailbox 背压时使用）。
// 该事件刚从同一分片取出，必然有空位；放回其键 FIFO 尾部以参与公平轮转。
func (q *shardedQueue) requeue(idx int, item queuedItem) {
	s := q.shards[idx]
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.requeueLocked(normalizePriority(item.ev.Priority), item)
	s.notifyLocked()
	s.mu.Unlock()
}

func (q *shardedQueue) depth() int {
	n := 0
	for _, s := range q.shards {
		s.mu.Lock()
		n += s.used
		s.mu.Unlock()
	}
	return n
}

// depthByPriority 返回按优先级分类的队列积压（高、普通）。
func (q *shardedQueue) depthByPriority() (high, normal int) {
	for _, s := range q.shards {
		s.mu.Lock()
		h, nm := s.levelCountsLocked()
		s.mu.Unlock()
		high += h
		normal += nm
	}
	return high, normal
}

// backlogByKey 汇总每个业务键当前在队列分片内的积压。
func (q *shardedQueue) backlogByKey() map[string]int {
	out := make(map[string]int)
	for _, s := range q.shards {
		s.mu.Lock()
		for lv := range s.levels {
			l := &s.levels[lv]
			for _, k := range l.order {
				out[k] += len(l.buckets[k])
			}
		}
		s.mu.Unlock()
	}
	return out
}

func (q *shardedQueue) capacity() int { return len(q.shards) * q.capPer }

// close 标记所有分片排空结束：pop 在队列取空后返回 false。必须在
// markStopping 之后、worker 仍在运行时调用，事件不会丢失（未 Ack 的事件
// 保留在 pending.log 中等待重放）。
func (q *shardedQueue) close() {
	for _, s := range q.shards {
		s.mu.Lock()
		s.closed = true
		s.notifyLocked()
		s.mu.Unlock()
	}
}
