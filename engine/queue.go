package engine

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// queuedItem 是内存有界队列中的一个条目。
type queuedItem struct {
	ev      Event
	attempt int
}

// shardedQueue 是按业务键哈希分片的有界内存队列。
//
// 每个分片是一条有界 channel，由固定 worker 串行消费，从而保证同一业务键
// 的事件严格有序；不同键分布在不同分片上并行消费。队列满时 Submit 按
// ctx 截止时间等待，超时即以 KindQueueFull 明确拒绝；停机期间则以
// KindStopping 拒绝，两种背压原因可区分，事件不被静默丢弃。
type shardedQueue struct {
	shards   []chan queuedItem
	mask     uint64
	capPer   int
	enqueued atomic.Int64 // 成功入队计数

	stopOnce sync.Once
	stopCh   chan struct{} // markStopping 时关闭
	closeMu  sync.RWMutex
	closed   bool // close() 后置位，发送前据此避免向已关闭通道写入
}

// nextPow2 返回 >= n 的最小 2 的幂。
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

// newShardedQueue 创建分片有界队列。shardCount 会向上取整为 2 的幂以用
// 位运算取模；capacityPerShard 是每个分片的容量（背压阈值）。
func newShardedQueue(shardCount, capacityPerShard int) *shardedQueue {
	n := nextPow2(shardCount)
	q := &shardedQueue{
		shards: make([]chan queuedItem, n),
		mask:   uint64(n - 1),
		capPer: capacityPerShard,
		stopCh: make(chan struct{}),
	}
	for i := range q.shards {
		q.shards[i] = make(chan queuedItem, capacityPerShard)
	}
	return q
}

// markStopping 标记队列进入停机阶段并唤醒所有等待中的 Submit。
func (q *shardedQueue) markStopping() {
	q.stopOnce.Do(func() { close(q.stopCh) })
}

// shardOf 返回某个业务键所属的分片下标。
func (q *shardedQueue) shardOf(key string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() & q.mask)
}

// Submit 将事件放入对应分片。
//
// 判定顺序：
//  1. ctx 已取消 -> KindCanceled；
//  2. 通道当前有空闲 -> 立即入队（停机后仍允许排空型入队，例如启动重放）；
//  3. 否则等待空闲 / ctx 截止 / 停机信号；
//     ctx 先到期 -> KindQueueFull（容量背压，原因可区分、可重试）；
//     停机先发生 -> KindStopping（停机背压，事件已在持久日志中，安全重排）。
func (q *shardedQueue) Submit(ctx context.Context, item queuedItem) *Error {
	const op = "queue.submit"
	ch := q.shards[q.shardOf(item.ev.Key)]

	// 快路径：有空位直接入队。调用方即使已取消，事件也能可靠进入队列，
	// 由引擎侧继续排空，符合“不静默丢弃”的要求。
	select {
	case ch <- item:
		q.enqueued.Add(1)
		return nil
	default:
	}

	// 慢路径：队列已满，背压必须有明确结论。
	// 停机优先于超时：二者同时就绪时，事件被“安全重排”比“容量拒绝”更准确。
	if q.isStopping() {
		return newError(KindStopping, op,
			"engine is shutting down; event retained in durable journal for replay", nil)
	}
	if ctx.Err() == context.DeadlineExceeded {
		return newError(KindQueueFull, op,
			"bounded queue full; submit timed out under backpressure", ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return normalize(op, err)
	}
	select {
	case ch <- item:
		q.enqueued.Add(1)
		return nil
	case <-q.stopCh:
		return newError(KindStopping, op,
			"engine is shutting down; event retained in durable journal for replay", nil)
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return newError(KindQueueFull, op,
				"bounded queue full; submit timed out under backpressure", ctx.Err())
		}
		return normalize(op, ctx.Err())
	}
}

// isStopping 判断队列是否已进入停机阶段。
func (q *shardedQueue) isStopping() bool {
	select {
	case <-q.stopCh:
		return true
	default:
		return false
	}
}

// isClosed 判断分片通道是否已被关闭。
func (q *shardedQueue) isClosed() bool {
	q.closeMu.RLock()
	defer q.closeMu.RUnlock()
	return q.closed
}

// trySend 非阻塞地向某分片发送；队列已关闭或当前已满时返回 false。
func (q *shardedQueue) trySend(idx int, item queuedItem) bool {
	q.closeMu.RLock()
	defer q.closeMu.RUnlock()
	if q.closed {
		return false
	}
	select {
	case q.shards[idx] <- item:
		return true
	default:
		return false
	}
}

// shardChan 暴露指定分片的消费通道（供 worker 使用）。
func (q *shardedQueue) shardChan(i int) <-chan queuedItem { return q.shards[i] }

// depth 返回当前所有分片的积压总数（用于可观测指标）。
func (q *shardedQueue) depth() int {
	n := 0
	for i := range q.shards {
		n += len(q.shards[i])
	}
	return n
}

// capacity 返回队列总容量。
func (q *shardedQueue) capacity() int { return len(q.shards) * q.capPer }

// close 关闭所有分片通道。调用方必须保证已没有阻塞型并发 Submit；
// 非阻塞的 trySend 会通过 closed 标志安全失败。
func (q *shardedQueue) close() {
	q.closeMu.Lock()
	defer q.closeMu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	for i := range q.shards {
		close(q.shards[i])
	}
}
