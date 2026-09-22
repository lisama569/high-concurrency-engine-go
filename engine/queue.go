package engine

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// queuedItem 是内存有界队列中的一个条目。
type queuedItem struct {
	ev      Event
	attempt int
}

// shardedQueue 是按业务键哈希分片的有界内存队列。
//
// 每个分片是一条有界 channel，由固定 worker 消费；不同键分布在不同分片上
// 并行。队列满时 Submit 按 ctx 截止时间等待，超时以 KindQueueFull 明确拒
// 绝，停机期间以 KindStopping 拒绝，两种背压原因可区分，事件不被静默丢弃。
type shardedQueue struct {
	shards []chan queuedItem
	mask   uint64
	capPer int

	stopOnce sync.Once
	stopCh   chan struct{} // markStopping 时关闭
	closed   atomic.Bool   // close() 置位，发送前快速失败
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

func (q *shardedQueue) markStopping() {
	q.stopOnce.Do(func() { close(q.stopCh) })
}

func (q *shardedQueue) shardOf(key string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() & q.mask)
}

// safeSend 非阻塞发送。用原子标志做快速失败，并用 recover 兜底“检查标志
// 后通道恰好被 close”的极端竞态，保证任何情况下都不会 panic。
func (q *shardedQueue) safeSend(ch chan queuedItem, item queuedItem) (ok bool) {
	if q.closed.Load() {
		return false
	}
	defer func() {
		if r := recover(); r != nil {
			ok = false
		}
	}()
	select {
	case ch <- item:
		return !q.closed.Load()
	default:
		return false
	}
}

// blockingSend 在队列已满（进入本函数即代表快路径 safeSend 失败）时，按
// 截止时间等待空位。等待期间 ctx 到期一律视为“容量背压等待超时”，归类为
// KindQueueFull；只有停机信号归类为 KindStopping。外部 ctx 被主动取消归
// KindCanceled。
func (q *shardedQueue) blockingSend(ch chan queuedItem, item queuedItem, ctx context.Context) *Error {
	const op = "queue.submit"
	// 区分“因 SubmitTimeout 到期”与“外部主动取消”：WithTimeout 的截止错误
	// 在队列满场景下按背压归类。
	queueFullErr := func() *Error {
		return newError(KindQueueFull, op,
			"bounded queue full; submit timed out under backpressure", ctx.Err())
	}
	ticker := time.NewTicker(200 * time.Microsecond)
	defer ticker.Stop()
	for {
		if q.safeSend(ch, item) {
			return nil
		}
		if q.isStopping() {
			return newError(KindStopping, op,
				"engine is shutting down; event retained in durable journal for replay", nil)
		}
		if err := ctx.Err(); err != nil {
			// 能走到这里说明队列满：deadline 到期=背压；主动取消=取消。
			if err == context.DeadlineExceeded {
				return queueFullErr()
			}
			return newError(KindCanceled, op, "submit canceled while queue full", err)
		}
		select {
		case <-q.stopCh:
			return newError(KindStopping, op,
				"engine is shutting down; event retained in durable journal for replay", nil)
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return queueFullErr()
			}
			return newError(KindCanceled, op, "submit canceled while queue full", ctx.Err())
		case <-ticker.C:
		}
	}
}

// Submit 将事件放入对应分片；满则在 ctx 截止前等待，否则按原因拒绝。
func (q *shardedQueue) Submit(ctx context.Context, item queuedItem) *Error {
	if q.closed.Load() {
		return newError(KindStopping, "queue.submit",
			"queue already closed; event retained in durable journal for replay", nil)
	}
	ch := q.shards[q.shardOf(item.ev.Key)]
	if q.safeSend(ch, item) {
		return nil
	}
	return q.blockingSend(ch, item, ctx)
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

func (q *shardedQueue) trySend(idx int, item queuedItem) bool {
	return q.safeSend(q.shards[idx], item)
}

func (q *shardedQueue) shardChan(i int) <-chan queuedItem { return q.shards[i] }

func (q *shardedQueue) depth() int {
	n := 0
	for i := range q.shards {
		n += len(q.shards[i])
	}
	return n
}

func (q *shardedQueue) capacity() int { return len(q.shards) * q.capPer }

// close 关闭所有分片通道。safeSend 的原子标志 + recover 保证并发发送
// 安全失败而不是 panic；排空期间的重投走 trySend，通道关闭后立即返回 false。
func (q *shardedQueue) close() {
	if q.closed.Swap(true) {
		return
	}
	for i := range q.shards {
		close(q.shards[i])
	}
}
