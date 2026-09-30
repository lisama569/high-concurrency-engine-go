package engine

import (
	"sync"
	"sync/atomic"
	"time"
)

// KeyBacklog 是单个业务键在引擎内部的积压明细。
type KeyBacklog struct {
	// Queue 仍停留在分片调度队列中的该键事件数。
	Queue int
	// Outstanding 已进入该键协调器（mailbox + 缺口堆 + 处理中）的事件数。
	Outstanding int
	// Total = Queue + Outstanding，是该键“已接收未结论”的总数。
	Total int
	// Hot 表示该键是否达到热点键积压阈值。
	Hot bool
}

// MetricsSnapshot 是某一时刻引擎可观测指标的快照。
type MetricsSnapshot struct {
	Received   int64 // 进入引擎的事件总数
	Accepted   int64 // 成功进入有界队列的事件数
	Rejected   int64 // 因背压等原因被拒绝的事件数
	InFlight   int64 // 正在处理中的事件数
	Applied    int64 // 正常应用的事件数
	Duplicates int64 // 幂等判重跳过的事件数
	Conflicts  int64 // 状态冲突被标记的事件数
	Failed     int64 // 最终处理失败（进死信）的事件数
	Retried    int64 // 重试次数
	QueueDepth int   // 当前队列积压总量
	QueueMax   int   // 队列容量上限（全分片之和）
	// RejectReason 按归一化错误类别统计的拒绝原因，例如 queue_full/stopping。
	RejectReason map[string]int64
	StartTime    time.Time

	// 按优先级分类的接收/接收成功/拒绝计数，键为 Priority.String()
	// （"high" / "normal"）。
	ReceivedByPriority map[string]int64
	AcceptedByPriority map[string]int64
	RejectedByPriority map[string]int64
	// QueueHigh / QueueNormal 当前队列中按优先级分类的积压。
	QueueHigh   int
	QueueNormal int
	// KeyBacklog 每个业务键的当前积压（含队列与协调器两侧）。
	KeyBacklog map[string]KeyBacklog
	// HotKeys 积压达到 HotKeyBacklogThreshold 的业务键及其积压量（无序，
	// 仅列出热点）；为空表示当前没有识别出热点键。
	HotKeys map[string]int
	// HotKeyThreshold 热点键判定阈值。
	HotKeyThreshold int
}

// Metrics 以并发安全的方式记录吞吐与积压指标。
type Metrics struct {
	received   atomic.Int64
	accepted   atomic.Int64
	rejected   atomic.Int64
	inFlight   atomic.Int64
	applied    atomic.Int64
	duplicates atomic.Int64
	conflicts  atomic.Int64
	failed     atomic.Int64
	retried    atomic.Int64

	// 按优先级分类的接收/接收成功/拒绝（下标为归一化后的 Priority）。
	receivedPri [2]atomic.Int64
	acceptedPri [2]atomic.Int64
	rejectedPri [2]atomic.Int64

	mu            sync.Mutex
	rejectReason  map[string]int64
	queueDepth    func() int
	queueDepthPri func() (high, normal int)
	keyBacklogFn  func() map[string]KeyBacklog
	queueCapacity int
	hotKeyThresh  int
	startTime     time.Time
}

// newMetrics 创建指标记录器。
//   - depthFn 实时读取队列积压总量；
//   - depthPriFn 实时读取按优先级分类的队列积压；
//   - keyBacklogFn 实时读取按业务键聚合的积压（队列 + 协调器）；
//   - hotKeyThreshold 为热点键判定阈值。
func newMetrics(queueCapacity int, hotKeyThreshold int,
	depthFn func() int,
	depthPriFn func() (high, normal int),
	keyBacklogFn func() map[string]KeyBacklog,
) *Metrics {
	return &Metrics{
		rejectReason:  make(map[string]int64),
		queueDepth:    depthFn,
		queueDepthPri: depthPriFn,
		keyBacklogFn:  keyBacklogFn,
		queueCapacity: queueCapacity,
		hotKeyThresh:  hotKeyThreshold,
		startTime:     time.Now(),
	}
}

func (m *Metrics) recordReceived(p Priority) {
	m.received.Add(1)
	m.receivedPri[normalizePriority(p)].Add(1)
}

func (m *Metrics) recordAccepted(p Priority) {
	m.accepted.Add(1)
	m.acceptedPri[normalizePriority(p)].Add(1)
}

// recordRejected 按归一化错误类别记录一次拒绝，保证“被拒绝原因可区分”，
// 同时按事件优先级维度计数。
func (m *Metrics) recordRejected(p Priority, kind ErrorKind) {
	m.rejected.Add(1)
	m.rejectedPri[normalizePriority(p)].Add(1)
	m.mu.Lock()
	m.rejectReason[kind.String()]++
	m.mu.Unlock()
}

func (m *Metrics) incInFlight() { m.inFlight.Add(1) }
func (m *Metrics) decInFlight() { m.inFlight.Add(-1) }

func (m *Metrics) recordOutcome(o Outcome) {
	switch o {
	case OutcomeApplied:
		m.applied.Add(1)
	case OutcomeDuplicate:
		m.duplicates.Add(1)
	case OutcomeConflict:
		m.conflicts.Add(1)
	case OutcomeFailed:
		m.failed.Add(1)
	}
}

func (m *Metrics) recordRetry() { m.retried.Add(1) }

// Snapshot 返回当前指标的一致性快照。
func (m *Metrics) Snapshot() MetricsSnapshot {
	m.mu.Lock()
	reasons := make(map[string]int64, len(m.rejectReason))
	for k, v := range m.rejectReason {
		reasons[k] = v
	}
	hotThreshold := m.hotKeyThresh
	m.mu.Unlock()

	depth, capTotal := 0, m.queueCapacity
	high, normal := 0, 0
	if m.queueDepth != nil {
		depth = m.queueDepth()
	}
	if m.queueDepthPri != nil {
		high, normal = m.queueDepthPri()
	}

	keyBacklogs := map[string]KeyBacklog{}
	hotKeys := map[string]int{}
	if m.keyBacklogFn != nil {
		for k, b := range m.keyBacklogFn() {
			keyBacklogs[k] = b
			if b.Total >= hotThreshold {
				b.Hot = true
				keyBacklogs[k] = b
				hotKeys[k] = b.Total
			}
		}
	}

	priNames := [2]string{PriorityNormal.String(), PriorityHigh.String()}
	recvByPri := map[string]int64{
		priNames[PriorityNormal]: m.receivedPri[PriorityNormal].Load(),
		priNames[PriorityHigh]:   m.receivedPri[PriorityHigh].Load(),
	}
	accByPri := map[string]int64{
		priNames[PriorityNormal]: m.acceptedPri[PriorityNormal].Load(),
		priNames[PriorityHigh]:   m.acceptedPri[PriorityHigh].Load(),
	}
	rejByPri := map[string]int64{
		priNames[PriorityNormal]: m.rejectedPri[PriorityNormal].Load(),
		priNames[PriorityHigh]:   m.rejectedPri[PriorityHigh].Load(),
	}

	return MetricsSnapshot{
		Received:     m.received.Load(),
		Accepted:     m.accepted.Load(),
		Rejected:     m.rejected.Load(),
		InFlight:     m.inFlight.Load(),
		Applied:      m.applied.Load(),
		Duplicates:   m.duplicates.Load(),
		Conflicts:    m.conflicts.Load(),
		Failed:       m.failed.Load(),
		Retried:      m.retried.Load(),
		QueueDepth:   depth,
		QueueMax:     capTotal,
		RejectReason: reasons,
		StartTime:    m.startTime,

		ReceivedByPriority: recvByPri,
		AcceptedByPriority: accByPri,
		RejectedByPriority: rejByPri,
		QueueHigh:          high,
		QueueNormal:        normal,
		KeyBacklog:         keyBacklogs,
		HotKeys:            hotKeys,
		HotKeyThreshold:    hotThreshold,
	}
}
