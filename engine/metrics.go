package engine

import (
	"sync"
	"sync/atomic"
	"time"
)

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

	mu            sync.Mutex
	rejectReason  map[string]int64
	queueDepth    func() int
	queueCapacity int
	startTime     time.Time
}

// newMetrics 创建指标记录器。depthFn 用于实时读取队列积压。
func newMetrics(queueCapacity int, depthFn func() int) *Metrics {
	return &Metrics{
		rejectReason:  make(map[string]int64),
		queueDepth:    depthFn,
		queueCapacity: queueCapacity,
		startTime:     time.Now(),
	}
}

func (m *Metrics) recordReceived() { m.received.Add(1) }

func (m *Metrics) recordAccepted() { m.accepted.Add(1) }

// recordRejected 按归一化错误类别记录一次拒绝，保证“被拒绝原因可区分”。
func (m *Metrics) recordRejected(kind ErrorKind) {
	m.rejected.Add(1)
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
	m.mu.Unlock()

	depth, capTotal := 0, m.queueCapacity
	if m.queueDepth != nil {
		depth = m.queueDepth()
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
	}
}
