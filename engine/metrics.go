package engine

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// KeyBacklog 是单个业务键的积压可观测快照：调度队列与每键协调器
// （mailbox + 缺口堆 + 处理中）两段合并，便于按业务键定位拥塞来源。
type KeyBacklog struct {
	Key    string `json:"key"`
	Total  int    `json:"total"`  // 调度队列 + 协调器在途总数
	Normal int    `json:"normal"` // 其中普通优先级事件数（调度队列段可拆分）
	High   int    `json:"high"`   // 其中高优先级事件数
	Sched  int    `json:"sched"`  // 尚在调度队列中的数量
	Coord  int    `json:"coord"`  // 已进入该键协调器的数量
	Hot    bool   `json:"hot"`    // 是否达到热点键积压阈值
}

// BacklogSnapshot 是按优先级与业务键维度的积压快照。
type BacklogSnapshot struct {
	Capacity     int
	Depth        int
	Normal       int
	High         int
	HotThreshold int
	HotKeyCount  int
	Keys         []KeyBacklog // 按积压量降序，便于直接观察最热点的键
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

	// AcceptedByPri 按优先级（normal/high）统计的接收成功数。
	AcceptedByPri map[string]int64
	// RejectedByPri 按优先级统计的拒绝数。
	RejectedByPri map[string]int64
	// RejectByKindPri 按“拒绝类别:优先级”二维统计，例如
	// queue_full:normal、priority_shed:normal，用于区分容量耗尽与优先级让位。
	RejectByKindPri map[string]int64
	// QueueDepthByPri 当前各优先级在调度队列中的积压量。
	QueueDepthByPri map[string]int
	// HotThreshold 热点键识别阈值（该键在调度队列中的积压达到即标记）。
	HotThreshold int
	// HotKeyCount 当前达到热点阈值的业务键数量。
	HotKeyCount int
	// KeyBacklogs 按业务键维度的积压快照（仅返回当前有积压的键）。
	KeyBacklogs []KeyBacklog

	StartTime time.Time
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

	acceptedPri [priorityCount]atomic.Int64
	rejectedPri [priorityCount]atomic.Int64

	mu              sync.Mutex
	rejectReason    map[string]int64
	rejectByKindPri map[string]int64
	queueDepth      func() int
	backlogFn       func() BacklogSnapshot
	startTime       time.Time
}

// newMetrics 创建指标记录器。depthFn 用于实时读取队列积压；backlogFn
// 返回按优先级/业务键合并的积压快照（可为 nil，表示暂未接入）。
func newMetrics(depthFn func() int, backlogFn func() BacklogSnapshot) *Metrics {
	return &Metrics{
		rejectReason:    make(map[string]int64),
		rejectByKindPri: make(map[string]int64),
		queueDepth:      depthFn,
		backlogFn:       backlogFn,
		startTime:       time.Now(),
	}
}

func (m *Metrics) recordReceived() { m.received.Add(1) }

// recordAccepted 记录一次成功入队，并按优先级维度计数。
func (m *Metrics) recordAccepted(pri int) {
	m.accepted.Add(1)
	m.acceptedPri[normalizePriority(pri)].Add(1)
}

// recordRejected 按归一化错误类别与事件优先级记录一次拒绝：既保留原有的
// “按原因”维度，也新增“按原因×优先级”与“按优先级”维度，保证容量耗尽
// （queue_full）与优先级让位（priority_shed）可区分、可观测。
func (m *Metrics) recordRejected(kind ErrorKind, pri int) {
	p := normalizePriority(pri)
	m.rejected.Add(1)
	m.rejectedPri[p].Add(1)
	m.mu.Lock()
	m.rejectReason[kind.String()]++
	m.rejectByKindPri[kind.String()+":"+priorityName(p)]++
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
	kindPri := make(map[string]int64, len(m.rejectByKindPri))
	for k, v := range m.rejectByKindPri {
		kindPri[k] = v
	}
	m.mu.Unlock()

	depth := 0
	if m.queueDepth != nil {
		depth = m.queueDepth()
	}
	snap := MetricsSnapshot{
		Received:        m.received.Load(),
		Accepted:        m.accepted.Load(),
		Rejected:        m.rejected.Load(),
		InFlight:        m.inFlight.Load(),
		Applied:         m.applied.Load(),
		Duplicates:      m.duplicates.Load(),
		Conflicts:       m.conflicts.Load(),
		Failed:          m.failed.Load(),
		Retried:         m.retried.Load(),
		QueueDepth:      depth,
		RejectReason:    reasons,
		RejectByKindPri: kindPri,
		AcceptedByPri: map[string]int64{
			priorityName(PriorityNormal): m.acceptedPri[PriorityNormal].Load(),
			priorityName(PriorityHigh):   m.acceptedPri[PriorityHigh].Load(),
		},
		RejectedByPri: map[string]int64{
			priorityName(PriorityNormal): m.rejectedPri[PriorityNormal].Load(),
			priorityName(PriorityHigh):   m.rejectedPri[PriorityHigh].Load(),
		},
		QueueDepthByPri: map[string]int{},
		StartTime:       m.startTime,
	}

	if m.backlogFn != nil {
		bl := m.backlogFn()
		snap.QueueMax = bl.Capacity
		snap.HotThreshold = bl.HotThreshold
		snap.HotKeyCount = bl.HotKeyCount
		snap.KeyBacklogs = bl.Keys
		snap.QueueDepthByPri[priorityName(PriorityNormal)] = bl.Normal
		snap.QueueDepthByPri[priorityName(PriorityHigh)] = bl.High
	}
	return snap
}

// sortKeyBacklogs 供合并逻辑复用：按总量降序、键名升序稳定排序。
func sortKeyBacklogs(keys []KeyBacklog) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Total != keys[j].Total {
			return keys[i].Total > keys[j].Total
		}
		return keys[i].Key < keys[j].Key
	})
}
