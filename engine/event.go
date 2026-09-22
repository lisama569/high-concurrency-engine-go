package engine

import (
	"context"
	"errors"
	"time"
)

// Event 是引擎处理的最小事件单元。
type Event struct {
	// ID 事件的全局唯一标识，也是幂等去重键。
	ID string `json:"id"`
	// Key 业务键。同一业务键的事件必须串行、按序生效。
	Key string `json:"key"`
	// Seq 业务键内的单调递增序号（从 1 开始），用于顺序与冲突判定。
	Seq int64 `json:"seq"`
	// Kind 业务事件类型，由用户注册的 Handler 解释。
	Kind string `json:"kind"`
	// Payload 事件负载（示例引擎中为可叠加的整数值）。
	Payload int64 `json:"payload"`
	// EnqueueAt 事件进入引擎的时间。
	EnqueueAt time.Time `json:"enqueue_at,omitempty"`
}

// Outcome 是单个事件的处理结论。
type Outcome int

const (
	// OutcomeInvalid 非法结论。
	OutcomeInvalid Outcome = iota
	// OutcomeApplied 事件正常应用到业务键状态。
	OutcomeApplied
	// OutcomeDuplicate 重复投递，幂等跳过。
	OutcomeDuplicate
	// OutcomeConflict 版本冲突，被显式标记，未覆盖状态。
	OutcomeConflict
	// OutcomeFailed 处理失败（将按策略重试或进入死信）。
	OutcomeFailed
)

var outcomeNames = map[Outcome]string{
	OutcomeInvalid:   "invalid",
	OutcomeApplied:   "applied",
	OutcomeDuplicate: "duplicate",
	OutcomeConflict:  "conflict",
	OutcomeFailed:    "failed",
}

// String 返回处理结论的可读名称。
func (o Outcome) String() string {
	if n, ok := outcomeNames[o]; ok {
		return n
	}
	return "unknown"
}

// Result 是事件处理的判定结果，会被记录进可观测指标。
type Result struct {
	Event   Event
	Outcome Outcome
	Err     *Error
	// Attempts 实际处理尝试次数（含重试）。
	Attempts int
}

// Handler 是用户提供的业务处理函数：根据事件推导业务键的新状态。
// 返回的 next 是该键应用事件后的期望值；引擎通过版本比较决定
// 应用、判重或标记冲突。Handler 必须是确定性的，以便安全重试。
type Handler func(ctx context.Context, ev Event, current KeyState) (next KeyState, err error)

// validate 检查事件字段是否足以参与处理。
func (ev Event) validate() error {
	if ev.ID == "" {
		return newError(KindInvalid, "event.validate", "event id is empty", nil)
	}
	if ev.Key == "" {
		return newError(KindInvalid, "event.validate", "event key is empty", errors.New("empty business key"))
	}
	if ev.Seq < 1 {
		return newError(KindInvalid, "event.validate", "event seq must be >= 1", nil)
	}
	return nil
}
