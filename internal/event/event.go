// Package event 定义事件模型与归一化错误类型。
package event

import "time"

// Event 是引擎处理的基本单元。
type Event struct {
	ID        string    // 全局唯一事件 ID（幂等键）
	Key       string    // 业务键，同键事件串行生效
	Seq       uint64    // 键内序号，用于乱序/冲突检测
	Payload   string    // 业务载荷
	CreatedAt time.Time // 产生时间
}

// Kind 表示归一化后的错误类别，调用方可据此区分处置策略。
type Kind int

const (
	KindUnknown   Kind = iota // 未分类
	KindQueueFull             // 背压拒绝：队列已满
	KindTimeout               // 超时（已归一化，不透出原始错误）
	KindCanceled              // 取消（已归一化）
	KindConflict              // 键状态冲突（乱序/版本不符）
	KindInternal              // 内部错误
)

// Error 是引擎对外暴露的统一错误类型。
type Error struct {
	Kind Kind
	Msg  string
	Err  error // 原始原因，仅用于日志，不向调用方透出语义
}

func (e *Error) Error() string { return e.Msg }
func (e *Error) Unwrap() error { return e.Err }
