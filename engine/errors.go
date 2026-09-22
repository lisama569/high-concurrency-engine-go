// Package engine 实现一个本地可运行的高并发事件处理引擎。
//
// 本文件定义对外暴露的、归一化之后的错误类型。所有来自下游（超时、上下文
// 取消、内部错误、存储冲突等）的失败都会被转换为 Error，调用方可以通过
// errors.As 或 errors.Is 按类别区分处理，原始错误不会被直接透出。
package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrorKind 是归一化后的错误类别。
type ErrorKind int

const (
	// KindUnknown 未分类错误。
	KindUnknown ErrorKind = iota
	// KindTimeout 处理超过截止时间。
	KindTimeout
	// KindCanceled 上下文已取消（通常发生在优雅停机过程中）。
	KindCanceled
	// KindQueueFull 有界队列已满，事件被拒绝（背压）。
	KindQueueFull
	// KindStopping 引擎正在停机，不再接收新事件（背压）。
	KindStopping
	// KindConflict 业务键状态版本冲突，事件被显式标记而非静默覆盖。
	KindConflict
	// KindDuplicate 事件重复投递，按幂等语义跳过。
	KindDuplicate
	// KindStorage 本地状态/日志存储故障。
	KindStorage
	// KindInvalid 输入不合法。
	KindInvalid
	// KindInternal 其它内部错误。
	KindInternal
)

var kindNames = map[ErrorKind]string{
	KindUnknown:   "unknown",
	KindTimeout:   "timeout",
	KindCanceled:  "canceled",
	KindQueueFull: "queue_full",
	KindStopping:  "stopping",
	KindConflict:  "conflict",
	KindDuplicate: "duplicate",
	KindStorage:   "storage",
	KindInvalid:   "invalid",
	KindInternal:  "internal",
}

// 各类错误的默认可重试性：背压拒绝与停机拒绝可稍后重投；取消/冲突/重复/
// 非法输入重试无意义；超时与存储故障可重试。
var kindRetryables = map[ErrorKind]bool{
	KindUnknown:   false,
	KindTimeout:   true,
	KindCanceled:  false,
	KindQueueFull: true,
	KindStopping:  true,
	KindConflict:  false,
	KindDuplicate: false,
	KindStorage:   true,
	KindInvalid:   false,
	KindInternal:  false,
}

// String 返回错误类别的可读名称。
func (k ErrorKind) String() string {
	return kindName(k)
}

// Retryable 表示该类错误是否可以安全重试。
func (k ErrorKind) Retryable() bool {
	return kindRetryable(k)
}

// Error 是引擎对外暴露的唯一错误形态。
type Error struct {
	// Kind 归一化错误类别。
	Kind ErrorKind
	// Op 发生错误的操作，用于定位链路。
	Op string
	// Message 人类可读的判定依据。
	Message string
	// cause 被归一化包装的原始错误，不外泄其类型。
	cause error
}

// Error 实现 error 接口，输出形如：
//
//	engine: worker.handle: timeout: event k1#3 deadline exceeded (cause: context deadline exceeded)
func (e *Error) Error() string {
	return errorString(e)
}

// Unwrap 支持 errors.Is/errors.As 访问被包装的内部错误。
func (e *Error) Unwrap() error {
	return e.cause
}

// newError 构造一个归一化错误。
func newError(kind ErrorKind, op, msg string, cause error) *Error {
	return &Error{Kind: kind, Op: op, Message: msg, cause: cause}
}

// normalize 将任意错误映射为可区分的 *Error。
//
// 归一化规则（不直接透出原始错误类型）：
//   - 已经是 *Error 的，保持其类别不变，仅补上操作名；
//   - context.DeadlineExceeded（含包装链）-> KindTimeout；
//   - context.Canceled（含包装链）            -> KindCanceled；
//   - nil                                     -> nil。
func normalize(op string, err error) *Error {
	if err == nil {
		return nil
	}
	var ee *Error
	if errors.As(err, &ee) {
		if ee.Op == "" {
			ee.Op = op
		}
		return ee
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return newError(KindTimeout, op, err.Error(), err)
	case errors.Is(err, context.Canceled):
		return newError(KindCanceled, op, err.Error(), err)
	default:
		return newError(KindInternal, op, err.Error(), err)
	}
}

func kindName(k ErrorKind) string {
	if n, ok := kindNames[k]; ok {
		return n
	}
	return fmt.Sprintf("kind(%d)", int(k))
}

func kindRetryable(k ErrorKind) bool { return kindRetryables[k] }

func errorString(e *Error) string {
	var b strings.Builder
	b.WriteString("engine: ")
	if e.Op != "" {
		b.WriteString(e.Op)
		b.WriteString(": ")
	}
	b.WriteString(e.Kind.String())
	if e.Message != "" {
		b.WriteString(": ")
		b.WriteString(e.Message)
	}
	if e.cause != nil && e.cause.Error() != e.Message {
		b.WriteString(" (cause: ")
		b.WriteString(e.cause.Error())
		b.WriteString(")")
	}
	return b.String()
}
