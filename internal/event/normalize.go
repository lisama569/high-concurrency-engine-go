package event

import (
	"context"
	"errors"
	"fmt"
)

// Normalize 把任意来源的错误（context 取消/超时、内部错误）映射为
// 可区分的 *Error，调用方只能看到归一化后的 Kind，原始错误不透出语义。
// 若 err 已是 *Error 则原样返回；err 为 nil 时返回 nil。
func Normalize(err error) error {
	if err == nil {
		return nil
	}
	var ee *Error
	if errors.As(err, &ee) {
		return ee
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return &Error{Kind: KindTimeout, Msg: "event: timeout", Err: err}
	case errors.Is(err, context.Canceled):
		return &Error{Kind: KindCanceled, Msg: "event: canceled", Err: err}
	default:
		return &Error{Kind: KindInternal, Msg: "event: internal error", Err: err}
	}
}

// New 构造一个归一化错误。
func New(kind Kind, format string, args ...any) *Error {
	return &Error{Kind: kind, Msg: fmt.Sprintf(format, args...)}
}

// IsKind 判断 err 归一化后是否属于指定类别。
func IsKind(err error, kind Kind) bool {
	var ee *Error
	if !errors.As(err, &ee) {
		return false
	}
	return ee.Kind == kind
}
