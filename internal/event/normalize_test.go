package event

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestNormalizeMapping(t *testing.T) {
	cases := []struct {
		name string
		in   error
		want Kind
	}{
		{"nil", nil, KindUnknown},
		{"context 超时", context.DeadlineExceeded, KindTimeout},
		{"包装后的超时", fmt.Errorf("read: %w", context.DeadlineExceeded), KindTimeout},
		{"context 取消", context.Canceled, KindCanceled},
		{"包装后的取消", fmt.Errorf("send: %w", context.Canceled), KindCanceled},
		{"未知内部错误", errors.New("boom"), KindInternal},
	}
	for _, c := range cases {
		got := Normalize(c.in)
		if c.in == nil {
			t.Logf("输入=%v 期望=nil 实际=%v", c.in, got)
			if got != nil {
				t.Errorf("nil 输入应返回 nil，实际 %v", got)
			}
			continue
		}
		var ee *Error
		if !errors.As(got, &ee) {
			t.Fatalf("输入=%v 归一化结果不是 *Error: %T", c.in, got)
		}
		t.Logf("输入=%q 判定依据=Kind 期望=%d 实际=%d", c.in, c.want, ee.Kind)
		if ee.Kind != c.want {
			t.Errorf("输入=%v 期望 Kind=%d 实际 Kind=%d", c.in, c.want, ee.Kind)
		}
	}
}

func TestNormalizePassthrough(t *testing.T) {
	orig := New(KindQueueFull, "queue full")
	got := Normalize(orig)
	t.Logf("输入已是 *Error(Kind=%d)，判定依据=应原样返回", orig.Kind)
	if got != error(orig) {
		t.Errorf("已归一化错误不应被二次包装")
	}
}

func TestIsKind(t *testing.T) {
	err := Normalize(fmt.Errorf("wrap: %w", context.Canceled))
	t.Logf("输入=包装后的 context.Canceled，判定依据=IsKind(KindCanceled)")
	if !IsKind(err, KindCanceled) {
		t.Errorf("IsKind 应识别包装后的取消错误")
	}
	if IsKind(err, KindTimeout) {
		t.Errorf("IsKind 不应把取消误判为超时")
	}
}
