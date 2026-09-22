package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestBackpressureRejectsWithReason 验证处理能力不足时事件不会被静默丢弃：
// 用极小队列 + 占住处理的 Handler 制造持续满载，Submit 在 SubmitTimeout
// 后必须以可区分的 KindQueueFull 被拒绝，且指标按原因计数。
func TestBackpressureRejectsWithReason(t *testing.T) {
	cfg := testConfig(t)
	cfg.Workers = 1
	cfg.QueuePerShard = 1
	cfg.MailboxPerKey = 1
	cfg.SubmitTimeout = 5 * time.Millisecond
	cfg.HandleTimeout = 5 * time.Second

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	var seen atomic.Int32
	blocking := func(ctx context.Context, ev Event, cur KeyState) (KeyState, error) {
		if seen.CompareAndSwap(0, 1) {
			close(started)
		} else {
			seen.Add(1)
		}
		// 模拟处理变慢：阻塞到测试释放，或引擎停机取消 ctx（保证优雅停机
		// 能在有在途慢任务时退出）。
		select {
		case <-release:
		case <-ctx.Done():
			return KeyState{}, ctx.Err()
		}
		return KeyState{Value: cur.Value + ev.Payload}, nil
	}
	eng, err := New(cfg, blocking)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		_ = eng.Shutdown(context.Background())
	}()

	// 先投递一个“种子事件”，它会进入唯一 worker 并在 handler 中阻塞，
	// 占住处理能力；再投递一个事件填满唯一队列位。之后的投递必须被拒绝。
	seed := Event{ID: "bp-seed", Key: "bpkey-seed", Seq: 1, Payload: 1}
	if p := eng.Submit(context.Background(), seed); p != nil {
		t.Fatalf("种子事件被拒绝: %v", p)
	}
	<-started
	// 填满唯一队列位（与种子不同键，不触发同键重排序）。
	for i := 0; i < 100; i++ {
		if p := eng.Submit(context.Background(), Event{
			ID: fmt.Sprintf("bp-fill-%d", i), Key: fmt.Sprintf("fillkey-%d", i), Seq: 1, Payload: 1,
		}); p != nil {
			break // 队列已满
		}
	}

	// 关键输入：队列容量 1 且唯一 worker 已被占住；继续投递必须被拒绝。
	// 使用互不相同的业务键，避免同键序号重排序把事件暂缓（那是另一条路径），
	// 纯粹压测“容量背压拒绝”。
	var queueFull, stopping, other int
	for i := 0; i < 50; i++ {
		ev := Event{ID: fmt.Sprintf("bp-%d", i), Key: fmt.Sprintf("bpkey-%d", i), Seq: 1, Payload: 1}
		perr := eng.Submit(context.Background(), ev)
		if perr == nil {
			continue // 个别可能恰好入队，不影响判定
		}
		switch perr.Kind {
		case KindQueueFull:
			queueFull++
		case KindStopping:
			stopping++
		default:
			other++
			t.Logf("非预期拒绝类别 kind=%s err=%v", perr.Kind, perr)
		}
	}
	t.Logf("判定依据: 容量背压拒绝 queue_full=%d stopping=%d other=%d; 指标 rejected=%d reasons=%v",
		queueFull, stopping, other, eng.Metrics().Rejected, eng.Metrics().RejectReason)
	if queueFull == 0 {
		t.Fatalf("期望出现 KindQueueFull 拒绝，实际 queue_full=0（事件可能被静默丢弃）")
	}
	if other != 0 {
		t.Fatalf("出现无法归类的拒绝 %d 个", other)
	}
	if r := eng.Metrics().RejectReason["queue_full"]; r == 0 {
		t.Fatal("指标未按 queue_full 原因统计拒绝")
	}
}

// TestSubmitRejectWhileStopping 验证停机期间新事件以 KindStopping 被拒绝。
func TestSubmitRejectWhileStopping(t *testing.T) {
	cfg := testConfig(t)
	eng, _ := New(cfg, nil)
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 用极短停机超时但无积压，停机很快完成；关键是停机标记后拒绝新事件。
	go func() { _ = eng.Shutdown(context.Background()) }()
	deadline := time.Now().Add(2 * time.Second)
	got := false
	for time.Now().Before(deadline) {
		ev := Event{ID: "stop-1", Key: "sk", Seq: 1, Payload: 1}
		if perr := eng.Submit(context.Background(), ev); perr != nil && perr.Kind == KindStopping {
			got = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !got {
		t.Fatal("停机期间 Submit 未返回 KindStopping")
	}
}

// TestNormalizeErrors 验证不同来源错误被映射为可区分类别，不透出原始类型。
func TestNormalizeErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want ErrorKind
	}{
		{"deadline", context.DeadlineExceeded, KindTimeout},
		{"canceled", context.Canceled, KindCanceled},
		{"wrapped-deadline", fmt.Errorf("wrap: %w", context.DeadlineExceeded), KindTimeout},
		{"plain", errors.New("boom"), KindInternal},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := normalize("test.op", c.err)
			if got.Kind != c.want {
				t.Fatalf("输入=%v 归一化 kind=%s want %s", c.err, got.Kind, c.want)
			}
			t.Logf("输入=%v -> kind=%s retryable=%v", c.err, got.Kind, got.Kind.Retryable())
		})
	}
	if e := normalize("op", nil); e != nil {
		t.Fatal("nil 必须归一化为 nil")
	}
	// 已是 *Error 的保持类别。
	orig := newError(KindStorage, "orig", "disk", nil)
	again := normalize("outer", orig)
	if again.Kind != KindStorage || again.Op != "orig" {
		t.Fatalf("已有 *Error 应保持类别与操作名，got=%v", again)
	}
}

// TestRetryThenDeadLetter 验证可重试错误被重试，超过次数后进死信且原因
// 可区分，且不会重复应用成功事件。
func TestRetryThenDeadLetter(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxAttempts = 3
	cfg.Backoff = time.Millisecond
	var calls atomic.Int32
	flaky := func(ctx context.Context, ev Event, cur KeyState) (KeyState, error) {
		// 对特定事件始终返回超时（可重试），其余正常。
		if ev.ID == "bad" {
			calls.Add(1)
			return KeyState{}, context.DeadlineExceeded
		}
		return KeyState{Value: cur.Value + ev.Payload}, nil
	}
	eng, _ := New(cfg, flaky)
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p := eng.Submit(context.Background(), Event{ID: "bad", Key: "dk", Seq: 1, Payload: 1}); p != nil {
		t.Fatal(p)
	}
	if p := eng.Submit(context.Background(), Event{ID: "good", Key: "gk", Seq: 1, Payload: 7}); p != nil {
		t.Fatal(p)
	}
	// 等待 bad 事件完成 3 次尝试并入死信，避免停机过早打断重试退避。
	waitFor(t, func() bool { return eng.Metrics().Failed >= 1 }, 2*time.Second, "bad 事件重试耗尽入死信")
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	m := eng.Metrics()
	t.Logf("判定: bad 事件尝试次数=%d (want 3) failed=%d retried=%d good 状态 value=%d",
		calls.Load(), m.Failed, m.Retried, mustGet(t, eng, "gk").Value)
	if calls.Load() != 3 {
		t.Fatalf("bad 应恰好尝试 3 次，实际 %d", calls.Load())
	}
	if m.Failed != 1 || m.Retried != 2 {
		t.Fatalf("failed=%d want 1, retried=%d want 2", m.Failed, m.Retried)
	}
	if v := mustGet(t, eng, "gk").Value; v != 7 {
		t.Fatalf("好事件状态被污染 value=%d want 7", v)
	}
	// 死信文件应包含归一化后的 timeout 原因。
	data := readDlq(t, cfg.DataDir)
	if !containsAll(data, `"kind":"timeout"`, `"id":"bad"`) {
		t.Fatalf("死信未记录归一化 timeout 原因或事件内容: %s", data)
	}
}

// TestStrongDurabilityStillCorrect 验证强同步模式功能同样正确（速度较慢，
// 仅少量事件），覆盖 fsync 路径。
func TestStrongDurabilityStillCorrect(t *testing.T) {
	cfg := testConfig(t)
	cfg.SyncEachWrite = true
	cfg.QueuePerShard = 128
	eng, _ := New(cfg, nil)
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 20; i++ {
		ev := Event{ID: fmt.Sprintf("s-%d", i), Key: "sk", Seq: i, Payload: 1}
		if p := eng.Submit(context.Background(), ev); p != nil {
			t.Fatal(p)
		}
	}
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := mustGet(t, eng, "sk")
	t.Logf("强同步判定: version=%d value=%d", st.Version, st.Value)
	if st.Version != 20 || st.Value != 20 {
		t.Fatalf("强同步模式终态错误 version=%d value=%d", st.Version, st.Value)
	}
}

func readDlq(t *testing.T, dataDir string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dataDir, "dead-letter.log"))
	if err != nil {
		t.Fatalf("read dead letter: %v", err)
	}
	return string(data)
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
