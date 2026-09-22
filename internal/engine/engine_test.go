package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/high-concurrency-engine-go/internal/event"
	"github.com/lisama569/high-concurrency-engine-go/internal/store"
)

func newEngine(t *testing.T, cfg Config) (*Engine, *store.FileStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return New(cfg, st), st, path
}

func shutdown(t *testing.T, e *Engine) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// 并发竞争：多生产者向少量键并发提交，同键事件必须串行生效，
// 最终状态可复现：每个键的 Seq 等于该键已提交事件中的最大 Seq。
func TestConcurrentSameKeyOrdering(t *testing.T) {
	e, st, _ := newEngine(t, Config{QueueCapacity: 1024, Workers: 8})

	const producers = 8
	const perProducer = 50
	const keys = 4

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				n := p*perProducer + i + 1
				ev := event.Event{
					ID:  fmt.Sprintf("evt-%d", n),
					Key: fmt.Sprintf("key-%d", n%keys),
					Seq: uint64(n),
				}
				if err := e.Submit(context.Background(), ev); err != nil {
					t.Errorf("Submit: %v", err)
				}
			}
		}(p)
	}
	wg.Wait()
	shutdown(t, e)

	m := e.Metrics()
	t.Logf("输入=%d 条事件/%d 个键 判定依据=Submitted=总数 且 Rejected=0 实际 %+v",
		producers*perProducer, keys, m)
	if m.Submitted != producers*perProducer || m.Rejected != 0 {
		t.Fatalf("容量充足时不应有背压拒绝: %+v", m)
	}
	for k := 0; k < keys; k++ {
		rec, err := st.Get(fmt.Sprintf("key-%d", k))
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		// 该键最大 Seq：n%keys==k 的最大 n
		wantSeq := uint64(0)
		for n := 1; n <= producers*perProducer; n++ {
			if n%keys == k {
				wantSeq = uint64(n)
			}
		}
		t.Logf("键=key-%d 判定依据=Seq 等于该键最大提交序号 %d 实际 seq=%d", k, wantSeq, rec.Seq)
		if rec.Seq != wantSeq {
			t.Errorf("key-%d 最终 Seq=%d，期望 %d（同键乱序生效）", k, rec.Seq, wantSeq)
		}
	}
}

// 背压：队列容量 1、工作者 1 且事件处理慢于提交时，
// 超出部分必须以 KindQueueFull 被拒绝，不得静默丢弃。
func TestBackpressureRejection(t *testing.T) {
	e, _, _ := newEngine(t, Config{QueueCapacity: 1, Workers: 1})

	const total = 200
	var rejected int
	for i := 0; i < total; i++ {
		err := e.Submit(context.Background(), event.Event{
			ID:  fmt.Sprintf("evt-%d", i),
			Key: "k1",
			Seq: uint64(i + 1),
		})
		if err != nil {
			if !event.IsKind(err, event.KindQueueFull) {
				t.Fatalf("背压错误应为 KindQueueFull，实际 %v", err)
			}
			rejected++
		}
	}
	shutdown(t, e)

	m := e.Metrics()
	t.Logf("输入=%d 条事件(容量=1) 判定依据=拒绝数>0 且 Submitted+Rejected=总尝试数 实际 submitted=%d rejected=%d 本地计数=%d",
		total, m.Submitted, m.Rejected, rejected)
	if rejected == 0 {
		t.Fatalf("容量为 1 时 200 条突发提交必须产生背压拒绝")
	}
	if m.Rejected != uint64(rejected) {
		t.Fatalf("指标 Rejected=%d 与本地计数 %d 不一致", m.Rejected, rejected)
	}
	if m.Submitted+m.Rejected != total {
		t.Fatalf("Submitted(%d)+Rejected(%d) 应等于总尝试数 %d", m.Submitted, m.Rejected, total)
	}
}

// 优雅停机：Shutdown 返回时队列已排空，全部已入队事件都被处理。
func TestGracefulShutdownDrains(t *testing.T) {
	e, _, _ := newEngine(t, Config{QueueCapacity: 500, Workers: 4})

	const total = 500
	for i := 0; i < total; i++ {
		if err := e.Submit(context.Background(), event.Event{
			ID:  fmt.Sprintf("evt-%d", i),
			Key: fmt.Sprintf("key-%d", i%8),
			Seq: uint64(i + 1),
		}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	shutdown(t, e)

	m := e.Metrics()
	processed := m.Applied + m.Duplicated + m.Conflicts + m.Failed
	t.Logf("输入=%d 条已入队事件 判定依据=停机后处理数=入队数 实际 submitted=%d processed=%d", total, m.Submitted, processed)
	if processed != m.Submitted {
		t.Fatalf("停机时在途任务未被排空: submitted=%d processed=%d", m.Submitted, processed)
	}

	// 停机后提交应被拒绝。
	err := e.Submit(context.Background(), event.Event{ID: "late", Key: "k", Seq: 1})
	t.Logf("停机后提交 判定依据=返回 KindCanceled 实际 err=%v", err)
	if !event.IsKind(err, event.KindCanceled) {
		t.Fatalf("停机后提交应返回 KindCanceled，实际 %v", err)
	}
}

// 停重启：第一轮处理部分事件后停机，重启后重复投递全部事件，
// 已应用的事件不得重复生效，状态不得丢失。
func TestRestartNoDuplicateApply(t *testing.T) {
	e, _, path := newEngine(t, Config{QueueCapacity: 256, Workers: 4})

	const total = 200
	submit := func(e *Engine) {
		for i := 0; i < total; i++ {
			for {
				err := e.Submit(context.Background(), event.Event{
					ID:      fmt.Sprintf("evt-%d", i),
					Key:     fmt.Sprintf("key-%d", i%4),
					Seq:     uint64(i/4 + 1),
					Payload: fmt.Sprintf("v%d", i),
				})
				if event.IsKind(err, event.KindQueueFull) {
					time.Sleep(time.Millisecond) // 背压下延迟重试
					continue
				}
				if err != nil {
					t.Fatalf("Submit: %v", err)
				}
				break
			}
		}
	}
	submit(e)
	shutdown(t, e)
	m1 := e.Metrics()
	// 多工作者下处理顺序不保证等于提交顺序，乱序事件会被标记冲突，
	// 因此第一轮的稳定不变量是：每条事件恰好被归类一次（生效或冲突）。
	t.Logf("第一轮: 判定依据=Applied+Conflicts=总数 %d 实际 %+v", total, m1)
	if m1.Applied+m1.Conflicts != total {
		t.Fatalf("第一轮每条事件应被归类一次: %+v", m1)
	}

	// 模拟重启：同一状态文件重建引擎，重复投递全部事件。
	st2, err := store.Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	e2 := New(Config{QueueCapacity: 256, Workers: 4}, st2)
	submit(e2)
	shutdown(t, e2)

	m2 := e2.Metrics()
	// 重启后重投：第一轮已生效的命中幂等去重，已冲突的仍判冲突，
	// 稳定不变量是 Applied=0 且 Duplicated+Conflicts=总数。
	t.Logf("第二轮(重启后重投): 判定依据=Applied=0 且 Duplicated+Conflicts=%d 实际 %+v", total, m2)
	if m2.Applied != 0 {
		t.Fatalf("重启后重复投递造成重复应用: applied=%d", m2.Applied)
	}
	if m2.Duplicated+m2.Conflicts != total {
		t.Fatalf("重启后每条事件应命中去重或冲突: %+v", m2)
	}
	// 跨重启的最终状态：每个键的 Seq 必须达到该键最大提交序号。
	for k := 0; k < 4; k++ {
		rec, err := st2.Get(fmt.Sprintf("key-%d", k))
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		t.Logf("键=key-%d 判定依据=Seq=该键最大序号 %d 实际 seq=%d", k, total/4, rec.Seq)
		if rec.Seq != uint64(total/4) {
			t.Fatalf("重启后 key-%d 状态错误: seq=%d 期望 %d", k, rec.Seq, total/4)
		}
	}
}

// 提交时 ctx 取消应归一化为 KindCanceled，而不是透出原始错误。
func TestSubmitCanceledContext(t *testing.T) {
	e, _, _ := newEngine(t, Config{QueueCapacity: 1, Workers: 1})
	defer shutdown(t, e)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := e.Submit(ctx, event.Event{ID: "evt-x", Key: "k", Seq: 1})
	t.Logf("输入=已取消的 ctx 判定依据=返回 KindCanceled 且不直接透出 context.Canceled 实际 err=%v", err)
	if !event.IsKind(err, event.KindCanceled) {
		t.Fatalf("取消的 ctx 应归一化为 KindCanceled，实际 %v", err)
	}
	var ee *event.Error
	if !errors.As(err, &ee) {
		t.Fatalf("对外错误应为 *event.Error，实际 %T", err)
	}
}
