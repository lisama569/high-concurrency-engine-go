package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// testConfig 返回一份快速、确定的测试配置。
func testConfig(t *testing.T) Config {
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "data")
	cfg.Workers = 4
	cfg.QueuePerShard = 64
	cfg.SubmitTimeout = 20 * time.Millisecond
	cfg.HandleTimeout = 200 * time.Millisecond
	cfg.MaxAttempts = 3
	cfg.Backoff = time.Millisecond
	cfg.ShutdownTimeout = 3 * time.Second
	// 测试默认使用组提交异步持久化：吞吐高且不受沙箱磁盘 fsync 物理延迟
	// 干扰；崩溃一致性由事件源 offset 重投 + 存储幂等保证，优雅停机会完整
	// 落盘。强同步模式由专门用例单独覆盖。
	cfg.SyncEachWrite = false
	return cfg
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waitFor timed out: %s", msg)
}

// TestConcurrentOrderingAndTotals 验证并发竞争下：同一键严格有序生效、
// 总量精确等于投递量、无重复应用。
func TestConcurrentOrderingAndTotals(t *testing.T) {
	cfg := testConfig(t)
	eng, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	const goroutines = 16
	const perKey = 150
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perKey; i++ {
				// 每个 goroutine 独占一个业务键，验证并发下不重不漏；
				// 同一业务键的并发竞争由 TestSameKeyConcurrentIsOrdered 覆盖。
				ev := Event{
					ID:      fmt.Sprintf("g%d-%d", g, i+1),
					Key:     fmt.Sprintf("gk%d", g),
					Seq:     int64(i + 1),
					Payload: 2,
				}
				if p := eng.Submit(context.Background(), ev); p != nil {
					t.Errorf("unexpected reject: %v", p)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	states, err := eng.StateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for g := 0; g < goroutines; g++ {
		key := fmt.Sprintf("gk%d", g)
		st := states[key]
		if st.Version != int64(perKey) {
			t.Fatalf("%s version=%d want %d", key, st.Version, perKey)
		}
		want := int64(perKey) * 2
		if st.Value != want {
			t.Fatalf("%s value=%d want %d (double apply?)", key, st.Value, want)
		}
		total += st.Value
	}
	wantTotal := int64(goroutines) * int64(perKey) * 2
	t.Logf("判定依据: %d 个独占键各 %d 条, 每条 payload=2, total=%d want=%d",
		goroutines, perKey, total, wantTotal)
	if total != wantTotal {
		t.Fatalf("total=%d want %d", total, wantTotal)
	}
}

// TestSameKeyConcurrentIsOrdered 验证多个 goroutine 同时往同一业务键投递，
// 事件按 Seq 顺序生效、不重不漏、终态版本正确。
func TestSameKeyConcurrentIsOrdered(t *testing.T) {
	cfg := testConfig(t)
	cfg.QueuePerShard = 256
	eng, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	const n = 400
	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ev := Event{
				ID:      fmt.Sprintf("same-%d", i),
				Key:     "hotkey",
				Seq:     int64(i),
				Payload: 1,
			}
			// 背压拒绝时重试，保证最终全部进入引擎。
			for {
				p := eng.Submit(context.Background(), ev)
				if p == nil {
					return
				}
				if p.Kind != KindQueueFull {
					t.Errorf("unexpected reject kind=%s: %v", p.Kind, p)
					return
				}
				time.Sleep(time.Millisecond)
			}
		}(i)
	}
	wg.Wait()
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	st, ok, _ := eng.store.Get(context.Background(), "hotkey")
	if !ok {
		t.Fatal("hotkey missing")
	}
	t.Logf("关键输入: hotkey 并发投递 %d 条; 判定: version=%d value=%d", n, st.Version, st.Value)
	if st.Version != int64(n) || st.Value != int64(n) {
		t.Fatalf("hotkey version=%d value=%d want %d/%d (乱序或重复应用)", st.Version, st.Value, n, n)
	}
}

// TestDuplicateIsIdempotent 验证重复投递（同 ID）不会重复累加。
func TestDuplicateIsIdempotent(t *testing.T) {
	cfg := testConfig(t)
	eng, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ev := Event{ID: "dup-1", Key: "k", Seq: 1, Payload: 10}
	for i := 0; i < 5; i++ {
		if p := eng.Submit(context.Background(), ev); p != nil {
			t.Fatalf("submit #%d: %v", i, p)
		}
	}
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := mustGet(t, eng, "k")
	t.Logf("关键输入: 同一事件 dup-1 投递 5 次 payload=10; 判定 value=%d version=%d dupMetrics=%d",
		st.Value, st.Version, eng.Metrics().Duplicates)
	if st.Value != 10 || st.Version != 1 {
		t.Fatalf("idempotency broken: value=%d version=%d", st.Value, st.Version)
	}
	if d := eng.Metrics().Duplicates; d != 4 {
		t.Fatalf("duplicates metric=%d want 4", d)
	}
}

// TestConflictExplicit 验证乱序（过期 Seq）事件被显式标记冲突，不覆盖状态。
func TestConflictExplicit(t *testing.T) {
	cfg := testConfig(t)
	eng, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 3; i++ {
		ev := Event{ID: fmt.Sprintf("c-%d", i), Key: "k", Seq: i, Payload: 1}
		if p := eng.Submit(context.Background(), ev); p != nil {
			t.Fatal(p)
		}
	}
	// 过期事件：Seq=2，携带试图把值改成 999 的负载。
	stale := Event{ID: "c-stale", Key: "k", Seq: 2, Payload: 999}
	if p := eng.Submit(context.Background(), stale); p != nil {
		t.Fatal(p)
	}
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := mustGet(t, eng, "k")
	t.Logf("关键输入: 正常 seq1..3 payload=1, 过期 seq2 payload=999; 判定 value=%d version=%d conflicts=%d",
		st.Value, st.Version, eng.Metrics().Conflicts)
	if st.Value != 3 || st.Version != 3 {
		t.Fatalf("conflict silently overwrote state: value=%d version=%d", st.Value, st.Version)
	}
	if c := eng.Metrics().Conflicts; c != 1 {
		t.Fatalf("conflicts=%d want 1", c)
	}
}

func mustGet(t *testing.T, eng *Engine, key string) KeyState {
	t.Helper()
	st, ok, err := eng.store.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("key %s not found", key)
	}
	return st
}
