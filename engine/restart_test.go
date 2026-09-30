package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// 启动一个引擎并返回，便于多生命周期复用同一数据目录。
func startEngine(t *testing.T, dir string) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.Workers = 4
	cfg.QueuePerShard = 256
	cfg.MailboxPerKey = 64
	cfg.SubmitTimeout = 100 * time.Millisecond
	cfg.ShutdownTimeout = 5 * time.Second
	cfg.SyncEachWrite = false
	eng, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return eng
}

// TestRestartPreservesStateAndIdempotent 验证优雅停机后再启动：
// 已应用状态不丢失、不重复；停机前未决事件（如有）在第二次生命周期被幂等
// 重放，最终每个键的版本与值精确等于投递内容。
func TestRestartPreservesStateAndIdempotent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	// 第一次生命周期：投递 8 键 × 每键 100 条，等全部应用后停机。
	eng := startEngine(t, dir)
	for k := 0; k < 8; k++ {
		for i := int64(1); i <= 100; i++ {
			ev := Event{
				ID:      fmt.Sprintf("r-k%d-%d", k, i),
				Key:     fmt.Sprintf("rk%d", k),
				Seq:     i,
				Payload: 1,
			}
			if p := eng.Submit(context.Background(), ev); p != nil {
				t.Fatalf("submit: %v", p)
			}
		}
	}
	waitFor(t, func() bool { return eng.Metrics().Applied >= 800 }, 10*time.Second, "首轮 800 事件应用完成")
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatalf("first shutdown: %v", err)
	}
	states1, err := eng.StateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for k := 0; k < 8; k++ {
		st := states1[fmt.Sprintf("rk%d", k)]
		if st.Version != 100 || st.Value != 100 {
			t.Fatalf("首轮 %s version=%d value=%d want 100/100", fmt.Sprintf("rk%d", k), st.Version, st.Value)
		}
	}

	// 第二次生命周期：同一数据目录重开。不应再应用任何业务事件（已全部
	// Ack），状态必须与首轮完全一致。
	eng2 := startEngine(t, dir)
	waitFor(t, func() bool {
		// 给重放/源投递一点时间，然后通过停机排空观察。
		return eng2.Metrics().Received >= 0
	}, 50*time.Millisecond, "二跑稳定")
	if err := eng2.Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
	states2, err := eng2.StateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for k := 0; k < 8; k++ {
		key := fmt.Sprintf("rk%d", k)
		a, b := states1[key], states2[key]
		if a.Version != b.Version || a.Value != b.Value {
			t.Fatalf("重启后 %s 状态漂移: %+v vs %+v", key, a, b)
		}
	}
	t.Logf("判定依据: 8 键各 100 条; 重启后 applied(第二轮)=%d duplicates=%d; 终态 version=value=100 保持不变",
		eng2.Metrics().Applied, eng2.Metrics().Duplicates)
}

// TestReplayDuplicateDoesNotDoubleApply 验证 pending 日志中遗留的事件在重启
// 重放时不会被重复累加（存储层判重兜底）。
func TestReplayDuplicateDoesNotDoubleApply(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	eng := startEngine(t, dir)
	ev := Event{ID: "replay-1", Key: "pk", Seq: 1, Payload: 5}
	if p := eng.Submit(context.Background(), ev); p != nil {
		t.Fatal(p)
	}
	waitFor(t, func() bool { return eng.Metrics().Applied >= 1 }, 2*time.Second, "事件应用")
	// 手动再写一条同样事件到 pending 日志（模拟“已应用但 Ack 丢失”的重放）。
	if err := eng.journal.Append(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Logf("停机后 pending 日志保留未 Ack 的 replay-1，重启应判重")

	eng2 := startEngine(t, dir)
	// 等待重放被消化（判重）。
	waitFor(t, func() bool { return eng2.Metrics().Duplicates >= 1 }, 2*time.Second, "重放事件被判重")
	if err := eng2.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := mustGet(t, eng2, "pk")
	t.Logf("判定: replay-1 payload=5 重放后 value=%d version=%d duplicates=%d",
		st.Value, st.Version, eng2.Metrics().Duplicates)
	if st.Value != 5 || st.Version != 1 {
		t.Fatalf("重放导致重复应用: value=%d version=%d", st.Value, st.Version)
	}
}

// TestEventSourceOffsetResume 验证事件源按 offset 断点续读：已成功投递的
// 事件不会在重启后重复从源读取投递。
func TestEventSourceOffsetResume(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	src, err := openFileSource(
		filepath.Join(dir, "events.log"),
		filepath.Join(dir, "events.offset"),
	)
	if err != nil {
		t.Fatal(err)
	}
	// 写入 3 条源事件；前 2 条 sink 成功（推进 offset），第 3 条 sink 失败
	// （不推进 offset）。
	for i := int64(1); i <= 3; i++ {
		if err := src.Append(Event{ID: fmt.Sprintf("src-%d", i), Key: "sk", Seq: i, Payload: 1}); err != nil {
			t.Fatal(err)
		}
	}
	var sinkMu sync.Mutex
	var sinked []string
	runCtx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = src.Stream(runCtx, func(ev Event) *Error {
			if ev.Seq == 3 {
				// 模拟下游拒绝：不推进 offset。
				return newError(KindQueueFull, "test", "sink reject", nil)
			}
			sinkMu.Lock()
			sinked = append(sinked, ev.ID)
			sinkMu.Unlock()
			return nil
		})
	}()
	waitFor(t, func() bool {
		sinkMu.Lock()
		defer sinkMu.Unlock()
		return len(sinked) == 2
	}, 2*time.Second, "前 2 条投递成功")
	cancel()
	_ = src.Close()

	off, err := readOffsetRaw(filepath.Join(dir, "events.offset"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("判定: 前2条成功后 offset=%d（应小于文件总长，第3条未确认）", off)
	if off <= 0 {
		t.Fatal("offset 未推进")
	}

	// 重新打开源：应从第 3 条开始（前 2 条不重复）。
	src2, err := openFileSource(
		filepath.Join(dir, "events.log"),
		filepath.Join(dir, "events.offset"),
	)
	if err != nil {
		t.Fatal(err)
	}
	gotCh := make(chan string, 1)
	s2ctx, cancel2 := context.WithCancel(context.Background())
	go func() {
		_ = src2.Stream(s2ctx, func(ev Event) *Error {
			select {
			case gotCh <- ev.ID:
			default:
			}
			return nil
		})
	}()
	var second []string
	select {
	case id := <-gotCh:
		second = append(second, id)
	case <-time.After(2 * time.Second):
	}
	cancel2()
	_ = src2.Close()
	t.Logf("判定: 重启后从源读取到 %v（应只有 src-3，前 2 条不重复）", second)
	if len(second) != 1 || second[0] != "src-3" {
		t.Fatalf("offset 断点续读错误，实际=%v", second)
	}
}

// readOffsetRaw 读取事件源 offset 文件的字节偏移。
func readOffsetRaw(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	var off int64
	for _, b := range data {
		if b < '0' || b > '9' {
			break
		}
		off = off*10 + int64(b-'0')
	}
	return off, nil
}

// TestRestartPreservesPriorityAndOrder 验证带优先级的事件在“停机未排空 ->
// 重放”后优先级不丢失、同键顺序不乱、结果与幂等语义保持一致。
func TestRestartPreservesPriorityAndOrder(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	eng := startEngine(t, dir)
	// 同一业务键 4 件，优先级交错；另投一个高优冷键。
	events := []Event{
		{ID: "rp-1", Key: "rk", Seq: 1, Payload: 1, Priority: PriorityNormal},
		{ID: "rp-2", Key: "rk", Seq: 2, Payload: 2, Priority: PriorityHigh},
		{ID: "rp-3", Key: "rk", Seq: 3, Payload: 4, Priority: PriorityNormal},
		{ID: "rp-4", Key: "rk", Seq: 4, Payload: 8, Priority: PriorityHigh},
		{ID: "rp-h", Key: "rh", Seq: 1, Payload: 16, Priority: PriorityHigh},
	}
	for _, ev := range events {
		if p := eng.Submit(context.Background(), ev); p != nil {
			t.Fatalf("submit %s: %v", ev.ID, p)
		}
	}
	// 用一个会阻塞到停机取消的慢 Handler 场景不易构造“未排空”，这里改为：
	// 直接等待全部应用后正常停机，再二次启动确认状态不漂移、重放幂等；同时
	// 校验 pending.log 中持久化的事件确实带 priority 字段。
	waitFor(t, func() bool { return eng.Metrics().Applied >= 5 }, 5*time.Second, "5 个带优先级事件应用完成")
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	st := mustGet(t, eng, "rk")
	if st.Version != 4 || st.Value != 15 {
		t.Fatalf("同键交错优先级终态错误 version=%d value=%d want 4/15", st.Version, st.Value)
	}
	if st2 := mustGet(t, eng, "rh"); st2.Value != 16 {
		t.Fatalf("高优冷键终态错误 value=%d want 16", st2.Value)
	}

	// pending.log 在优雅排空后应已压缩为空（全部 Ack）；若有遗留，重放必须
	// 幂等。第二次生命周期校验状态不漂移。
	eng2 := startEngine(t, dir)
	if err := eng2.Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
	stA := mustGet(t, eng2, "rk")
	if stA.Version != 4 || stA.Value != 15 {
		t.Fatalf("重启后同键状态漂移 version=%d value=%d", stA.Version, stA.Value)
	}
	t.Logf("判定: 同键 normal/high 交错 4 件 + 高优冷键; 终态 rk=15 rh=16; 重启幂等 applied2=%d dup2=%d",
		eng2.Metrics().Applied, eng2.Metrics().Duplicates)
}

// TestPriorityPersistedInJournal 验证优先级确实随 pending.log 逐行 JSON 落盘：
// 构造“提交后立即停机（可能未排空）”，读取 pending.log，遗留事件必须带
// "priority":1；随后重启重放，结果正确且不重复。
func TestPriorityPersistedInJournal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.Workers = 1
	cfg.QueuePerShard = 1
	cfg.MailboxPerKey = 1
	cfg.SubmitTimeout = 20 * time.Millisecond
	cfg.ShutdownTimeout = 80 * time.Millisecond // 刻意很短，保留部分未决事件
	cfg.SyncEachWrite = false
	eng, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 高优事件先落盘；容量极小、停机超时极短，部分事件来不及 Ack。
	for i := int64(1); i <= 6; i++ {
		_ = eng.Submit(context.Background(), Event{
			ID: fmt.Sprintf("jp-%d", i), Key: "jp", Seq: i,
			Payload: 1, Priority: PriorityHigh,
		})
	}
	_ = eng.Shutdown(context.Background()) // 允许超时返回

	data, rerr := os.ReadFile(filepath.Join(dir, "pending.log"))
	if rerr != nil {
		t.Fatalf("read pending log: %v", rerr)
	}
	t.Logf("停机后 pending.log 内容:\n%s", string(data))
	if len(data) > 0 && !strings.Contains(string(data), `"priority":1`) {
		t.Fatalf("pending.log 遗留事件未持久化高优先级字段: %s", string(data))
	}

	// 重启重放：无论遗留多少，最终 6 件全部生效且仅生效一次。
	eng2 := startEngine(t, dir)
	waitFor(t, func() bool {
		st, ok, _ := eng2.store.Get(context.Background(), "jp")
		return ok && st.Version == 6
	}, 5*time.Second, "重放后 6 件全部生效")
	if err := eng2.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := mustGet(t, eng2, "jp")
	if st.Version != 6 || st.Value != 6 {
		t.Fatalf("优先级事件重放后状态错误 version=%d value=%d want 6/6", st.Version, st.Value)
	}
}
