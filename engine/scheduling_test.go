package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// schedConfig 返回单 worker（单分片）、小容量的调度测试配置：所有业务键
// 落在同一个公平分片内，便于确定性地观察优先级与键间轮转。
func schedConfig(t *testing.T) Config {
	cfg := testConfig(t)
	cfg.Workers = 1
	cfg.QueuePerShard = 32
	cfg.MailboxPerKey = 4
	cfg.SubmitTimeout = 200 * time.Millisecond
	cfg.ShutdownTimeout = 10 * time.Second
	cfg.HotKeyBacklogThreshold = 50
	return cfg
}

// orderedRecorder 记录事件在 Handler 中实际生效的 ID 顺序（Handler 在每键
// 分片存储锁内串行调用，append 顺序即生效顺序）。
func orderedRecorder() (func(ctx context.Context, ev Event, cur KeyState) (KeyState, error), *[]string, *sync.Mutex) {
	var mu sync.Mutex
	var order []string
	h := func(ctx context.Context, ev Event, cur KeyState) (KeyState, error) {
		mu.Lock()
		order = append(order, ev.ID)
		mu.Unlock()
		return KeyState{Key: ev.Key, Value: cur.Value + ev.Payload}, nil
	}
	return h, &order, &mu
}

// submitMust 投递事件，背压/限流时短退避重试直到成功或超时。
func submitMust(t *testing.T, eng *Engine, ev Event) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if p := eng.Submit(context.Background(), ev); p == nil {
			return
		} else if p.Kind != KindQueueFull && p.Kind != KindPriorityThrottled {
			t.Fatalf("unexpected reject for %s: %v", ev.ID, p)
		}
		if time.Now().After(deadline) {
			t.Fatalf("submit %s stuck under backpressure", ev.ID)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestPriorityServedBeforeNormal 验证拥塞时高优先级事件比更早投递的低优
// 先级事件更早被消费（不同业务键，故不存在同键顺序约束）。
//
// 构造方式：把在途信号量收紧到 1，并用一个阻塞中的“占位事件”占住唯一槽
// 位。worker 弹出占位事件后在 deliver 获取第二个槽位时永久阻塞，于是停止
// 继续 pop；此后投递的高/普通事件全部确定性地滞留在分片队列中。释放占位
// 后，队列必须先服务全部高优先级、再服务普通优先级。
func TestPriorityServedBeforeNormal(t *testing.T) {
	cfg := schedConfig(t)
	cfg.QueuePerShard = 256
	cfg.MailboxPerKey = 8

	handler, order, omu := orderedRecorder()
	release := make(chan struct{})
	started := make(chan struct{})
	var sawSeed atomic.Bool
	wrapped := func(ctx context.Context, ev Event, cur KeyState) (KeyState, error) {
		if ev.ID == "seed" && !sawSeed.Swap(true) {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return KeyState{}, ctx.Err()
			}
		}
		return handler(ctx, ev, cur)
	}
	eng, err := New(cfg, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	// 在 Start 前把在途信号量收紧为 1：seed 持有唯一槽位，worker 在投递
	// seed-2 时阻塞，停止继续消费队列。
	eng.inflightSemCap = 1
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	submitMust(t, eng, Event{ID: "seed", Key: "seed-key", Seq: 1, Payload: 1})
	<-started
	submitMust(t, eng, Event{ID: "seed-2", Key: "seed-key", Seq: 2, Payload: 1})
	// 等待 worker 弹出 seed-2 并阻塞在第二个在途槽位上。
	waitFor(t, func() bool { return eng.queue.depth() == 0 }, time.Second, "seed-2 被 worker 取走")
	time.Sleep(5 * time.Millisecond)

	// 此时 worker 已停在 deliver：先投递一批低优先级，再投递高优先级。
	const lowN = 30
	for i := 0; i < lowN; i++ {
		submitMust(t, eng, Event{
			ID: fmt.Sprintf("low-%d", i), Key: fmt.Sprintf("low-key-%d", i),
			Seq: 1, Priority: PriorityNormal, Payload: 1,
		})
	}
	const highN = 5
	for i := 0; i < highN; i++ {
		submitMust(t, eng, Event{
			ID: fmt.Sprintf("high-%d", i), Key: fmt.Sprintf("high-key-%d", i),
			Seq: 1, Priority: PriorityHigh, Payload: 1,
		})
	}
	if d := eng.queue.depth(); d != lowN+highN {
		t.Fatalf("拥塞构造失败：队列积压=%d want %d", d, lowN+highN)
	}

	close(release)
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	omu.Lock()
	got := append([]string(nil), (*order)...)
	omu.Unlock()

	// seed、seed-2 最先；此后所有 high 必须早于最后一个 low 生效。
	lastLow := -1
	for i, id := range got {
		if len(id) > 4 && id[:4] == "low-" {
			lastLow = i
		}
	}
	highAfterLastLow := 0
	highSeen := 0
	for i, id := range got {
		if len(id) > 5 && id[:5] == "high-" {
			highSeen++
			if lastLow >= 0 && i > lastLow {
				highAfterLastLow++
			}
		}
	}
	t.Logf("判定依据: %d 条低优先级先入队、%d 条高优先级后入队；生效顺序中 lastLow 位置=%d，落后于它的 high=%d",
		lowN, highSeen, lastLow, highAfterLastLow)
	if highSeen != highN {
		t.Fatalf("高优先级生效数=%d want %d", highSeen, highN)
	}
	if highAfterLastLow != 0 {
		t.Fatalf("有 %d 条高优先级事件排在最后一个低优先级之后生效，优先级未被尊重", highAfterLastLow)
	}
}

// TestSameKeyPriorityPreservesOrder 验证同键事件无论优先级/投递先后，最终
// 生效顺序严格等于 Seq 顺序（优先级不打乱同键顺序与幂等）。
func TestSameKeyPriorityPreservesOrder(t *testing.T) {
	cfg := schedConfig(t)
	cfg.MailboxPerKey = 8
	handler, order, omu := orderedRecorder()
	eng, err := New(cfg, handler)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 故意以“高优先级的后继 seq2 先投、普通优先级的前驱 seq1 后投”的
	// 乱序 + 混优先级方式投递；缺口堆必须把 seq2 按住直到 seq1 生效。
	submitMust(t, eng, Event{ID: "k-2", Key: "k", Seq: 2, Priority: PriorityHigh, Payload: 2})
	submitMust(t, eng, Event{ID: "k-1", Key: "k", Seq: 1, Priority: PriorityNormal, Payload: 1})
	submitMust(t, eng, Event{ID: "k-4", Key: "k", Seq: 4, Priority: PriorityHigh, Payload: 4})
	submitMust(t, eng, Event{ID: "k-3", Key: "k", Seq: 3, Priority: PriorityNormal, Payload: 3})

	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	omu.Lock()
	got := append([]string(nil), (*order)...)
	omu.Unlock()
	want := []string{"k-1", "k-2", "k-3", "k-4"}
	t.Logf("判定依据: 混优先级乱序投递 seq=2(H),1(N),4(H),3(N)；实际生效顺序=%v", got)
	if len(got) != len(want) {
		t.Fatalf("生效事件数=%d want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("同键生效顺序被优先级打乱: got=%v want=%v", got, want)
		}
	}
	st := mustGet(t, eng, "k")
	if st.Version != 4 || st.Value != 10 {
		t.Fatalf("同键终态错误 version=%d value=%d want 4/10", st.Version, st.Value)
	}
}

// TestFairAcrossKeysHotDoesNotStarveCold 验证单键大量涌入时，其他键不会被
// 长期饿死：公平轮转让冷键事件在热点键只处理了部分积压时就得到处理。
func TestFairAcrossKeysHotDoesNotStarveCold(t *testing.T) {
	cfg := schedConfig(t)
	cfg.QueuePerShard = 512
	cfg.MailboxPerKey = 64
	cfg.SubmitTimeout = time.Second
	handler, order, omu := orderedRecorder()

	coldDone := make(chan struct{})
	coldSawHot := false
	gated := func(ctx context.Context, ev Event, cur KeyState) (KeyState, error) {
		if ev.Kind == "cold-marker" {
			omu.Lock()
			// 记录此刻热点键已生效多少条。
			n := 0
			for _, id := range *order {
				if len(id) > 4 && id[:4] == "hot-" {
					n++
				}
			}
			coldSawHot = n > 0
			omu.Unlock()
			close(coldDone)
		}
		return handler(ctx, ev, cur)
	}
	eng, err := New(cfg, gated)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 热点键涌入大批事件（单分片）。
	const hotN = 300
	for i := 0; i < hotN; i++ {
		submitMust(t, eng, Event{
			ID: fmt.Sprintf("hot-%d", i), Key: "hot-key",
			Seq: int64(i + 1), Payload: 1,
		})
	}
	// 冷键事件：在热点键之后才投递。若 FIFO 偏袒热点键，它要等全部
	// hotN 条之后才会被消费；公平轮转下它应在热点键只推进一部分时完成。
	submitMust(t, eng, Event{ID: "cold-1", Key: "cold-key", Seq: 1, Kind: "cold-marker", Payload: 1})

	select {
	case <-coldDone:
	case <-time.After(5 * time.Second):
		t.Fatal("冷键事件被热点键长期饿死（公平调度失效）")
	}

	omu.Lock()
	hotAtCold := 0
	for _, id := range *order {
		if len(id) > 4 && id[:4] == "hot-" {
			hotAtCold++
		}
		if id == "cold-1" {
			break
		}
	}
	omu.Unlock()
	t.Logf("判定依据: 热点键 %d 条先入队，冷键在热点键仅生效 %d 条时即被调度", hotN, hotAtCold)
	if !coldSawHot || hotAtCold == 0 {
		t.Fatalf("冷键先于任何热点事件生效，测试未形成积压竞争 hotAtCold=%d", hotAtCold)
	}
	if hotAtCold >= hotN {
		t.Fatalf("冷键等全部 %d 条热点事件处理完才被调度，缺乏公平性", hotN)
	}

	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := mustGet(t, eng, "hot-key")
	if st.Version != hotN {
		t.Fatalf("热点键终态 version=%d want %d", st.Version, hotN)
	}
}

// TestHotKeyDetectedInMetrics 验证热点键积压达到阈值时被可观测地识别出来。
func TestHotKeyDetectedInMetrics(t *testing.T) {
	cfg := schedConfig(t)
	cfg.QueuePerShard = 512
	cfg.MailboxPerKey = 512
	cfg.SubmitTimeout = time.Second
	cfg.HotKeyBacklogThreshold = 50
	release := make(chan struct{})
	started := make(chan struct{})
	var sawBlocker atomic.Bool
	blocking := func(ctx context.Context, ev Event, cur KeyState) (KeyState, error) {
		if ev.ID == "blocker" && !sawBlocker.Swap(true) {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return KeyState{}, ctx.Err()
			}
		}
		return SumHandler(ctx, ev, cur)
	}
	eng, err := New(cfg, blocking)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	submitMust(t, eng, Event{ID: "blocker", Key: "spike-key", Seq: 1, Payload: 1})
	<-started
	// blocker 占住协调协程；同键后续事件堆积在 mailbox（容量 512）。
	const burst = 120
	for i := 0; i < burst; i++ {
		submitMust(t, eng, Event{
			ID: fmt.Sprintf("spike-%d", i), Key: "spike-key",
			Seq: int64(i + 2), Payload: 1,
		})
	}

	waitFor(t, func() bool {
		m := eng.Metrics()
		n, ok := m.HotKeys["spike-key"]
		return ok && n >= cfg.HotKeyBacklogThreshold
	}, 3*time.Second, "热点键被指标识别")
	m := eng.Metrics()
	t.Logf("判定依据: 阈值=%d spike-key 积压=%d HotKeys=%v KeyBacklog=%+v",
		m.HotKeyThreshold, m.HotKeys["spike-key"], m.HotKeys, m.KeyBacklog["spike-key"])
	if m.KeyBacklog["spike-key"].Total < cfg.HotKeyBacklogThreshold {
		t.Fatal("KeyBacklog 未按业务键维度反映积压")
	}

	close(release)
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestPriorityThrottledDistinctFromQueueFull 验证队列被高优先级事件占满时，
// 普通优先级立即得到 KindPriorityThrottled，而高优先级等待超时得到
// KindQueueFull——两种拒绝原因可区分，且指标按原因与优先级双维度可见。
func TestPriorityThrottledDistinctFromQueueFull(t *testing.T) {
	cfg := schedConfig(t)
	cfg.QueuePerShard = 4
	cfg.MailboxPerKey = 8
	cfg.SubmitTimeout = 40 * time.Millisecond
	cfg.HandleTimeout = 5 * time.Second
	release := make(chan struct{})
	started := make(chan struct{})
	var sawSeed atomic.Bool
	blocking := func(ctx context.Context, ev Event, cur KeyState) (KeyState, error) {
		if ev.ID == "thr-seed" && !sawSeed.Swap(true) {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return KeyState{}, ctx.Err()
			}
		}
		return KeyState{Value: cur.Value + ev.Payload}, nil
	}
	eng, err := New(cfg, blocking)
	if err != nil {
		t.Fatal(err)
	}
	// 在途槽位收紧为 1：seed 占住唯一槽位，worker 弹出 seed-2 后卡在
	// deliver，停止继续消费队列。
	eng.inflightSemCap = 1
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		_ = eng.Shutdown(context.Background())
	}()

	submitMust(t, eng, Event{ID: "thr-seed", Key: "thr-seed-key", Seq: 1, Priority: PriorityHigh, Payload: 1})
	<-started
	submitMust(t, eng, Event{ID: "thr-seed-2", Key: "thr-seed-key", Seq: 2, Priority: PriorityNormal, Payload: 1})
	waitFor(t, func() bool { return eng.queue.depth() == 0 }, time.Second, "seed-2 被取走，worker 卡在 deliver")
	time.Sleep(5 * time.Millisecond)
	// 此刻 worker 已停：用高优先级事件把整个分片队列填满（每键 1 条）。
	for i := 0; i < cfg.QueuePerShard; i++ {
		submitMust(t, eng, Event{
			ID: fmt.Sprintf("fill-h-%d", i), Key: fmt.Sprintf("fill-h-key-%d", i),
			Seq: 1, Priority: PriorityHigh, Payload: 1,
		})
	}
	if d := eng.queue.depth(); d != cfg.QueuePerShard {
		t.Fatalf("拥塞构造失败：队列深度=%d want %d", d, cfg.QueuePerShard)
	}

	// 普通优先级：分片被高优先级占满，必须立即以 priority_throttled 拒绝
	// （不应吃掉 SubmitTimeout）。
	begin := time.Now()
	perr := eng.Submit(context.Background(), Event{
		ID: "low-blocked", Key: "low-blocked-key", Seq: 1, Priority: PriorityNormal, Payload: 1,
	})
	elapsed := time.Since(begin)
	if perr == nil || perr.Kind != KindPriorityThrottled {
		t.Fatalf("普通优先级拥塞拒绝 kind=%v want priority_throttled (err=%v)", perr, perr)
	}
	if elapsed > cfg.SubmitTimeout {
		t.Fatalf("priority_throttled 应立即返回，实际等待 %v", elapsed)
	}
	t.Logf("判定: 普通优先级被限流 kind=%s 耗时=%v", perr.Kind, elapsed)

	// 高优先级：容量真正耗尽，等待 SubmitTimeout 后以 queue_full 拒绝。
	perr = eng.Submit(context.Background(), Event{
		ID: "high-blocked", Key: "high-blocked-key", Seq: 1, Priority: PriorityHigh, Payload: 1,
	})
	if perr == nil || perr.Kind != KindQueueFull {
		t.Fatalf("高优先级容量耗尽拒绝 kind=%v want queue_full (err=%v)", perr, perr)
	}
	t.Logf("判定: 高优先级容量耗尽 kind=%s", perr.Kind)

	snap := eng.Metrics()
	if snap.RejectReason["priority_throttled"] == 0 {
		t.Fatal("指标缺少 priority_throttled 原因计数")
	}
	if snap.RejectReason["queue_full"] == 0 {
		t.Fatal("指标缺少 queue_full 原因计数")
	}
	if snap.RejectedByPriority["normal"] == 0 {
		t.Fatalf("指标未按普通优先级统计拒绝: %v", snap.RejectedByPriority)
	}
	if snap.RejectedByPriority["high"] == 0 {
		t.Fatalf("指标未按高优先级统计拒绝: %v", snap.RejectedByPriority)
	}
}

// TestPriorityBacklogMetricsByPriority 验证队列积压按优先级分别可见。
func TestPriorityBacklogMetricsByPriority(t *testing.T) {
	cfg := schedConfig(t)
	cfg.QueuePerShard = 32
	cfg.MailboxPerKey = 8
	cfg.HandleTimeout = 5 * time.Second
	release := make(chan struct{})
	started := make(chan struct{})
	var sawSeed atomic.Bool
	blocking := func(ctx context.Context, ev Event, cur KeyState) (KeyState, error) {
		if ev.ID == "bm-seed" && !sawSeed.Swap(true) {
			close(started)
			<-release
		}
		return KeyState{Value: cur.Value + ev.Payload}, nil
	}
	eng, _ := New(cfg, blocking)
	eng.inflightSemCap = 1 // worker 弹出 seed-2 后卡在 deliver，队列停止消费
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		_ = eng.Shutdown(context.Background())
	}()
	submitMust(t, eng, Event{ID: "bm-seed", Key: "bm-seed-key", Seq: 1, Priority: PriorityHigh, Payload: 1})
	<-started
	submitMust(t, eng, Event{ID: "bm-seed-2", Key: "bm-seed-key", Seq: 2, Priority: PriorityNormal, Payload: 1})
	waitFor(t, func() bool { return eng.queue.depth() == 0 }, time.Second, "seed-2 被取走，worker 停住")
	time.Sleep(5 * time.Millisecond)
	for i := 0; i < 6; i++ {
		submitMust(t, eng, Event{ID: fmt.Sprintf("bm-h-%d", i), Key: fmt.Sprintf("bm-h-%d", i), Seq: 1, Priority: PriorityHigh})
	}
	for i := 0; i < 4; i++ {
		submitMust(t, eng, Event{ID: fmt.Sprintf("bm-n-%d", i), Key: fmt.Sprintf("bm-n-%d", i), Seq: 1, Priority: PriorityNormal})
	}
	// worker 已停：6 高 + 4 普通全部滞留队列（seed-2 已被取走，不在队列）。
	m := eng.Metrics()
	t.Logf("判定: QueueHigh=%d QueueNormal=%d QueueDepth=%d", m.QueueHigh, m.QueueNormal, m.QueueDepth)
	if m.QueueHigh < 6 || m.QueueNormal < 4 {
		t.Fatalf("按优先级积压不符: high=%d(n>=6) normal=%d(n>=4)", m.QueueHigh, m.QueueNormal)
	}
	if m.QueueDepth != m.QueueHigh+m.QueueNormal {
		t.Fatalf("QueueDepth=%d 不等于分级积压之和 %d+%d", m.QueueDepth, m.QueueHigh, m.QueueNormal)
	}
}

// TestPrioritySurvivesReplay 验证优先级随事件持久化：混入高低优先级的事件
// 在停机重启后仍被全部幂等处理，结果与投递内容精确一致（新增的优先级
// 状态不破坏既有排空与重启一致性）。
func TestPrioritySurvivesReplay(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.Workers = 2
	cfg.QueuePerShard = 64
	cfg.MailboxPerKey = 16
	cfg.SubmitTimeout = time.Second
	cfg.ShutdownTimeout = 200 * time.Millisecond // 故意很短，制造未决事件重放
	cfg.SyncEachWrite = false
	cfg.HotKeyBacklogThreshold = 1 << 30

	start := func() *Engine {
		eng, err := New(cfg, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := eng.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return eng
	}

	eng := start()
	const keys = 6
	const perKey = 40
	for k := 0; k < keys; k++ {
		for i := 1; i <= perKey; i++ {
			ev := Event{
				ID:      fmt.Sprintf("p-k%d-%d", k, i),
				Key:     fmt.Sprintf("pk%d", k),
				Seq:     int64(i),
				Payload: 1,
			}
			if i%2 == 0 {
				ev.Priority = PriorityHigh
			}
			submitMust(t, eng, ev)
		}
	}
	// 短超时停机：可能有未 Ack 事件留在 pending.log。
	_ = eng.Shutdown(context.Background())

	eng2 := start()
	waitFor(t, func() bool {
		states, err := eng2.StateSnapshot(context.Background())
		if err != nil {
			return false
		}
		for k := 0; k < keys; k++ {
			if states[fmt.Sprintf("pk%d", k)].Version != perKey {
				return false
			}
		}
		return true
	}, 10*time.Second, "重放后全部键达到最终版本")
	if err := eng2.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	states, _ := eng2.StateSnapshot(context.Background())
	for k := 0; k < keys; k++ {
		st := states[fmt.Sprintf("pk%d", k)]
		if st.Version != perKey || st.Value != perKey {
			t.Fatalf("pk%d version=%d value=%d want %d/%d（优先级重放破坏了一致性）",
				k, st.Version, st.Value, perKey, perKey)
		}
	}
	t.Logf("判定: %d 键各 %d 条（半数高优先级）短超时停机+重启重放后，全部 version=value=%d",
		keys, perKey, perKey)
}
