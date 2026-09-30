package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestEnginePriorityConsumedFirst 是引擎级的跨键优先级端到端验证：跨键的
// 严格出队顺序由白盒 TestSchedulerHighPriorityFirstCrossKey 确定性证明
// （worker 会把不同键事件异步投递到各自独立协调协程，引擎层 handler 完成
// 天然并发，不宜断言跨键完成顺序）。这里验证端到端：混合优先级事件不丢不
// 重、各自业务键终态正确，并按优先级维度记录接收指标与队列积压。
func TestEnginePriorityConsumedFirst(t *testing.T) {
	cfg := testConfig(t)
	cfg.QueuePerShard = 64
	cfg.SubmitTimeout = time.Second

	var popMu sync.Mutex
	var highPopped, normalPopped int
	eng, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	eng.testOnPop = func(it queuedItem) {
		popMu.Lock()
		if normalizePriority(it.ev.Priority) == PriorityHigh {
			highPopped++
		} else {
			normalPopped++
		}
		popMu.Unlock()
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	submit := func(id, key string, pri int) {
		t.Helper()
		if p := eng.Submit(context.Background(), Event{
			ID: id, Key: key, Seq: 1, Payload: 1, Priority: pri,
		}); p != nil {
			t.Fatalf("submit %s: %v", id, p)
		}
	}

	// 3 个普通事件（不同键）+ 2 个高优事件（不同键）混合提交。
	submit("normA", "k-a", PriorityNormal)
	submit("normB", "k-b", PriorityNormal)
	submit("normC", "k-c", PriorityNormal)
	submit("high1", "k-h1", PriorityHigh)
	submit("high2", "k-h2", PriorityHigh)

	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{"k-a", "k-b", "k-c", "k-h1", "k-h2"} {
		if st := mustGet(t, eng, k); st.Value != 1 {
			t.Fatalf("%s 终态 value=%d want 1（事件丢失或重复）", k, st.Value)
		}
	}
	if highPopped != 2 || normalPopped != 3 {
		t.Fatalf("出队计数错误 high=%d normal=%d", highPopped, normalPopped)
	}
	t.Logf("判定: 3 普通 + 2 高优混合投递, 全部正确生效; 出队 high=%d normal=%d",
		highPopped, normalPopped)
	m := eng.Metrics()
	if m.AcceptedByPri["high"] != 2 || m.AcceptedByPri["normal"] != 3 {
		t.Fatalf("按优先级接收统计错误: %+v", m.AcceptedByPri)
	}
}

// TestEngineSameKeyPriorityKeepsOrder 验证同一业务键上高低优先级混合投递，
// 最终生效顺序仍严格等于投递（Seq）顺序，且结果正确、不重不漏。
func TestEngineSameKeyPriorityKeepsOrder(t *testing.T) {
	cfg := testConfig(t)
	cfg.SubmitTimeout = time.Second
	cfg.QueuePerShard = 512

	var mu sync.Mutex
	var appliedSeq []int64
	var appliedID []string
	recording := func(ctx context.Context, ev Event, cur KeyState) (KeyState, error) {
		mu.Lock()
		appliedSeq = append(appliedSeq, ev.Seq)
		appliedID = append(appliedID, ev.ID)
		mu.Unlock()
		return KeyState{Key: ev.Key, Value: cur.Value + ev.Payload}, nil
	}
	eng, err := New(cfg, recording)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	// seq 与投递顺序一致，但优先级故意高低交错（含高优落在普通之后）。
	pris := []int{PriorityNormal, PriorityHigh, PriorityHigh, PriorityNormal,
		PriorityHigh, PriorityNormal}
	for i, p := range pris {
		seq := int64(i + 1)
		if perr := eng.Submit(context.Background(), Event{
			ID: fmt.Sprintf("sk-%d", seq), Key: "same", Seq: seq,
			Payload: 1, Priority: p,
		}); perr != nil {
			t.Fatalf("submit seq=%d: %v", seq, perr)
		}
	}
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	for i, seq := range appliedSeq {
		if seq != int64(i+1) {
			t.Fatalf("同键生效顺序被优先级打乱: got=%v want=1..%d", appliedSeq, len(pris))
		}
	}
	st := mustGet(t, eng, "same")
	t.Logf("关键输入: 同键 6 件优先级交错; 判定生效顺序=%v value=%d", appliedSeq, st.Value)
	if st.Value != int64(len(pris)) || st.Version != int64(len(pris)) {
		t.Fatalf("同键结果错误 value=%d version=%d", st.Value, st.Version)
	}
}

// TestEngineHotKeyFairnessAndDetection 验证热点键大量积压时：
//  1. 冷键事件仍能很快得到处理（不被热点键长期压住）——键间逐键轮转；
//  2. 热点键在指标中被显式识别（HotKeyCount / KeyBacklogs[].Hot）。
func TestEngineHotKeyFairnessAndDetection(t *testing.T) {
	cfg := testConfig(t)
	cfg.Workers = 4
	cfg.QueuePerShard = 256
	cfg.HotKeyBacklog = 16
	cfg.SubmitTimeout = time.Second
	cfg.HandleTimeout = 5 * time.Second

	eng, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	const hotN = 400
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= hotN; i++ {
			for {
				p := eng.Submit(context.Background(), Event{
					ID: fmt.Sprintf("hot-%d", i), Key: "hotkey", Seq: i, Payload: 1,
				})
				if p == nil {
					break
				}
				if p.Kind != KindQueueFull && p.Kind != KindPriorityShed {
					t.Errorf("热点投递意外拒绝: %v", p)
					return
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// 给热点键先积压一小批，再投冷键；冷键应很快完成，而热点仍在积压。
	time.Sleep(20 * time.Millisecond)
	coldStart := time.Now()
	if p := eng.Submit(context.Background(), Event{
		ID: "cold-1", Key: "coldkey", Seq: 1, Payload: 7,
	}); p != nil {
		t.Fatalf("冷键投递: %v", p)
	}
	waitFor(t, func() bool {
		_, ok, _ := eng.store.Get(context.Background(), "coldkey")
		return ok
	}, 3*time.Second, "冷键在热点键积压期间完成处理")
	coldElapsed := time.Since(coldStart)

	// 冷键完成时热点大概率仍有积压；从指标读取按业务键积压与热点标记。
	m := eng.Metrics()
	var hotBL *KeyBacklog
	for i := range m.KeyBacklogs {
		if m.KeyBacklogs[i].Key == "hotkey" {
			b := m.KeyBacklogs[i]
			hotBL = &b
		}
	}
	t.Logf("判定: 冷键耗时=%v; 热点识别数=%d; hotkey 积压=%+v; 按优先级积压=%v",
		coldElapsed, m.HotKeyCount, hotBL, m.QueueDepthByPri)
	if m.HotKeyCount < 1 || hotBL == nil || !hotBL.Hot {
		t.Fatalf("热点键未被识别: hotCount=%d backlog=%+v", m.HotKeyCount, hotBL)
	}

	wg.Wait()
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := mustGet(t, eng, "hotkey"); st.Value != hotN || st.Version != hotN {
		t.Fatalf("热点键终态错误 value=%d version=%d want %d", st.Value, st.Version, hotN)
	}
	if st := mustGet(t, eng, "coldkey"); st.Value != 7 {
		t.Fatalf("冷键状态错误 value=%d want 7", st.Value)
	}
}

// TestEnginePriorityShedVsQueueFullMetrics 验证引擎层两类容量拒绝可区分、
// 且背压/拒绝可按优先级维度观测。
//
// 稳态构造（1 worker / 调度容量 2 / mailbox 2 / 在途 4，handler 全阻塞）：
// 同键高优事件最终 h1..h4 占住 处理中+mailbox+worker，h5,h6 占住调度队列
// 的全部 2 个容量。此时：普通事件立即 priority_shed；高优事件等待超时为
// queue_full（真正容量耗尽）。
func TestEnginePriorityShedVsQueueFullMetrics(t *testing.T) {
	cfg := testConfig(t)
	cfg.Workers = 1
	cfg.QueuePerShard = 2
	cfg.MailboxPerKey = 2
	cfg.SubmitTimeout = 2 * time.Second
	cfg.HandleTimeout = 5 * time.Second

	release := make(chan struct{})
	blocking := func(ctx context.Context, ev Event, cur KeyState) (KeyState, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return KeyState{}, ctx.Err()
		}
		return KeyState{Key: ev.Key, Value: cur.Value + ev.Payload}, nil
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

	for i := int64(1); i <= 6; i++ {
		p := eng.Submit(context.Background(), Event{
			ID: fmt.Sprintf("h-%d", i), Key: "wedge", Seq: i,
			Payload: 1, Priority: PriorityHigh,
		})
		if p != nil {
			t.Fatalf("填充高优 #%d: %v", i, p)
		}
	}
	// 等待进入稳态：调度队列被 2 件高优占满。
	waitFor(t, func() bool {
		return eng.Metrics().QueueDepthByPri["high"] >= 2
	}, 2*time.Second, "调度队列被高优占满")

	// 普通事件：必须立即让位拒绝，而不是干等到超时。
	start := time.Now()
	perr := eng.Submit(context.Background(), Event{
		ID: "n-shed", Key: "other-normal", Seq: 1, Payload: 1, Priority: PriorityNormal,
	})
	if perr == nil || perr.Kind != KindPriorityShed {
		t.Fatalf("普通事件应 priority_shed, got=%v", perr)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("让位拒绝应立即返回，实际耗时 %v", d)
	}

	// 高优事件：容量真正耗尽，短超时后必须是 queue_full（不是 priority_shed）。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	perr = eng.Submit(ctx, Event{
		ID: "h-full", Key: "other-high", Seq: 1, Payload: 1, Priority: PriorityHigh,
	})
	if perr == nil || perr.Kind != KindQueueFull {
		t.Fatalf("高优事件容量耗尽应 queue_full, got=%v", perr)
	}

	m := eng.Metrics()
	t.Logf("判定: reasons=%v kindPri=%v depthByPri=%v acceptedByPri=%v",
		m.RejectReason, m.RejectByKindPri, m.QueueDepthByPri, m.AcceptedByPri)
	if m.RejectReason["priority_shed"] == 0 {
		t.Fatal("指标缺少 priority_shed 原因计数")
	}
	if m.RejectByKindPri["priority_shed:normal"] == 0 {
		t.Fatal("指标缺少 priority_shed:normal 二维计数")
	}
	if m.RejectByKindPri["queue_full:high"] == 0 {
		t.Fatal("指标缺少 queue_full:high 二维计数")
	}
	if m.QueueDepthByPri["high"] < 2 {
		t.Fatalf("按优先级积压统计错误: %v", m.QueueDepthByPri)
	}
}

// TestEventPriorityJSONRoundTrip 验证优先级随事件持久化（pending.log /
// events.log 均为逐行 JSON），重启重放后优先级保持不变。
func TestEventPriorityJSONRoundTrip(t *testing.T) {
	ev := Event{ID: "p1", Key: "k", Seq: 1, Payload: 3, Priority: PriorityHigh}
	data, err := json.Marshal(&ev)
	if err != nil {
		t.Fatal(err)
	}
	var got Event
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Priority != PriorityHigh {
		t.Fatalf("优先级未随 JSON 持久化: got=%d data=%s", got.Priority, data)
	}
	// 默认（缺省）优先级归一化为普通。
	var def Event
	if err := json.Unmarshal([]byte(`{"id":"p2","key":"k","seq":1}`), &def); err != nil {
		t.Fatal(err)
	}
	if normalizePriority(def.Priority) != PriorityNormal {
		t.Fatalf("缺省优先级应为普通, got=%d", def.Priority)
	}
}
