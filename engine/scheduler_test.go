package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func item(id, key string, seq int64, pri int) queuedItem {
	return queuedItem{ev: Event{ID: id, Key: key, Seq: seq, Payload: 1, Priority: pri}, attempt: 1}
}

// TestSchedulerHighPriorityFirstCrossKey 验证跨业务键拥塞时：高优先级事件
// 先于普通事件被消费；各自档位内按业务键逐键轮转。
func TestSchedulerHighPriorityFirstCrossKey(t *testing.T) {
	s := newFairScheduler(100, 8)
	// 两个普通键各两件；两个高优键各两件。
	mustSubmit(t, s, item("n-a1", "na", 1, PriorityNormal))
	mustSubmit(t, s, item("n-b1", "nb", 1, PriorityNormal))
	mustSubmit(t, s, item("n-a2", "na", 2, PriorityNormal))
	mustSubmit(t, s, item("n-b2", "nb", 2, PriorityNormal))
	mustSubmit(t, s, item("h-c1", "hc", 1, PriorityHigh))
	mustSubmit(t, s, item("h-d1", "hd", 1, PriorityHigh))
	mustSubmit(t, s, item("h-c2", "hc", 2, PriorityHigh))
	mustSubmit(t, s, item("h-d2", "hd", 2, PriorityHigh))

	var got []string
	for i := 0; i < 8; i++ {
		got = append(got, popConfirmed(t, s).ev.ID)
	}
	t.Logf("消费顺序: %v", got)

	// 前四个必须全部是高优先级，且逐键轮转：hc,hd,hc,hd。
	wantHigh := []string{"h-c1", "h-d1", "h-c2", "h-d2"}
	for i := 0; i < 4; i++ {
		if got[i] != wantHigh[i] {
			t.Fatalf("高优先级消费顺序错误 got=%v want=%v", got[:4], wantHigh)
		}
	}
	// 之后消费普通键，逐键轮转：na,nb,na,nb。
	wantNorm := []string{"n-a1", "n-b1", "n-a2", "n-b2"}
	for i := 0; i < 4; i++ {
		if got[4+i] != wantNorm[i] {
			t.Fatalf("普通优先级轮转顺序错误 got=%v want=%v", got[4:], wantNorm)
		}
	}
	if d := s.depth(); d != 0 {
		t.Fatalf("取完后 depth=%d want 0", d)
	}
}

// TestSchedulerSameKeyFIFORegardlessOfPriority 验证同一业务键上无论优先级
// 如何混合，出队顺序都严格等于入队顺序——优先级不能打乱同键顺序。
func TestSchedulerSameKeyFIFORegardlessOfPriority(t *testing.T) {
	s := newFairScheduler(100, 8)
	seq := []int{PriorityNormal, PriorityHigh, PriorityHigh, PriorityNormal, PriorityNormal}
	for i, p := range seq {
		mustSubmit(t, s, item(fmt.Sprintf("k-%d", i+1), "k", int64(i+1), p))
	}
	for i := 1; i <= len(seq); i++ {
		it := popConfirmed(t, s)
		if it.ev.ID != fmt.Sprintf("k-%d", i) {
			t.Fatalf("同键 FIFO 被优先级打乱: 第 %d 件得到 %s want k-%d", i, it.ev.ID, i)
		}
	}
}

// TestSchedulerFairRoundRobin 验证单个热点键大量积压时不会霸占消费：三个
// 键各 3 件（同优先级），消费顺序必须是逐键轮转。
func TestSchedulerFairRoundRobin(t *testing.T) {
	s := newFairScheduler(100, 8)
	for r := 1; r <= 3; r++ {
		for _, k := range []string{"hot", "cold1", "cold2"} {
			mustSubmit(t, s, item(fmt.Sprintf("%s-%d", k, r), k, int64(r), PriorityNormal))
		}
	}
	wantKeys := []string{"hot", "cold1", "cold2", "hot", "cold1", "cold2", "hot", "cold1", "cold2"}
	for i, want := range wantKeys {
		it := popConfirmed(t, s)
		if it.ev.Key != want {
			t.Fatalf("第 %d 件键=%s want %s（热点键未被公平轮转）", i, it.ev.Key, want)
		}
	}
}

// TestSchedulerPriorityShedVsQueueFull 验证拒绝原因区分：
//   - 队列满且有高优积压：普通事件立即 priority_shed；
//   - 队列满且全部普通：普通事件等到超时 queue_full（容量耗尽）；
//   - 高优先级事件在队列满时等待，空位出现后成功入队。
func TestSchedulerPriorityShedVsQueueFull(t *testing.T) {
	s := newFairScheduler(2, 8)
	mustSubmit(t, s, item("h1", "h", 1, PriorityHigh))
	mustSubmit(t, s, item("h2", "h", 2, PriorityHigh))

	// 普通事件：高优占满 -> 立即让位拒绝（不等待）。
	perr := s.Submit(context.Background(), item("n1", "n", 1, PriorityNormal))
	if perr == nil || perr.Kind != KindPriorityShed {
		t.Fatalf("普通事件在高优拥塞下应 priority_shed, got=%v", perr)
	}

	// 高优先级事件：满 -> 短超时后 queue_full（容量耗尽，而非让位）。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	perr = s.Submit(ctx, item("h3", "h", 3, PriorityHigh))
	if perr == nil || perr.Kind != KindQueueFull {
		t.Fatalf("高优事件容量耗尽应 queue_full, got=%v", perr)
	}

	// 取走一件腾出空位后，高优等待者可以入队。
	popConfirmed(t, s)
	if perr := s.Submit(context.Background(), item("h4", "x", 1, PriorityHigh)); perr != nil {
		t.Fatalf("空位出现后高优事件应入队: %v", perr)
	}
}

// TestSchedulerAllNormalWedgeIsQueueFull 验证“全普通事件占满”是真正的
// 容量耗尽：普通事件等待超时为 queue_full，绝不能误报为 priority_shed。
func TestSchedulerAllNormalWedgeIsQueueFull(t *testing.T) {
	s := newFairScheduler(1, 8)
	mustSubmit(t, s, item("n0", "n0", 1, PriorityNormal))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	perr := s.Submit(ctx, item("n1", "n1", 1, PriorityNormal))
	if perr == nil || perr.Kind != KindQueueFull {
		t.Fatalf("全普通拥塞应为 queue_full, got=%v", perr)
	}
}

// TestSchedulerStatsAndHotKey 验证按优先级/业务键的积压统计与热点标记。
func TestSchedulerStatsAndHotKey(t *testing.T) {
	s := newFairScheduler(100, 3)
	for i := int64(1); i <= 4; i++ {
		mustSubmit(t, s, item(fmt.Sprintf("hot-%d", i), "hot", i, PriorityNormal))
	}
	mustSubmit(t, s, item("hi-1", "hi", 1, PriorityHigh))
	mustSubmit(t, s, item("cold-1", "cold", 1, PriorityNormal))

	st := s.stats()
	if st.Depth != 6 || st.ByPri[PriorityNormal] != 5 || st.ByPri[PriorityHigh] != 1 {
		t.Fatalf("整体/按优先级统计错误: %+v", st)
	}
	if st.HotKeyCount != 1 {
		t.Fatalf("热点键数量=%d want 1", st.HotKeyCount)
	}
	var hot *KeyQueueStat
	for i := range st.Keys {
		if st.Keys[i].Key == "hot" {
			hot = &st.Keys[i]
		}
	}
	if hot == nil || !hot.Hot || hot.Total != 4 {
		t.Fatalf("热点键 hot 统计错误: %+v", hot)
	}
	t.Logf("统计: depth=%d normal=%d high=%d hotKeys=%d", st.Depth,
		st.ByPri[PriorityNormal], st.ByPri[PriorityHigh], st.HotKeyCount)
}

// TestSchedulerStoppingRejects 验证停机标记后 Submit 返回 stopping，而已在
// 队事件仍可排空；close 后取空 pop 返回 false。
func TestSchedulerStoppingRejects(t *testing.T) {
	s := newFairScheduler(10, 8)
	mustSubmit(t, s, item("a", "a", 1, PriorityNormal))
	s.markStopping()
	if perr := s.Submit(context.Background(), item("b", "b", 1, PriorityNormal)); perr == nil || perr.Kind != KindStopping {
		t.Fatalf("停机后 Submit 应 stopping, got=%v", perr)
	}
	// 停机后排空阶段仍能取出并确认已在队事件。
	popConfirmed(t, s)
	s.close()
	if _, _, ok := s.pop(); ok {
		t.Fatal("close 且取空后 pop 必须返回 false")
	}
}

func mustSubmit(t *testing.T, s *fairScheduler, it queuedItem) {
	t.Helper()
	if perr := s.Submit(context.Background(), it); perr != nil {
		t.Fatalf("submit %s: %v", it.ev.ID, perr)
	}
}

// popConfirmed 模拟一个 worker：领取键交接，取出事件并确认“已成功投递到
// 该键 coordinator”，使仍有积压的键重新挂环继续参与轮转。
func popConfirmed(t *testing.T, s *fairScheduler) queuedItem {
	t.Helper()
	kq, it, ok := s.pop()
	if !ok {
		t.Fatal("pop: scheduler closed unexpectedly")
	}
	s.confirmDelivered(kq)
	return it
}

// TestSchedulerConcurrentSingleFlightFIFO 验证多个 worker 并发领取 + 单生产者
// 按序入队时，同一业务键的真实出队顺序（onPop 持锁记录）严格等于入队顺序；
// 并验证 claim 期间该键离环（交接中不会被第二个 worker 重复领取）。
func TestSchedulerConcurrentSingleFlightFIFO(t *testing.T) {
	for iter := 0; iter < 100; iter++ {
		s := newFairScheduler(8, 8)
		const n = 120
		const workers = 4
		var mu sync.Mutex
		popped := make([]int64, 0, n)
		s.onPop = func(it queuedItem) { popped = append(popped, it.ev.Seq) }

		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < n/workers; i++ {
					kq, _, ok := s.pop()
					if !ok {
						return
					}
					// 交接期间该键不得在环中（单航班）：统计快照里该键不会
					// 以“交接中”身份被重复计数，这里通过 flightNode 校验。
					s.mu.Lock()
					inFlight := kq.flightNode != nil
					s.mu.Unlock()
					if !inFlight {
						t.Errorf("claim 后该键应处于交接中（flightNode 非空）")
						return
					}
					s.confirmDelivered(kq)
				}
			}()
		}
		for i := int64(1); i <= n; i++ {
			mustSubmit(t, s, item(fmt.Sprintf("e-%d", i), "k", i, PriorityNormal))
		}
		wg.Wait()
		s.close()

		mu.Lock()
		defer mu.Unlock()
		if len(popped) != n {
			t.Fatalf("iter=%d popped=%d want %d", iter, len(popped), n)
		}
		for i, v := range popped {
			if v != int64(i+1) {
				a, b := i-3, i+3
				if a < 0 {
					a = 0
				}
				if b > n {
					b = n
				}
				t.Fatalf("iter=%d 同键 FIFO 乱序 at %d got %d around=%v", iter, i, v, popped[a:b])
			}
		}
	}
}
