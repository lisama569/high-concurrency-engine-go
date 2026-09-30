// Command demo 是引擎的本地最小可运行示例：
//
//  1. 准备一批事件（含重复投递、乱序冲突、高并发压力）；
//  2. 第一次启动引擎并并发投递，观察背压拒绝、判重与冲突指标；
//  3. 优雅停机；
//  4. 第二次启动（同一数据目录），重放 pending.log 并校验状态不丢不重；
//  5. 打印最终业务键状态与全部判定指标。
//
// 运行：
//
//	go run ./cmd/demo
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lisama569/high-concurrency-engine-go/engine"
)

func main() {
	logger := log.New(os.Stdout, "[demo] ", log.LstdFlags|log.Lmsgprefix)

	dataDir := filepath.Join(os.TempDir(), "hce-demo")
	// 每次演示清理旧目录，保证可复现。
	if err := os.RemoveAll(dataDir); err != nil {
		log.Fatalf("clean data dir: %v", err)
	}

	cfg := engine.DefaultConfig()
	cfg.DataDir = dataDir
	cfg.Workers = 8
	cfg.QueuePerShard = 16 // 刻意很小，便于在演示中观察 queue_full 背压
	cfg.MailboxPerKey = 16
	cfg.HotKeyBacklog = 24 // 单键在途积压达到即标记热点键
	cfg.SubmitTimeout = 5 * time.Millisecond
	cfg.MaxAttempts = 3
	cfg.Backoff = time.Millisecond
	cfg.HandleTimeout = 200 * time.Millisecond
	cfg.ShutdownTimeout = 5 * time.Second
	// 演示默认使用组提交异步持久化以获得高吞吐；进程崩溃由事件源 offset
	// 重投 + 存储幂等兜底，优雅停机完整落盘。需要最强崩溃安全可改为 true。
	cfg.SyncEachWrite = false

	ctx := context.Background()

	eng, err := engine.New(cfg, nil)
	if err != nil {
		log.Fatalf("new engine: %v", err)
	}
	if err := eng.Start(ctx); err != nil {
		log.Fatalf("start engine: %v", err)
	}
	logger.Printf("engine started: workers=%d queueMax=%d dataDir=%s",
		cfg.Workers, cfg.Workers*cfg.QueuePerShard, dataDir)

	// 高并发投递：32 个 goroutine 往 8 个业务键各投递 200 个有序事件。
	// 队列容量刻意很小，处理能力不足时 Submit 会快速返回 queue_full；这里
	// 生产端按该（可重试）原因退避重试，既演示背压，也保证最终全部生效、
	// 结果可复现。拒绝原因被统计用于观察。
	const goroutines = 32
	const perKey = 200
	var wg sync.WaitGroup
	var mu sync.Mutex
	rejects := map[string]int{}

	submitWithRetry := func(ev engine.Event) {
		for {
			perr := eng.Submit(ctx, ev)
			if perr == nil {
				return
			}
			var ee *engine.Error
			if errors.As(perr, &ee) {
				mu.Lock()
				rejects[ee.Kind.String()]++
				mu.Unlock()
				// queue_full=真正容量耗尽；priority_shed=拥塞让位高优先级。
				// 两者都可安全重试（事件已在持久日志），稍后重投即可。
				if ee.Kind == engine.KindQueueFull || ee.Kind == engine.KindPriorityShed {
					time.Sleep(time.Millisecond) // 背压/让位：稍后重试，事件不丢
					continue
				}
			}
			return // 其它原因（如非法/停机）不重试
		}
	}

	t0 := time.Now()
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perKey; i++ {
				key := fmt.Sprintf("k%d", g%8)
				// 每键每 50 件放 1 件高优先级，演示同键优先级交错但顺序不乱、
				// 以及跨键拥塞时高优先被更早消费。
				pri := engine.PriorityNormal
				if i%50 == 49 {
					pri = engine.PriorityHigh
				}
				submitWithRetry(engine.Event{
					ID:       fmt.Sprintf("e-%s-%d", key, i+1),
					Key:      key,
					Seq:      int64(i + 1),
					Kind:     "add",
					Payload:  1,
					Priority: pri,
				})
			}
		}(g)
	}
	wg.Wait()
	logger.Printf("burst finished in %v; backpressure rejections(retried) by reason: %v",
		time.Since(t0), rejects)

	// 公平性演示：单个热点键大量涌入时，一个冷键高优事件仍应很快被处理，
	// 且热点键会在指标中被显式识别而不是无声拖慢全局。
	doneHot := make(chan struct{})
	go func() {
		defer close(doneHot)
		for i := int64(1); i <= 2000; i++ {
			submitWithRetry(engine.Event{
				ID: fmt.Sprintf("hotburst-%d", i), Key: "hotburst",
				Seq: i, Kind: "add", Payload: 1, Priority: engine.PriorityNormal,
			})
		}
	}()
	time.Sleep(10 * time.Millisecond) // 让热点键先积压
	coldStart := time.Now()
	submitWithRetry(engine.Event{
		ID: "cold-urgent", Key: "coldburst", Seq: 1, Kind: "add",
		Payload: 1, Priority: engine.PriorityHigh,
	})
	logger.Printf("cold high-priority key enqueued behind a hot-key burst; waiting for it to be applied ...")
	// 轮询直到冷键生效，打印等待耗时与此刻热点键可观测积压。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		states, _ := eng.StateSnapshot(ctx)
		if _, ok := states["coldburst"]; ok {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mc := eng.Metrics()
	logger.Printf("fairness: cold high-priority key applied within %v; hotKeyCount=%d threshold=%d",
		time.Since(coldStart), mc.HotKeyCount, mc.HotThreshold)
	for _, kb := range mc.KeyBacklogs {
		if kb.Key == "hotburst" {
			logger.Printf("fairness: hotburst backlog total=%d (sched=%d coord=%d) hot=%v",
				kb.Total, kb.Sched, kb.Coord, kb.Hot)
		}
	}
	<-doneHot

	// 追加重复投递：相同 ID 必须被幂等拦截。
	dup := engine.Event{ID: "e-k0-1", Key: "k0", Seq: 1, Kind: "add", Payload: 1}
	if perr := eng.Submit(ctx, dup); perr != nil {
		logger.Printf("duplicate submit rejected: %v", perr)
	}
	// 追加乱序事件：Seq 落后于当前版本，必须被标记冲突而不是覆盖。
	stale := engine.Event{ID: "e-k1-stale", Key: "k1", Seq: 1, Kind: "add", Payload: 999}
	if perr := eng.Submit(ctx, stale); perr != nil {
		logger.Printf("stale submit rejected: %v", perr)
	}

	// 停机前给在途事件一点排空时间（Shutdown 本身也会等待）。
	if err := eng.Shutdown(ctx); err != nil {
		var ee *engine.Error
		if errors.As(err, &ee) {
			logger.Printf("shutdown returned: kind=%s msg=%s", ee.Kind, ee.Message)
		} else {
			log.Fatalf("shutdown: %v", err)
		}
	}
	printMetrics(logger, eng)

	// ---- 第二次生命周期：验证重启后状态不丢不重、pending 安全重放 ----
	logger.Printf("restarting engine against same data dir ...")
	eng2, err := engine.New(cfg, nil)
	if err != nil {
		log.Fatalf("new engine(2): %v", err)
	}
	if err := eng2.Start(ctx); err != nil {
		log.Fatalf("start engine(2): %v", err)
	}
	if err := eng2.Shutdown(ctx); err != nil {
		log.Fatalf("shutdown(2): %v", err)
	}
	states, err := eng2.StateSnapshot(ctx)
	if err != nil {
		log.Fatalf("state snapshot: %v", err)
	}
	total := int64(0)
	for k := 0; k < 8; k++ {
		key := fmt.Sprintf("k%d", k)
		st := states[key]
		total += st.Value
		logger.Printf("final state key=%s version=%d value=%d lastEvent=%s",
			key, st.Version, st.Value, st.LastEventID)
	}
	logger.Printf("deterministic check: sum across keys = %d (each key expected 200)", total)
	m2 := eng2.Metrics()
	logger.Printf("second run metrics: applied=%d duplicates=%d conflicts=%d failed=%d pendingReplayed",
		m2.Applied, m2.Duplicates, m2.Conflicts, m2.Failed)
	logger.Printf("inspect files under: %s", dataDir)
}

func printMetrics(logger *log.Logger, eng *engine.Engine) {
	m := eng.Metrics()
	logger.Printf("metrics: received=%d accepted=%d rejected=%d inFlight=%d",
		m.Received, m.Accepted, m.Rejected, m.InFlight)
	logger.Printf("metrics: applied=%d duplicates=%d conflicts=%d failed=%d retried=%d",
		m.Applied, m.Duplicates, m.Conflicts, m.Failed, m.Retried)
	logger.Printf("backlog: depth=%d/%d byPriority=%+v rejectReasons=%v",
		m.QueueDepth, m.QueueMax, m.QueueDepthByPri, m.RejectReason)
	logger.Printf("priority: acceptedByPri=%+v rejectedByPri=%+v rejectByKindPri=%+v",
		m.AcceptedByPri, m.RejectedByPri, m.RejectByKindPri)
	logger.Printf("hot keys: threshold=%d hotKeyCount=%d", m.HotThreshold, m.HotKeyCount)
	for i, kb := range m.KeyBacklogs {
		if i >= 8 {
			logger.Printf("hot keys: ...(%d more)", len(m.KeyBacklogs)-8)
			break
		}
		logger.Printf("  key=%s total=%d (sched=%d coord=%d normal=%d high=%d hot=%v)",
			kb.Key, kb.Total, kb.Sched, kb.Coord, kb.Normal, kb.High, kb.Hot)
	}
}
