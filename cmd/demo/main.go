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
	cfg.SubmitTimeout = 5 * time.Millisecond
	cfg.MaxAttempts = 3
	cfg.Backoff = time.Millisecond
	cfg.HandleTimeout = 200 * time.Millisecond
	cfg.ShutdownTimeout = 5 * time.Second
	// 演示队列很小，把热点阈值也调小，便于在突发期间观察到热点键识别。
	cfg.HotKeyBacklogThreshold = 32
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
				// queue_full=容量耗尽；priority_throttled=普通级被高优先级
				// 拥塞占位。两者都属于可重试背压，稍后重投，事件不丢。
				if ee.Kind == engine.KindQueueFull || ee.Kind == engine.KindPriorityThrottled {
					time.Sleep(time.Millisecond)
					continue
				}
			}
			return // 其它原因（如非法/停机）不重试
		}
	}

	t0 := time.Now()

	// 突发期间周期性采样一次热点键与分级积压（排空后热点即消失，故在运行
	// 中观察才有意义）；仅演示可观测性，不影响投递。
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			m := eng.Metrics()
			if len(m.HotKeys) > 0 {
				logger.Printf("in-flight sample: hotKeys=%v queueHigh=%d queueNormal=%d depth=%d/%d",
					m.HotKeys, m.QueueHigh, m.QueueNormal, m.QueueDepth, m.QueueMax)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perKey; i++ {
				key := fmt.Sprintf("k%d", g%8)
				submitWithRetry(engine.Event{
					ID:      fmt.Sprintf("e-%s-%d", key, i+1),
					Key:     key,
					Seq:     int64(i + 1),
					Kind:    "add",
					Payload: 1,
				})
			}
		}(g)
	}
	wg.Wait()
	logger.Printf("burst finished in %v; backpressure rejections(retried) by reason: %v",
		time.Since(t0), rejects)

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

	// 优先级演示：向两个独立业务键分别投递普通与高优先级事件。优先级只
	// 影响“不同键之间”拥塞时被消费的先后；同键仍严格按 Seq 生效。
	for i := 0; i < 10; i++ {
		submitWithRetry(engine.Event{
			ID: fmt.Sprintf("pri-normal-%d", i+1), Key: "priNormal",
			Seq: int64(i + 1), Kind: "add", Payload: 1, Priority: engine.PriorityNormal,
		})
		submitWithRetry(engine.Event{
			ID: fmt.Sprintf("pri-high-%d", i+1), Key: "priHigh",
			Seq: int64(i + 1), Kind: "add", Payload: 1, Priority: engine.PriorityHigh,
		})
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
	logger.Printf("deterministic check: sum across k0..k7 = %d (each key expected 200)", total)
	for _, key := range []string{"priNormal", "priHigh"} {
		if st, ok := states[key]; ok {
			logger.Printf("priority key=%s version=%d value=%d (高/普通优先级各 10 条，终态均为 10)",
				key, st.Version, st.Value)
		}
	}
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
	logger.Printf("backlog: depth=%d/%d (high=%d normal=%d) rejectReasons=%v",
		m.QueueDepth, m.QueueMax, m.QueueHigh, m.QueueNormal, m.RejectReason)
	logger.Printf("by priority: received=%v accepted=%v rejected=%v",
		m.ReceivedByPriority, m.AcceptedByPriority, m.RejectedByPriority)
	if len(m.HotKeys) > 0 {
		logger.Printf("hot keys (backlog >= %d): %v", m.HotKeyThreshold, m.HotKeys)
	} else {
		logger.Printf("hot keys: none (threshold=%d)", m.HotKeyThreshold)
	}
}
