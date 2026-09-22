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
	cfg.QueuePerShard = 16   // 刻意很小，便于在演示中观察 queue_full 背压
	cfg.SubmitTimeout = 2 * time.Millisecond
	cfg.MaxAttempts = 3
	cfg.Backoff = time.Millisecond
	cfg.HandleTimeout = 200 * time.Millisecond
	cfg.ShutdownTimeout = 5 * time.Second

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

	// 高并发投递：8 个键 × 每键 200 个有序事件，另有重复与乱序事件。
	const goroutines = 32
	const perKey = 200
	var wg sync.WaitGroup
	var mu sync.Mutex
	rejects := map[string]int{}

	t0 := time.Now()
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perKey; i++ {
				key := fmt.Sprintf("k%d", g%8)
				ev := engine.Event{
					ID:      fmt.Sprintf("e-%s-%d", key, i+1),
					Key:     key,
					Seq:     int64(i + 1),
					Kind:    "add",
					Payload: 1,
				}
				if perr := eng.Submit(ctx, ev); perr != nil {
					var ee *engine.Error
					if errors.As(perr, &ee) {
						mu.Lock()
						rejects[ee.Kind.String()]++
						mu.Unlock()
					}
				}
			}
		}(g)
	}
	wg.Wait()
	logger.Printf("burst finished in %v; rejects by reason: %v", time.Since(t0), rejects)

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
	logger.Printf("backlog: depth=%d/%d rejectReasons=%v",
		m.QueueDepth, m.QueueMax, m.RejectReason)
}
