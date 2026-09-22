// 命令 engine 在本地演示引擎的并发提交、背压拒绝与优雅停机。
// 状态文件默认落在 ./data/state.json，重复运行可观察幂等去重效果。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lisama569/high-concurrency-engine-go/internal/engine"
	"github.com/lisama569/high-concurrency-engine-go/internal/event"
	"github.com/lisama569/high-concurrency-engine-go/internal/store"
)

func main() {
	var (
		statePath = flag.String("state", "data/state.json", "状态文件路径")
		events    = flag.Int("events", 1000, "提交事件总数")
		producers = flag.Int("producers", 8, "并发生产者数")
		workers   = flag.Int("workers", 4, "消费工作者数")
		queueCap  = flag.Int("queue", 64, "有界队列容量")
	)
	flag.Parse()

	st, err := store.Open(*statePath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	eng := engine.New(engine.Config{QueueCapacity: *queueCap, Workers: *workers}, st)

	start := time.Now()
	var wg sync.WaitGroup
	var rejected, canceled uint64
	var mu sync.Mutex
	for p := 0; p < *producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := p; i < *events; i += *producers {
				ev := event.Event{
					ID:        fmt.Sprintf("evt-%d", i),
					Key:       fmt.Sprintf("key-%d", i%16),
					Seq:       uint64(i/16 + 1),
					Payload:   fmt.Sprintf("payload-%d", i),
					CreatedAt: time.Now(),
				}
				// 背压时短暂退避重试（延迟而非丢弃），最终仍失败才计数。
				var err error
				for attempt := 0; attempt < 5; attempt++ {
					err = eng.Submit(context.Background(), ev)
					if !event.IsKind(err, event.KindQueueFull) {
						break
					}
					time.Sleep(time.Duration(attempt+1) * time.Millisecond)
				}
				if err == nil {
					continue
				}
				mu.Lock()
				switch {
				case event.IsKind(err, event.KindQueueFull):
					rejected++
				default:
					canceled++
				}
				mu.Unlock()
			}
		}(p)
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := eng.Shutdown(ctx); err != nil {
		log.Fatalf("shutdown: %v", err)
	}

	m := eng.Metrics()
	fmt.Printf("耗时: %v\n", time.Since(start).Round(time.Millisecond))
	fmt.Printf("入队=%d 背压拒绝=%d 生效=%d 幂等去重=%d 冲突=%d 失败=%d 其他拒绝=%d\n",
		m.Submitted, m.Rejected, m.Applied, m.Duplicated, m.Conflicts, m.Failed, canceled)
}
