package engine

import (
	"context"
	"sync"
	"testing"
	"time"
)

func runPerf(t *testing.T, n, workers int, syncEach bool) {
	dir := t.TempDir()
	st, err := openStoreWith(dir, syncEach)
	if err != nil {
		t.Fatal(err)
	}
	q := newShardedQueue(workers, 1024)
	var wg sync.WaitGroup
	t0 := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ch := q.shardChan(w)
			for item := range ch {
				ctx := context.Background()
				cur, _, _ := st.Get(ctx, item.ev.Key)
				cur.Value += item.ev.Payload
				if _, _, err := st.Apply(ctx, item.ev, cur); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	for i := 0; i < n; i++ {
		key := kstr(i % 16)
		q.Submit(context.Background(), queuedItem{ev: Event{ID: "e" + kstr(i), Key: key, Seq: int64(i/16 + 1), Payload: 1}})
	}
	q.close()
	wg.Wait()
	el := time.Since(t0)
	snap, _ := st.Snapshot(context.Background())
	total := int64(0)
	for _, v := range snap {
		total += v.Value
	}
	t.Logf("syncEach=%v n=%d elapsed=%v throughput=%d/s totalValue=%d", syncEach, n, el, int(float64(n)/el.Seconds()), total)
	st.Close()
}

func TestPerfSync(t *testing.T)   { runPerf(t, 4000, 8, true) }
func TestPerfNoSync(t *testing.T) { runPerf(t, 4000, 8, false) }

func kstr(i int) string {
	if i == 0 {
		return "k0"
	}
	var b [12]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return "k" + string(b[p:])
}
