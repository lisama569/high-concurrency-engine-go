package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestShutdownRestartPriorityStress 多轮“投递中快速停机 -> 重启重放”，验证新增的
// 优先级/公平交接状态不会破坏排空与重启一致性：每轮所有事件最终恰好生效一次。
func TestShutdownRestartPriorityStress(t *testing.T) {
	for iter := 0; iter < 12; iter++ {
		dir := filepath.Join(t.TempDir(), "data")
		cfg := DefaultConfig()
		cfg.DataDir = dir
		cfg.Workers = 4
		cfg.QueuePerShard = 4
		cfg.MailboxPerKey = 4
		cfg.HotKeyBacklog = 8
		cfg.SubmitTimeout = 5 * time.Millisecond
		cfg.ShutdownTimeout = 30 * time.Millisecond
		cfg.SyncEachWrite = false
		eng, err := New(cfg, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := eng.Start(context.Background()); err != nil {
			t.Fatal(err)
		}

		const perKey = 40
		for k := 0; k < 4; k++ {
			for i := int64(1); i <= perKey; i++ {
				pri := PriorityNormal
				if i%7 == 0 {
					pri = PriorityHigh
				}
				_ = eng.Submit(context.Background(), Event{
					ID:  fmt.Sprintf("i%d-k%d-%d", iter, k, i),
					Key: fmt.Sprintf("ssk%d", k), Seq: i, Payload: 1, Priority: pri,
				})
			}
		}
		_ = eng.Shutdown(context.Background())

		eng2, err := New(cfg, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := eng2.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool {
			total := int64(0)
			states, _ := eng2.StateSnapshot(context.Background())
			for k := 0; k < 4; k++ {
				total += states[fmt.Sprintf("ssk%d", k)].Version
			}
			return total == 4*perKey
		}, 5*time.Second, fmt.Sprintf("iter %d 重放收敛", iter))
		if err := eng2.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		states, _ := eng2.StateSnapshot(context.Background())
		for k := 0; k < 4; k++ {
			st := states[fmt.Sprintf("ssk%d", k)]
			if st.Version != perKey || st.Value != perKey {
				t.Fatalf("iter=%d key=%d version=%d value=%d want %d/%d",
					iter, k, st.Version, st.Value, perKey, perKey)
			}
		}
	}
}
