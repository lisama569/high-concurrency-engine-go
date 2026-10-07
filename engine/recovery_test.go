package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// seedPendingLog 直接在 dir 的 pending.log 中写入一批未 Ack 事件，模拟
// “上次停机时遗留的积压”（可含高/普通优先级混合、总量超过单分片容量）。
func seedPendingLog(t *testing.T, dir string, events []Event) {
	t.Helper()
	j, err := openJournalWith(filepath.Join(dir, "pending.log"), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if err := j.Append(context.Background(), ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("场景构造: %d 条遗留事件（未 Ack）已写入 %s", len(events), filepath.Join(dir, "pending.log"))
}

// mixedBacklog 生成 keys 个业务键、每键 perKey 条的积压：优先级按奇偶
// 交替（同键高/普通混合），Payload=Seq 便于校验求和结果。
func mixedBacklog(keys, perKey int) []Event {
	var out []Event
	for k := 0; k < keys; k++ {
		for i := int64(1); i <= int64(perKey); i++ {
			pr := PriorityNormal
			if i%2 == 0 {
				pr = PriorityHigh
			}
			out = append(out, Event{
				ID:       fmt.Sprintf("rc-k%d-%d", k, i),
				Key:      fmt.Sprintf("rck%d", k),
				Seq:      i,
				Payload:  i,
				Priority: pr,
			})
		}
	}
	return out
}

// orderRecorder 返回一个记录“各业务键实际生效 Seq 序列”的 Handler。
func orderRecorder(release <-chan struct{}) (Handler, *sync.Mutex, map[string][]int64) {
	var mu sync.Mutex
	applied := map[string][]int64{}
	h := func(ctx context.Context, ev Event, cur KeyState) (KeyState, error) {
		if release != nil {
			select {
			case <-release:
			case <-ctx.Done():
				return KeyState{}, ctx.Err()
			}
		}
		mu.Lock()
		applied[ev.Key] = append(applied[ev.Key], ev.Seq)
		mu.Unlock()
		return KeyState{Key: ev.Key, Value: cur.Value + ev.Payload}, nil
	}
	return h, &mu, applied
}

// runBaseline 不重启直接跑完同一批事件，返回各业务键终态，作为“恢复结果
// 一致性”的对照。
func runBaseline(t *testing.T, cfg Config, events []Event) map[string]KeyState {
	t.Helper()
	eng, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		submitMust(t, eng, ev)
	}
	waitFor(t, func() bool { return eng.Metrics().Applied >= int64(len(events)) },
		15*time.Second, "基线引擎跑完全部事件")
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatalf("baseline shutdown: %v", err)
	}
	states, err := eng.StateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return states
}

// assertSameStates 校验恢复终态与基线终态逐键一致。
func assertSameStates(t *testing.T, want, got map[string]KeyState) {
	t.Helper()
	for key, w := range want {
		g, ok := got[key]
		if !ok {
			t.Fatalf("恢复后缺少业务键 %s", key)
		}
		if g.Version != w.Version || g.Value != w.Value {
			t.Fatalf("业务键 %s 恢复终态与基线不一致: got version=%d value=%d, want version=%d value=%d",
				key, g.Version, g.Value, w.Version, w.Value)
		}
	}
	t.Logf("一致性判定: %d 个业务键的 version/value 与“不重启直接跑完”完全一致", len(want))
}

// assertPerKeyOrder 校验恢复期内各业务键的生效顺序严格等于投递 Seq 顺序。
func assertPerKeyOrder(t *testing.T, mu *sync.Mutex, applied map[string][]int64, perKey int) {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	for key, seqs := range applied {
		if len(seqs) != perKey {
			t.Fatalf("业务键 %s 生效次数=%d want %d（漏事件或重复生效）", key, len(seqs), perKey)
		}
		for i, s := range seqs {
			if s != int64(i+1) {
				t.Fatalf("业务键 %s 恢复期生效顺序错乱: 第 %d 次生效 Seq=%d want %d", key, i, s, i+1)
			}
		}
	}
	t.Logf("顺序判定: %d 个业务键恢复期内均按 Seq 1..%d 严格有序生效，无漏事件、无重复生效",
		len(applied), perKey)
}

// TestRecoveryMixedPriorityBacklogRestart 复现并回归：上次停机遗留了
// “高/普通优先级混合、总量超过单分片容量”的积压时，重启必须启动成功并
// 把存量全部消化，且各键终态与不重启直接跑完一致。
//
// 业务覆盖：恢复链路 × 优先级限流边界 × 停重启一致性。
func TestRecoveryMixedPriorityBacklogRestart(t *testing.T) {
	const (
		keys     = 3
		perKey   = 20
		shardN   = 1
		shardCap = 8
	)
	mkCfg := func(dir string) Config {
		cfg := DefaultConfig()
		cfg.DataDir = dir
		cfg.Workers = shardN
		cfg.QueuePerShard = shardCap
		cfg.MailboxPerKey = 4
		cfg.SubmitTimeout = 50 * time.Millisecond
		cfg.ShutdownTimeout = 10 * time.Second
		cfg.SyncEachWrite = false
		return cfg
	}
	backlog := mixedBacklog(keys, perKey)
	t.Logf("业务场景: %d 键 × %d 条 = %d 条遗留积压（高/普通交替），单分片容量仅 %d",
		keys, perKey, len(backlog), shardCap)

	// 基线：不重启直接跑完。
	baseStates := runBaseline(t, mkCfg(filepath.Join(t.TempDir(), "base")), backlog)

	// 重启场景：同一批事件作为遗留积压写入 pending.log。Handler 先堵住，
	// 使恢复入队期间分片确定性地被占满（复现“恢复被运行期限流规则打断”），
	// Start 返回后再放行。
	dir := filepath.Join(t.TempDir(), "restart")
	seedPendingLog(t, dir, backlog)
	release := make(chan struct{})
	handler, mu, applied := orderRecorder(release)
	eng, err := New(mkCfg(dir), handler)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatalf("恢复启动失败: 遗留 %d 条积压（超过单分片容量 %d）未能全部放回队列: %v",
			len(backlog), shardCap, err)
	}
	t.Logf("恢复启动成功: %d 条遗留积压已全部重新入队（不受优先级限流/容量背压拒绝）", len(backlog))
	close(release)

	waitFor(t, func() bool { return eng.Metrics().Applied >= int64(len(backlog)) },
		15*time.Second, "恢复积压全部生效")
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatalf("recovery shutdown: %v", err)
	}
	states, err := eng.StateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertSameStates(t, baseStates, states)
	assertPerKeyOrder(t, mu, applied, perKey)
	if d := eng.Metrics().Duplicates; d != 0 {
		t.Fatalf("恢复期出现重复生效: duplicates=%d", d)
	}
}

// TestRecoveryConcurrentShardsRestart 覆盖多 worker、多业务键并发恢复：
// 每个分片的遗留积压都超过分片容量且优先级混合，恢复必须全部生效一次、
// 不漏不重、不死等，终态与基线一致。
//
// 业务覆盖：恢复链路 × 多分片并发 × 幂等去重 × 停机排空。
func TestRecoveryConcurrentShardsRestart(t *testing.T) {
	const (
		keys   = 16
		perKey = 25
	)
	mkCfg := func(dir string) Config {
		cfg := DefaultConfig()
		cfg.DataDir = dir
		cfg.Workers = 4
		cfg.QueuePerShard = 16
		cfg.MailboxPerKey = 4
		cfg.SubmitTimeout = 50 * time.Millisecond
		cfg.ShutdownTimeout = 15 * time.Second
		cfg.SyncEachWrite = false
		return cfg
	}
	backlog := mixedBacklog(keys, perKey)
	t.Logf("业务场景: %d 键 × %d 条 = %d 条遗留积压分布在 4 个分片（每分片容量 16）",
		keys, perKey, len(backlog))

	baseStates := runBaseline(t, mkCfg(filepath.Join(t.TempDir(), "base")), backlog)

	dir := filepath.Join(t.TempDir(), "restart")
	seedPendingLog(t, dir, backlog)
	handler, mu, applied := orderRecorder(nil)
	eng, err := New(mkCfg(dir), handler)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatalf("并发恢复启动失败: %v", err)
	}
	waitFor(t, func() bool { return eng.Metrics().Applied >= int64(len(backlog)) },
		20*time.Second, "并发恢复积压全部生效")
	if err := eng.Shutdown(context.Background()); err != nil {
		t.Fatalf("recovery shutdown: %v", err)
	}
	states, err := eng.StateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertSameStates(t, baseStates, states)
	assertPerKeyOrder(t, mu, applied, perKey)
	m := eng.Metrics()
	if d := m.Duplicates; d != 0 {
		t.Fatalf("并发恢复出现重复生效: duplicates=%d", d)
	}
	t.Logf("并发恢复判定: applied=%d duplicates=%d failed=%d，无漏事件、无重复生效、无死等",
		m.Applied, m.Duplicates, m.Failed)
}
