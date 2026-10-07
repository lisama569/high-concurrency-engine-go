package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// mixedBacklogEvents 构造 keys×perKey 个事件：按投递顺序前 highFirst 个为
// 高优先级、其余为普通优先级（同一业务键上两种优先级交错），负载为键内
// 序号，便于精确校验终态（每键 Version=perKey，Value=1+..+perKey）。
func mixedBacklogEvents(keys, perKey, highFirst int) []Event {
	evs := make([]Event, 0, keys*perKey)
	n := 0
	for i := 1; i <= perKey; i++ {
		for k := 0; k < keys; k++ {
			n++
			ev := Event{
				ID:      fmt.Sprintf("rb-k%d-%d", k, i),
				Key:     fmt.Sprintf("rbk%d", k),
				Seq:     int64(i),
				Payload: int64(i),
			}
			if n <= highFirst {
				ev.Priority = PriorityHigh
			}
			evs = append(evs, ev)
		}
	}
	return evs
}

// blockUntilCanceled 是第一生命周期专用的 Handler：一直阻塞到停机取消，
// 保证所有已投递事件都未被 Ack、完整留存在 pending.log 中形成积压。
func blockUntilCanceled(ctx context.Context, ev Event, cur KeyState) (KeyState, error) {
	<-ctx.Done()
	return KeyState{}, ctx.Err()
}

// backlogConfig 返回“小容量、快速拒绝”的第一生命周期配置：队列容量远小于
// 待投递积压，超容量事件被背压/限流拒绝但全部留存在 pending.log。
func backlogConfig(t *testing.T, dir string, workers, queuePerShard int) Config {
	cfg := testConfig(t)
	cfg.DataDir = dir
	cfg.Workers = workers
	cfg.QueuePerShard = queuePerShard
	cfg.MailboxPerKey = 4
	cfg.SubmitTimeout = 0 // 容量竞争立即拒绝，快速把积压堆进 pending.log
	cfg.HandleTimeout = 10 * time.Second
	cfg.MaxAttempts = 1
	cfg.ShutdownTimeout = 200 * time.Millisecond // 故意很短：带积压停机
	return cfg
}

// pileIntoJournal 投递全部事件；容量拒绝（queue_full / priority_throttled）
// 是预期行为——事件已落盘 pending.log，统计各拒绝原因并返回。
func pileIntoJournal(t *testing.T, eng *Engine, evs []Event) map[ErrorKind]int {
	t.Helper()
	rejects := map[ErrorKind]int{}
	for _, ev := range evs {
		if p := eng.Submit(context.Background(), ev); p != nil {
			if p.Kind != KindQueueFull && p.Kind != KindPriorityThrottled {
				t.Fatalf("投递 %s 出现非背压类拒绝: %v", ev.ID, p)
			}
			rejects[p.Kind]++
		}
	}
	return rejects
}

// pendingCount 直接打开数据目录的 pending.log 统计未 Ack 事件数。
func pendingCount(t *testing.T, dir string) int {
	t.Helper()
	j, err := openJournalWith(filepath.Join(dir, "pending.log"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j.Close() }()
	return j.Pending()
}

// expectFinalState 校验每个业务键的终态与投递内容精确一致。
func expectFinalState(t *testing.T, eng *Engine, keys, perKey int) {
	t.Helper()
	states, err := eng.StateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantValue := int64(perKey) * int64(perKey+1) / 2
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("rbk%d", k)
		st := states[key]
		if st.Version != int64(perKey) || st.Value != wantValue {
			t.Fatalf("%s 终态不符: version=%d value=%d want %d/%d",
				key, st.Version, st.Value, perKey, wantValue)
		}
	}
}

// TestRestartRecoveryBypassesPriorityThrottle 是“带混合优先级超容量积压重启”
// 的确定性复现：停机时 pending.log 中同时存在高/普通优先级事件且总量远超
// 单分片容量；重启恢复必须启动成功并把全部存量消化完，终态与“不重启直接
// 跑完”的参考组完全一致。
//
// 复现要点：第一生命周期 Handler 阻塞到停机取消，240 个事件全部滞留
// pending.log；第二生命周期 Handler 先被闸门挡住，使恢复时信号量与分片
// 队列被高优先级事件占满——旧实现此时对普通优先级的存量事件返回
// priority_throttled 并让整个 Start 失败；修复后恢复路径改为阻塞等待空位。
func TestRestartRecoveryBypassesPriorityThrottle(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	const (
		keys      = 8
		perKey    = 30
		total     = keys * perKey
		highFirst = 36 // 前 36 个投递为高优先级：占满信号量(20)+分片队列(16)
	)
	evs := mixedBacklogEvents(keys, perKey, highFirst)

	// ---- 第一生命周期：堆出超容量混合优先级积压 ----
	cfg := backlogConfig(t, dir, 1, 16)
	eng, err := New(cfg, blockUntilCanceled)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	rejects := pileIntoJournal(t, eng, evs)
	// 带积压停机（预期返回 timeout，属正常）。
	_ = eng.Shutdown(context.Background())
	if n := pendingCount(t, dir); n != total {
		t.Fatalf("停机后 pending.log 留存=%d want %d", n, total)
	}
	t.Logf("第一生命周期: 投递=%d 背压拒绝=%v, 全部滞留 pending.log", total, rejects)

	// ---- 第二生命周期：闸门挡住 Handler，恢复必须在拥塞现场下完成 ----
	release := make(chan struct{})
	gated := func(ctx context.Context, ev Event, cur KeyState) (KeyState, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return KeyState{}, ctx.Err()
		}
		return KeyState{Key: ev.Key, Value: cur.Value + ev.Payload}, nil
	}
	eng2, err := New(cfg, gated)
	if err != nil {
		t.Fatal(err)
	}
	startCh := make(chan error, 1)
	go func() { startCh <- eng2.Start(context.Background()) }()
	// 给恢复留出“填满在途信号量+分片队列”的时间：旧实现此刻已对普通
	// 优先级存量事件返回 priority_throttled 并令 Start 失败。
	time.Sleep(300 * time.Millisecond)
	select {
	case err := <-startCh:
		t.Fatalf("恢复在拥塞现场下提前返回（旧限流缺陷）: %v", err)
	default:
	}
	close(release) // 放行 Handler，恢复应阻塞等待空位并最终完成
	select {
	case err := <-startCh:
		if err != nil {
			t.Fatalf("重启恢复失败: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("重启恢复卡死：Handler 放行后 Start 仍未完成")
	}
	waitFor(t, func() bool { return eng2.Metrics().Applied >= total },
		15*time.Second, "恢复积压全部生效")
	if err := eng2.Shutdown(context.Background()); err != nil {
		t.Fatalf("恢复后排空停机应成功: %v", err)
	}
	expectFinalState(t, eng2, keys, perKey)
	m := eng2.Metrics()
	t.Logf("第二生命周期: applied=%d duplicates=%d, 8 键终态 version=%d value=%d",
		m.Applied, m.Duplicates, perKey, int64(perKey)*int64(perKey+1)/2)

	// ---- 参考组：同一批事件不重启直接跑完，终态必须逐键一致 ----
	refCfg := testConfig(t)
	refCfg.Workers = 1
	refCfg.QueuePerShard = 16
	refCfg.MailboxPerKey = 4
	ref, err := New(refCfg, nil) // 内置 SumHandler，与放行后的 gated 等价
	if err != nil {
		t.Fatal(err)
	}
	if err := ref.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, ev := range evs {
		submitMust(t, ref, ev)
	}
	waitFor(t, func() bool { return ref.Metrics().Applied >= total },
		15*time.Second, "参考组全部生效")
	if err := ref.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	refStates, err := ref.StateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	gotStates, err := eng2.StateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("rbk%d", k)
		a, b := gotStates[key], refStates[key]
		if a.Version != b.Version || a.Value != b.Value {
			t.Fatalf("恢复组与参考组 %s 终态不一致: %+v vs %+v", key, a, b)
		}
	}
	t.Logf("判定: 恢复组与不重启参考组 %d 键终态逐键一致", keys)
}

// TestRestartRecoveryConcurrentShards 验证多 worker、多业务键并发恢复：
// 大积压（800 事件、16 键、4 分片、优先级交错）重启后全部按键生效，
// 不漏、不重、不死等。
func TestRestartRecoveryConcurrentShards(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	const (
		keys   = 16
		perKey = 50
		total  = keys * perKey
	)
	// 优先级按投递顺序高低交错（奇数高、偶数普通），每键内部两种混杂。
	evs := make([]Event, 0, total)
	for i := 1; i <= perKey; i++ {
		for k := 0; k < keys; k++ {
			ev := Event{
				ID:      fmt.Sprintf("cb-k%d-%d", k, i),
				Key:     fmt.Sprintf("cbk%d", k),
				Seq:     int64(i),
				Payload: 1,
			}
			if i%2 == 1 {
				ev.Priority = PriorityHigh
			}
			evs = append(evs, ev)
		}
	}

	cfg := backlogConfig(t, dir, 4, 32)
	eng, err := New(cfg, blockUntilCanceled)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	rejects := pileIntoJournal(t, eng, evs)
	_ = eng.Shutdown(context.Background())
	if n := pendingCount(t, dir); n != total {
		t.Fatalf("停机后 pending.log 留存=%d want %d", n, total)
	}
	t.Logf("第一生命周期: 4 worker/16 键 投递=%d 背压拒绝=%v", total, rejects)

	// 第二生命周期：正常 Handler，多 worker 并发恢复全部积压。
	eng2, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng2.Start(context.Background()); err != nil {
		t.Fatalf("并发恢复启动失败: %v", err)
	}
	waitFor(t, func() bool { return eng2.Metrics().Applied >= total },
		20*time.Second, "并发恢复积压全部生效")
	if err := eng2.Shutdown(context.Background()); err != nil {
		t.Fatalf("恢复后排空停机应成功: %v", err)
	}
	states, err := eng2.StateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("cbk%d", k)
		st := states[key]
		if st.Version != perKey || st.Value != perKey {
			t.Fatalf("%s 终态不符: version=%d value=%d want %d/%d",
				key, st.Version, st.Value, perKey, perKey)
		}
	}
	m := eng2.Metrics()
	t.Logf("判定: 16 键 × 50 事件并发恢复完成 applied=%d duplicates=%d conflicts=%d, 终态 version=value=%d",
		m.Applied, m.Duplicates, m.Conflicts, perKey)
}
