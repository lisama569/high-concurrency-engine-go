package store

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func openTemp(t *testing.T) (*FileStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return st, path
}

func TestApplyIdempotent(t *testing.T) {
	st, _ := openTemp(t)
	defer st.Close()

	applied, conflict, err := st.Apply("k1", 1, "evt-1", "v1")
	t.Logf("首次应用: 输入(key=k1,seq=1,id=evt-1) 判定依据=applied=true 实际 applied=%v conflict=%v err=%v", applied, conflict, err)
	if !applied || conflict || err != nil {
		t.Fatalf("首次应用应生效: applied=%v conflict=%v err=%v", applied, conflict, err)
	}

	applied, conflict, err = st.Apply("k1", 1, "evt-1", "v1")
	t.Logf("重复投递: 输入(key=k1,seq=1,id=evt-1) 判定依据=幂等命中 applied=false 实际 applied=%v", applied)
	if applied || conflict || err != nil {
		t.Fatalf("重复事件不应重复应用: applied=%v conflict=%v err=%v", applied, conflict, err)
	}

	rec, _ := st.Get("k1")
	t.Logf("状态校验: 判定依据=Value 仍为 v1 且 Applied 只含 1 条 实际 value=%q applied=%v", rec.Value, rec.Applied)
	if rec.Value != "v1" || len(rec.Applied) != 1 {
		t.Fatalf("重复投递造成状态污染: %+v", rec)
	}
}

func TestApplyConflictOnOutOfOrder(t *testing.T) {
	st, _ := openTemp(t)
	defer st.Close()

	if _, _, err := st.Apply("k1", 5, "evt-5", "v5"); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	applied, conflict, err := st.Apply("k1", 3, "evt-3", "v3")
	t.Logf("乱序事件: 输入(seq=3 当前已生效 seq=5) 判定依据=conflict=true 且不覆盖 实际 applied=%v conflict=%v err=%v", applied, conflict, err)
	if applied || !conflict || err != nil {
		t.Fatalf("乱序事件应标记冲突: applied=%v conflict=%v err=%v", applied, conflict, err)
	}

	rec, _ := st.Get("k1")
	t.Logf("状态校验: 判定依据=Value 仍为 v5、Conflict=true、Conflicts 含 evt-3 实际 %+v", rec)
	if rec.Value != "v5" || !rec.Conflict || len(rec.Conflicts) != 1 || rec.Conflicts[0] != "evt-3" {
		t.Fatalf("冲突未被明确标记或状态被覆盖: %+v", rec)
	}
}

func TestRestartPersistence(t *testing.T) {
	st, path := openTemp(t)
	for i := 1; i <= 3; i++ {
		if _, _, err := st.Apply("k1", uint64(i), fmt.Sprintf("evt-%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 模拟重启：重新打开同一文件。
	st2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()

	rec, _ := st2.Get("k1")
	t.Logf("重启后状态: 判定依据=Seq=3 Value=v3 实际 seq=%d value=%q applied=%v", rec.Seq, rec.Value, rec.Applied)
	if rec.Seq != 3 || rec.Value != "v3" || len(rec.Applied) != 3 {
		t.Fatalf("重启后状态丢失: %+v", rec)
	}

	applied, _, err := st2.Apply("k1", 2, "evt-2", "v2")
	t.Logf("重启后重投旧事件: 输入(id=evt-2) 判定依据=幂等命中 applied=false 实际 applied=%v err=%v", applied, err)
	if applied || err != nil {
		t.Fatalf("重启后重复投递不应重复应用: applied=%v err=%v", applied, err)
	}
}

func TestConcurrentApplySameKey(t *testing.T) {
	st, _ := openTemp(t)
	defer st.Close()

	const n = 64
	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := st.Apply("k1", uint64(i), fmt.Sprintf("evt-%d", i), fmt.Sprintf("v%d", i))
			if err != nil {
				t.Errorf("Apply: %v", err)
			}
		}(i)
	}
	wg.Wait()

	rec, _ := st.Get("k1")
	t.Logf("并发输入=%d 条同键乱序事件 判定依据=最终 Seq=%d、Value=v%d、生效+冲突=总数 实际 seq=%d value=%q applied=%d 条 conflicts=%d 条",
		n, n, n, rec.Seq, rec.Value, len(rec.Applied), len(rec.Conflicts))
	if rec.Seq != n || rec.Value != fmt.Sprintf("v%d", n) {
		t.Fatalf("并发下同键最大序号应最终生效: seq=%d value=%q", rec.Seq, rec.Value)
	}
	if got := len(rec.Applied) + len(rec.Conflicts); got != n {
		t.Fatalf("每条事件应且只应被归类一次（生效或冲突），实际归类 %d 条", got)
	}
	if !rec.Conflict {
		t.Fatalf("乱序到达应被标记为冲突")
	}
}
