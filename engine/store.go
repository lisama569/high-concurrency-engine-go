package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// KeyState 是单个业务键的持久化状态。
type KeyState struct {
	Key            string   `json:"key"`
	Version        int64    `json:"version"`
	Value          int64    `json:"value"`
	LastEventID    string   `json:"last_event_id"`
	LastAppliedIDs []string `json:"applied_ids,omitempty"`
}

// ApplyVerdict 是状态应用的判定结论。
type ApplyVerdict int

const (
	ApplyOK ApplyVerdict = iota
	ApplyDuplicate
	ApplyConflict
)

const (
	appliedIDWindow = 1024
	stateShardCount = 32
	snapshotEvery   = 2048
	// groupCommitWindow 是 committer 聚合并发提交的窗口：窗口内到达的
	// 记录由同一次 flush+fsync 覆盖。
	groupCommitWindow = 200 * time.Microsecond
)

// Store 是按业务键持久化状态的本地存储抽象。
type Store interface {
	// Modify 在单个业务键上原子地完成“读取当前状态 -> 调用 fn 推导下一
	// 状态 -> 判重/判冲突 -> 持久化提交”。这是消除读-改-写竞态的唯一入口：
	// 同一业务键的判定与生效在同一把分片锁内完成，不会用旧状态覆盖新状态。
	Modify(ctx context.Context, ev Event, fn func(cur KeyState) (KeyState, error)) (KeyState, ApplyVerdict, error)
	// Apply 按调用方给定的 next 直接判定并写入（保留给不需要读改写的场景）。
	Apply(ctx context.Context, ev Event, next KeyState) (KeyState, ApplyVerdict, error)
	Get(ctx context.Context, key string) (KeyState, bool, error)
	Snapshot(ctx context.Context) (map[string]KeyState, error)
	Close() error
}

type walRecord struct {
	Key            string   `json:"key"`
	Version        int64    `json:"version"`
	Value          int64    `json:"value"`
	LastEventID    string   `json:"last_event_id"`
	LastAppliedIDs []string `json:"applied_ids,omitempty"`
}

type stateShard struct {
	mu     sync.Mutex
	states map[string]*KeyState
}

// fileStore 基于“快照文件 + 追加式 WAL”的内存/文件混合存储。
//
// 持久化（syncEach=true）：worker 把 WAL 字节写入共享缓冲并取得一个单调
// 代次，随后等待；专用 committer 协程按微小窗口聚合并发写入，一次
// flush+fsync 覆盖整批并把已落盘代次推进到该批最大代次，唤醒等待者。
// 每个成功返回的 Apply，其记录必然已被某次 fsync 覆盖。
//
// 快照：先冻结新在途 Apply，等待存量在途排空，再停止 committer 并统一
// flush+fsync，收集状态、原子写快照、截断 WAL，保证不丢已确认记录。
type fileStore struct {
	dir      string
	shards   [stateShardCount]*stateShard
	syncEach bool

	walMu sync.Mutex // 保护 walW 缓冲写入与 assignedGen
	wal   *os.File
	walW  *bufio.Writer

	// 组提交代次：assignedGen 已分配的最大代次；committedGen 已被
	// flush+fsync 覆盖的最大代次。
	assignedGen  uint64
	commitMu     sync.Mutex // 保护 committedGen / commitErr / commitCond
	committedGen uint64
	commitErr    error
	commitCond   *sync.Cond
	reqCh        chan struct{}

	// committerMu 串行化提交协程的启停与快照接管，防止重复关闭。
	committerMu   sync.Mutex
	committerOn   bool
	committerStop chan struct{}
	committerDone chan struct{}
	closing       bool

	// 在途 Apply（WAL 字节已写、内存状态未提交）。
	inflightMu sync.Mutex
	inflight   int
	frozen     bool
	inflightCv *sync.Cond

	applies atomic.Int64
}

func openStore(dataDir string) (Store, error) {
	return openStoreWith(dataDir, true)
}

func openStoreWith(dataDir string, syncEach bool) (*fileStore, error) {
	const op = "store.open"
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, newError(KindStorage, op, "mkdir data dir: "+err.Error(), err)
	}
	s := &fileStore{
		dir:           dataDir,
		syncEach:      syncEach,
		reqCh:         make(chan struct{}, 1),
		committerStop: make(chan struct{}),
		committerDone: make(chan struct{}),
	}
	s.commitCond = sync.NewCond(&s.commitMu)
	s.inflightCv = sync.NewCond(&s.inflightMu)
	for i := range s.shards {
		s.shards[i] = &stateShard{states: make(map[string]*KeyState)}
	}

	f, err := os.OpenFile(filepath.Join(dataDir, "state.wal"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, newError(KindStorage, op, "open wal: "+err.Error(), err)
	}
	s.wal = f
	s.walW = bufio.NewWriterSize(f, 64*1024)

	if err := s.loadSnapshot(); err != nil {
		return nil, err
	}
	if err := s.replayWAL(); err != nil {
		return nil, err
	}

	if syncEach {
		s.committerMu.Lock()
		s.startCommitter()
		s.committerMu.Unlock()
	}
	return s, nil
}

func keyShardIndex(key string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() % stateShardCount)
}

func (s *fileStore) shardFor(key string) *stateShard {
	return s.shards[keyShardIndex(key)]
}

func (s *fileStore) loadSnapshot() error {
	const op = "store.loadSnapshot"
	data, err := os.ReadFile(filepath.Join(s.dir, "state.snapshot.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return newError(KindStorage, op, "read snapshot: "+err.Error(), err)
	}
	if len(data) == 0 {
		return nil
	}
	var snap map[string]KeyState
	if err := json.Unmarshal(data, &snap); err != nil {
		return newError(KindStorage, op, "decode snapshot: "+err.Error(), err)
	}
	for k, v := range snap {
		st := v
		s.shardFor(k).states[k] = &st
	}
	return nil
}

// replayWAL 在启动阶段（无并发 Apply、committer 未启动）重放 WAL。
func (s *fileStore) replayWAL() error {
	const op = "store.replayWAL"
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return newError(KindStorage, op, "seek wal: "+err.Error(), err)
	}
	sc := bufio.NewScanner(s.wal)
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	n := 0
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec walRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return newError(KindStorage, op, "decode wal line: "+err.Error(), err)
		}
		st := KeyState{
			Key:            rec.Key,
			Version:        rec.Version,
			Value:          rec.Value,
			LastEventID:    rec.LastEventID,
			LastAppliedIDs: rec.LastAppliedIDs,
		}
		s.shardFor(rec.Key).states[rec.Key] = &st
		n++
	}
	if err := sc.Err(); err != nil {
		return newError(KindStorage, op, "scan wal: "+err.Error(), err)
	}
	if n > 0 {
		if err := s.snapshotLocked(); err != nil {
			return err
		}
	}
	if _, err := s.wal.Seek(0, io.SeekEnd); err != nil {
		return newError(KindStorage, op, "seek wal end: "+err.Error(), err)
	}
	s.walW.Reset(s.wal)
	return nil
}

// signalCommit 非阻塞通知 committer。
func (s *fileStore) signalCommit() {
	select {
	case s.reqCh <- struct{}{}:
	default:
	}
}

// runCommitter 是唯一执行 flush+fsync 的协程。
func (s *fileStore) runCommitter() {
	defer close(s.committerDone)
	for {
		select {
		case <-s.committerStop:
			return
		case <-s.reqCh:
		}
		// 聚合窗口：合并此刻正在写入缓冲的并发记录。
		select {
		case <-s.committerStop:
			return
		case <-time.After(groupCommitWindow):
		}

		s.commitMu.Lock()
		s.walMu.Lock()
		ferr := s.walW.Flush()
		var serr error
		if ferr == nil {
			serr = s.wal.Sync()
		}
		gen := s.assignedGen
		s.walMu.Unlock()

		switch {
		case ferr != nil:
			s.commitErr = newError(KindStorage, "store.commit", "flush wal: "+ferr.Error(), ferr)
		case serr != nil:
			s.commitErr = newError(KindStorage, "store.commit", "sync wal: "+serr.Error(), serr)
		default:
			s.commitErr = nil
			s.committedGen = gen
		}
		s.commitCond.Broadcast()
		s.commitMu.Unlock()
	}
}

// startCommitter 启动提交协程（调用方通过 committerMu 保证不重复启动）。
func (s *fileStore) startCommitter() {
	if !s.syncEach {
		return
	}
	s.committerStop = make(chan struct{})
	s.committerDone = make(chan struct{})
	s.committerOn = true
	go s.runCommitter()
}

// stopCommitterLocked 停止提交协程并把残余缓冲落盘。必须持有 committerMu。
func (s *fileStore) stopCommitterLocked(flush bool) error {
	if s.syncEach && s.committerOn {
		close(s.committerStop)
		<-s.committerDone
		s.committerOn = false
	}
	if !flush {
		return nil
	}
	s.walMu.Lock()
	ferr := s.walW.Flush()
	var serr error
	if ferr == nil {
		serr = s.wal.Sync()
	}
	s.walMu.Unlock()
	if ferr != nil {
		return newError(KindStorage, "store.commit", "flush tail: "+ferr.Error(), ferr)
	}
	if serr != nil {
		return newError(KindStorage, "store.commit", "sync tail: "+serr.Error(), serr)
	}
	return nil
}

// waitCommitted 等待自己的代次被 flush+fsync 覆盖。
func (s *fileStore) waitCommitted(gen uint64) error {
	s.commitMu.Lock()
	for s.committedGen < gen {
		s.commitCond.Wait()
	}
	err := s.commitErr
	s.commitMu.Unlock()
	return err
}

func (s *fileStore) enterInflight() error {
	s.inflightMu.Lock()
	// 冻结窗口极短（仅快照刷盘与截断）：等待冻结结束再登记，避免把
	// 短暂的内部维护暴露成对外错误。Close 后调用方不会再进入这里。
	for s.frozen {
		s.inflightCv.Wait()
	}
	s.inflight++
	s.inflightMu.Unlock()
	return nil
}

func (s *fileStore) leaveInflight() {
	s.inflightMu.Lock()
	s.inflight--
	if s.inflight == 0 {
		s.inflightCv.Broadcast()
	}
	s.inflightMu.Unlock()
}

// appendWAL 写入一条 WAL 记录并（syncEach 时）等待其落盘。
func (s *fileStore) appendWAL(rec *walRecord) error {
	const op = "store.appendWAL"
	data, err := json.Marshal(rec)
	if err != nil {
		return newError(KindStorage, op, "encode wal: "+err.Error(), err)
	}
	data = append(data, '\n')

	s.walMu.Lock()
	if _, err := s.walW.Write(data); err != nil {
		s.walMu.Unlock()
		return newError(KindStorage, op, "write wal: "+err.Error(), err)
	}
	var gen uint64
	if s.syncEach {
		s.assignedGen++
		gen = s.assignedGen
	}
	s.walMu.Unlock()

	if !s.syncEach {
		return nil
	}
	s.signalCommit()
	return s.waitCommitted(gen)
}

// applyDecision 是第一段判定的结果。
type applyDecision int

const (
	decideApply applyDecision = iota
	decideDuplicate
	decideConflict
)

// Modify 在单个业务键上原子完成读-改-写：同一分片锁内依次做判重、
// 冲突判定、调用 fn 推导下一状态、WAL 持久化与内存提交。持锁期间执行
// fsync 只会串行化“同一业务键”的操作（这正是顺序语义要求），不同业务键
// 位于不同分片锁上，仍然完全并行。
func (s *fileStore) Modify(
	ctx context.Context,
	ev Event,
	fn func(cur KeyState) (KeyState, error),
) (KeyState, ApplyVerdict, error) {
	return s.applyInternal(ctx, ev, fn)
}

// Apply 以调用方给定的 next 直接判定并写入（内部同样在分片锁内完成）。
func (s *fileStore) Apply(ctx context.Context, ev Event, next KeyState) (KeyState, ApplyVerdict, error) {
	return s.applyInternal(ctx, ev, func(_ KeyState) (KeyState, error) {
		return next, nil
	})
}

func (s *fileStore) applyInternal(
	ctx context.Context,
	ev Event,
	fn func(cur KeyState) (KeyState, error),
) (KeyState, ApplyVerdict, error) {
	const op = "store.modify"
	if err := ctx.Err(); err != nil {
		return KeyState{}, ApplyOK, normalize(op, err)
	}
	sh := s.shardFor(ev.Key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	cur, exists := sh.states[ev.Key]
	var curCopy KeyState
	if exists {
		curCopy = *cur
		for _, id := range cur.LastAppliedIDs {
			if id == ev.ID {
				return curCopy, ApplyDuplicate, nil
			}
		}
		if ev.Seq < cur.Version {
			return curCopy, ApplyConflict, nil
		}
	}

	// 在锁内基于当前状态推导下一状态，杜绝读到旧状态后覆盖新值。
	next, err := fn(curCopy)
	if err != nil {
		return KeyState{}, ApplyOK, normalize(op, err)
	}
	if cerr := ctx.Err(); cerr != nil {
		return KeyState{}, ApplyOK, normalize(op, cerr)
	}

	staged := KeyState{
		Key:            ev.Key,
		Version:        ev.Seq,
		Value:          next.Value,
		LastEventID:    ev.ID,
		LastAppliedIDs: appendID(curCopy.LastAppliedIDs, ev.ID),
	}

	s.enterInflightLocked()
	werr := s.appendWAL(&walRecord{
		Key:            staged.Key,
		Version:        staged.Version,
		Value:          staged.Value,
		LastEventID:    staged.LastEventID,
		LastAppliedIDs: staged.LastAppliedIDs,
	})
	s.leaveInflight()
	if werr != nil {
		return KeyState{}, ApplyOK, werr
	}

	st := staged
	sh.states[ev.Key] = &st
	s.noteApplied()
	return st, ApplyOK, nil
}

// enterInflightLocked 在已持有 shard 锁时登记在途（等待冻结解除）。
func (s *fileStore) enterInflightLocked() {
	s.inflightMu.Lock()
	for s.frozen {
		s.inflightCv.Wait()
	}
	s.inflight++
	s.inflightMu.Unlock()
}

// noteApplied 维护周期快照计数（仅 syncEach 模式运行期快照）。
func (s *fileStore) noteApplied() {
	if !s.syncEach {
		return
	}
	for {
		cur := s.applies.Load()
		if cur < snapshotEvery {
			if s.applies.CompareAndSwap(cur, cur+1) {
				return
			}
			continue
		}
		if s.applies.CompareAndSwap(cur, 0) {
			// 快照失败不影响本次已确认应用；Close 统一落盘兜底。
			_ = s.maybeSnapshot()
		}
		return
	}
}

// maybeSnapshot 尝试做一次周期快照；冻结失败（正有关闭流程）时跳过。
func (s *fileStore) maybeSnapshot() error {
	s.inflightMu.Lock()
	if s.frozen {
		s.inflightMu.Unlock()
		return nil
	}
	s.frozen = true
	s.inflightMu.Unlock()
	defer func() {
		s.inflightMu.Lock()
		s.frozen = false
		s.inflightCv.Broadcast() // 唤醒冻结期间等待登记在途的 appender
		s.inflightMu.Unlock()
	}()
	s.inflightMu.Lock()
	for s.inflight > 0 {
		s.inflightCv.Wait()
	}
	s.inflightMu.Unlock()
	return s.snapshotLocked()
}

func (s *fileStore) Get(ctx context.Context, key string) (KeyState, bool, error) {
	if err := ctx.Err(); err != nil {
		return KeyState{}, false, normalize("store.get", err)
	}
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st, ok := sh.states[key]
	if !ok {
		return KeyState{}, false, nil
	}
	return *st, true, nil
}

func (s *fileStore) Snapshot(ctx context.Context) (map[string]KeyState, error) {
	if err := ctx.Err(); err != nil {
		return nil, normalize("store.snapshot", err)
	}
	out := make(map[string]KeyState)
	for i := range s.shards {
		s.shards[i].mu.Lock()
		for k, v := range s.shards[i].states {
			out[k] = *v
		}
		s.shards[i].mu.Unlock()
	}
	return out, nil
}

// snapshotLocked 原子写出状态快照并截断 WAL（对外入口，自行管理 committer 锁）。
//
// 调用前置条件：调用方已冻结新在途 Apply 并等待存量在途归零（启动重放
// 阶段除外，此时 committer 未启动且无并发写入）。
func (s *fileStore) snapshotLocked() error {
	s.committerMu.Lock()
	defer s.committerMu.Unlock()
	return s.snapshotBody("store.snapshot")
}

// snapshotBody 执行快照主体；调用方必须持有 committerMu。
func (s *fileStore) snapshotBody(op string) error {
	// 停止提交协程并统一刷盘，接管 wal 句柄；之后缓冲中不存在游离字节。
	if err := s.stopCommitterLocked(true); err != nil {
		return err
	}
	restart := func(err error) error {
		// 关闭流程中不重启；正常快照（含启动重放）后恢复提交协程。
		if !s.closing && s.syncEach {
			s.startCommitter()
		}
		return err
	}

	snap := make(map[string]KeyState)
	for i := range s.shards {
		s.shards[i].mu.Lock()
		for k, v := range s.shards[i].states {
			snap[k] = *v
		}
		s.shards[i].mu.Unlock()
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return restart(newError(KindStorage, op, "encode snapshot: "+err.Error(), err))
	}

	s.walMu.Lock()
	defer s.walMu.Unlock()

	tmp := filepath.Join(s.dir, "state.snapshot.json.tmp")
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return restart(newError(KindStorage, op, "open tmp: "+err.Error(), err))
	}
	if _, err := tf.Write(data); err != nil {
		tf.Close()
		return restart(newError(KindStorage, op, "write tmp: "+err.Error(), err))
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return restart(newError(KindStorage, op, "sync tmp: "+err.Error(), err))
	}
	if err := tf.Close(); err != nil {
		return restart(newError(KindStorage, op, "close tmp: "+err.Error(), err))
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, "state.snapshot.json")); err != nil {
		return restart(newError(KindStorage, op, "rename: "+err.Error(), err))
	}
	if dir, err := os.Open(s.dir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	if err := s.wal.Truncate(0); err != nil {
		return restart(newError(KindStorage, op, "truncate wal: "+err.Error(), err))
	}
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return restart(newError(KindStorage, op, "seek: "+err.Error(), err))
	}
	if err := s.wal.Sync(); err != nil {
		return restart(newError(KindStorage, op, "sync wal: "+err.Error(), err))
	}
	s.walW.Reset(s.wal)
	// 代次计数器刻意保持单调、不随 WAL 截断而重置：进入快照前已冻结并
	// 等待在途归零，此刻不存在等待旧代次的 appender；重启后的提交协程
	// 继续分配更大的代次即可，避免“归零导致新写入误判已落盘”。
	return restart(nil)
}

func appendID(ids []string, id string) []string {
	ids = append(ids, id)
	if len(ids) > appliedIDWindow {
		ids = ids[len(ids)-appliedIDWindow:]
	}
	return ids
}

// Close 冻结存储、等待在途排空，然后在 committer 锁保护下执行最终快照：
// 停止提交协程、写出快照、截断 WAL（closing=true 保证提交协程不再重启）。
func (s *fileStore) Close() error {
	s.inflightMu.Lock()
	s.frozen = true
	s.inflightMu.Unlock()
	s.inflightMu.Lock()
	for s.inflight > 0 {
		s.inflightCv.Wait()
	}
	s.inflightMu.Unlock()

	s.committerMu.Lock()
	s.closing = true
	snapErr := s.snapshotBody("store.closeSnapshot")
	s.committerMu.Unlock()
	if err := s.wal.Close(); err != nil && snapErr == nil {
		snapErr = newError(KindStorage, "store.close", "close wal: "+err.Error(), err)
	}
	return snapErr
}
