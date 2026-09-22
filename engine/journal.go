package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Journal 是“待处理事件日志”（持久化有界队列的落盘部分）。
//
// 事件在进入内存有界队列前先被追加（Append），处理成功后确认（Ack）。
// 停机时仍未 Ack 的事件会在下次启动通过 Replay 重放；存储层幂等保证重放
// 不会重复应用。文件为逐行 JSON（add / ack），Close 时按未完成集合压缩。
type Journal interface {
	Append(ctx context.Context, ev Event) error
	Ack(ctx context.Context, eventID string) error
	// AckAsync 把确认写入日志缓冲并由组提交异步落盘后立即返回。Ack 丢失
	// 的唯一后果是重启时该事件被多做一次（存储层幂等会判重跳过），因此可
	// 安全异步，用于消除处理热路径上的第二次 fsync。
	AckAsync(eventID string) error
	Replay(ctx context.Context) ([]Event, error)
	Pending() int
	Close() error
}

type journalLine struct {
	Type  string `json:"type"`
	Event *Event `json:"event,omitempty"`
	ID    string `json:"id,omitempty"`
}

// journalCommitInterval 是强持久化模式下组提交的聚合窗口。窗口内的所有
// add/ack 由同一次 flush+fsync 覆盖；高并发下 fsync 次数从“每事件一次”
// 降到“每窗口一次”，而每个调用仍在自己那批落盘后才返回。
const journalCommitInterval = 500 * time.Microsecond

type fileJournal struct {
	mu      sync.Mutex // 保护缓冲写入、gen、内存索引与压缩
	path    string
	f       *os.File
	w       *bufio.Writer
	pending map[string]Event
	order   []string
	closed  bool
	durable bool // Append/Ack 是否等待自己那批 fsync 完成

	gen        uint64 // 已分配的最大代次
	flushedGen uint64 // 已 flush+fsync 覆盖的最大代次
	flushMu    sync.Mutex
	flushCv    *sync.Cond
	flushErr   error

	reqCh         chan struct{}
	committerStop chan struct{}
	committerDone chan struct{}
}

func openJournal(path string) (Journal, error) {
	return openJournalWith(path, true)
}

// openJournalWith 打开待处理日志。两种持久化级别都使用组提交协程，区别
// 仅在于 Append/Ack 是否等待自己那批被 fsync：
//   - syncEach=true：等待落盘后才返回（崩溃不丢已确认写入）；
//   - syncEach=false：写入缓冲即返回（崩溃可能丢失最近窗口内记录，事件
//     源 offset 未推进，重启重新投递兜底）。
func openJournalWith(path string, syncEach bool) (Journal, error) {
	const op = "journal.open"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, newError(KindStorage, op, "mkdir: "+err.Error(), err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, newError(KindStorage, op, "open: "+err.Error(), err)
	}
	j := &fileJournal{
		path:          path,
		f:             f,
		w:             bufio.NewWriterSize(f, 64*1024),
		pending:       make(map[string]Event),
		reqCh:         make(chan struct{}, 1),
		committerStop: make(chan struct{}),
		committerDone: make(chan struct{}),
	}
	j.flushCv = sync.NewCond(&j.flushMu)
	j.durable = syncEach
	if err := j.load(); err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return nil, newError(KindStorage, op, "seek end: "+err.Error(), err)
	}
	go j.runCommitter()
	return j, nil
}

func (j *fileJournal) load() error {
	const op = "journal.load"
	if _, err := j.f.Seek(0, 0); err != nil {
		return newError(KindStorage, op, "seek start: "+err.Error(), err)
	}
	sc := bufio.NewScanner(j.f)
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var l journalLine
		if err := json.Unmarshal(line, &l); err != nil {
			return newError(KindStorage, op, "decode line: "+err.Error(), err)
		}
		switch l.Type {
		case "add":
			if l.Event != nil {
				if _, ok := j.pending[l.Event.ID]; !ok {
					j.order = append(j.order, l.Event.ID)
				}
				j.pending[l.Event.ID] = *l.Event
			}
		case "ack":
			if _, ok := j.pending[l.ID]; ok {
				delete(j.pending, l.ID)
			}
		}
	}
	if err := sc.Err(); err != nil {
		return newError(KindStorage, op, "scan: "+err.Error(), err)
	}
	order := j.order[:0]
	for _, id := range j.order {
		if _, ok := j.pending[id]; ok {
			order = append(order, id)
		}
	}
	j.order = order
	return nil
}

// runCommitter 是唯一执行 flush+fsync 的协程。
func (j *fileJournal) runCommitter() {
	defer close(j.committerDone)
	for {
		select {
		case <-j.committerStop:
			return
		case <-j.reqCh:
		}
		select {
		case <-j.committerStop:
			return
		case <-time.After(journalCommitInterval):
		}
		j.mu.Lock()
		ferr := j.w.Flush()
		var serr error
		if ferr == nil {
			serr = j.f.Sync()
		}
		gen := j.gen
		j.mu.Unlock()

		j.flushMu.Lock()
		switch {
		case ferr != nil:
			j.flushErr = newError(KindStorage, "journal.commit", "flush: "+ferr.Error(), ferr)
		case serr != nil:
			j.flushErr = newError(KindStorage, "journal.commit", "sync: "+serr.Error(), serr)
		default:
			j.flushErr = nil
			j.flushedGen = gen
		}
		j.flushCv.Broadcast()
		j.flushMu.Unlock()
	}
}

// writeLocked 在持有 j.mu 时写入一行、分配代次并按 durableWait 决定是否
// 等待自己那批 fsync 完成。
func (j *fileJournal) writeLocked(l journalLine, durableWait bool) error {
	const op = "journal.write"
	data, err := json.Marshal(&l)
	if err != nil {
		return newError(KindStorage, op, "encode: "+err.Error(), err)
	}
	if _, err := j.w.Write(data); err != nil {
		return newError(KindStorage, op, "write: "+err.Error(), err)
	}
	if err := j.w.WriteByte('\n'); err != nil {
		return newError(KindStorage, op, "write: "+err.Error(), err)
	}
	j.gen++
	myGen := j.gen
	select {
	case j.reqCh <- struct{}{}:
	default:
	}

	if !durableWait {
		return nil // 异步：交给 committer 稍后落盘
	}

	j.mu.Unlock()
	j.flushMu.Lock()
	for j.flushedGen < myGen && j.flushErr == nil {
		j.flushCv.Wait()
	}
	err = j.flushErr
	j.flushMu.Unlock()
	j.mu.Lock()
	return err
}

func (j *fileJournal) Append(ctx context.Context, ev Event) error {
	const op = "journal.append"
	if err := ctx.Err(); err != nil {
		return normalize(op, err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return newError(KindStorage, op, "journal closed", nil)
	}
	if err := j.writeLocked(journalLine{Type: "add", Event: &ev}, j.durable); err != nil {
		return err
	}
	if _, ok := j.pending[ev.ID]; !ok {
		j.order = append(j.order, ev.ID)
	}
	j.pending[ev.ID] = ev
	return nil
}

func (j *fileJournal) Ack(ctx context.Context, eventID string) error {
	const op = "journal.ack"
	if err := ctx.Err(); err != nil {
		return normalize(op, err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return newError(KindStorage, op, "journal closed", nil)
	}
	if err := j.writeLocked(journalLine{Type: "ack", ID: eventID}, j.durable); err != nil {
		return err
	}
	delete(j.pending, eventID)
	return nil
}

func (j *fileJournal) AckAsync(eventID string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return newError(KindStorage, "journal.ackAsync", "journal closed", nil)
	}
	if err := j.writeLocked(journalLine{Type: "ack", ID: eventID}, false); err != nil {
		return err
	}
	delete(j.pending, eventID)
	return nil
}

func (j *fileJournal) Replay(ctx context.Context) ([]Event, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]Event, 0, len(j.order))
	for _, id := range j.order {
		if ev, ok := j.pending[id]; ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

func (j *fileJournal) Pending() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.pending)
}

// compactLocked 把当前未完成集合压缩重写到临时文件并原子替换（持 j.mu）。
func (j *fileJournal) compactLocked() error {
	const op = "journal.compact"
	tmp := j.path + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return newError(KindStorage, op, "open tmp: "+err.Error(), err)
	}
	bw := bufio.NewWriter(tf)
	for _, id := range j.order {
		ev, ok := j.pending[id]
		if !ok {
			continue
		}
		data, err := json.Marshal(&journalLine{Type: "add", Event: &ev})
		if err != nil {
			tf.Close()
			return newError(KindStorage, op, "encode: "+err.Error(), err)
		}
		if _, err := bw.Write(data); err != nil {
			tf.Close()
			return newError(KindStorage, op, "write tmp: "+err.Error(), err)
		}
		if err := bw.WriteByte('\n'); err != nil {
			tf.Close()
			return newError(KindStorage, op, "write tmp: "+err.Error(), err)
		}
	}
	if err := bw.Flush(); err != nil {
		tf.Close()
		return newError(KindStorage, op, "flush tmp: "+err.Error(), err)
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return newError(KindStorage, op, "sync tmp: "+err.Error(), err)
	}
	if err := tf.Close(); err != nil {
		return newError(KindStorage, op, "close tmp: "+err.Error(), err)
	}
	if err := os.Rename(tmp, j.path); err != nil {
		return newError(KindStorage, op, "rename: "+err.Error(), err)
	}
	if dir, err := os.Open(filepath.Dir(j.path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func (j *fileJournal) Close() error {
	// 停止提交协程并强制把残留缓冲落盘。
	close(j.committerStop)
	<-j.committerDone

	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	var firstErr error
	if err := j.w.Flush(); err == nil {
		if serr := j.f.Sync(); serr != nil {
			firstErr = newError(KindStorage, "journal.close", "sync: "+serr.Error(), serr)
		}
	} else {
		firstErr = newError(KindStorage, "journal.close", "flush: "+err.Error(), err)
	}
	if err := j.compactLocked(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := j.f.Close(); err != nil && firstErr == nil {
		firstErr = newError(KindStorage, "journal.close", "close: "+err.Error(), err)
	}
	return firstErr
}

// DeadLetter 记录被最终放弃的事件及其归一化错误原因，保证失败可解释、
// 不会被静默丢弃。
type DeadLetter interface {
	Write(ev Event, reason *Error) error
	Close() error
}

type dlqLine struct {
	Event     Event  `json:"event"`
	Kind      string `json:"kind"`
	Op        string `json:"op"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// fileDeadLetter 是追加式逐行 JSON 死信文件。写入量很小（仅最终失败的
// 事件），直接逐次 flush+fsync 以保证失败记录不丢。
type fileDeadLetter struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

func openDeadLetter(path string) (DeadLetter, error) {
	const op = "dlq.open"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, newError(KindStorage, op, "mkdir: "+err.Error(), err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, newError(KindStorage, op, "open: "+err.Error(), err)
	}
	return &fileDeadLetter{f: f, w: bufio.NewWriter(f)}, nil
}

func (d *fileDeadLetter) Write(ev Event, reason *Error) error {
	const op = "dlq.write"
	if reason == nil {
		reason = newError(KindUnknown, op, "unspecified failure", nil)
	}
	line := dlqLine{
		Event:     ev,
		Kind:      reason.Kind.String(),
		Op:        reason.Op,
		Message:   reason.Message,
		Retryable: reason.Kind.Retryable(),
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	data, err := json.Marshal(&line)
	if err != nil {
		return newError(KindStorage, op, "encode: "+err.Error(), err)
	}
	if _, err := d.w.Write(data); err != nil {
		return newError(KindStorage, op, "write: "+err.Error(), err)
	}
	if err := d.w.WriteByte('\n'); err != nil {
		return newError(KindStorage, op, "write: "+err.Error(), err)
	}
	if err := d.w.Flush(); err != nil {
		return newError(KindStorage, op, "flush: "+err.Error(), err)
	}
	return d.f.Sync()
}

func (d *fileDeadLetter) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.w.Flush(); err != nil {
		return newError(KindStorage, "dlq.close", "flush: "+err.Error(), err)
	}
	return d.f.Close()
}
