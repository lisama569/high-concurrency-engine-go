package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Journal 是“待处理事件日志”（持久化有界队列的落盘部分）。
//
// 事件在进入内存有界队列前先被追加（Append），处理成功后确认（Ack）。
// 停机时仍未 Ack 的事件会在下次启动时通过 Replay 重放；由于状态写入具备
// 幂等判重，重放不会造成重复应用。
//
// 文件布局（逐行 JSON，单文件）：
//
//	{"type":"add","event":{...}}
//	{"type":"ack","id":"..."}
//
// Close 时按当前未完成集合压缩重写，避免日志无限增长。
type Journal interface {
	// Append 持久化一个待处理事件。
	Append(ctx context.Context, ev Event) error
	// Ack 标记一个事件已处理完成。
	Ack(ctx context.Context, eventID string) error
	// Replay 返回所有尚未 Ack 的事件（保持追加顺序）。
	Replay(ctx context.Context) ([]Event, error)
	// Pending 返回尚未 Ack 的事件数量。
	Pending() int
	// Close 重写压缩日志并关闭。
	Close() error
}

type journalLine struct {
	Type  string `json:"type"` // "add" 或 "ack"
	Event *Event `json:"event,omitempty"`
	ID    string `json:"id,omitempty"`
}

type fileJournal struct {
	mu       sync.Mutex
	path     string
	f        *os.File
	w        *bufio.Writer
	pending  map[string]Event // id -> 事件，同时保留首次出现顺序于 order
	order    []string
	closed   bool
	syncEach bool

	// 组提交：dirty 表示缓冲有未落盘字节；commit 协程按固定节拍一次
	// flush+fsync 覆盖这一节拍内的所有 add/ack。
	commitStop chan struct{}
	commitDone chan struct{}
	dirtyMu    sync.Mutex
	dirty      bool
}

// journalCommitInterval 是组提交刷盘节拍。
const journalCommitInterval = 2 * 1000 * 1000 // 2ms（纳秒）

// openJournal 以每行 fsync 的强持久化级别打开待处理日志。
func openJournal(path string) (Journal, error) {
	return openJournalWith(path, true)
}

// openJournalWith 打开待处理日志。syncEach=false 时采用组提交（后台节拍
// 刷盘），吞吐显著提高；进程崩溃可能丢失最近一个节拍内的待处理记录，但
// 这些事件通常仍停留在事件源 offset 之前，重启会重新投递（幂等兜底）。
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
		path:       path,
		f:          f,
		w:          bufio.NewWriterSize(f, 64*1024),
		pending:    make(map[string]Event),
		syncEach:   syncEach,
		commitStop: make(chan struct{}),
		commitDone: make(chan struct{}),
	}
	if err := j.load(); err != nil {
		return nil, err
	}
	// 把追加位置移动到文件尾。
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return nil, newError(KindStorage, op, "seek end: "+err.Error(), err)
	}
	if !syncEach {
		go j.runCommit()
	}
	return j, nil
}

// runCommit 周期性把脏缓冲 flush+fsync（组提交）。
func (j *fileJournal) runCommit() {
	defer close(j.commitDone)
	t := time.NewTicker(time.Duration(journalCommitInterval))
	defer t.Stop()
	for {
		select {
		case <-j.commitStop:
			return
		case <-t.C:
			j.dirtyMu.Lock()
			dirty := j.dirty
			j.dirtyMu.Unlock()
			if !dirty {
				continue
			}
			j.mu.Lock()
			_ = j.w.Flush()
			_ = j.f.Sync()
			j.mu.Unlock()
			j.dirtyMu.Lock()
			j.dirty = false
			j.dirtyMu.Unlock()
		}
	}
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
	// 重建 order，剔除已 Ack 的条目。
	order := j.order[:0]
	for _, id := range j.order {
		if _, ok := j.pending[id]; ok {
			order = append(order, id)
		}
	}
	j.order = order
	return nil
}

// writeLineLocked 在已持有 j.mu 时写入一行；按持久化级别决定是否立即
// flush+fsync，否则只标记脏，由 runCommit 组提交。
func (j *fileJournal) writeLineLocked(l journalLine) error {
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
	if j.syncEach {
		if err := j.w.Flush(); err != nil {
			return newError(KindStorage, op, "flush: "+err.Error(), err)
		}
		if err := j.f.Sync(); err != nil {
			return newError(KindStorage, op, "sync: "+err.Error(), err)
		}
		return nil
	}
	// 组提交：标记脏即可，节拍协程负责落盘。
	j.dirtyMu.Lock()
	j.dirty = true
	j.dirtyMu.Unlock()
	return nil
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
	if err := j.writeLineLocked(journalLine{Type: "add", Event: &ev}); err != nil {
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
	if err := j.writeLineLocked(journalLine{Type: "ack", ID: eventID}); err != nil {
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

// compactLocked 把当前未完成集合压缩重写到临时文件并原子替换。
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
	// 组提交模式：先停止节拍协程，并把残留缓冲强制落盘，保证优雅停机不
	// 丢最近一个节拍内的 add/ack。
	if !j.syncEach {
		close(j.commitStop)
		<-j.commitDone
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	var firstErr error
	if err := j.w.Flush(); err != nil && firstErr == nil {
		firstErr = newError(KindStorage, "journal.close", "flush: "+err.Error(), err)
	}
	if err := j.f.Sync(); err != nil && firstErr == nil {
		firstErr = newError(KindStorage, "journal.close", "sync: "+err.Error(), err)
	}
	if err := j.compactLocked(); err != nil && firstErr == nil {
		firstErr = err
	}
	// 压缩已用新文件替换旧文件，旧句柄直接关闭即可。
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

type fileDeadLetter struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// openDeadLetter 打开/创建死信文件（追加式逐行 JSON）。
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
		reason = newError(KindUnknown, op, "unspecified failure", errors.New("nil reason"))
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
