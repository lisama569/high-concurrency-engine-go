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
	mu      sync.Mutex
	path    string
	f       *os.File
	w       *bufio.Writer
	pending map[string]Event // id -> 事件，同时保留首次出现顺序于 order
	order   []string
	closed  bool
}

// openJournal 打开/创建待处理日志并重放已有内容到内存索引。
func openJournal(path string) (Journal, error) {
	const op = "journal.open"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, newError(KindStorage, op, "mkdir: "+err.Error(), err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, newError(KindStorage, op, "open: "+err.Error(), err)
	}
	j := &fileJournal{
		path:    path,
		f:       f,
		w:       bufio.NewWriter(f),
		pending: make(map[string]Event),
	}
	if err := j.load(); err != nil {
		return nil, err
	}
	// 把追加位置移动到文件尾。
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return nil, newError(KindStorage, op, "seek end: "+err.Error(), err)
	}
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

func (j *fileJournal) writeLine(l journalLine) error {
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
	if err := j.w.Flush(); err != nil {
		return newError(KindStorage, op, "flush: "+err.Error(), err)
	}
	if err := j.f.Sync(); err != nil {
		return newError(KindStorage, op, "sync: "+err.Error(), err)
	}
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
	if err := j.writeLine(journalLine{Type: "add", Event: &ev}); err != nil {
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
	if err := j.writeLine(journalLine{Type: "ack", ID: eventID}); err != nil {
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
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	var firstErr error
	if err := j.compactLocked(); err != nil {
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
