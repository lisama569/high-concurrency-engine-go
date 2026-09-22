package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// EventSource 是事件来源抽象。示例实现从本地追加式日志文件读取事件，
// 并在独立的偏移量文件中记录已投递位置，重启后从断点续读。
//
// 投递语义：只有 sink 成功接收（事件已进入引擎持久日志）后才推进并持久化
// 偏移；sink 拒绝、停机或崩溃都不推进偏移，重启后重投，配合引擎侧的幂等
// 判重实现“至少一次 + 不重复生效”。
type EventSource interface {
	// Stream 将事件持续投递到 sink；sink 返回非空错误时停止投递并返回。
	// ctx 取消时返回归一化的 KindCanceled/KindTimeout。读到文件尾且没有
	// 新数据时按轮询间隔等待，模拟实时事件流。
	Stream(ctx context.Context, sink func(Event) *Error) *Error
	// Append 往事件源尾部追加一个事件（用于演示与测试造数）。
	Append(ev Event) error
	// Close 关闭事件源。
	Close() error
}

// sourcePollInterval 是文件尾无新数据时的轮询间隔。
const sourcePollInterval = 2 * time.Millisecond

type fileSource struct {
	mu         sync.Mutex
	eventPath  string
	offsetPath string
	rf         *os.File // 只读句柄（读偏移独立）
	wf         *os.File // 只写句柄（始终追加到文件尾）
	rd         *bufio.Reader
	offset     int64 // 已成功投递到 sink 的字节偏移
	closed     bool
}

// openFileSource 打开/创建基于本地文件的事件源，并恢复已记录偏移。
func openFileSource(eventFile, offsetFile string) (EventSource, error) {
	const op = "source.open"
	if err := os.MkdirAll(filepath.Dir(eventFile), 0o755); err != nil {
		return nil, newError(KindStorage, op, "mkdir: "+err.Error(), err)
	}
	rf, err := os.OpenFile(eventFile, os.O_CREATE|os.O_RDONLY, 0o644)
	if err != nil {
		return nil, newError(KindStorage, op, "open events read: "+err.Error(), err)
	}
	wf, err := os.OpenFile(eventFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		_ = rf.Close()
		return nil, newError(KindStorage, op, "open events write: "+err.Error(), err)
	}
	s := &fileSource{
		eventPath:  eventFile,
		offsetPath: offsetFile,
		rf:         rf,
		wf:         wf,
		rd:         bufio.NewReaderSize(rf, 64*1024),
	}
	if err := s.loadOffset(); err != nil {
		return nil, err
	}
	if _, err := rf.Seek(s.offset, io.SeekStart); err != nil {
		return nil, newError(KindStorage, op, "seek offset: "+err.Error(), err)
	}
	return s, nil
}

func (s *fileSource) loadOffset() error {
	const op = "source.loadOffset"
	data, err := os.ReadFile(s.offsetPath)
	if errors.Is(err, os.ErrNotExist) {
		s.offset = 0
		return nil
	}
	if err != nil {
		return newError(KindStorage, op, "read offset: "+err.Error(), err)
	}
	off, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		// 损坏的偏移文件不静默归零（归零会导致整条流重放，虽然幂等但可能
		// 掩盖故障）；交回存储类错误让调用方决策。
		return newError(KindStorage, op, "parse offset: "+err.Error(), err)
	}
	s.offset = off
	return nil
}

// persistOffset 原子写入当前偏移量（临时文件 + rename）。
func (s *fileSource) persistOffset() error {
	const op = "source.persistOffset"
	tmp := s.offsetPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(s.offset, 10)), 0o644); err != nil {
		return newError(KindStorage, op, "write offset tmp: "+err.Error(), err)
	}
	if err := os.Rename(tmp, s.offsetPath); err != nil {
		return newError(KindStorage, op, "rename offset: "+err.Error(), err)
	}
	return nil
}

// Append 以逐行 JSON 的形式追加事件并 fsync。
func (s *fileSource) Append(ev Event) error {
	const op = "source.append"
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return newError(KindStorage, op, "source closed", nil)
	}
	data, err := json.Marshal(&ev)
	if err != nil {
		return newError(KindStorage, op, "encode: "+err.Error(), err)
	}
	if _, err := s.wf.Write(append(data, '\n')); err != nil {
		return newError(KindStorage, op, "write: "+err.Error(), err)
	}
	if err := s.wf.Sync(); err != nil {
		return newError(KindStorage, op, "sync: "+err.Error(), err)
	}
	return nil
}

// Stream 见 EventSource 接口注释。
func (s *fileSource) Stream(ctx context.Context, sink func(Event) *Error) *Error {
	const op = "source.stream"
	for {
		if err := ctx.Err(); err != nil {
			return normalize(op, err)
		}
		line, rerr := s.rd.ReadBytes('\n')
		if len(line) > 0 {
			if line[len(line)-1] == '\n' {
				perr := s.deliver(ctx, line, sink)
				if perr != nil {
					return perr
				}
				continue
			}
			// 读到不完整行（追加者写了一半或尚未 flush）：回到已确认偏移
			// 重读。本引擎的 Append 是单次 write+fsync 一行，正常情况下
			// 只会在整行可见后读到，这里是防御性处理。
			if serr := s.seekToOffsetLocked(); serr != nil {
				return serr
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				if werr := s.waitForMore(ctx); werr != nil {
					return werr
				}
				continue
			}
			return newError(KindStorage, op, "read: "+rerr.Error(), rerr)
		}
	}
}

// seekToOffsetLocked 把读取位置重置为已确认偏移（调用语义：Stream 单读协程）。
func (s *fileSource) seekToOffsetLocked() *Error {
	const op = "source.seekToOffset"
	s.rd.Reset(s.rf)
	if _, err := s.rf.Seek(s.offset, io.SeekStart); err != nil {
		return newError(KindStorage, op, "seek: "+err.Error(), err)
	}
	return nil
}

// deliver 解析、投递一行事件，并在成功后推进、持久化偏移。
func (s *fileSource) deliver(ctx context.Context, line []byte, sink func(Event) *Error) *Error {
	const op = "source.deliver"
	var ev Event
	if err := json.Unmarshal(line[:len(line)-1], &ev); err != nil {
		return newError(KindStorage, op, "decode event line: "+err.Error(), err)
	}
	if err := ctx.Err(); err != nil {
		return normalize(op, err)
	}
	if perr := sink(ev); perr != nil {
		// 不推进偏移：事件尚未被引擎可靠接收，重启后重投（幂等兜底）。
		return perr
	}
	s.mu.Lock()
	s.offset += int64(len(line))
	perr := s.persistOffset()
	s.mu.Unlock()
	if perr != nil {
		return normalize(op, perr)
	}
	return nil
}

// waitForMore 在文件尾轮询等待新数据。
func (s *fileSource) waitForMore(ctx context.Context) *Error {
	const op = "source.waitForMore"
	t := time.NewTimer(sourcePollInterval)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return normalize(op, ctx.Err())
	case <-t.C:
		return nil
	}
}

func (s *fileSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if err := s.rf.Close(); err != nil {
		_ = s.wf.Close()
		return newError(KindStorage, "source.close", "close read: "+err.Error(), err)
	}
	if err := s.wf.Close(); err != nil {
		return newError(KindStorage, "source.close", "close write: "+err.Error(), err)
	}
	return nil
}
