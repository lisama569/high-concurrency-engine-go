// Package store 提供基于本地文件的键状态存储，保证幂等与重启不丢状态。
package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Record 是单个业务键的持久化状态。
type Record struct {
	Key       string   `json:"key"`
	Seq       uint64   `json:"seq"`       // 已生效的最大键内序号
	Value     string   `json:"value"`     // 最近一次生效的载荷
	Applied   []string `json:"applied"`   // 已应用的事件 ID（幂等去重）
	Conflict  bool     `json:"conflict"`  // 是否被标记为冲突
	Conflicts []string `json:"conflicts"` // 冲突事件 ID 列表
}

// Store 是状态存储接口。
type Store interface {
	// Apply 幂等地应用事件：重复 ID 不重复应用，乱序标记冲突。
	Apply(key string, seq uint64, eventID, value string) (applied bool, conflict bool, err error)
	// Get 读取键状态，不存在时返回零值 Record。
	Get(key string) (Record, error)
	// Close 落盘并释放资源。
	Close() error
}

// FileStore 以单个 JSON 文件持久化全部键状态。
// 每次 Apply 成功后整体落盘（写临时文件 + rename + fsync），
// 保证崩溃时要么看到旧状态、要么看到新状态，不出现半写文件。
type FileStore struct {
	mu   sync.Mutex
	path string
	data map[string]Record
}

// Open 加载或创建位于 path 的存储文件。
func Open(path string) (*FileStore, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("store: mkdir %s: %w", dir, err)
		}
	}
	s := &FileStore{path: path, data: map[string]Record{}}
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(b, &s.data); err != nil {
			return nil, fmt.Errorf("store: decode %s: %w", path, err)
		}
	case os.IsNotExist(err):
		// 首次启动，空状态。
	default:
		return nil, fmt.Errorf("store: read %s: %w", path, err)
	}
	return s, nil
}

// Apply 应用一条事件，规则：
//   - 事件 ID 已应用过：幂等命中，applied=false，不产生副作用；
//   - seq <= 当前已生效 Seq：乱序/过期事件，标记冲突，不覆盖状态；
//   - 否则生效并落盘。
func (s *FileStore) Apply(key string, seq uint64, eventID, value string) (bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur := s.data[key]
	if cur.Key == "" {
		cur = Record{Key: key}
	}
	for _, id := range cur.Applied {
		if id == eventID {
			return false, false, nil
		}
	}
	if cur.Seq > 0 && seq <= cur.Seq {
		cur.Conflict = true
		cur.Conflicts = append(cur.Conflicts, eventID)
		s.data[key] = cur
		if err := s.flushLocked(); err != nil {
			return false, true, err
		}
		return false, true, nil
	}
	cur.Seq = seq
	cur.Value = value
	cur.Applied = append(cur.Applied, eventID)
	s.data[key] = cur
	if err := s.flushLocked(); err != nil {
		return false, false, err
	}
	return true, false, nil
}

// Get 读取键状态。
func (s *FileStore) Get(key string) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[key], nil
}

// Close 落盘并释放资源。
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

// flushLocked 原子落盘：写临时文件 -> fsync -> rename。
func (s *FileStore) flushLocked() error {
	b, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("store: encode: %w", err)
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("store: open tmp: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return fmt.Errorf("store: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("store: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("store: close tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("store: rename: %w", err)
	}
	return nil
}
