package engine

import (
	"runtime"
	"time"
)

// Config 是引擎的全部可配置项。
type Config struct {
	// DataDir 本地状态、事件源、待处理日志与死信文件所在目录。
	DataDir string
	// Workers 并发 worker 数（每个 worker 负责一个键分片）。
	// 也是队列分片数；不同业务键在不同 worker 上并行，同一业务键串行。
	Workers int
	// QueuePerShard 每个键分片有界队列的容量。
	QueuePerShard int
	// MailboxPerKey 每个业务键串行协调协程的 mailbox 容量。同键突发事件先
	// 进入 mailbox；当某键处理能力不足导致其分片队列与 mailbox 都满时，
	// Submit 在 SubmitTimeout 后以 KindQueueFull 明确拒绝（背压）。
	MailboxPerKey int
	// SubmitTimeout Submit 在队列满时愿意阻塞等待的最长时间；
	// 超过后以 KindQueueFull 明确拒绝。0 表示不等待，立即拒绝。
	SubmitTimeout time.Duration
	// HandleTimeout 单个事件单次处理的截止时间；超时归一化为 KindTimeout。
	HandleTimeout time.Duration
	// MaxAttempts 单个事件的最大尝试次数（1 表示不重试）。
	MaxAttempts int
	// Backoff 失败重试之间的基础退避时间（第 n 次重试退避 n*Backoff）。
	Backoff time.Duration
	// ShutdownTimeout 优雅停机最长等待时间；超时后未完成事件安全重排。
	ShutdownTimeout time.Duration
	// SyncEachWrite 为 true（默认）时，每次状态应用都通过组提交 fsync，
	// 保证操作系统崩溃也不丢已确认状态。false 时仅在优雅停机时统一落盘，
	// 吞吐显著更高，但断电/OS 崩溃可能丢失最近未落盘的状态（进程崩溃与
	// 重启重放的幂等一致性不受影响）。
	SyncEachWrite bool
}

// DefaultConfig 返回一份带默认值的配置。
func DefaultConfig() Config {
	return Config{
		DataDir:         "data",
		Workers:         runtime.NumCPU(),
		QueuePerShard:   1024,
		MailboxPerKey:   256,
		SubmitTimeout:   50 * time.Millisecond,
		HandleTimeout:   time.Second,
		MaxAttempts:     3,
		Backoff:         5 * time.Millisecond,
		ShutdownTimeout: 5 * time.Second,
		SyncEachWrite:   true,
	}
}

// Validate 检查配置合法性并就地填充缺省值。
func (c *Config) Validate() error {
	def := DefaultConfig()
	if c.DataDir == "" {
		c.DataDir = def.DataDir
	}
	if c.Workers <= 0 {
		c.Workers = def.Workers
	}
	if c.QueuePerShard <= 0 {
		c.QueuePerShard = def.QueuePerShard
	}
	if c.MailboxPerKey <= 0 {
		c.MailboxPerKey = def.MailboxPerKey
	}
	if c.SubmitTimeout < 0 {
		return newError(KindInvalid, "config.validate", "SubmitTimeout must be >= 0", nil)
	}
	if c.HandleTimeout <= 0 {
		c.HandleTimeout = def.HandleTimeout
	}
	if c.MaxAttempts < 1 {
		c.MaxAttempts = def.MaxAttempts
	}
	if c.Backoff < 0 {
		return newError(KindInvalid, "config.validate", "Backoff must be >= 0", nil)
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = def.ShutdownTimeout
	}
	return nil
}
