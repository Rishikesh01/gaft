package node

type Persistence interface {
	ReadLogsFrom(index int64) (AppendLog, error)
	// 0 index input means just append
	AppendLog(index int64) error
}

type AppendLog struct {
	index int64
	term  int64
	data  []byte
}
