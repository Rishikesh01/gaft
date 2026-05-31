package persistence

type AppendLog struct {
	index int64
	term  int64
	data  []byte
}
