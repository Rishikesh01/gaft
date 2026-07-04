package node

type Snapshot interface {
	Restore(name string) error
	Compact(tillIndex int64) error
}
