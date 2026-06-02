package persistence

import "github.com/Rishikesh01/gaft/pkg/rafttypes"

type Persistence interface {
	ReadLogs(startIndex int64, endIndex int64) (rafttypes.AppendLog, error)
	// 0 index input means just append
	Append(index int64, log rafttypes.AppendLog) error
}
