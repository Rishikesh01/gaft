package persistence

import "github.com/Rishikesh01/gaft/pkg/rafttypes"

type Persistence interface {
	LastReadVoteState() (currentTerm int64, votedFor string, err error)
	SaveVoteState(currentTerm int64, votedFor string) error
	ReadLogs(startIndex int64, endIndex int64) ([]rafttypes.AppendLog, error)
	Append(log rafttypes.AppendLog) error
}
