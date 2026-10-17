package persistence

const (
	fileVoteState = "vote_state.gaft"
	fileSnapshot  = "snapshot.gaft"
	logDirName    = "log"
)

var segmentEntries int64 = 100
