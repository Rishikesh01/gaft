package rafttypes

type AppendEntriesInput struct {
	Term         int64
	PrevLogIndex int64
	PrevLogTerm  int64
	LeaderCommit int64
	LeaderName   string
	Entries      []AppendLog
}

type AppendEntiresResponse struct {
	Term    int64
	Success bool
}

type RequestVoteInput struct {
	Term          int64
	LastLogIndex  int64
	LastLogTerm   int64
	CandidateName string
}

type RequestVoteResponse struct {
	Term  int64
	Voted bool
}

type AppendLog struct {
	Index uint64
	Term  uint64
	Data  []byte
}

func (a *AppendLog) Size() int64 {
	return 8 + 8 + int64(len(a.Data))
}

type InstallSnapshotInput struct {
	Term              int64
	LeaderName        string
	LastIncludedIndex int64
	LastIncludedTerm  int64
	Offset            int64
	Data              []byte
	Done              bool
}

func (i *InstallSnapshotInput) Size() int64 {
	return int64(8 + 8 + 8 + 8 + 1 + len(i.LeaderName) + len(i.Data))
}

type InstallSnapshotResponse struct {
	Term int64
}
