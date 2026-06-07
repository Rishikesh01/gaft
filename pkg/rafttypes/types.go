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
	LastIndex     int64
	LastTerm      int64
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
