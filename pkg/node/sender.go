package node

import "github.com/Rishikesh01/gaft/pkg/persistence"

type SendMsg interface {
	// Used to replicate Log and as heartBeat by leader
	AppendEntries(input AppendEntriesInput) AppendEntiresResponse
	/*
		Used by follower who pomoted himself to candidate, when he did not recieve heartbeat from leader within the randomized timeout
		randomized timeout to minimize majority of clusters members to become candiate at same time which can result in re-election as
		candidates can't vote for others
	*/
	RequestVote(input RequestVoteInput) RequestVoteResponse
}

type AppendEntriesInput struct {
	Term         int64
	PrevLogIndex int64
	PrevLogTerm  int64
	LeaderCommit int64
	LeaderName   string
	Entries      []persistence.AppendLog
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
