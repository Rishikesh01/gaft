package node

import "github.com/Rishikesh01/gaft/pkg/rafttypes"

type Handler interface {
	// Used to replicate Log and as heartBeat by leader
	AppendEntries(input rafttypes.AppendEntriesInput) (*rafttypes.AppendEntiresResponse, error)
	/*
		Used by follower who pomoted himself to candidate, when he did not recieve heartbeat from leader within the randomized timeout
		randomized timeout to minimize majority of clusters members to become candiate at same time which can result in re-election as
		candidates can't vote for others
	*/
	RequestVote(input rafttypes.RequestVoteInput) (*rafttypes.RequestVoteResponse, error)
}
