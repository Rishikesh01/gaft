package node

import "github.com/Rishikesh01/gaft/pkg/rafttypes"

type Sender interface {
	// Used to replicate Log and as heartBeat by leader
	UpdatePeers(map[string]string)
	AppendEntries(memeber string, input rafttypes.AppendEntriesInput) (rafttypes.AppendEntiresResponse, error)
	/*
		Used by follower who pomoted himself to candidate, when he did not recieve heartbeat from leader within the randomized timeout
		randomized timeout to minimize majority of clusters members to become candiate at same time which can result in re-election as
		candidates can't vote for others
	*/
	RequestVote(memeber string, input rafttypes.RequestVoteInput) (rafttypes.RequestVoteResponse, error)
}
