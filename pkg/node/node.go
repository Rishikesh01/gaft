package node

import (
	"sync"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
	"go.uber.org/zap"
)

type NodeRole string

const (
	RoleLeader    NodeRole = "Leader"
	RoleCandidate NodeRole = "Candidate"
	RoleFollower  NodeRole = "Follower"
)

type ClusterNode struct {
	mu sync.Mutex
	// identity of the node
	nodeName string
	// name and address
	clusterMembers map[string]string
	currentRole    string

	leaderName  string
	currentTerm int64

	lastAppliedIndex   int64
	lastCommittedIndex int64

	nextIndexs int64
	appendLogs []rafttypes.AppendLog
	// member name
	votedFor string
	log      zap.SugaredLogger

	leaderMode *leaderMode

	transport Sender
}

type leaderMode struct {
	followerState map[string]followerState
}

type followerState struct {
	matchIndex int64
	nextIndx   int64
}

var _ Handler = (*ClusterNode)(nil)

func NewClusterNode(nodeName string, log zap.SugaredLogger) *ClusterNode {
	return &ClusterNode{log: log, nodeName: nodeName}
}

// AppendEntries implements [Handler].
func (c *ClusterNode) AppendEntries(input rafttypes.AppendEntriesInput) (*rafttypes.AppendEntiresResponse, error) {
	panic("unimplemented")
}

// RequestVote implements [Handler].
func (c *ClusterNode) RequestVote(input rafttypes.RequestVoteInput) (*rafttypes.RequestVoteResponse, error) {
	panic("unimplemented")
}
