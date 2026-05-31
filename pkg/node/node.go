package node

import (
	"sync"

	"github.com/Rishikesh01/gaft/pkg/persistence"
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
	appendLogs []persistence.AppendLog
	// member name
	votedFor string
	log      zap.SugaredLogger

	leaderMode *leaderMode
}

type leaderMode struct {
	followerState map[string]followerState
}

type followerState struct {
	matchIndex int64
	nextIndx   int64
}

func NewClusterNode(nodeName string, log zap.SugaredLogger) *ClusterNode {
	return &ClusterNode{log: log, nodeName: nodeName}
}
