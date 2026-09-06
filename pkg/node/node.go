package node

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rishikesh01/gaft/pkg/persistence"
	"go.uber.org/zap"
)

type NodeRole string

const (
	RoleLeader    NodeRole = "Leader"
	RoleCandidate NodeRole = "Candidate"
	RoleFollower  NodeRole = "Follower"
	RoleLearner   NodeRole = "Learner"
)

type ClusterNode struct {
	mu sync.Mutex
	// identity of the node
	nodeName string
	// name and address
	clusterMembers map[string]string
	currentRole    atomic.Pointer[NodeRole]

	leaderName  string
	currentTerm atomic.Int64

	heartBeatTimeout time.Duration

	lastAppliedIndex   int64
	lastCommittedIndex atomic.Int64

	nextIndexs atomic.Int64
	// member name
	votedFor string
	log      zap.SugaredLogger

	transport Sender
	snapshot  Snapshot
	persist   persistence.Persistence
}

func NewClusterNode(nodeName string, log zap.SugaredLogger) *ClusterNode {
	node := &ClusterNode{log: log, nodeName: nodeName}
	node.nextIndexs.Store(1)
	return node
}
