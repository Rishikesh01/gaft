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

const (
	logTypeApplication = "application"
	logTypeRaftCluster = "raft_cluster"
)

type ClusterNode struct {
	mu sync.Mutex
	// identity of the node
	nodeName string
	// name and address
	currentRole    atomic.Pointer[NodeRole]
	clusterManager *ClusterMemberManager

	leaderName  string
	currentTerm atomic.Int64

	heartBeatTimeout time.Duration

	lastAppliedIndex   int64
	lastCommittedIndex atomic.Int64

	nextIndexs atomic.Int64
	// member name
	votedFor string
	log      zap.SugaredLogger

	transport     Sender
	snapshot      Snapshot
	persist       persistence.Persistence
	snapshotIndex atomic.Int64
}

func NewClusterNode(nodeName string, log zap.SugaredLogger) *ClusterNode {
	node := &ClusterNode{log: log, nodeName: nodeName}
	node.nextIndexs.Store(1)
	node.currentRole.Store(new(RoleLearner))
	return node
}

func BootStrapCluster(nodeName string, log zap.SugaredLogger, clusterMember map[string]string) *ClusterNode {
	node := NewClusterNode(nodeName, log)
	node.clusterManager = NewClusterMemberManager(clusterMember)
	return node
}

func (c *ClusterNode) NodeTypeWatcher() {
	for {
		switch *c.currentRole.Load() {
		case RoleLeader:
		case RoleCandidate:
		case RoleLearner:
		default:
		}
	}
}
