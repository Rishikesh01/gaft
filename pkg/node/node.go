package node

import (
	"context"
	"errors"
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
	currentRole    atomic.Pointer[NodeRole]
	clusterManager *clusterMemberManager

	leaderName  string
	currentTerm atomic.Int64

	heartBeatTimeout time.Duration

	lastAppliedIndex    int64
	lastCommittedIndex  atomic.Int64
	commitIndexAdvanced chan struct{}

	startIndex atomic.Int64
	nextIndexs atomic.Int64
	// member name
	votedFor atomic.Value
	log      zap.SugaredLogger

	transport     Sender
	snapshot      Snapshot
	persist       persistence.Persistence
	snapshotIndex atomic.Int64
	ctx           context.Context
	cancel        context.CancelFunc

	writeTimeout time.Duration
	heartBeat    time.Duration
}

func NewClusterNode(ctx context.Context, nodeName string, log zap.SugaredLogger, writeTimeout time.Duration, heartBeat time.Duration) *ClusterNode {
	childCtx, cancel := context.WithCancel(ctx)
	node := &ClusterNode{log: log, nodeName: nodeName, commitIndexAdvanced: make(chan struct{}, 1), ctx: childCtx, cancel: cancel}
	node.startIndex.Store(1)
	node.nextIndexs.Store(1)
	node.currentRole.Store(new(RoleLearner))
	node.votedFor.Store("")
	node.writeTimeout = writeTimeout
	node.heartBeat = heartBeat

	return node
}

func BootStrapCluster(ctx context.Context, nodeName string, log zap.SugaredLogger, clusterMember map[string]string, writeTimeout time.Duration, heartBeat time.Duration) *ClusterNode {
	node := NewClusterNode(ctx, nodeName, log, writeTimeout, heartBeat)
	node.clusterManager = NewClusterMemberManager(clusterMember)
	return node
}

func (c *ClusterNode) NodeTypeWatcher() {
	var leader *leaderMode
	for {
		switch *c.currentRole.Load() {
		case RoleLeader:
			leader.run()
		case RoleCandidate:
			leaderConstuctionData, err := newCandidate(c).RequestVoteFromPeers()
			if err == nil {
				leader = newLeader(c, leaderConstuctionData)
			}
			if err != nil && !errors.Is(err, ErrCannotCampain) && !errors.Is(err, ErrElectionNotWon) {
				return
			}
		case RoleLearner:
		default:
		}
	}
}
