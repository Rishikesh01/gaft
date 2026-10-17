package node

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rishikesh01/gaft/pkg/persistence"
	"github.com/Rishikesh01/gaft/pkg/rafttypes"
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

	nextIndexs atomic.Int64
	// member name
	votedFor atomic.Value
	log      zap.SugaredLogger

	transport     Transport
	snapshot      Snapshot
	persist       persistence.Persistence
	snapshotIndex atomic.Int64
	ctx           context.Context
	cancel        context.CancelFunc

	writeTimeout time.Duration
	heartBeat    time.Duration

	electionTimer *time.Timer
}

func NewClusterNode(ctx context.Context, nodeName string, log zap.SugaredLogger, writeTimeout time.Duration, heartBeat time.Duration) *ClusterNode {
	childCtx, cancel := context.WithCancel(ctx)
	node := &ClusterNode{log: log, nodeName: nodeName, commitIndexAdvanced: make(chan struct{}, 1), ctx: childCtx, cancel: cancel}
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

func (c *ClusterNode) Run() {
	c.electionTimer = time.NewTimer(c.randomizedElectionTimerDuration())
	defer c.electionTimer.Stop()
	var leader *leaderMode
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
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
				c.RunLearner()
			default:
				c.RunFollower()
			}
		}
	}
}

func (c *ClusterNode) RunLearner() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.electionTimer.C:
			c.electionTimer.Reset(c.randomizedElectionTimerDuration())
			return
		case rpc := <-c.transport.Receiver():
			c.processRPC(rpc)
		}
	}
}

func (c *ClusterNode) RunFollower() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.electionTimer.C:
			c.electionTimer.Reset(c.randomizedElectionTimerDuration())
			c.currentRole.Store(new(RoleCandidate))
			return
		case rpc := <-c.transport.Receiver():
			c.processRPC(rpc)
		}
	}
}

func (c *ClusterNode) processRPC(rpc rafttypes.RPC) {
	if rpc.AppendEntries != nil {
		reply, err := c.AppendEntries(*rpc.AppendEntries)
		rpc.Reply <- rafttypes.RPCReply{AppendEntries: reply, Err: err}
		if rpc.AppendEntries.Term >= c.currentTerm.Load() {
			c.electionTimer.Reset(c.randomizedElectionTimerDuration())
		}
	}
	if rpc.RequestVote != nil {
		reply, err := c.RequestVote(*rpc.RequestVote)
		rpc.Reply <- rafttypes.RPCReply{RequestVote: reply, Err: err}
		if err == nil && reply.Voted {
			c.electionTimer.Reset(c.randomizedElectionTimerDuration())
		}
	}
}

func (c *ClusterNode) randomizedElectionTimerDuration() time.Duration {
	return c.heartBeatTimeout + (time.Duration(rand.IntN(100)) * time.Millisecond)
}
