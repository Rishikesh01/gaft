package node

import (
	"context"
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
)

type ClusterNode struct {
	mu sync.Mutex
	// identity of the node
	nodeName string
	// name and address
	clusterMembers map[string]string
	currentRole    atomic.Pointer[NodeRole]

	leaderName       string
	currentTerm      int64
	heartBeatTimeout time.Duration

	lastAppliedIndex   int64
	lastCommittedIndex int64

	nextIndexs int64
	// member name
	votedFor string
	log      zap.SugaredLogger

	leaderMode *leaderMode

	transport Sender
	snapshot  Snapshot
	persist   persistence.Persistence
}

type waiter struct {
	index  int
	commit chan bool
}

type leaderMode struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	pulse               time.Duration
	pending             *waiter
	newAppend           chan waiter
	nextCommitIndexChan chan int
	followerStateMap    map[string]*followerState
}

type followerState struct {
	newAppendEntry chan struct{}
	matchIndex     int64
	nextIndx       int64
}

func NewClusterNode(nodeName string, log zap.SugaredLogger) *ClusterNode {
	return &ClusterNode{log: log, nodeName: nodeName}
}

func (c *ClusterNode) AppendEntries(input rafttypes.AppendEntriesInput) (*rafttypes.AppendEntiresResponse, error) {
	panic("unimplemented")
}

func (c *ClusterNode) RequestVote(input rafttypes.RequestVoteInput) (*rafttypes.RequestVoteResponse, error) {
	panic("unimplemented")
}
