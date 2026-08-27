package node

import (
	"errors"
	"sync"
	"sync/atomic"

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

	leaderName  string
	currentTerm int64

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

type leaderMode struct {
	mu            sync.Mutex
	followerState map[string]*followerState
}

type followerState struct {
	mu         sync.RWMutex
	matchIndex int64
	nextIndx   int64
}

func NewClusterNode(nodeName string, log zap.SugaredLogger) *ClusterNode {
	return &ClusterNode{log: log, nodeName: nodeName}
}

func (c *ClusterNode) ProposeLogEntry(input []Proposal) (*appendLogEntriesLeaderRsp, error) {
	if !c.mu.TryLock() {
		return nil, errors.New("a proposal is in progress")
	}
	defer c.mu.Unlock()

	if c.leaderMode == nil {
		return nil, errors.New("not a leader, can't accept writes")
	}
	c.leaderMode.mu.Lock()
	defer c.leaderMode.mu.Unlock()
	tmpNextIndex := c.nextIndexs
	raftLogs := make([]rafttypes.AppendLog, 0, len(input))
	for _, proposal := range input {
		raftLogs = append(raftLogs, rafttypes.AppendLog{
			Index: uint64(tmpNextIndex),
			Term:  uint64(c.currentTerm),
			Data:  proposal.Data,
		})
		tmpNextIndex++
	}

	if err := c.persist.Append(raftLogs...); err != nil {
		return nil, err
	}

	clusterMembers := c.clusterMembers
	totalClusterMembers := len(clusterMembers)
	majorityConfirmationRequired := (totalClusterMembers / 2) + 1
	appendEntriesCallsRsp := make(chan appendEntriesRspFrom, totalClusterMembers)
	c.transport.UpdatePeers(clusterMembers)

	var wg sync.WaitGroup
	for member, ip := range clusterMembers {
		if member == c.nodeName {
			continue
		}
		wg.Go(
			func() {
				c.leaderMode.followerState[member].mu.RLock()
				nextIndex := c.leaderMode.followerState[member].nextIndx
				c.leaderMode.followerState[member].mu.RUnlock()
				logs, err := c.persist.ReadLogs(nextIndex-1, tmpNextIndex-1)
				if err != nil {
					appendEntriesCallsRsp <- appendEntriesRspFrom{
						rsp:               rafttypes.AppendEntiresResponse{},
						error:             err,
						clusterMemberIP:   ip,
						clusterMemberName: member,
					}
					return
				}
				rsp, err := c.transport.AppendEntries(ip, rafttypes.AppendEntriesInput{
					Term:         c.currentTerm,
					PrevLogIndex: int64(logs[0].Index),
					PrevLogTerm:  int64(logs[0].Term),
					LeaderCommit: c.lastCommittedIndex,
					LeaderName:   c.nodeName,
					Entry:        logs[1:],
				})
				appendEntriesCallsRsp <- appendEntriesRspFrom{
					rsp:               rsp,
					error:             err,
					clusterMemberIP:   ip,
					clusterMemberName: member,
				}
			},
		)
	}

	go func() {
		wg.Wait()
		close(appendEntriesCallsRsp)
	}()

	totalAppendEntriesAccepted := 1
	commitChan := make(chan bool, 1)
	go func() {
		isCommitedChanUsed := false
		for call := range appendEntriesCallsRsp {
			if call.error != nil {
				c.log.Error(
					"an error occured while making append call to a cluster member",
					zap.Error(call.error),
					zap.String("cluster_member_ip", call.clusterMemberIP),
					zap.String("cluster_member_name", call.clusterMemberName),
				)
				continue
			}
			if !call.rsp.Success {
				followerState := c.leaderMode.followerState[call.clusterMemberName]
				followerState.mu.Lock()
				followerState.nextIndx -= 1
				followerState.mu.Unlock()
				c.leaderMode.followerState[call.clusterMemberName] = followerState
			}
			if call.rsp.Term > c.currentTerm {
				c.currentRole.Swap(new(RoleFollower))
				if !isCommitedChanUsed {
					commitChan <- false
					isCommitedChanUsed = true
				}
				c.log.Info("stepping down as leader")
				c.leaderMode = nil
				c.currentTerm = call.rsp.Term
				c.persist.SaveVoteState(c.currentTerm, "")
				return
				// step down as
			}
			if call.rsp.Success {
				totalAppendEntriesAccepted++
				followerState := c.leaderMode.followerState[call.clusterMemberName]
				followerState.mu.Lock()
				followerState.matchIndex = tmpNextIndex - 1
				followerState.nextIndx = tmpNextIndex
				followerState.mu.Unlock()
				c.leaderMode.followerState[call.clusterMemberName] = followerState
				if totalAppendEntriesAccepted >= majorityConfirmationRequired && !isCommitedChanUsed {
					isCommitedChanUsed = true
					commitChan <- true
				}
				continue
			}

			c.log.Info(
				"append entry call is reported false",
				zap.String("cluster_member_ip", call.clusterMemberIP),
				zap.String("cluster_member_name", call.clusterMemberName),
				zap.Any("rsp", call.rsp),
			)
		}
		if len(clusterMembers) == 1 {
			commitChan <- true
			isCommitedChanUsed = true
			return
		}
		if isCommitedChanUsed {
			return
		}

		commitChan <- false
	}()

	if <-commitChan {
		c.lastCommittedIndex = tmpNextIndex - 1
		c.nextIndexs = tmpNextIndex
		return &appendLogEntriesLeaderRsp{
			commit: true,
		}, nil
	}

	c.nextIndexs = tmpNextIndex
	return &appendLogEntriesLeaderRsp{
		commit: false,
	}, nil
}

func (c *ClusterNode) AppendEntries(input rafttypes.AppendEntriesInput) (*rafttypes.AppendEntiresResponse, error) {
	panic("unimplemented")
}

func (c *ClusterNode) RequestVote(input rafttypes.RequestVoteInput) (*rafttypes.RequestVoteResponse, error) {
	panic("unimplemented")
}
