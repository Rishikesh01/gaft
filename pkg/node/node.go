package node

import (
	"errors"
	"sync"

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
	currentRole    NodeRole

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
	followerState map[string]followerState
}

type followerState struct {
	matchIndex int64
	nextIndx   int64
}

func NewClusterNode(nodeName string, log zap.SugaredLogger) *ClusterNode {
	return &ClusterNode{log: log, nodeName: nodeName}
}

func (c *ClusterNode) ProposeLogEntry(input []Proposal) (*appendLogEntriesLeaderRsp, error) {
	if c.leaderMode == nil {
		return nil, errors.New("not a leader, can't accept writes")
	}
	tmpNextIndex := c.nextIndexs
	for _, proposal := range input {
		c.persist.Append(rafttypes.AppendLog{
			Index: uint64(tmpNextIndex),
			Term:  uint64(c.currentTerm),
			Data:  proposal.Data,
		})
		tmpNextIndex++
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
				nextIndex := c.leaderMode.followerState[member].nextIndx
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
					Entry:        logs,
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
		for call := range appendEntriesCallsRsp {
			if call.rsp.Term > c.currentTerm {
				c.currentRole = RoleFollower
				commitChan <- false
				c.log.Info("stepping down as leader")
				return
				// step down as
			}
			if call.rsp.Success {
				totalAppendEntriesAccepted++
				followerState := c.leaderMode.followerState[call.clusterMemberName]
				followerState.matchIndex = tmpNextIndex - 1
				followerState.nextIndx = tmpNextIndex
				c.leaderMode.followerState[call.clusterMemberName] = followerState
				if totalAppendEntriesAccepted >= majorityConfirmationRequired {
					c.lastCommittedIndex += int64(len(input))
					commitChan <- true
				}
				continue
			}

			if call.error != nil {
				c.log.Error(
					"an error occured while making append call to a cluster member",
					zap.Error(call.error),
					zap.String("cluster_member_ip", call.clusterMemberIP),
					zap.String("cluster_member_name", call.clusterMemberName),
				)
			}

			c.log.Info(
				"append entry call is reported false",
				zap.String("cluster_member_ip", call.clusterMemberIP),
				zap.String("cluster_member_name", call.clusterMemberName),
				zap.Any("rsp", call.rsp),
			)
		}

		commitChan <- false
	}()

	return &appendLogEntriesLeaderRsp{
		commit: <-commitChan,
	}, nil
}

func (c *ClusterNode) AppendEntries(input rafttypes.AppendEntriesInput) (*rafttypes.AppendEntiresResponse, error) {
	panic("unimplemented")
}

func (c *ClusterNode) RequestVote(input rafttypes.RequestVoteInput) (*rafttypes.RequestVoteResponse, error) {
	panic("unimplemented")
}
