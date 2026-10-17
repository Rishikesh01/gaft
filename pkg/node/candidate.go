package node

import (
	"context"
	"errors"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
	"go.uber.org/zap"
)

var (
	ErrElectionNotWon = errors.New("failed to get elected")
	ErrCannotCampain  = errors.New("cannot compain, due to being a learner")
)

type candidate struct {
	ctx    context.Context
	cancel context.CancelFunc
	node   *ClusterNode
}

func newCandidate(node *ClusterNode) *candidate {
	ctx, cancel := context.WithCancel(node.ctx)
	return &candidate{
		ctx:    ctx,
		cancel: cancel,
		node:   node,
	}
}

func (c *candidate) RequestVoteFromPeers() (map[string]rafttypes.RequestVoteResponse, error) {
	defer c.cancel()
	currentTerm, clusterMembmers, log, err := c.prepareForCompain()
	if err != nil {
		return nil, err
	}
	if len(clusterMembmers.members) == 1 {
		return c.campainResult(clusterMembmers, map[string]rafttypes.RequestVoteResponse{}, currentTerm)
	}

	voteResponseMap := map[string]rafttypes.RequestVoteResponse{}
	type voteRsp struct {
		member string
		vote   rafttypes.RequestVoteResponse
		err    error
	}
	voteRespChan := make(chan voteRsp, len(clusterMembmers.members)-1)
	for member, details := range clusterMembmers.members {
		if member == c.node.nodeName {
			continue
		}
		go func() {
			resp, err := c.node.transport.RequestVote(member, rafttypes.RequestVoteInput{
				Term:          currentTerm,
				LastLogIndex:  log.Index,
				LastLogTerm:   log.Term,
				CandidateName: c.node.nodeName,
			})
			if err != nil {
				// retry with backoff needs to be added
				c.node.log.Error("requesting vote failed with error", zap.Error(err), zap.String("member", member), zap.String("ip", details.ip))
				voteRespChan <- voteRsp{
					member: member,
					err:    err,
				}
				return
			}
			voteRespChan <- voteRsp{
				member: member,
				vote:   resp,
			}
		}()
	}

	totalResponseCount := 0
outer:
	for {
		select {
		case <-c.ctx.Done():
			return nil, c.ctx.Err()
		case <-c.node.electionTimer.C:
			c.node.electionTimer.Reset(c.node.randomizedElectionTimerDuration())
			c.node.currentRole.Store(new(RoleCandidate))
			c.cancel()
			return nil, ErrElectionNotWon
		case rpc := <-c.node.transport.Receiver():
			c.node.processRPC(rpc)
			if *c.node.currentRole.Load() != RoleCandidate {
				c.cancel()
				return nil, ErrElectionNotWon
			}
		case v := <-voteRespChan:
			totalResponseCount++
			if v.err == nil {
				voteResponseMap[v.member] = v.vote
			}
			if totalResponseCount == len(clusterMembmers.members)-1 {
				break outer
			}

		}
	}

	return c.campainResult(clusterMembmers, voteResponseMap, currentTerm)
}

func (c *candidate) campainResult(clusterMembmers *clusterMemberConfig, voteResponseMap map[string]rafttypes.RequestVoteResponse, currentTerm int64) (map[string]rafttypes.RequestVoteResponse, error) {
	if *c.node.currentRole.Load() != RoleCandidate {
		return nil, ErrElectionNotWon
	}
	var (
		oldMembersCount int
		newMembersCount int
		highestTerm     int64
	)
	if clusterMembmers.members[c.node.nodeName].state.inNewSet() {
		newMembersCount++
	}
	if clusterMembmers.members[c.node.nodeName].state.inOldSet() {
		oldMembersCount++
	}

	for member, resp := range voteResponseMap {
		if resp.Voted && clusterMembmers.members[member].state.inNewSet() {
			newMembersCount++
		}
		if resp.Voted && clusterMembmers.members[member].state.inOldSet() {
			oldMembersCount++
		}

		if !resp.Voted {
			highestTerm = max(highestTerm, resp.Term)
		}
	}

	if currentTerm < highestTerm {
		if err := c.node.persist.SaveVoteState(highestTerm, ""); err != nil {
			c.node.log.Error("failed to save term update while campaining", zap.Error(err))
			return nil, err
		}
		c.node.currentTerm.Store(highestTerm)
		c.node.votedFor.Store("")
		c.node.currentRole.Store(new(RoleFollower))
		return nil, ErrElectionNotWon
	}

	if (oldMembersCount < (int(clusterMembmers.oldMembers)/2 + 1)) || (newMembersCount < int(clusterMembmers.newMembers)/2+1) {
		c.node.currentRole.Store(new(RoleFollower))
		return nil, ErrElectionNotWon
	}

	c.node.currentRole.Store(new(RoleLeader))
	return voteResponseMap, nil
}

func (c *candidate) prepareForCompain() (int64, *clusterMemberConfig, *rafttypes.AppendLog, error) {
	currentTerm := c.node.currentTerm.Load() + 1
	clusterMembmers := c.node.clusterManager.GetClusterMembers()
	currentIndex := c.node.nextIndexs.Load() - 1

	if *c.node.currentRole.Load() == RoleLearner || currentIndex == 0 {
		// if candidate does not has any entry then they can't campain
		c.node.currentRole.Store(new(RoleLearner))
		return 0, nil, nil, ErrCannotCampain
	}

	logs, err := c.node.persist.ReadLogIndex(currentIndex)
	if err != nil {
		c.node.log.Error("failed to get latest log entry for vote campain", zap.Error(err))
		return 0, nil, nil, err
	}

	if err := c.node.persist.SaveVoteState(currentTerm, c.node.nodeName); err != nil {
		c.node.log.Error("failed to save self vote", zap.Error(err))
		return 0, nil, nil, err
	}

	c.node.currentTerm.Store(currentTerm)
	c.node.votedFor.Store(c.node.nodeName)

	return currentTerm, clusterMembmers, logs, nil
}
