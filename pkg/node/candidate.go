package node

import (
	"errors"
	"sync"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
	"go.uber.org/zap"
)

var (
	ErrElectionNotWon = errors.New("failed to get elected")
	ErrCannotCampain  = errors.New("cannot compain, due to being a learner")
)

type candidate struct {
	node *ClusterNode
}

func newCandidate(node *ClusterNode) *candidate {
	return &candidate{
		node: node,
	}
}

func (c *candidate) RequestVoteFromPeers() (map[string]rafttypes.RequestVoteResponse, error) {
	currentTerm, clusterMembmers, log, err := c.prepareForCompain()
	if err != nil {
		return nil, err
	}

	voteResponseMap := sync.Map{}

	var wg sync.WaitGroup
	for member, details := range clusterMembmers.members {
		if member == c.node.nodeName {
			continue
		}
		wg.Go(func() {
			resp, err := c.node.transport.RequestVote(member, rafttypes.RequestVoteInput{
				Term:          currentTerm,
				LastLogIndex:  log.Index,
				LastLogTerm:   log.Term,
				CandidateName: c.node.nodeName,
			})
			if err != nil {
				// retry with backoff needs to be added
				c.node.log.Error("requesting vote failed with error", zap.Error(err), zap.String("member", member), zap.String("ip", details.ip))
				return
			}

			voteResponseMap.Store(member, resp)
		})
	}

	wg.Wait()

	return c.campainResult(clusterMembmers, &voteResponseMap, currentTerm)
}

func (c *candidate) campainResult(clusterMembmers *clusterMemberConfig, voteResponseMap *sync.Map, currentTerm int64) (map[string]rafttypes.RequestVoteResponse, error) {
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
	leaderConstructionMap := map[string]rafttypes.RequestVoteResponse{}
	voteResponseMap.Range(func(key, value any) bool {
		member := key.(string)
		resp := value.(rafttypes.RequestVoteResponse)
		leaderConstructionMap[member] = resp
		if resp.Voted && clusterMembmers.members[member].state.inNewSet() {
			newMembersCount++
		}
		if resp.Voted && clusterMembmers.members[member].state.inOldSet() {
			oldMembersCount++
		}

		if !resp.Voted {
			highestTerm = max(highestTerm, resp.Term)
		}

		return true
	})

	if currentTerm < highestTerm {
		c.node.currentTerm.Store(highestTerm)
		if err := c.node.persist.SaveVoteState(highestTerm, ""); err != nil {
			c.node.log.Error("failed to save term update while campaining", zap.Error(err))
			return nil, err
		}
		c.node.votedFor.Store("")
		c.node.currentRole.Store(new(RoleFollower))
		return nil, ErrElectionNotWon
	}

	if (oldMembersCount < (int(clusterMembmers.oldMembers)/2 + 1)) || (newMembersCount < int(clusterMembmers.newMembers)/2+1) {
		c.node.currentRole.Store(new(RoleFollower))
		return nil, ErrElectionNotWon
	}

	c.node.currentRole.Store(new(RoleLeader))
	return leaderConstructionMap, nil
}

func (c *candidate) prepareForCompain() (int64, *clusterMemberConfig, *rafttypes.AppendLog, error) {
	currentTerm := c.node.currentTerm.Load() + 1
	clusterMembmers := c.node.clusterManager.GetClusterMembers()
	currentIndex := c.node.nextIndexs.Load() - 1

	if *c.node.currentRole.Load() == RoleLearner {
		// if candidate does not has any entry then they can't campain
		return 0, nil, nil, ErrCannotCampain
	}

	logs, err := c.node.persist.ReadLogIndex(currentIndex)
	if err != nil {
		c.node.log.Error("failed to get latest log entry for vote campain", zap.Error(err))
		return 0, nil, nil, err
	}

	c.node.currentTerm.Store(currentTerm)
	if err := c.node.persist.SaveVoteState(currentTerm, c.node.nodeName); err != nil {
		c.node.log.Error("failed to save self vote", zap.Error(err))
		return 0, nil, nil, err
	}
	c.node.votedFor.Store(c.node.nodeName)

	return currentTerm, clusterMembmers, logs, nil
}
