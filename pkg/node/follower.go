package node

import (
	"errors"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
	"go.uber.org/zap"
)

var ErrAlreadyVoted = errors.New("voted for the current term")

func (c *ClusterNode) AppendEntries(input rafttypes.AppendEntriesInput) (*rafttypes.AppendEntiresResponse, error) {
	panic("unimplemented")
}

func (c *ClusterNode) RequestVote(input rafttypes.RequestVoteInput) (*rafttypes.RequestVoteResponse, error) {
	currentTerm := c.currentTerm.Load()
	currentRole := *c.currentRole.Load()
	noVote := &rafttypes.RequestVoteResponse{
		Term:          currentTerm,
		Voted:         false,
		CommitedIndex: 0,
	}

	vote := &rafttypes.RequestVoteResponse{
		Term:          input.Term,
		Voted:         true,
		CommitedIndex: c.lastCommittedIndex.Load(),
	}
	if input.Term < currentTerm {
		return noVote, nil
	}

	noVote.Term = input.Term

	if (currentRole == RoleCandidate || currentRole == RoleLeader) && currentTerm != input.Term {
		c.currentRole.Store(new(RoleFollower))
	}

	if input.Term > currentTerm {
		if err := c.recordVote("", currentTerm, input.Term); err != nil {
			if errors.Is(err, ErrAlreadyVoted) {
				return noVote, nil
			}
			return noVote, err
		}
	}

	currentIndex := c.nextIndexs.Load() - 1
	if currentIndex != 0 {
		log, err := c.persist.ReadLogIndex(currentIndex)
		if err != nil {
			c.log.Error("failed to get log for voting", zap.Error(err))
			return noVote, err
		}
		if input.LastLogTerm != log.Term {
			if input.LastLogTerm < log.Term {
				return noVote, nil
			}
		} else if input.LastLogIndex < currentIndex {
			return noVote, nil
		}
	}

	if err := c.recordVote(input.CandidateName, currentTerm, input.Term); err != nil {
		if errors.Is(err, ErrAlreadyVoted) {
			return noVote, nil
		}
		return noVote, err
	}

	return vote, nil
}

func (c *ClusterNode) recordVote(candidateName string, currentTerm int64, newTerm int64) error {
	if newTerm > currentTerm {
		c.votedFor.Store("")
	}
	lastCandidatedVoted := c.votedFor.Load().(string)
	// on crash we will restore this from disk
	if lastCandidatedVoted != candidateName && lastCandidatedVoted != "" {
		return ErrAlreadyVoted
	}

	if err := c.persist.SaveVoteState(newTerm, candidateName); err != nil {
		c.log.Error("failed to save vote", zap.Error(err))
		return err
	}
	c.currentTerm.Store(newTerm)
	c.votedFor.Store(candidateName)

	return nil
}
