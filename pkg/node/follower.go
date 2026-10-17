package node

import (
	"errors"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
	"go.uber.org/zap"
)

var ErrAlreadyVoted = errors.New("voted for the current term")

func (c *ClusterNode) AppendEntries(input rafttypes.AppendEntriesInput) (*rafttypes.AppendEntiresResponse, error) {
	switch *c.currentRole.Load() {
	case RoleCandidate:
		if input.Term >= c.currentTerm.Load() {
			c.currentRole.Store(new(RoleFollower))
		}
		rsp, err := c.appendEntriesFollower(input)
		if err != nil {
			return nil, err
		}
		return rsp, err
	case RoleFollower:
		return c.appendEntriesFollower(input)
	case RoleLeader:
		if input.Term >= c.currentTerm.Load() {
			c.currentRole.Store(new(RoleFollower))
		}
		rsp, err := c.appendEntriesFollower(input)
		if err != nil {
			return nil, err
		}
		return rsp, err
	case RoleLearner:
		return c.appendEntriesFollower(input)
	}

	return nil, nil
}

func (c *ClusterNode) appendEntriesFollower(input rafttypes.AppendEntriesInput) (*rafttypes.AppendEntiresResponse, error) {
	curretTerm := c.currentTerm.Load()
	commitIndex := c.lastCommittedIndex.Load()
	if input.Term < curretTerm {
		return &rafttypes.AppendEntiresResponse{
			Term:        curretTerm,
			Success:     false,
			CommitIndex: commitIndex,
		}, nil
	}
	if input.Term > curretTerm {
		if err := c.recordVote("", curretTerm, input.Term); err != nil {
			return nil, err
		}
		curretTerm = c.currentTerm.Load()
	}

	if c.nextIndexs.Load()-1 != 0 || input.PrevLogIndex != 0 {
		if input.PrevLogIndex > c.nextIndexs.Load()-1 {
			return &rafttypes.AppendEntiresResponse{
				Term:        curretTerm,
				Success:     false,
				CommitIndex: commitIndex,
			}, nil
		}

		appendLog, err := c.persist.ReadLogIndex(input.PrevLogIndex)
		if err != nil {
			return nil, err
		}

		if appendLog.Term != input.PrevLogTerm {
			return &rafttypes.AppendEntiresResponse{
				Term:        curretTerm,
				Success:     false,
				CommitIndex: commitIndex,
			}, nil
		}
	}

	if len(input.Entry) == 0 {
		c.lastCommittedIndex.Store(max(min(input.LeaderCommit, input.PrevLogIndex), commitIndex))
		return &rafttypes.AppendEntiresResponse{
			Term:        curretTerm,
			Success:     true,
			CommitIndex: commitIndex,
		}, nil
	}

	if err := c.persist.Append(input.Entry...); err != nil {
		return nil, err
	}

	c.lastCommittedIndex.Store(max(min(input.LeaderCommit, input.Entry[len(input.Entry)-1].Index), commitIndex))
	c.nextIndexs.Store(input.Entry[len(input.Entry)-1].Index + 1)
	return &rafttypes.AppendEntiresResponse{
		Term:        curretTerm,
		Success:     true,
		CommitIndex: commitIndex,
	}, nil
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
	lastCandidatedVoted := c.votedFor.Load().(string)
	if newTerm > currentTerm {
		lastCandidatedVoted = ""
	}
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
