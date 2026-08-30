package node

import (
	"context"
	"errors"
	"time"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
)

func (c *ClusterNode) ProposeLogEntry(input []Proposal) (*appendLogEntriesLeaderRsp, error) {
	if !c.mu.TryLock() {
		return nil, errors.New("a proposal is in progress")
	}
	defer c.mu.Unlock()
	role := c.currentRole.Load()
	if role != nil && *role != RoleLeader {
		return nil, errors.New("not a leader, can't accept writes")
	}

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

	c.nextIndexs = tmpNextIndex
	wait := waiter{
		index:  int(c.nextIndexs),
		commit: make(chan bool),
	}
	c.leaderMode.newAppend <- wait

	if <-wait.commit {
		c.lastCommittedIndex = c.nextIndexs - 1
		return &appendLogEntriesLeaderRsp{
			commit: true,
		}, nil
	}

	c.leaderMode.cancel()
	c.currentRole.Swap(new(RoleFollower))

	return &appendLogEntriesLeaderRsp{
		commit: false,
	}, nil
}

func (l *leaderMode) replicationManager() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case waiter := <-l.newAppend:
			for member := range l.followerStateMap {
				l.followerStateMap[member].newAppendEntry <- struct{}{}
			}
			l.pending = &waiter
		case <-l.nextCommitIndexChan:
		}
	}
}

func (f *followerState) replicationWorker(ctx context.Context, pulse time.Duration) {
	beat := time.NewTicker(pulse)
	defer beat.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-beat.C:
		case <-f.newAppendEntry:
		}
	}
}
