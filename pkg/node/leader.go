package node

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
	"go.uber.org/zap"
)

var (
	ErrProposalTimeOut               = errors.New("proposal timeout, commit status unknown")
	ErrRaftCommitFailure             = errors.New("raft commit failure, commit status unknown")
	ErrInflightPropose               = errors.New("inflight propose")
	ErrProposalInputCapLimitExceeded = errors.New("proposal input cap limit exceeded")
	ErrCurrentRoleNotLeader          = errors.New("currently not a leader")
	ErrMembersCurrentTermHigher      = errors.New("term of cluster member is higer than leader")
	ErrAppendEntryMisMatch           = errors.New("append entry failed to successed due to mismatch")
)

const inputEntriesCap = 20

type leaderMode struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	mu                  sync.Mutex
	proposalTimeout     time.Duration
	pulse               time.Duration
	pending             *waiter
	newAppend           chan waiter
	followerStateChange chan string
	followerStateMap    map[string]*followerState
	node                *ClusterNode
}

type waiter struct {
	index  int64
	commit chan bool
}

type followerState struct {
	newAppendEntry chan struct{}
	matchIndex     atomic.Int64
}

func (c *leaderMode) ProposeLogEntry(inputs []Proposal) (*appendLogEntriesLeaderRsp, error) {
	if *c.node.currentRole.Load() != RoleLeader {
		return nil, ErrCurrentRoleNotLeader
	}

	if len(inputs) > inputEntriesCap {
		return nil, ErrProposalInputCapLimitExceeded
	}

	if !c.mu.TryLock() {
		return nil, ErrInflightPropose
	}
	defer c.mu.Unlock()

	nextIndex := c.node.nextIndexs.Load()
	appendLogs := make([]rafttypes.AppendLog, 0, len(inputs))
	for i := range inputs {
		appendLogs = append(appendLogs, rafttypes.AppendLog{
			Index: uint64(nextIndex),
			Term:  uint64(c.node.currentTerm.Load()),
			Data:  inputs[i].Data,
		})
		nextIndex++
	}

	if err := c.node.persist.Append(appendLogs...); err != nil {
		return nil, err
	}

	c.node.nextIndexs.Store(nextIndex)

	commitChan := make(chan bool, 1)
	c.newAppend <- waiter{
		index:  nextIndex - 1,
		commit: commitChan,
	}
	timeOutTimer := time.NewTimer(c.proposalTimeout)
	defer timeOutTimer.Stop()
	select {
	case commit := <-commitChan:
		if commit {
			c.node.lastCommittedIndex.Store(nextIndex - 1)
			return &appendLogEntriesLeaderRsp{
				commit: commit,
			}, nil
		}
		return nil, ErrRaftCommitFailure
	case <-timeOutTimer.C:
		return nil, ErrProposalTimeOut
	}
}

func (l *leaderMode) replicationManager() {
	for {
		select {
		case <-l.ctx.Done():
			if l.pending != nil {
				l.pending.commit <- false
			}
			return
		case pendingWaiter := <-l.newAppend:
			for member := range l.followerStateMap {
				l.followerStateMap[member].newAppendEntry <- struct{}{}
			}
			l.pending = &pendingWaiter
			l.matchIndex()
		case <-l.followerStateChange:
			l.matchIndex()
		}
	}
}

func (l *leaderMode) matchIndex() {
	majority := ((len(l.followerStateMap) + 1) / 2) + 1
	confirmed := 1
	if l.pending == nil {
		return
	}
	for member := range l.followerStateMap {
		if l.pending.index <= l.followerStateMap[member].matchIndex.Load() {
			confirmed++
		}
	}
	if confirmed >= majority {
		l.pending.commit <- true
		l.pending = nil
	}
}

func (l *leaderMode) stepDown(term int64) {
	l.node.mu.Lock()
	l.node.log.Info("stepping down as leader")
	defer l.node.mu.Unlock()
	if l.node.currentTerm.Load() >= term {
		return
	}
	l.node.currentTerm.Store(term)
	if err := l.node.persist.SaveVoteState(term, ""); err != nil {
		l.node.log.Error("failed to save vote state", zap.Error(err))
	}
	l.cancel()
	l.node.currentRole.Store(new(RoleFollower))
	l.node.log.Info("completed stepping down as leader")
}

func (l *leaderMode) replicationWorker(ctx context.Context, member string, pulse time.Duration) {
	beat := time.NewTicker(pulse)
	mp := l.followerStateMap[member]
	defer beat.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-beat.C:
			l.replicate(mp, member)
		case <-mp.newAppendEntry:
			l.replicate(mp, member)
		}
	}
}

func (l *leaderMode) replicate(mp *followerState, member string) error {
	replicaMatchIndex := mp.matchIndex.Load()
	leadersNextIndex := l.node.nextIndexs.Load()
	leadersCommitedIndex := l.node.lastCommittedIndex.Load()
	leadersCurrentTerm := l.node.currentTerm.Load()
	// matchIndex 0 because during election phase we will also add support for getting commitIndex in response from voter which will be initlized as matchIndex
	if replicaMatchIndex == 0 && leadersCommitedIndex > 0 || replicaMatchIndex < (l.node.lastCommittedIndex.Load()-100) {
		// TODO: need to add logic to get snapshot from application or check for existing snapshot to send to follower
		l.node.transport.InstallSnapshot(member, rafttypes.InstallSnapshotInput{})
		return nil
	}

	replicationTargetIndex := min(leadersNextIndex, replicaMatchIndex+inputEntriesCap)

	resp, err := l.appendLog(replicationTargetIndex, leadersCommitedIndex, leadersCurrentTerm, replicaMatchIndex, member)
	if err != nil && !errors.Is(err, ErrAppendEntryMisMatch) {
		l.node.log.Error("error occured while trying to append entry", zap.Error(err), zap.String("member", member), zap.String("member_ip", l.node.clusterMembers[member]))
		return err
	}

	newMatchIndex := replicationTargetIndex - 1
	if resp != nil && errors.Is(err, ErrAppendEntryMisMatch) {
		newMatchIndex = resp.CommitIndex
		if replicaMatchIndex == resp.CommitIndex {
			newMatchIndex = 0
		}
	}

	mp.matchIndex.Store(newMatchIndex)
	l.followerStateChange <- member
	return nil
}

func (l *leaderMode) appendLog(replicationTargetIndex int64, leadersCommitedIndex int64, leadersCurrentTerm int64, startIndex int64, member string) (*rafttypes.AppendEntiresResponse, error) {
	appendEntries, err := l.getAppendEntries(startIndex, replicationTargetIndex, leadersCurrentTerm, leadersCommitedIndex)
	if err != nil {
		return nil, err
	}
	resp, err := l.node.transport.AppendEntries(member, appendEntries)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		if resp.Term > leadersCurrentTerm {
			l.node.log.Info("appending entry to follower failed, due to follower having greater term", zap.String("member", member), zap.String("member_ip", l.node.clusterMembers[member]))
			l.stepDown(resp.Term)
			return nil, ErrMembersCurrentTermHigher
		}

		l.node.log.Info("appending entry to follower failed", zap.String("member", member), zap.String("member_ip", l.node.clusterMembers[member]))
		return &resp, ErrAppendEntryMisMatch
	}
	return nil, nil
}

func (l *leaderMode) getAppendEntries(followersMatchIndex int64, leadersNextIndex int64, leadersCurrentTerm int64, leadersCommitedIndex int64) (rafttypes.AppendEntriesInput, error) {
	startIndex := followersMatchIndex
	endIndex := max(leadersNextIndex-1, 0)

	if endIndex == 0 {
		return rafttypes.AppendEntriesInput{
			Term:         leadersCurrentTerm,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			LeaderCommit: leadersCommitedIndex,
			LeaderName:   l.node.nodeName,
			Entry:        make([]*rafttypes.AppendLog, 0),
		}, nil
	}

	if startIndex == endIndex && endIndex != 0 {
		startIndex--
	}
	logs, err := l.node.persist.ReadLogs(startIndex, endIndex)
	if err != nil {
		return rafttypes.AppendEntriesInput{}, err
	}
	if len(logs) == 0 {
		return rafttypes.AppendEntriesInput{}, io.EOF
	}

	appendLogEntries := rafttypes.AppendEntriesInput{
		Term:         leadersCurrentTerm,
		PrevLogIndex: int64(logs[0].Index),
		PrevLogTerm:  int64(logs[0].Term),
		LeaderCommit: leadersCommitedIndex,
		LeaderName:   l.node.nodeName,
		Entry:        logs,
	}
	if startIndex == 0 {
		appendLogEntries.PrevLogIndex = 0
		appendLogEntries.PrevLogTerm = 0
	}
	if len(logs) >= 2 && startIndex != 0 {
		appendLogEntries.Entry = logs[1:]
	}

	return appendLogEntries, nil
}
