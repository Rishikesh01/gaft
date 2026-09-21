package node

import (
	"context"
	"errors"
	"io"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
	"go.uber.org/zap"
)

const inputEntriesCap = 20

var (
	ErrProposalTimeOut               = errors.New("proposal timeout, commit status unknown")
	ErrRaftCommitFailure             = errors.New("raft commit failure, commit status unknown")
	ErrInflightPropose               = errors.New("inflight propose")
	ErrProposalInputCapLimitExceeded = errors.New("proposal input cap limit exceeded")
	ErrCurrentRoleNotLeader          = errors.New("currently not a leader")
	ErrMembersCurrentTermHigher      = errors.New("term of cluster member is higer than leader")
	ErrAppendEntryMisMatch           = errors.New("append entry failed to successed due to mismatch")
	ErrResizeInProgress              = errors.New("an resize event is already in progress")
)

type leaderMode struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	mu                  sync.Mutex
	proposalTimeout     time.Duration
	pulse               time.Duration
	waiters             []Waiter
	newAppend           chan struct{}
	followerStateChange chan string
	followerStateMap    map[string]*followerState
	node                *ClusterNode
}

type Waiter struct {
	index  int64
	commit chan bool
}

type followerState struct {
	newAppendEntry chan struct{}
	matchIndex     atomic.Int64
	ctx            context.Context
	cancel         context.CancelFunc
}

func (l *leaderMode) ProposeClusterResize(input Proposal) (*appendLogEntriesLeaderRsp, error) {
	if *l.node.currentRole.Load() != RoleLeader {
		return nil, ErrCurrentRoleNotLeader
	}

	if l.node.clusterManager.IsResizeInProgress() {
		return nil, ErrResizeInProgress
	}

	waiter, result, err := l.appendLogs([]Proposal{input}, rafttypes.LogTypeRaftCluster)
	if err != nil {
		return result, err
	}

	l.newAppend <- struct{}{}
	return &appendLogEntriesLeaderRsp{
		promise: &waiter,
	}, nil
}

func (l *leaderMode) ProposeLogEntry(inputs []Proposal) (*appendLogEntriesLeaderRsp, error) {
	if *l.node.currentRole.Load() != RoleLeader {
		return nil, ErrCurrentRoleNotLeader
	}

	if len(inputs) > inputEntriesCap {
		return nil, ErrProposalInputCapLimitExceeded
	}

	waiter, result, err := l.appendLogs(inputs, rafttypes.LogTypeApplication)
	if err != nil {
		return result, err
	}

	l.newAppend <- struct{}{}
	return &appendLogEntriesLeaderRsp{
		promise: &waiter,
	}, nil
}

func (l *leaderMode) appendLogs(inputs []Proposal, logType string) (Waiter, *appendLogEntriesLeaderRsp, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	nextIndex := l.node.nextIndexs.Load()
	appendLogs := make([]rafttypes.AppendLog, 0, len(inputs))
	for i := range inputs {
		appendLogs = append(appendLogs, rafttypes.AppendLog{
			Index: uint64(nextIndex),
			Term:  uint64(l.node.currentTerm.Load()),
			Data:  inputs[i].Data,
			Type:  logType,
		})
		nextIndex++
	}
	if err := l.node.persist.Append(appendLogs...); err != nil {
		return Waiter{}, nil, err
	}

	waiter := Waiter{
		index:  nextIndex - 1,
		commit: make(chan bool, 1),
	}

	l.waiters = append(l.waiters, waiter)

	l.node.nextIndexs.Store(nextIndex)
	return waiter, nil, nil
}

func (l *leaderMode) replicationManager() {
	for {
		select {
		case <-l.ctx.Done():
			l.mu.Lock()
			defer l.mu.Unlock()
			for _, waiter := range l.waiters {
				waiter.commit <- false
			}

			return
		case <-l.newAppend:
			for member := range l.followerStateMap {
				select {
				case l.followerStateMap[member].newAppendEntry <- struct{}{}:
				default:
				}
			}
			l.matchIndex()
		case <-l.followerStateChange:
			l.matchIndex()
		}
	}
}

func (l *leaderMode) followerStateManager() {
	// var raftClusterChanges []rafttypes.RaftClusterState
	// if err := json.NewDecoder(bytes.NewReader(input.Data)).Decode(&raftClusterChanges); err != nil {
	// 	return nil, err
	// }
	members := l.node.clusterManager.GetClusterMembers().members
	for member := range members {
		if member == l.node.nodeName {
			continue
		}
		if _, ok := l.followerStateMap[member]; ok {
			continue
		}
		ctx, cancel := context.WithCancel(l.ctx)
		stateOfFollower := &followerState{
			newAppendEntry: make(chan struct{}, 1),
			ctx:            ctx,
			cancel:         cancel,
			matchIndex:     atomic.Int64{},
		}
		l.followerStateMap[member] = stateOfFollower
		go l.replicationWorker(member, stateOfFollower, l.pulse)
		l.node.log.Info("spawned replication worker", zap.String("member", member))
	}

	for member, fs := range l.followerStateMap {
		if _, ok := members[member]; ok {
			continue
		}
		fs.cancel()
		delete(l.followerStateMap, member)
		l.node.log.Info("removed replication worker", zap.String("member", member))
	}
}

func (l *leaderMode) matchIndex() {
	clusterMembershipState := l.node.clusterManager.GetClusterMembers()

	oldSet := make([]int64, 0, clusterMembershipState.oldMembers)
	newSet := make([]int64, 0, clusterMembershipState.newMembers)

	selfState := clusterMembershipState.members[l.node.nodeName].state
	leaderIndex := l.node.nextIndexs.Load() - 1
	if selfState.inOldSet() {
		oldSet = append(oldSet, leaderIndex)
	}
	if selfState.inNewSet() {
		newSet = append(newSet, leaderIndex)
	}
	for member, state := range l.followerStateMap {
		if clusterMembershipState.members[member].state.inOldSet() {
			oldSet = append(oldSet, state.matchIndex.Load())
		}

		if clusterMembershipState.members[member].state.inNewSet() {
			newSet = append(newSet, state.matchIndex.Load())
		}
	}

	quromState := min(median(oldSet), median(newSet))
	if quromState <= l.node.lastCommittedIndex.Load() || quromState < l.node.startIndex.Load() {
		return
	}

	l.node.lastCommittedIndex.Swap(quromState)
	select {
	case l.node.commitIndexAdvanced <- struct{}{}:
	default:
	}
}

func median(v []int64) int64 {
	if len(v) == 0 {
		return math.MaxInt64
	}
	slices.Sort(v)
	medianIndex := (len(v) - 1) / 2
	return v[medianIndex]
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

func (l *leaderMode) replicationWorker(member string, followerState *followerState, pulse time.Duration) {
	beat := time.NewTicker(pulse)
	defer beat.Stop()
	for {
		select {
		case <-followerState.ctx.Done():
			return
		case <-beat.C:
			l.replicate(followerState, member)
		case <-followerState.newAppendEntry:
			l.replicate(followerState, member)
		}
	}
}

func (l *leaderMode) replicate(mp *followerState, member string) error {
	replicaMatchIndex := mp.matchIndex.Load()
	leadersNextIndex := l.node.nextIndexs.Load()
	leadersCommitedIndex := l.node.lastCommittedIndex.Load()
	leadersCurrentTerm := l.node.currentTerm.Load()
	clusterMembers := l.node.clusterManager.GetClusterMembers().members
	if l.node.snapshotIndex.Load() > replicaMatchIndex {
		if _, err := l.node.transport.InstallSnapshot(member, rafttypes.InstallSnapshotInput{}); err != nil {
			l.node.log.Error("install snapshot failed", zap.Error(err), zap.String("member", string(member)))
		}
		return nil
	}

	replicationTargetIndex := min(leadersNextIndex, replicaMatchIndex+inputEntriesCap)

	resp, err := l.appendLog(clusterMembers, replicationTargetIndex, leadersCommitedIndex, leadersCurrentTerm, replicaMatchIndex, member)
	if err != nil && !errors.Is(err, ErrAppendEntryMisMatch) {
		l.node.log.Error("error occured while trying to append entry", zap.Error(err), zap.String("member", string(member)), zap.String("member_ip", clusterMembers[member].ip))
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

func (l *leaderMode) appendLog(clusterMembers map[string]memberDetails, replicationTargetIndex int64, leadersCommitedIndex int64, leadersCurrentTerm int64, startIndex int64, member string) (*rafttypes.AppendEntiresResponse, error) {
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
			l.node.log.Info("appending entry to follower failed, due to follower having greater term", zap.String("member", string(member)), zap.String("member_ip", clusterMembers[member].ip))
			l.stepDown(resp.Term)
			return nil, ErrMembersCurrentTermHigher
		}

		l.node.log.Info("appending entry to follower failed", zap.String("member", string(member)), zap.String("member_ip", clusterMembers[member].ip))
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
