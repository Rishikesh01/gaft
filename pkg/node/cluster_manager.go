package node

import (
	"sync/atomic"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
)

type (
	memberState string
)

const (
	memberStateMember memberState = "member"
	memberStateExiter memberState = "exiter"
	memberStateJoiner memberState = "new_joiner"
	memberStateLeaner memberState = "leaner"
)

func (m memberState) inOldSet() bool {
	return m == memberStateMember || m == memberStateExiter
}

func (m memberState) inNewSet() bool {
	return m == memberStateJoiner || m == memberStateMember
}

type clusterMemberConfig struct {
	members    map[string]memberDetails
	oldMembers uint64
	newMembers uint64
}

type clusterMemberManager struct {
	cfg              atomic.Pointer[clusterMemberConfig]
	resizeInProgress atomic.Bool
}

type memberDetails struct {
	ip    string
	state memberState
}

func NewClusterMemberManager(members map[string]string) *clusterMemberManager {
	cmm := &clusterMemberManager{}
	mmap := make(map[string]memberDetails)

	for member, ip := range members {
		mmap[member] = memberDetails{
			ip:    ip,
			state: memberStateMember,
		}
	}

	cmm.cfg.Store(&clusterMemberConfig{
		members:    mmap,
		oldMembers: uint64(len(members)),
		newMembers: uint64(len(members)),
	})
	return cmm
}

func (c *clusterMemberManager) GetClusterMembers() *clusterMemberConfig {
	return c.cfg.Load()
}

func deepCopyMap(cfg *clusterMemberConfig) *clusterMemberConfig {
	newCfg := &clusterMemberConfig{
		members:    make(map[string]memberDetails, len(cfg.members)),
		oldMembers: cfg.oldMembers,
		newMembers: cfg.newMembers,
	}

	for name, details := range cfg.members {
		newCfg.members[name] = details
	}

	return newCfg
}

func (c *clusterMemberManager) IsResizeInProgress() bool {
	return c.resizeInProgress.Load()
}

func (c *clusterMemberManager) ProcessResizeClusterEvent(stateChanges []rafttypes.RaftClusterState) {
	mmap := deepCopyMap(c.GetClusterMembers())
	// validation of resize type will be done at http controller level
	for _, stateChange := range stateChanges {
		if val, ok := mmap.members[stateChange.NodeName]; ok {
			val.state = memberStateExiter
			mmap.members[stateChange.NodeName] = val
			mmap.newMembers--
			continue
		}
		mmap.members[stateChange.NodeName] = memberDetails{
			ip:    stateChange.NodeIP,
			state: memberStateLeaner,
		}
	}

	c.resizeInProgress.Store(true)
	c.cfg.Store(mmap)
}
