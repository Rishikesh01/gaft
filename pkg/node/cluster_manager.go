package node

import "sync/atomic"

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

type ClusterMemberManager struct {
	cfg atomic.Pointer[clusterMemberConfig]
}

type memberDetails struct {
	ip    string
	state memberState
}

func NewClusterMemberManager(members map[string]string) *ClusterMemberManager {
	cmm := &ClusterMemberManager{}
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

func (c *ClusterMemberManager) GetClusterMembers() *clusterMemberConfig {
	return c.cfg.Load()
}
