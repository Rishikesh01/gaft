package node

import "github.com/Rishikesh01/gaft/pkg/rafttypes"

type appendLogEntriesLeaderRsp struct {
	promise *Waiter
}

type Proposal struct {
	RequestID string
	Data      []byte
}

type ProposeClusterResize struct {
	RequestID string
	Data      []rafttypes.RaftClusterState
}
