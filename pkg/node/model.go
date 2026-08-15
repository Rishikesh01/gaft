package node

import "github.com/Rishikesh01/gaft/pkg/rafttypes"

type appendEntriesRspFrom struct {
	rsp               rafttypes.AppendEntiresResponse
	error             error
	clusterMemberIP   string
	clusterMemberName string
}

type appendLogEntriesLeaderRsp struct {
	commit bool
}

type Proposal struct {
	RequestID string
	Data      []byte
}
