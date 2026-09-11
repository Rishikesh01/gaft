package node

type raftClusterState struct {
	nodeName string
	nodeIp   string
	nodeRole NodeRole
	isMember bool
}

type appendLogEntriesLeaderRsp struct {
	commit bool
}

type Proposal struct {
	RequestID string
	Data      []byte
}
