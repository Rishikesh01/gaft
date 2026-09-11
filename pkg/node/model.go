package node

type ResizeType string

const (
	ResizeAdd    = "add"
	ResizeRemove = "remove"
)

type RaftClusterState struct {
	NodeName string
	NodeIp   string
	Change   ResizeType
}

type appendLogEntriesLeaderRsp struct {
	commit bool
}

type Proposal struct {
	RequestID string
	Data      []byte
}

type ProposeClusterResize struct {
	RequestID string
	Data      []RaftClusterState
}
