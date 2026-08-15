package transport

import (
	"github.com/Rishikesh01/gaft/pkg/node"
	"github.com/Rishikesh01/gaft/pkg/rafttypes"
	"go.uber.org/zap"
)

type rpcHandler struct {
	node   *node.ClusterNode
	logger zap.SugaredLogger
}

func (r *rpcHandler) AppendEntries(args rafttypes.AppendEntriesInput, reply *rafttypes.AppendEntiresResponse) error {
	resp, err := r.node.AppendEntries(args)
	if err != nil {
		return err
	}
	*reply = *resp
	return nil
}

func (r *rpcHandler) RequestVote(args rafttypes.RequestVoteInput, reply *rafttypes.RequestVoteResponse) error {
	resp, err := r.node.RequestVote(args)
	if err != nil {
		return err
	}
	*reply = *resp
	return nil
}
