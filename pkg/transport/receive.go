package transport

import (
	"context"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
)

type rpcHandler struct {
	ctx       context.Context
	consumeCh chan<- rafttypes.RPC
}

func (r *rpcHandler) AppendEntries(args rafttypes.AppendEntriesInput, reply *rafttypes.AppendEntiresResponse) error {
	rpcReply, err := r.dispatch(rafttypes.RPC{
		AppendEntries: &args,
	})
	if err != nil {
		return err
	}
	*reply = *rpcReply.AppendEntries

	return nil
}

func (r *rpcHandler) RequestVote(args rafttypes.RequestVoteInput, reply *rafttypes.RequestVoteResponse) error {
	rpcReply, err := r.dispatch(rafttypes.RPC{
		RequestVote: &args,
	})
	if err != nil {
		return err
	}
	*reply = *rpcReply.RequestVote
	return nil
}

func (r *rpcHandler) dispatch(req rafttypes.RPC) (rafttypes.RPCReply, error) {
	req.Reply = make(chan rafttypes.RPCReply, 1)
	select {
	case r.consumeCh <- req:
	case <-r.ctx.Done():
		return rafttypes.RPCReply{}, r.ctx.Err()
	}

	select {
	case resp := <-req.Reply:
		return resp, resp.Err
	case <-r.ctx.Done():
		return rafttypes.RPCReply{}, r.ctx.Err()
	}
}
