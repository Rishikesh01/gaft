package transport

import (
	"errors"
	"fmt"
	"net/rpc"
	"time"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
)

var ErrCallTimeout = errors.New("rpc call timed out")

func (r *rpcTransport) rpcCall(member string, method string, args any, reply any) error {
	p, ok := (*r.peers.Load())[member]
	if !ok {
		return fmt.Errorf("unknown member %q", member)
	}
	client, err := p.get()
	if err != nil {
		return err
	}

	call := client.Go("node."+method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if errors.Is(call.Error, rpc.ErrShutdown) {
			p.drop(client)
		}
		return call.Error
	case <-time.After(r.timeout):
		return ErrCallTimeout
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}

func (r *rpcTransport) AppendEntries(member string, input rafttypes.AppendEntriesInput) (rafttypes.AppendEntiresResponse, error) {
	var resp rafttypes.AppendEntiresResponse
	if err := r.rpcCall(member, "AppendEntries", input, &resp); err != nil {
		return rafttypes.AppendEntiresResponse{}, err
	}
	return resp, nil
}

func (r *rpcTransport) RequestVote(member string, input rafttypes.RequestVoteInput) (rafttypes.RequestVoteResponse, error) {
	var resp rafttypes.RequestVoteResponse
	if err := r.rpcCall(member, "RequestVote", input, &resp); err != nil {
		return rafttypes.RequestVoteResponse{}, err
	}
	return resp, nil
}

func (r *rpcTransport) UpdatePeers(peersMap map[string]string) {
	old := *r.peers.Load()
	peers := make(map[string]*peer, len(peersMap))
	for member, address := range peersMap {
		if p, ok := old[member]; ok && p.address == address {
			peers[member] = p
			continue
		}
		peers[member] = &peer{address: address}
	}
	r.peers.Store(&peers)
	for member, p := range old {
		if peers[member] != p {
			p.mu.Lock()
			if p.client != nil {
				p.client.Close()
			}
			p.mu.Unlock()
		}
	}
}

// InstallSnapshot implements [node.Transport].
func (r *rpcTransport) InstallSnapshot(member string, input rafttypes.InstallSnapshotInput) (rafttypes.InstallSnapshotResponse, error) {
	panic("unimplemented")
}
