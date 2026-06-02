package transport

import (
	"log"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"

	"github.com/Rishikesh01/gaft/pkg/node"
	"github.com/Rishikesh01/gaft/pkg/rafttypes"
	"go.uber.org/zap"
)

var _ node.Sender = (*sender)(nil)

type sender struct {
	mu           sync.Mutex
	clusterPeers atomic.Pointer[map[string]*peer]
}

type peer struct {
	client    *rpc.Client
	isRemoved atomic.Bool
	address   string
	mu        sync.Mutex
}

func NewSender() *sender {
	sender := &sender{
		mu:           sync.Mutex{},
		clusterPeers: atomic.Pointer[map[string]*peer]{},
	}
	defaultMap := make(map[string]*peer)
	sender.clusterPeers.Store(&defaultMap)

	return sender
}

// AppendEntries implements [node.Sender].
func (s *sender) AppendEntries(memeber string, input rafttypes.AppendEntriesInput) (rafttypes.AppendEntiresResponse, error) {
	panic("unimplemented")
}

// RequestVote implements [node.Sender].
func (s *sender) RequestVote(memeber string, input rafttypes.RequestVoteInput) (rafttypes.RequestVoteResponse, error) {
	panic("unimplemented")
}

// UpdatePeers implements [node.Sender].
func (s *sender) UpdatePeers(map[string]string) {
	panic("unimplemented")
}

func Run(logger zap.SugaredLogger, clusterNode node.Handler) {
	server := rpc.NewServer()
	port := ":9100"
	server.Register(&rpcHandler{
		node:   clusterNode,
		logger: logger,
	})
	ln, err := net.Listen("tcp", port)
	if err != nil {
		logger.Panic("failed to listen at tcp port:", zap.Error(err))
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go server.ServeConn(conn)
	}
}
