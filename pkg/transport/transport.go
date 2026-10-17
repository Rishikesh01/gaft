package transport

import (
	"context"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rishikesh01/gaft/pkg/node"
	"github.com/Rishikesh01/gaft/pkg/rafttypes"
	"go.uber.org/zap"
)

var _ node.Transport = (*rpcTransport)(nil)

type peer struct {
	address string
	mu      sync.Mutex
	client  *rpc.Client
}

type rpcTransport struct {
	ctx        context.Context
	logger     zap.SugaredLogger
	listenAddr string
	timeout    time.Duration
	receiverCh chan rafttypes.RPC
	peers      atomic.Pointer[map[string]*peer]
}

func NewNetTransport(ctx context.Context, logger zap.SugaredLogger, listenAddr string, timeout time.Duration) *rpcTransport {
	t := &rpcTransport{
		ctx:        ctx,
		logger:     logger,
		listenAddr: listenAddr,
		timeout:    timeout,
		receiverCh: make(chan rafttypes.RPC, 1),
	}
	peers := make(map[string]*peer)
	t.peers.Store(&peers)
	return t
}

func (r *rpcTransport) Receiver() <-chan rafttypes.RPC {
	return r.receiverCh
}

func (r *rpcTransport) Serve() error {
	server := rpc.NewServer()
	if err := server.RegisterName("node", &rpcHandler{ctx: r.ctx, consumeCh: r.receiverCh}); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", r.listenAddr)
	if err != nil {
		return err
	}
	go func() {
		<-r.ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if r.ctx.Err() != nil {
				return nil
			}
			r.logger.Error("accept failed", zap.Error(err))
			continue
		}
		go server.ServeConn(conn)
	}
}

func (p *peer) get() (*rpc.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		return p.client, nil
	}
	client, err := rpc.Dial("tcp", p.address)
	if err != nil {
		return nil, err
	}
	p.client = client
	return client, nil
}

func (p *peer) drop(client *rpc.Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client == client {
		client.Close()
		p.client = nil
	}
}
