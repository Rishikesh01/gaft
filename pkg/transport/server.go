package transport

import (
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/Rishikesh01/gaft/pkg/node"
	"go.uber.org/zap"
)

type sender struct {
	mu             sync.Mutex
	clusterMembers map[string]string
	clients        map[string]*rpc.Client
}

func NewSender(clusterMembers map[string]string) *sender {
	return &sender{
		mu:             sync.Mutex{},
		clusterMembers: clusterMembers,
		clients:        map[string]*rpc.Client{},
	}
}

func Run(logger zap.SugaredLogger, clusterNode *node.ClusterNode) {
	server := rpc.NewServer()
	port := ":9100"
	server.Register(clusterNode)
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
