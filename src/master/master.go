package main

import (
	"net"
	"net/http"
	"net/rpc"

	log "github.com/sirupsen/logrus"
)

// Master defines a master process.
type Master struct {
	Files []string
}

// Args defines a type for RPC exchange.
type Args struct{}

// Reply defines a type for RPC exchange.
type Reply struct{}

// NewMaster creates a new Master instance.
func NewMaster() *Master {
	return &Master{}
}

// StartServer starts a gRPC server.
func (m *Master) StartServer() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":8080")
	if e != nil {
		log.WithFields(log.Fields{
			"err": e,
		}).Fatal("failed to start gRPC server")
	}
	go http.Serve(l, nil)
}

// GetWork assigns work to worker nodes.
func (m *Master) GetWork(args *Args, reply *Reply) error {
	return nil
}

func main() {}
