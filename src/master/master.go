package master

import "net/rpc"

// Master defines a master process.
type Master struct {
	Files []string
}

// Args defines a type for RPC exchange.
type Args struct {}

// Reply defines a type for RPC exchange.
type Reply struct {}

// StartServer starts a gRPC server.
func (m *Master) StartServer() {
	rpc.Register(m)
	rpc.HandleHTTP()
}

// GetWork assigns work to worker nodes.
func (m *Master) GetWork(args *Args, reply *Reply) error {
	return nil
}