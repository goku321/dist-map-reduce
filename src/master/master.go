package main

import (
	"net"
	"net/http"
	"net/rpc"

	log "github.com/sirupsen/logrus"
	"github.com/goku321/dist-map-reduce/src/model"
)

// Master defines a master process.
type Master struct {
	Files []string
}

// New creates a new Master instance.
func New() *Master {
	return &Master{}
}

// GetWork assigns work to worker nodes.
func (m *Master) GetWork(args *model.Args, reply *model.Reply) error {
	log.Infof("worker node asking for work")
	return nil
}

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	m := New()
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":8080")
	if e != nil {
		log.WithFields(log.Fields{
			"err": e,
		}).Fatal("failed to start gRPC server")
	}
	http.Serve(l, nil)
	// log.Info("gRPC server listening on :8080")
}
