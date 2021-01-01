package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"github.com/goku321/dist-map-reduce/src/model"
	log "github.com/sirupsen/logrus"
)

// Master defines a master process.
type Master struct {
	Files []string
}

// New creates a new Master instance.
func New(files []string, nReduce int) *Master {
	return &Master{}
}

// GetWork assigns work to worker nodes.
func (m *Master) GetWork(args *model.Args, reply *model.Reply) error {
	log.Infof("worker node asking for work")
	return nil
}

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: master inputfiles...\n")
		os.Exit(1)
	}
	m := New(os.Args[1:], 10)
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
