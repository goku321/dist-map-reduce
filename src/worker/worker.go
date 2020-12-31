package main

import (
	"net/rpc"

	"github.com/goku321/dist-map-reduce/src/model"
	log "github.com/sirupsen/logrus"
)

// Worker defines a worker process.
type Worker struct {
}

// New creates a new instance of Worker.
func New() *Worker {
	return &Worker{}
}

// Start starts a worker process.
func (w *Worker) Start() {
	client, err := rpc.DialHTTP("tcp", ":8080")
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("failed to start worker")
	}
	args := &model.Args{}
	reply := &model.Reply{}
	err = client.Call("Master.GetWork", args, reply)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Warn("error calling server's method")
	}
}

func main() {
	w := New()
	w.Start()
}
