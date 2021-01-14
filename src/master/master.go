package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/goku321/dist-map-reduce/src/model"
	log "github.com/sirupsen/logrus"
)

// Master defines a master process.
type Master struct {
	tasks       []string
	mapTasks    []string
	reduceTasks []string
	done        chan struct{}
	mutex       sync.RWMutex
	nReduce     int
}

// Task represents a task to be done.
type Task struct {
	file   string
	f      []string
	ticker time.Ticker
}

// New creates a new Master instance.
func New(files []string, nReduce int) *Master {
	ch := make(chan struct{}, 0)
	return &Master{
		tasks: files,
		done:  ch,
		mutex: sync.RWMutex{},
	}
}

// GetWork assigns work to worker nodes.
func (m *Master) GetWork(args *model.Args, reply *model.MapTask) error {
	// Handle assigning both map and reduce tasks.
	log.Infof("worker %s asking for work", args.ID)
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if len(m.tasks) > 0 {
		t := m.tasks[0]
		reply.File = t
		reply.NReduce = m.nReduce
	}
	return nil
}

// Done signal if the entire job is done.
func (m *Master) Done() chan struct{} {
	return m.done
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
	go http.Serve(l, nil)
	// log.Info("gRPC server listening on :8080")
	<-m.Done()
}
