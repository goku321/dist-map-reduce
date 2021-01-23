package main

import (
	"context"
	"errors"
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
	mapTasks    []Task
	reduceTasks []string
	done        chan struct{}
	mutex       sync.RWMutex
	nReduce     int
	timeout     chan string
}

// Task represents a task to be done.
type Task struct {
	file    string
	nReduce int
	f       []string
	ticker  *time.Timer
}

// New creates a new Master instance.
func New(files []string, nReduce int) *Master {
	ch := make(chan struct{}, 0)
	var mapTasks []Task
	for _, f := range files {
		t := Task{
			file:    f,
			nReduce: nReduce,
		}
		mapTasks = append(mapTasks, t)
	}
	return &Master{
		tasks:    files,
		mapTasks: mapTasks,
		done:     ch,
		mutex:    sync.RWMutex{},
	}
}

// GetWork assigns work to worker nodes.
func (m *Master) GetWork(args *model.Args, reply *model.MapTask) error {
	// Handle assigning both map and reduce tasks.
	log.Infof("worker %s asking for work", args.ID)
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if len(m.mapTasks) > 0 {
		t := m.mapTasks[0]
		reply.File = t.file
		reply.NReduce = t.nReduce

		// Start the timer to keep track of the task.
		go func(timeout chan string, file string) {
			t := time.NewTimer(time.Second * 10)
			defer t.Stop()
			select {
			case <-t.C:
				timeout <- file
			}
		}(m.timeout, t.file)

		return nil
	}
	return errors.New("task not available")
}

// Done signal if the entire job is done.
func (m *Master) Done() chan struct{} {
	return m.done
}

// SignalTaskStatus will be called by the worker to signal the task's status.
func (m *Master) SignalTaskStatus(args *model.TaskStatus, reply *bool) error {
	if !args.Success {

	}
	return nil
}

func (m *Master) checkTimeout(ctx context.Context) {
	for {
		select {
		case <-m.timeout:
			m.mutex.Lock()
			// Put the task back in queue.
			m.mutex.Unlock()
		}
	}
}

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: master inputfiles...\n")
		os.Exit(1)
	}
	m := New(os.Args[1:], 4)
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
