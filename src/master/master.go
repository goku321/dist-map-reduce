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

const (
	pending = iota
	inprogress
	completed
)

// ErrNoPendingTask is used when tasks queue is empty.
var ErrNoPendingTask = errors.New("no pending task")

type phase int

// Master defines a master process.
type Master struct {
	tasks         []string
	mapTasks      []model.Task
	reduceTasks   []model.Task
	done          chan struct{}
	mutex         sync.RWMutex
	nReduce       int
	timeout       chan *model.Task
	phase         phase
	phaseMutex    sync.Mutex
	cancelers     []context.CancelFunc
	cancelerMutex sync.Mutex
}

// New creates a new Master instance.
func New(files []string, nReduce int) *Master {
	ch := make(chan struct{})
	timeoutCh := make(chan *model.Task)
	var mapTasks []model.Task
	for _, f := range files {
		t := model.Task{
			Files:   []string{f},
			NReduce: nReduce,
			Type:    model.Map,
		}
		mapTasks = append(mapTasks, t)
	}
	return &Master{
		tasks:    files,
		mapTasks: mapTasks,
		done:     ch,
		mutex:    sync.RWMutex{},
		timeout:  timeoutCh,
		phase:    model.Map,
	}
}

// GetWork assigns work to worker nodes.
func (m *Master) GetWork(args *model.Args, reply *model.Task) error {
	// Handle assigning both map and reduce tasks.
	log.Infof("worker %s asking for work", args.WorkerID)

	if m.phase == model.Map {
		// Hand over a map task.
		mt := m.getPendingMapTask()
		if mt == nil {
			return ErrNoPendingTask
		}

		reply.Files = append(reply.Files, mt.Files[0])
		reply.NReduce = mt.NReduce
		reply.Type = model.Map

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		m.cancelerMutex.Lock()
		m.cancelers = append(m.cancelers, cancel)
		m.cancelerMutex.Unlock()
		// Start the timer to keep track of the task.
		go func(ctx context.Context, timeout chan *model.Task, task *model.Task) {
			t := time.NewTimer(time.Second * 10)
			defer t.Stop()
			select {
			case <-t.C:
				timeout <- task
			case <-ctx.Done():
				return
			}
		}(ctx, m.timeout, mt)
	} else if m.phase == model.Reduce {
		// Hand over a reduce task.
	} else if m.phase == model.Shutdown {
		// Signal workers to exit.
		reply.Type = model.Shutdown
		return nil
	}

	return ErrNoPendingTask
}

// Done signal if the entire job is done.
func (m *Master) Done() chan struct{} {
	return m.done
}

// SignalTaskStatus will be called by the worker to signal the task's status.
func (m *Master) SignalTaskStatus(args *model.TaskStatus, reply *bool) error {
	if !args.Success {
		return nil
	}

	if m.phase == model.Map {
		log.Infof("map phase for %s completed", args.File)
		m.mutex.Lock()
		defer m.mutex.Unlock()
		for i, task := range m.mapTasks {
			if task.Files[0] == args.File && task.Status == inprogress {
				task.Status = completed
				task.Files = append(task.Files, args.OutFiles...)
				m.mapTasks[i] = task
				break
			}
		}
	} else if m.phase == model.Reduce {

	}
	// check if all the tasks are done.
	return nil
}

func (m *Master) checkStatus(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if m.phase == model.Map && m.hasMapPhaseCompleted() {
				m.phaseMutex.Lock()
				defer m.phaseMutex.Unlock()
				m.phase = model.Reduce
			} else if m.phase == model.Reduce && m.hasReducePhaseCompleted() {
				m.phaseMutex.Lock()
				defer m.phaseMutex.Unlock()
				m.phase = model.Shutdown
			}
		case <-ctx.Done():
			return
		}
	}
}

func (m *Master) hasMapPhaseCompleted() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, t := range m.mapTasks {
		if t.Status == pending || t.Status == inprogress {
			return false
		}
	}
	return true
}

func (m *Master) hasReducePhaseCompleted() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, t := range m.reduceTasks {
		if t.Status == pending || t.Status == inprogress {
			return false
		}
	}
	return true
}

func (m *Master) checkTimeout(ctx context.Context) {
	for {
		select {
		case t := <-m.timeout:
			m.mutex.Lock()
			// Change the task status back to "pending".
			// Todo: use a map for tasks to avoid iteration.
			for i, task := range m.mapTasks {
				if t.Files[0] == task.Files[0] {
					t.Status = pending
					m.mapTasks[i] = *t
				}
			}
			m.mutex.Unlock()
		}
	}
}

// Returns a pending task in a (not)thread-safe manner.
func (m *Master) getPendingMapTask() *model.Task {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for i, t := range m.mapTasks {
		if t.Status == pending {
			t.Status = inprogress
			m.mapTasks[i] = t
			return &t
		}
	}
	return nil
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go m.checkStatus(ctx)
	go m.checkTimeout(ctx)
	// log.Info("gRPC server listening on :8080")
	<-m.Done()
}
