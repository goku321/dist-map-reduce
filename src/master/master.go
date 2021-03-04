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

type taskStatus int
type phase int

// Master defines a master process.
type Master struct {
	tasks         []string
	mapTasks      []Task
	reduceTasks   []Task
	done          chan struct{}
	mutex         sync.RWMutex
	nReduce       int
	timeout       chan *Task
	phase         phase
	phaseMutex    sync.Mutex
	cancelers     []context.CancelFunc
	cancelerMutex sync.Mutex
}

// Task represents a task to be done.
type Task struct {
	file    string
	nReduce int
	f       []string
	ticker  *time.Timer
	status  taskStatus
}

// New creates a new Master instance.
func New(files []string, nReduce int) *Master {
	ch := make(chan struct{}, 0)
	var mapTasks []Task
	for _, f := range files {
		t := Task{
			file:    f,
			nReduce: nReduce,
			status:  pending,
		}
		mapTasks = append(mapTasks, t)
	}
	return &Master{
		tasks:    files,
		mapTasks: mapTasks,
		done:     ch,
		mutex:    sync.RWMutex{},
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

		reply.Files = append(reply.Files, mt.file)
		reply.NReduce = mt.nReduce
		reply.Type = model.Map

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		m.cancelerMutex.Lock()
		m.cancelers = append(m.cancelers, cancel)
		m.cancelerMutex.Unlock()
		// Start the timer to keep track of the task.
		go func(ctx context.Context, timeout chan *Task, task *Task) {
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
			if task.file == args.File && task.status == inprogress {
				task.status = completed
				task.f = append(task.f, args.OutFiles...)
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
		if t.status == pending || t.status == inprogress {
			return false
		}
	}
	return true
}

func (m *Master) hasReducePhaseCompleted() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, t := range m.reduceTasks {
		if t.status == pending || t.status == inprogress {
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
				if t.file == task.file {
					t.status = pending
					m.mapTasks[i] = *t
				}
			}
			m.mutex.Unlock()
		}
	}
}

// Returns a pending task in a (not)thread-safe manner.
func (m *Master) getPendingMapTask() *Task {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for i, t := range m.mapTasks {
		if t.status == pending {
			t.status = inprogress
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
	// log.Info("gRPC server listening on :8080")
	<-m.Done()
}
