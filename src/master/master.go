package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/goku321/dist-map-reduce/src/model"
	log "github.com/sirupsen/logrus"
)

const (
	pending = iota
	inprogress
	completed
)

// Represents current phase of master - map/reduce.
type phase int

// Master defines a master process.
type Master struct {
	mapTasks      map[string]model.Task
	reduceTasks   map[int]model.Task
	done          chan struct{}
	mutex         sync.RWMutex
	timeout       chan string
	phase         phase
	phaseMutex    sync.Mutex
	cancelers     []context.CancelFunc
	cancelerMutex sync.Mutex
}

// New creates a new Master instance.
func New(files []string, nReduce int) *Master {
	mapTasks := map[string]model.Task{}
	for _, f := range files {
		t := model.Task{
			Files:   []string{f},
			NReduce: nReduce,
			Type:    model.Map,
			Status:  pending,
		}
		// Sanitize filename.
		mapTasks[path.Base(f)] = t
	}

	// Create empty reduce tasks.
	reduceTasks := map[int]model.Task{}
	for i := 1; i <= nReduce; i++ {
		reduceTasks[i] = model.Task{
			Type:   model.Reduce,
			Status: pending,
		}
	}

	return &Master{
		mapTasks:    mapTasks,
		reduceTasks: reduceTasks,
		done:        make(chan struct{}),
		mutex:       sync.RWMutex{},
		timeout:     make(chan string),
		phase:       model.Map,
	}
}

// GetWork assigns work to worker nodes.
func (m *Master) GetWork(args *model.Args, reply *model.Task) error {
	// Handle assigning both map and reduce tasks.
	log.Infof("[%d] worker %s asking for work", m.phase, args.WorkerID)

	if m.phase == model.Map {
		// Hand over a map task.
		mt := m.getPendingMapTask()
		if mt == nil {
			return model.ErrNoPendingTask
		}

		reply.Files = append(reply.Files, mt.Files[0])
		reply.NReduce = mt.NReduce
		reply.Type = model.Map

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		m.cancelerMutex.Lock()
		m.cancelers = append(m.cancelers, cancel)
		m.cancelerMutex.Unlock()
		// Start the timer to keep track of the task.
		go func(ctx context.Context, timeout chan string, task string) {
			t := time.NewTimer(time.Second * 10)
			defer t.Stop()
			select {
			case <-t.C:
				timeout <- task
			case <-ctx.Done():
				return
			}
		}(ctx, m.timeout, path.Base(mt.Files[0]))

		return nil
	} else if m.phase == model.Reduce {
		// Hand over a reduce task.
		rt := m.getPendingReduceTask()
		if rt == nil {
			return model.ErrNoPendingTask
		}

		reply.Type = model.Reduce
		reply.Files = rt.Files
		// ?Watch over reduce task for 10 seconds.
		return nil
	} else if m.phase == model.Shutdown {
		// Signal workers to exit.
		reply.Type = model.Shutdown
		return nil
	}
	return errors.New("unknown rpc")
}

// watch over task for a given duration.
func (m *Master) watch(taskName string, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	// Start the timer to keep track of the task.
	go func(ctx context.Context, timeout chan string, task string) {
		t := time.NewTimer(time.Second * 10)
		defer t.Stop()
		select {
		case <-t.C:
			timeout <- task
		case <-ctx.Done():
			return
		}
	}(ctx, m.timeout, taskName)

	// Append to the cancelers slice.
	m.cancelerMutex.Lock()
	defer m.cancelerMutex.Unlock()
	m.cancelers = append(m.cancelers, cancel)
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
		f := path.Base(args.File)
		if t, ok := m.mapTasks[f]; ok {
			if t.Status == inprogress {
				t.Status = completed
				t.Files = append(t.Files, args.OutFiles...)
				m.mapTasks[f] = t
			}

			// Build up reduce tasks.
			for i, v := range args.OutFiles {
				t := m.reduceTasks[i+1]
				t.Files = append(t.Files, v)
				m.reduceTasks[i+1] = t
			}
		}
	} else if m.phase == model.Reduce {
		log.Infof("reduce phase %s completed", args.File)
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
			if task, ok := m.mapTasks[t]; ok && task.Status != completed {
				task.Status = pending
				m.mapTasks[t] = task
			}
			m.mutex.Unlock()
		}
	}
}

// Returns a pending map task in a thread-safe manner.
func (m *Master) getPendingMapTask() *model.Task {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for k, t := range m.mapTasks {
		if t.Status == pending {
			t.Status = inprogress
			m.mapTasks[k] = t
			return &t
		}
	}
	return nil
}

// Returns a pending reduce task in a thread-safe manner.
func (m *Master) getPendingReduceTask() *model.Task {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for k, t := range m.reduceTasks {
		if t.Status == pending {
			t.Status = inprogress
			m.reduceTasks[k] = t
			return &t
		}
	}
	return nil
}

// Handles signals.
func (m *Master) signalHandler(ctx context.Context, srv *http.Server) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	defer signal.Stop(sigs)

	<-sigs

	// Stop all the running goroutines.
	for _, cancel := range m.cancelers {
		cancel()
	}
	log.Infof("let me gracefully shutdown myself...")
	srv.Shutdown(ctx)
	m.done <- struct{}{}
}

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: master inputfiles...\n")
		os.Exit(1)
	}
	m := New(os.Args[1:], 4)
	rpcSrv := rpc.NewServer()
	if err := rpcSrv.Register(m); err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("failed to create rpc server")
	}

	srv := &http.Server{
		Addr:    ":8080",
		Handler: rpcSrv,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("failed to start rpc server")
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	// Add cancel() to cancelers so that it will be called eventually.
	m.cancelerMutex.Lock()
	m.cancelers = append(m.cancelers, cancel)
	m.cancelerMutex.Unlock()

	go m.checkStatus(ctx)
	go m.checkTimeout(ctx)
	go m.signalHandler(ctx, srv)

	<-m.Done()
}
