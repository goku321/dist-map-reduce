package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
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

// Maximum time to wait for a task completion.
var taskTimeout = 10 * time.Second

// Shorthand to convert integer to string type.
var toString = fmt.Sprint

// Represents current phase of master - map/reduce.
type phase int

// Master defines a master process.
type Master struct {
	mapTasks      map[string]model.Task
	reduceTasks   map[string]model.Task
	done          chan struct{}
	mutex         sync.RWMutex
	timeout       chan struct{ taskName, taskType string }
	phase         phase
	phaseMutex    sync.Mutex
	cancelers     []context.CancelFunc
	cancelerMutex sync.Mutex
}

// New creates a new Master instance.
//
// files is the list of files to perform map phase on.
//
// nReduce is the number of reduce tasks.
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
	reduceTasks := map[string]model.Task{}
	for i := 1; i <= nReduce; i++ {
		reduceTasks[toString(i)] = model.Task{
			Type:   model.Reduce,
			Status: pending,
		}
	}

	return &Master{
		mapTasks:    mapTasks,
		reduceTasks: reduceTasks,
		done:        make(chan struct{}),
		mutex:       sync.RWMutex{},
		timeout:     make(chan struct{ taskName, taskType string }),
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

		// todo: use a type instead of literal for task type.
		m.watch(path.Base(mt.Files[0]), "map")
		return nil
	} else if m.phase == model.Reduce {
		// Hand over a reduce task.
		name, rt := m.getPendingReduceTask()
		if rt == nil {
			return model.ErrNoPendingTask
		}

		reply.Type = model.Reduce
		reply.Files = rt.Files
		// todo: use a type instead of literal for task type
		m.watch(name, "reduce")
		return nil
	} else if m.phase == model.Shutdown {
		// Signal workers to exit.
		reply.Type = model.Shutdown
		return nil
	}
	return errors.New("unknown rpc")
}

// watch over task for a given duration.
func (m *Master) watch(taskName, taskType string) {
	ctx, cancel := context.WithCancel(context.Background())
	// Start the timer to keep track of the task.
	go func(ctx context.Context, timeout chan struct{ taskName, taskType string }, task string) {
		t := time.NewTimer(taskTimeout)
		defer t.Stop()
		select {
		case <-t.C:
			timeout <- struct {
				taskName string
				taskType string
			}{taskName, taskType}
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
				key := toString(i + 1)
				t := m.reduceTasks[key]
				t.Files = append(t.Files, v)
				m.reduceTasks[key] = t
			}
		}
	} else if m.phase == model.Reduce {
		log.Infof("reduce phase %s completed", args.File)
		i, _ := strconv.ParseInt(args.File, 10, 32)
		key := toString(i + 1)
		m.mutex.Lock()
		defer m.mutex.Unlock()
		if t, ok := m.reduceTasks[key]; ok {
			if t.Status == inprogress {
				t.Status = completed
				m.reduceTasks[key] = t
			}
		}
	}

	return nil
}

func (m *Master) checkStatus(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if m.phase == model.Map && m.hasMapPhaseCompleted() {
				m.updatePhase(model.Reduce)
			} else if m.phase == model.Reduce && m.hasReducePhaseCompleted() {
				m.updatePhase(model.Shutdown)
			} else if m.phase == model.Shutdown {
				log.Info("map/reduce phase completed. master will shutdown...")
				m.done <- struct{}{}
			}
		case <-ctx.Done():
			return
		}
	}
}

// updates phase atomically.
func (m *Master) updatePhase(p phase) {
	m.phaseMutex.Lock()
	defer m.phaseMutex.Unlock()
	m.phase = p
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
			if t.taskType == "map" {
				if task, ok := m.mapTasks[t.taskName]; ok && task.Status != completed {
					task.Status = pending
					m.mapTasks[t.taskName] = task
				}
			} else if t.taskType == "reduce" {
				if task, ok := m.mapTasks[t.taskName]; ok && task.Status != completed {
					task.Status = pending
					m.mapTasks[t.taskName] = task
				}
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
func (m *Master) getPendingReduceTask() (string, *model.Task) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for k, t := range m.reduceTasks {
		if t.Status == pending {
			t.Status = inprogress
			m.reduceTasks[k] = t
			return k, &t
		}
	}
	return "", nil
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

	dir := os.Args[1]
	// Read the filenames from the input directory.
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatalf("failed to read contents of the directory: %s", dir)
	}

	fnames := make([]string, len(files))
	for i, f := range files {
		fnames[i] = filepath.Join(dir, f.Name())
	}

	m := New(fnames, 4)
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
