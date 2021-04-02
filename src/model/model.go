package model

import "errors"

const (
	Pending = iota
	InProgress
	Completed
)

// ErrNoPendingTask is used when tasks queue is empty.
var ErrNoPendingTask = errors.New("no pending task")

// Args defines a type for RPC exchange.
type Args struct {
	WorkerID string
	File     string
}

// Reply defines a type for RPC exchange.
type Reply struct {
	File string
}

const (
	// Map represents map task.
	Map = iota
	// Reduce represent reduce task.
	Reduce
	// Shutdown represents completion stage.
	// Can be used to signal workers to exit.
	Shutdown
)

type Status int

// Task represent a map or reduce task to used for rpc exchange.
type Task struct {
	Files   []string
	Type    int
	NReduce int // Number of reduce tasks.
	Status  Status
}

// TaskStatus represents status of a task.
type TaskStatus struct {
	Success  bool
	File     string
	OutFiles []string
}
