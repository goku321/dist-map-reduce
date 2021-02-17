package model

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
)

// Task represent a map task to used for rpc exchange.
type Task struct {
	Files   []string
	Type    int
	NReduce int // Number of reduce tasks.
}

// TaskStatus represents status of a task.
type TaskStatus struct {
	Success  bool
	File     string
	OutFiles []string
}
