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

// MapTask represent a map task to used for rpc exchange.
type MapTask struct {
	File    string
	NReduce int // Number of reduce tasks.
}

// TaskStatus represents status of a task.
type TaskStatus struct {
	Success  bool
	WorkerID string
	Files    []string
}
