package main

import (
	"fmt"
	"net/rpc"
	"os"
	"plugin"

	"github.com/goku321/dist-map-reduce/src/model"
	log "github.com/sirupsen/logrus"
)

// Worker defines a worker process.
type Worker struct {
}

// KeyValue represents a key-value pair.
type KeyValue struct {
	Key   string
	Value string
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
	log.SetFormatter(&log.JSONFormatter{})
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: worker app.so\n")
		os.Exit(1)
	}
	w := New()
	w.Start()
}

// Load the application map and reduce function from a plugin file.
func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
