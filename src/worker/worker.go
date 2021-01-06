package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"plugin"

	"github.com/goku321/dist-map-reduce/src/model"
	log "github.com/sirupsen/logrus"
)

type mapFunc func(string, string) []KeyValue
type reduceFunc func(string, []string) string

// Worker defines a worker process.
type Worker struct {
	client  *rpc.Client
	mapf    mapFunc
	reducef reduceFunc
}

// KeyValue represents a key-value pair.
type KeyValue struct {
	Key   string
	Value string
}

// New creates a new instance of Worker.
func New() (*Worker, error) {
	c, err := rpc.DialHTTP("tcp", ":8080")
	if err != nil {
		return &Worker{}, fmt.Errorf("failed to connect to rpc server: %s", err)
	}
	return &Worker{
		client: c,
	}, nil
}

// Start starts a worker process.
func (w *Worker) Start() {
	args := &model.Args{}
	reply := &model.Reply{}
	err := w.client.Call("Master.GetWork", args, reply)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Warn("error calling server's method")
	}
	log.Infof("starting map phase on file: %s", reply.File)
	mapf, reducef := loadPlugin("")
	w.mapf = mapf
	w.reducef = reducef
	w.startMap(reply.File)
}

func (w *Worker) startMap(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("cannot open %s: %s", file, err)
	}
	content, err := ioutil.ReadAll(f)
	if err != nil {
		return fmt.Errorf("cannot read %s: %s", file, err)
	}
	f.Close()
	kv := w.mapf(file, string(content))
	f, err = os.Create("m-x-y")
	if err != nil {
		return fmt.Errorf("cannot open file for writing: %s", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, v := range kv {
		err = enc.Encode(&v)
		if err != nil {
			return fmt.Errorf("error writing intermediate data to file: %s", err)
		}
	}
	return nil
}

func hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: worker app.so\n")
		os.Exit(1)
	}
	w, err := New()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("failed to start worker process")
	}
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
