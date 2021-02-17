package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"plugin"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/goku321/dist-map-reduce/src/model"
	log "github.com/sirupsen/logrus"
)

const mapOutPrefix = "../../output/map"

var ErrNoPendingTask = errors.New("no pending task")

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
	for {
		args := &model.Args{}
		reply := &model.Task{}
		err := w.client.Call("Master.GetWork", args, reply)
		if err == ErrNoPendingTask {
			time.Sleep(5 * time.Second)
			continue
		}
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Warn("error calling server's method")
			return
		}

		// Check type of task (Map or Reduce).
		if reply.Type == model.Map {
			log.Infof("starting map phase on file: %s", reply.Files[0])

			w.mapf = mapf
			// w.reducef = reducef
			buckets, err := w.startMap(reply.Files[0], reply.NReduce)
			if err != nil {
				log.WithFields(log.Fields{
					"err":  err,
					"file": reply.Files[0],
				}).Warn("map phase failed")
				args := &model.TaskStatus{
					Success: false,
				}
				var reply *bool
				// ignore if there's any error calling Master's method.
				_ = w.client.Call("Master.SignalTaskStatus", args, reply)
			}
			statusArgs := &model.TaskStatus{
				Success:  true,
				File:     reply.Files[0],
				OutFiles: buckets,
			}
			var statusReply *bool
			// Ignore the error or retry?
			_ = w.client.Call("Master.SignalTaskStatus", statusArgs, statusReply)
			// Possible ways to signal master about failure or success.
			// 1. Expose a gRPC method that a worker can call.
			// 2. Expose a gRPC method on worker that master can periodically probe.
			// 3. Any other approaches?
		} else if reply.Type == model.Reduce {
			// Logic to handle Reduce task.
		}
	}
}

// startMap transforms contents of a file into a key:value pair
// and partition them across n files.
func (w *Worker) startMap(file string, n int) ([]string, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("cannot open %s: %s", file, err)
	}
	content, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %s", file, err)
	}
	f.Close()
	kv := w.mapf(file, string(content))

	// partition phase
	m := partition(kv, n)
	buckets := []string{}
	for k, values := range m {
		bucketName := fmt.Sprintf("%s/m-x-%d", mapOutPrefix, k)
		buckets = append(buckets, bucketName)
		bucket, err := os.Create(bucketName)
		if err != nil {
			return nil, fmt.Errorf("cannot open file for writing partition: %s", err)
		}
		enc := json.NewEncoder(bucket)
		for _, v := range values {
			err := enc.Encode(&v)
			if err != nil {
				return nil, fmt.Errorf("error writing intermediate data to file: %s", err)
			}
		}
		bucket.Close()
	}
	return buckets, nil
}

// splits the []KeyValue into n partitions.
func partition(kv []KeyValue, n int) map[int][]KeyValue {
	m := map[int][]KeyValue{}
	for _, v := range kv {
		p := hash(v.Key) % n
		m[p] = append(m[p], v)
	}
	return m
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

func mapf(filename string, contents string) []KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

func reducef(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}
