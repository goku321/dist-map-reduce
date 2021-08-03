package main

import (
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/goku321/dist-map-reduce/src/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	mockFiles = []string{"file1", "file2", "file3"}
)

func TestGoroutinesCount(t *testing.T) {
	os.Args = []string{"master.go", "../../input/map"}
	go func() { main() }()
	// Wait for goroutines to start.
	time.Sleep(1 * time.Second)
	require.Equal(t, 8, runtime.NumGoroutine())
}

func TestGetPendingMapTask(t *testing.T) {
	mockMaster := New(mockFiles, 4)
	task := mockMaster.getPendingMapTask()

	t.Run("task should not be nil", func(t *testing.T) {
		assert.NotNil(t, task)
	})

	t.Run("task should of map type", func(t *testing.T) {
		assert.Equal(t, model.Map, task.Type)
	})
}
