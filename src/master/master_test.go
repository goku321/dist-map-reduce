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

	t.Run("task status should be inprogress", func(t *testing.T) {
		assert.Equal(t, inprogress, task.Status)
	})

	// empty map tasks list
	mockMaster.mapTasks = nil
	task = mockMaster.getPendingMapTask()
	t.Run("task should be nil", func(t *testing.T) {
		assert.Nil(t, task)
	})
}

func TestHasMapPhaseCompleted(t *testing.T) {
	mockMaster := New(mockFiles, 4)
	status := mockMaster.hasMapPhaseCompleted()
	t.Run("status should be false", func(t *testing.T) {
		require.False(t, status)
	})

	mockMaster.mapTasks = nil
	status = mockMaster.hasMapPhaseCompleted()
	t.Run("status should be true", func(t *testing.T) {
		require.True(t, status)
	})
}

func TestHasReducePhaseCompleted(t *testing.T) {
	mockMaster := New(mockFiles, 4)
	status := mockMaster.hasReducePhaseCompleted()
	t.Run("status should be false", func(t *testing.T) {
		require.False(t, status)
	})

	mockMaster.reduceTasks = nil
	status = mockMaster.hasReducePhaseCompleted()
	t.Run("status should be true", func(t *testing.T) {
		require.True(t, status)
	})
}

func TestGetWork(t *testing.T) {
	// Test when requesting a task during map phase.
	mockMaster := New(mockFiles, 4)
	args := &model.Args{}
	reply := &model.Task{}
	err := mockMaster.GetWork(args, reply)
	require.NoError(t, err)
	// Files field in task should not be empty.
	assert.NotEmpty(t, reply.Files)
	// Number of reduce tasks should be 4.
	assert.Equal(t, reply.NReduce, 4)
	// Task's status should be in-progress.
	assert.Equal(t, inprogress, reply.Status)
	// Task type should be a map task.
	assert.Equal(t, model.Map, reply.Type)
}
