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
	// Mock filenames for map phase.
	mockFiles = []string{"file1", "file2", "file3"}
)

func cleanup(mockMaster *Master) {
	for _, cancel := range mockMaster.cancelers {
		cancel()
	}
}

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

func buildReduceTasksHelper(m *Master) {
	for k, task := range m.reduceTasks {
		task.Files = mockFiles
		m.reduceTasks[k] = task
	}
}

func TestGetWork(t *testing.T) {
	// Test when requesting a task during map phase.
	mockMaster := New(mockFiles, 4)
	// Cleanup any goroutines that fired up when calling Master.GetWork
	defer func() {
		cleanup(mockMaster)
	}()
	args := &model.Args{}
	reply := &model.Task{}

	t.Run("when requesting a task during map phase", func(t *testing.T) {
		err := mockMaster.GetWork(args, reply)
		require.NoError(t, err)
		assert.Len(t, reply.Files, 1)
		assert.Equal(t, reply.NReduce, 4)
		assert.Equal(t, inprogress, reply.Status)
		assert.Equal(t, model.Map, reply.Type)

		// Check if cancel function is added to cancelers slice.
		assert.Len(t, mockMaster.cancelers, 1)
	})

	// When no map task is pending during map phase.
	mockMaster.mapTasks = nil
	t.Run("should return error", func(t *testing.T) {
		err := mockMaster.GetWork(args, reply)
		assert.Equal(t, model.ErrNoPendingTask, err)
	})

	// Requesting a task during reduce phase.
	mockMaster.updatePhase(model.Reduce)
	buildReduceTasksHelper(mockMaster)

	t.Run("should return a reduce task", func(t *testing.T) {
		err := mockMaster.GetWork(args, reply)
		require.NoError(t, err)
		assert.Len(t, reply.Files, 3)
		assert.Equal(t, reply.NReduce, 4)
		assert.Equal(t, inprogress, reply.Status)
		assert.Equal(t, model.Reduce, reply.Type)
		assert.Equal(t, inprogress, reply.Status)

		// Add a case to check if a new go6s is running.
	})

	// When no reduce task is pending during reduce phase.
	mockMaster.reduceTasks = nil
	t.Run("should return error", func(t *testing.T) {
		err := mockMaster.GetWork(args, reply)
		assert.Equal(t, model.ErrNoPendingTask, err)
	})

	// When master is in shutdown phase.
	mockMaster.updatePhase(model.Shutdown)
	t.Run("should reply with shutdown signal", func(t *testing.T) {
		err := mockMaster.GetWork(args, reply)
		require.NoError(t, err)
		assert.Equal(t, model.Shutdown, reply.Type)
	})

	// Invalid RPC.
	mockMaster.updatePhase(4)
	t.Run("should return error with unknown rpc", func(t *testing.T) {
		err := mockMaster.GetWork(args, reply)
		assert.Equal(t, "unknown rpc", err.Error())
	})
}

func TestGetPendingReduceTask(t *testing.T) {
	mockMaster := New(mockFiles, 4)
	_, task := mockMaster.getPendingReduceTask()
	t.Run("task files should be empty", func(t *testing.T) {
		assert.Empty(t, task.Files)
	})

	// Populate reduce tasks.
	buildReduceTasksHelper(mockMaster)
	_, task = mockMaster.getPendingReduceTask()
	t.Run("should return a valid reduce task", func(t *testing.T) {
		assert.Len(t, task.Files, 3)
		assert.Equal(t, task.Status, inprogress)
		assert.Equal(t, task.Type, model.Reduce)
	})
}
