package main

import (
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGoroutinesCount(t *testing.T) {
	os.Args = []string{"master.go", "../../input/map"}
	go func() { main() }()
	// Wait for goroutines to start.
	time.Sleep(1 * time.Second)
	require.Equal(t, 8, runtime.NumGoroutine())
}
