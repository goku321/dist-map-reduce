package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStartMap(t *testing.T) {
	w := &Worker{
		mapf: mapf,
	}
	_, err := w.startMap("../../input/map/pg-being_ernest.txt", 5)
	require.NoError(t, err)

	// Verify number of output files.
}
