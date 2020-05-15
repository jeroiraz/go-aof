package aof

import (
	"math/rand"
	"os"
	"testing"
	"time"
)

func randomBytes(size int) []byte {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, size)
	rand.Read(b)
	for i, v := range b {
		if v == 0 {
			b[i]++
		}
	}
	return b
}

func TestEmptyFile(t *testing.T) {
	app, err := Open("test_file.aof")
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	defer os.Remove("test_file.aof")

	b := randomBytes(1)
	off, err := app.Append(b)
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}

	if off != 0 {
		t.Errorf("Expected offset to be 0 but %d was returned instead", off)
	}

	app.Close()
}
