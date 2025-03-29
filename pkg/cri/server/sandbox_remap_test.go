package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWatchFileCreation(t *testing.T) {
	// Step 1: Create temporary test directory
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "watchtest")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Define test paths for two files
	subDir1 := filepath.Join(tmpDir, "aaa")
	subDir2 := filepath.Join(tmpDir, "bbb")
	targetFile1 := filepath.Join(subDir1, "file1")
	targetFile2 := filepath.Join(subDir2, "file2")

	// Step 2: Start watching both files
	done1Ch, err1Ch := watchFileCreation(ctx, targetFile1)
	done2Ch, err2Ch := watchFileCreation(ctx, targetFile2)

	// Step 3: Create directories and files
	go func() {
		time.Sleep(500 * time.Millisecond)

		// Create first directory and file
		if err := os.Mkdir(subDir1, 0755); err != nil {
			t.Errorf("Failed to create subdir1: %v", err)
		}
		if err := os.Mkdir(subDir2, 0755); err != nil {
			t.Errorf("Failed to create subdir2: %v", err)
		}

		time.Sleep(1 * time.Second)

		// Create both files
		for _, path := range []string{targetFile1, targetFile2} {
			file, err := os.Create(path)
			if err != nil {
				t.Errorf("Failed to create file %s: %v", path, err)
				return
			}
			file.Close()
		}
	}()

	// Step 4: Wait for both files or timeout
	timeout := time.After(3 * time.Second)

	type watchResult struct {
		doneCh <-chan struct{}
		errCh  <-chan error
	}

	// Store watch results in a slice
	watches := []watchResult{{done1Ch, err1Ch}, {done2Ch, err2Ch}}
	completed := make(map[int]bool)

	for len(completed) < len(watches) {
		select {
		case <-timeout:
			t.Fatalf("Test timed out waiting for file creation")
		default:
			for i, w := range watches {
				if completed[i] {
					continue
				}
				select {
				case <-w.doneCh:
					completed[i] = true
				case err := <-w.errCh:
					t.Fatalf("Error watching file %d: %v", i+1, err)
				case <-timeout:
					t.Fatalf("Test timed out waiting for file creation")
				default:
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Log("Both files created successfully")
}
