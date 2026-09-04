package common

import (
	"context"
	"testing"
	"time"
)

func TestNewAsyncFileLock(t *testing.T) {
	tmpDir := t.TempDir()
	lockFile := tmpDir + "/test.lock"

	lock := NewAsyncFileLock(lockFile)
	if lock == nil {
		t.Fatal("expected NewAsyncFileLock to return non-nil lock")
	}
	if lock.lock == nil {
		t.Error("expected lock.lock to be initialized")
	}
}

func TestAsyncFileLock_AcquireAndRelease(t *testing.T) {
	tmpDir := t.TempDir()
	lockFile := tmpDir + "/test.lock"

	lock := NewAsyncFileLock(lockFile)
	ctx := context.Background()

	err := lock.Acquire(ctx)
	if err != nil {
		t.Errorf("expected no error acquiring lock, got %v", err)
	}

	lock.Release()
	// Release should not panic, even if already released.
	lock.Release()
}

func TestAsyncFileLock_AcquireWithContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	lockFile := tmpDir + "/test.lock"

	lock1 := NewAsyncFileLock(lockFile)
	lock2 := NewAsyncFileLock(lockFile)

	ctx := context.Background()

	err := lock1.Acquire(ctx)
	if err != nil {
		t.Fatalf("expected no error acquiring first lock, got %v", err)
	}
	defer lock1.Release()

	// Create a context with a timeout so the second lock acquisition must time out.
	ctx2, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = lock2.Acquire(ctx2)
	if err == nil {
		t.Error("expected timeout error when acquiring lock held by another process")
	}
}

func TestAsyncFileLock_AcquireWithCancelledContext(t *testing.T) {
	tmpDir := t.TempDir()
	lockFile := tmpDir + "/test.lock"

	lock := NewAsyncFileLock(lockFile)

	// Acquire must fail when the context is already cancelled before the call.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := lock.Acquire(ctx)
	if err == nil {
		t.Error("expected error when acquiring lock with cancelled context")
	}
}
