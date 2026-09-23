// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build cgo

// Pure Go unit tests for the native GCS package. They exercise the Go-side
// helpers only, so they need neither CGO calls into GCS nor a running cluster.

package native

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ============ async.go ============

func Test_runAsync_success(t *testing.T) {
	ch := runAsync(context.Background(), func() (int, error) {
		return 42, nil
	})

	result := <-ch
	if result.data != 42 {
		t.Errorf("expected data 42, got %d", result.data)
	}
	if result.err != nil {
		t.Errorf("expected no error, got %v", result.err)
	}
}

func Test_runAsync_error(t *testing.T) {
	expectedErr := errors.New("test error")
	ch := runAsync(context.Background(), func() (string, error) {
		return "", expectedErr
	})

	result := <-ch
	if result.data != "" {
		t.Errorf("expected empty data, got %v", result.data)
	}
	if !errors.Is(result.err, expectedErr) {
		t.Errorf("expected test error, got %v", result.err)
	}
}

func Test_waitContext_success(t *testing.T) {
	ch := runAsync(context.Background(), func() (string, error) {
		return "success", nil
	})

	ctx := context.Background()
	data, err := waitContext(ctx, ch)

	if data != "success" {
		t.Errorf("expected 'success', got %s", data)
	}
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func Test_waitContext_withTimeout(t *testing.T) {
	// Start a slow operation.
	ch := runAsync(context.Background(), func() (string, error) {
		time.Sleep(100 * time.Millisecond)
		return "slow", nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := waitContext(ctx, ch)

	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

func Test_waitContext_cancelled(t *testing.T) {
	// Start a slow operation.
	ch := runAsync(context.Background(), func() (string, error) {
		time.Sleep(100 * time.Millisecond)
		return "slow", nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	_, err := waitContext(ctx, ch)

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
