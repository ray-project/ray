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

package api

import (
	"sync"
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
)

// TestReleaseWorkerStopIdempotent ensures stopReleaseWorker does not panic on a
// repeated call. A double Shutdown (e.g. an explicit Shutdown plus a deferred
// one) previously closed the stop channel twice, panicking with
// "close of closed channel".
func TestReleaseWorkerStopIdempotent(t *testing.T) {
	for i := 0; i < 3; i++ {
		stopReleaseWorker()
	}
}

// TestReleaseWorkerReinit ensures the release worker can be re-initialized
// after shutdown: stop resets the channels to nil so a later init starts a
// fresh worker instead of silently returning.
func TestReleaseWorkerReinit(t *testing.T) {
	// Force a populated state then stop it.
	releaseMu.Lock()
	if releaseQueue == nil {
		releaseQueue = make(chan ids.ObjectID, 16)
		releaseWorkerStop = make(chan struct{})
	}
	releaseMu.Unlock()
	stopReleaseWorker()

	// A subsequent init must restart the worker (not return early).
	releaseMu.Lock()
	wasNil := releaseQueue == nil
	releaseMu.Unlock()
	if !wasNil {
		t.Fatal("expected releaseQueue reset to nil after stopReleaseWorker")
	}
	initReleaseWorker()
	releaseMu.Lock()
	ok := releaseQueue != nil && releaseWorkerStop != nil
	releaseMu.Unlock()
	if !ok {
		t.Fatal("expected release worker to be re-initialized after re-init")
	}
	// Clean up for other tests.
	stopReleaseWorker()
}

// TestRemoveLocalRefSafeAfterShutdown ensures removeLocalRefSafe is a safe
// no-op once shutdown has completed (no CGO call is attempted).
func TestRemoveLocalRefSafeAfterShutdown(t *testing.T) {
	shutdownComplete.Store(true)
	// Must not panic and must not block: takes finalizerMu.RLock and returns.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		removeLocalRefSafe(ids.ObjectID{})
	}()
	wg.Wait()
	// Restore for other tests in this package that assume a live runtime.
	shutdownComplete.Store(false)
}
