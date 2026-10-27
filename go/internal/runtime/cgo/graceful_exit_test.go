//go:build cgo
// +build cgo

// Copyright 2025 The Ray Authors.
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

package cgo

/*
#include <stdlib.h>
#include <stdbool.h>

// The prototypes below mirror the extern "C" bridges defined in
// src/ray/core_worker/lib/go/runtime_ops.cc. They are declared directly
// (instead of including the C++ header) to keep the cgo preamble C-only; the
// symbols resolve at link time against the native_runtime library closure,
// the same pattern used by the placement group bridges in
// native_submitter.go.
int ray_go_worker_check_signals_status(void);
void ray_go_worker_set_graceful_exit_requested(void);
*/
import "C"

import "testing"

// check_signals status codes, mirroring the constants in runtime_ops.cc.
const (
	checkSignalsStatusOK                    = 0
	checkSignalsStatusIntentionalSystemExit = 1
)

// TestCheckSignalsStatusBridge pins the graceful-exit flag contract shared by
// the production check_signals callback and this test bridge: the status is
// OK (0) until the graceful exit has been requested, and
// IntentionalSystemExit (1) afterwards. The flag is process-global and never
// resets, so both states are asserted in a single test in a fixed order.
func TestCheckSignalsStatusBridge(t *testing.T) {
	if got := int(C.ray_go_worker_check_signals_status()); got != checkSignalsStatusOK {
		t.Fatalf("check_signals status before request = %d, want %d (OK)", got, checkSignalsStatusOK)
	}

	C.ray_go_worker_set_graceful_exit_requested()

	if got := int(C.ray_go_worker_check_signals_status()); got != checkSignalsStatusIntentionalSystemExit {
		t.Fatalf("check_signals status after request = %d, want %d (IntentionalSystemExit)",
			got, checkSignalsStatusIntentionalSystemExit)
	}
}
