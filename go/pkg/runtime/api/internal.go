// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"fmt"

	"github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
)

// submitterNotAvailable is the runtime error type reported when the task
// submitter is unavailable even though the runtime is initialized.
const submitterNotAvailable = "submitter_not_available"

// internal returns the current runtime abstraction.
// It is the Go equivalent of Java's Ray.internal():
//   - Java: Ray.internal() returns the RayRuntime singleton injected by Ray.init()
//   - Go:   internal() returns the contract.Runtime obtained from the handle
//     injected by InitWithOptions / SetRuntimeHandleForWorker.
//
// Callers should handle the returned error instead of panicking (Go idiom).
func internal() (contract.Runtime, error) {
	h, ok := tryGetHandle()
	if !ok || h == nil {
		return nil, errors.ErrRuntimeNotInitialized
	}
	rt := h.Runtime()
	if rt == nil {
		return nil, fmt.Errorf("runtime instance not available")
	}
	return rt, nil
}
