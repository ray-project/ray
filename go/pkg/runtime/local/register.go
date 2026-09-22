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

// Package local is the public adapter for Ray's local (in-process) runtime.
//
// Importing this package registers the local-mode initializer with the API
// layer (via the internal local_mode package's init()), so that
// api.InitLocal() can route to LocalModeRuntime without loading the
// go_runtime.so plugin. It is a thin adapter only: the actual implementation
// lives in go/internal/runtime/local_mode, and the public api layer stays free
// of internal imports (dependency inversion).
//
// Application code that runs in local mode should import this package (or
// call local.Enable()) once, then call api.InitLocal():
//
//	import (
//	    "github.com/ray-project/ray/go/pkg/runtime/api"
//	    "github.com/ray-project/ray/go/pkg/runtime/local"
//	)
//
//	func main() {
//	    local.Enable() // registers the local-mode initializer
//	    if err := api.InitLocal(); err != nil { ... }
//	}
package local

import (
	_ "github.com/ray-project/ray/go/internal/runtime/local_mode"
)

// Enable registers the local-mode initializer with the API layer so that
// api.InitLocal() can create a LocalModeRuntime without loading the
// go_runtime.so plugin. It is idempotent (registration happens in the package
// init); calling it makes the intent explicit and immune to future refactors
// that remove the blank import.
func Enable() {
	// The registration side effect happens in the internal local_mode package
	// init(), which this package's blank import already triggered. Nothing to
	// do here beyond forcing the import to be retained.
}
