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

// Package plugin is the entry point for the userfuncs plugin.
// It exports the RegisterFunctions symbol, which the Go worker calls when it
// loads the plugin (see registerUserFunctions in go/internal/worker/worker.go),
// and also registers during init() so the functions are available to any code
// path that links the package directly.
package main

import (
	"github.com/ray-project/ray/go/e2e/userfuncs"
)

// RegisterFunctions registers all user-defined functions with the Ray runtime.
// The Go worker looks up this symbol by name when the plugin is loaded.
func RegisterFunctions() error {
	return userfuncs.RegisterFunctions()
}

// init is called when the plugin is loaded.
// It registers all user-defined functions with the runtime.
func init() {
	_ = userfuncs.RegisterFunctions()
}

func main() {
	// This is a plugin, not an executable.
	// The plugin runtime calls init() when the plugin is loaded.
}
