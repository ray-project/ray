// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package userfuncs provides user-defined functions for Ray Go applications.
// It is compiled as a plugin (.so) that worker processes load via the job
// code_search_path so the same function symbols resolve on both the driver and
// the worker, allowing remote tasks to actually execute.
package userfuncs

import (
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

// Add is a simple addition function executed remotely on a worker.
func Add(x, y int) int {
	return x + y
}

// RegisterFunctions registers all user-defined functions with the runtime.
func RegisterFunctions() error {
	if err := api.RegisterFunction(Add); err != nil {
		return err
	}
	return nil
}

// init registers the functions when the package is imported (driver side) or
// when the .so plugin is loaded (worker side).
func init() {
	_ = RegisterFunctions()
}
