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
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

// Add is a simple addition function executed remotely on a worker.
func Add(x, y int) int {
	return x + y
}

// Counter is a stateful actor whose method is invoked remotely. It lives in
// this module package (not in a driver main package) so the method descriptor
// resolves the same module/import path on both the driver and the worker.
type Counter struct {
	value int
}

// Inc increments the counter and returns the new value.
func (c *Counter) Inc() int {
	c.value++
	return c.value
}

// goAdd is a simple addition helper registered as a remote task.
func goAdd(x, y int) int {
	return x + y
}

// goMultiply multiplies two integers.
func goMultiply(x, y int) int {
	return x * y
}

// goConcat concatenates two strings.
func goConcat(a, b string) string {
	return a + b
}

// goCompute performs a compound computation.
func goCompute(x, y, z int) int {
	return x*y + z
}

// GetGoAdd returns the goAdd function for direct use, without going through the
// remote task mechanism.
func GetGoAdd() func(int, int) int {
	return goAdd
}

// GetGoMultiply returns the goMultiply function for direct use.
func GetGoMultiply() func(int, int) int {
	return goMultiply
}

// GetGoConcat returns the goConcat function for direct use.
func GetGoConcat() func(string, string) string {
	return goConcat
}

// GetGoCompute returns the goCompute function for direct use.
func GetGoCompute() func(int, int, int) int {
	return goCompute
}

// RegisterFunctions registers all user-defined functions with the runtime.
func RegisterFunctions() error {
	for _, fn := range []interface{}{
		Add,
		(*Counter).Inc,
		goAdd,
		goMultiply,
		goConcat,
		goCompute,
		echoBytes,
	} {
		if err := api.RegisterFunction(fn); err != nil {
			return err
		}
	}
	// Register the actor constructor under the reserved "<init>" descriptor so
	// a Go worker can build Counter instances (see actors.go).
	return registerActors()
}

// init registers the functions when the package is imported (driver side) or
// when the .so plugin is loaded (worker side).
func init() {
	if err := RegisterFunctions(); err != nil {
		log.Log.Error(err, "register user functions failed")
	}
}
