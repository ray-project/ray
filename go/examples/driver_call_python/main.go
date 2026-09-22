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

// driver_call_python is a Ray Go driver that verifies Go -> Python
// cross-language calls. It submits Python tasks via the RemotePython API and
// waits for results.
//
// Build & submit as a Ray job:
//
//	bazel build //go/examples/driver_call_python
//	mkdir -p /tmp/go_call_python_job
//	cp bazel-bin/go/examples/driver_call_python_/driver_call_python /tmp/go_call_python_job/
//	cp go/examples/test_go_call_python.py /tmp/go_call_python_job/
//	cd /tmp/go_call_python_job
//	ray job submit --working-dir . -- ./driver_call_python
//
// The code_search_path is set to the current working directory so that the
// Python worker enables load_code_from_local and can import the
// test_go_call_python module shipped via --working-dir.
package main

import (
	"fmt"
	"os"

	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/log/zap"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

func main() {
	if err := zap.SetupDefaultLogger(); err != nil {
		fmt.Printf("ERROR: failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	logger := log.WithName("driver_call_python")

	cwd, err := os.Getwd()
	if err != nil {
		logger.Error(err, "failed to get current working directory")
		os.Exit(1)
	}

	// A non-empty code_search_path triggers load_code_from_local on the
	// Python worker (see python/ray/_raylet.pyx maybe_initialize_job_config),
	// which lets it import the test_go_call_python module from the working
	// directory distributed by `ray job submit --working-dir .`.
	jobConfig := options.NewJobConfigBuilder().WithCodeSearchPath(cwd)
	if err := api.Instance().InitializeWithJobConfig(jobConfig); err != nil {
		logger.Error(err, "failed to initialize Ray")
		os.Exit(1)
	}
	defer api.Shutdown()

	logger.Info("Ray initialized, running Go->Python cross-language tests")

	if err := testModuleLevelFunction(); err != nil {
		logger.Error(err, "AC1 failed")
		os.Exit(1)
	}
	if err := testClassMethod(); err != nil {
		logger.Error(err, "AC2 failed")
		os.Exit(1)
	}
	if err := testExceptionPropagation(); err != nil {
		logger.Error(err, "AC3 failed")
		os.Exit(1)
	}

	fmt.Println("\nAll Go->Python cross-language tests passed!")
}

// testModuleLevelFunction verifies AC1: Go calls a Python module-level function.
func testModuleLevelFunction() error {
	caller := api.RemotePython[int]("test_go_call_python", "add", "")
	ref, err := caller.Call(10, 20)
	if err != nil {
		return fmt.Errorf("call add failed: %w", err)
	}
	result, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("get add result failed: %w", err)
	}
	fmt.Printf("AC1 add(10, 20) = %d (expected 30)\n", result)
	if result != 30 {
		return fmt.Errorf("add(10, 20) = %d, want 30", result)
	}
	return nil
}

// testClassMethod verifies AC2: Go calls a Python actor method.
func testClassMethod() error {
	handle, err := api.RemotePythonActor("test_go_call_python", "Calculator").Create()
	if err != nil {
		return fmt.Errorf("create Calculator actor failed: %w", err)
	}
	ref, err := api.ActorTask[int](handle, "add", 5).Remote()
	if err != nil {
		return fmt.Errorf("call Calculator.add failed: %w", err)
	}
	result, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("get Calculator.add result failed: %w", err)
	}
	fmt.Printf("AC2 Calculator.add(5) = %v (expected 5)\n", result)
	if result != 5 {
		return fmt.Errorf("Calculator.add(5) = %v, want 5", result)
	}
	return nil
}

// testExceptionPropagation verifies AC3: Python exception is propagated to Go.
func testExceptionPropagation() error {
	caller := api.RemotePython[int]("test_go_call_python", "divide", "")
	ref, err := caller.Call(10, 0)
	if err != nil {
		return fmt.Errorf("call divide failed: %w", err)
	}
	_, err = api.Get(ref)
	if err == nil {
		return fmt.Errorf("expected error from divide by zero, got nil")
	}
	fmt.Printf("AC3 divide(10, 0) propagated error: %v\n", err)
	return nil
}
