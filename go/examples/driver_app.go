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

// driver_app is a Ray Go driver application that submits tasks to a Ray cluster.
//
// This application demonstrates how to:
// 1. Define user functions in a separate package (userfuncs)
// 2. Compile userfuncs as a plugin (.so file) for worker loading
// 3. Use the functions in driver mode for remote task submission
//
// The worker process is separate and is started by the Ray framework through
// the raygo setup_worker / default_worker entry points under go/cmd/raygo.
//
// Usage:
//
//	# Step 1: Compile userfuncs as plugin
//	bazel build //go/examples/userfuncs/plugin:userfuncs_plugin
//
//	# Step 2: Compile driver_app
//	bazel build //go/examples:driver_app
//
//	# Step 3: Copy plugin and driver to the same directory
//	mkdir -p job_submit && cp bazel-bin/go/examples/userfuncs/plugin/userfuncs.so job_submit/
//	cp bazel-bin/go/examples/driver_app_/driver_app job_submit/
//
//	# Step 4: Submit as a Ray job (driver mode). The cluster-mode runtime is
//	# loaded from go_runtime.so, discovered through RAY_GO_RUNTIME_PATH or next
//	# to the executing binary.
//	cd job_submit && ray job submit --working-dir . -- ./driver_app
//
//	# Step 5: Or run in local mode for testing
//	./driver_app --run-mode=local
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ray-project/ray/go/examples/userfuncs"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/log/zap"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/local"
)

// ============================================================================
// Application configuration
// ============================================================================

// RunMode specifies how the application should run.
type RunMode int

const (
	// RunModeDriver runs as a Ray driver, submitting tasks to the cluster.
	RunModeDriver RunMode = iota
	// RunModeLocal runs in local mode without a Ray cluster (for testing).
	RunModeLocal
)

var (
	runModeFlag      = flag.String("run-mode", "driver", "Run mode: driver or local")
	gcsAddressFlag   = flag.String("gcs-address", "", "GCS address (host:port)")
	nodeIPFlag       = flag.String("node-ip", "127.0.0.1", "Node IP address")
	nodeManagerPort  = flag.Int("node-manager-port", 0, "Node manager port (from raylet)")
	storeSocketFlag  = flag.String("store-socket", "", "Plasma store socket path")
	rayletSocketFlag = flag.String("raylet-socket", "", "Raylet socket path")
	logDirFlag       = flag.String("log-dir", "", "Log directory")
)

func parseRunMode(s string) RunMode {
	switch s {
	case "driver":
		return RunModeDriver
	case "local":
		return RunModeLocal
	default:
		return RunModeDriver
	}
}

// ============================================================================
// Application entry point
// ============================================================================

func main() {
	flag.Parse()
	runMode := parseRunMode(*runModeFlag)

	// Initialize logger
	if err := zap.SetupDefaultLogger(); err != nil {
		fmt.Printf("ERROR: failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	logger := log.WithName("driver_app")
	logger.Info("starting Ray Go application", "runMode", runMode)

	switch runMode {
	case RunModeDriver:
		runDriver()
	case RunModeLocal:
		runLocal()
	default:
		logger.Error(fmt.Errorf("unknown run mode"), "invalid run mode", "mode", runMode)
		os.Exit(1)
	}
}

// runDriver runs as a Ray driver.
// This submits tasks to the cluster and waits for results.
//
// Driver flow:
// 1. Initialize Ray runtime with JobConfig specifying code_search_path
// 2. Functions are already registered via userfuncs package import (init() auto-registers)
// 3. Submit remote tasks using api.Remote[T](userfuncs.GetGoAdd()).Call(...)
func runDriver() {
	logger := log.WithName("driver")
	logger.Info("running as driver")

	// Initialize Ray with JobConfig specifying code_search_path and runtime_env.
	// This ensures worker processes can load user plugins.
	//
	// Note: We use InitializeWithJobConfig (not InitializeWithJobConfigAndNetwork)
	// because the GCS address is automatically read from the RAY_ADDRESS environment
	// variable, which is set by `ray job submit`. This simplifies the code.
	//
	// Use current working directory for codeSearchPath.
	// When running with `ray job submit --working-dir .`, the working directory
	// is distributed to worker nodes via GCS. The worker nodes will have their
	// own copy of the working directory in their local session directory.
	//
	// The key is that runtime_env's working_dir must be configured so that
	// autoscaler worker nodes also get the working_dir files distributed.
	cwd, err := os.Getwd()
	if err != nil {
		logger.Error(err, "failed to get current working directory")
		os.Exit(1)
	}
	codeSearchPath := filepath.Join(cwd, "userfuncs.so")

	// DEBUG: Print codeSearchPath and verify file exists
	logger.Info("DEBUG: driver codeSearchPath", "cwd", cwd, "codeSearchPath", codeSearchPath)
	if _, statErr := os.Stat(codeSearchPath); os.IsNotExist(statErr) {
		logger.Error(nil, "DEBUG: userfuncs.so does NOT exist at codeSearchPath", "path", codeSearchPath)
	} else {
		logger.Info("DEBUG: userfuncs.so exists at codeSearchPath", "path", codeSearchPath)
	}

	// Build JobConfig with codeSearchPath.
	// Note: runtime_env is automatically merged from RAY_JOB_CONFIG_JSON_ENV_VAR
	// if present (e.g., when submitted via `ray job submit --working-dir .`).
	// This matches the behavior of Python and Java runtimes.
	jobConfigBuilder := options.NewJobConfigBuilder().
		WithCodeSearchPath(codeSearchPath)

	if err := api.Instance().InitializeWithJobConfig(jobConfigBuilder); err != nil {
		logger.Error(err, "failed to initialize Ray with JobConfig")
		os.Exit(1)
	}
	defer api.Shutdown()

	logger.Info("Ray initialized successfully with JobConfig")

	// Note: Functions are already registered via userfuncs package import.
	// The userfuncs package's init() function automatically registers all functions.
	// We don't need to call registerFunctions() explicitly.

	// Submit and execute tasks
	fmt.Println("Submitting remote tasks...")
	if err := submitTasks(); err != nil {
		logger.Error(err, "task submission failed")
		os.Exit(1)
	}

	fmt.Println("\n✅ All basic tasks completed successfully!")

	// Demonstrate Wait API
	fmt.Println("\n=== Demonstrating Wait API ===")
	if err := submitTasksWithWait(); err != nil {
		logger.Error(err, "Wait demonstration failed")
		os.Exit(1)
	}

	// Demonstrate concurrent task submission
	fmt.Println("\n=== Demonstrating Concurrent Task Submission ===")
	if err := submitTasksConcurrently(); err != nil {
		logger.Error(err, "Concurrent submission demonstration failed")
		os.Exit(1)
	}

	// Demonstrate the driver-side KillActor API.
	fmt.Println("\n=== Demonstrating KillActor (driver-side actor kill) ===")
	if err := demonstrateKillActor(); err != nil {
		logger.Error(err, "KillActor demonstration failed")
		os.Exit(1)
	}

	// Demonstrate the cross-process zero-copy Get path (large plasma object).
	fmt.Println("\n=== Demonstrating Cross-Process Zero-Copy Get ===")
	if err := demonstrateZeroCopyGet(); err != nil {
		logger.Error(err, "zero-copy Get demonstration failed")
		os.Exit(1)
	}

	// Demonstrate pass-by-reference task arguments (large payload >100KB).
	// This exercises the explicit-ID Put path (PutRawWithID -> CreateExisting ->
	// WriteData -> SealExisting) inside convertArgToFunctionArg when the argument
	// exceeds the pass-by-value threshold.
	fmt.Println("\n=== Demonstrating Pass-By-Reference Large Argument (zero-copy Put) ===")
	if err := demonstratePassByRefArg(); err != nil {
		logger.Error(err, "pass-by-reference argument demonstration failed")
		os.Exit(1)
	}

	fmt.Println("\n✅ All tasks completed successfully!")
	fmt.Println("[DEBUG] driver_app: about to call api.Shutdown()")
}

// runLocal runs in local mode without a Ray cluster.
// This is useful for testing and development.
func runLocal() {
	logger := log.WithName("local")
	logger.Info("running in local mode")

	// Register the local-mode runtime before initializing: importing
	// go/pkg/runtime/local triggers the internal local_mode init() that the
	// api.InitLocal() short-circuit depends on.
	local.Enable()

	// Initialize Ray in local mode (pure in-process, no go_runtime.so plugin)
	if err := api.InitLocal(); err != nil {
		logger.Error(err, "failed to initialize Ray in local mode")
		os.Exit(1)
	}
	defer api.Shutdown()

	logger.Info("Ray initialized in local mode")

	// Note: Functions are already registered via userfuncs package import.

	// Submit tasks
	fmt.Println("Submitting tasks in local mode...")
	if err := submitTasks(); err != nil {
		logger.Error(err, "task submission failed")
		os.Exit(1)
	}

	fmt.Println("\n✅ All basic tasks completed successfully!")

	// Demonstrate Wait API (also works in local mode)
	fmt.Println("\n=== Demonstrating Wait API (local mode) ===")
	if err := submitTasksWithWait(); err != nil {
		logger.Error(err, "Wait demonstration failed")
		os.Exit(1)
	}

	fmt.Println("\n✅ All tasks completed successfully!")
}

// submitTasks submits remote tasks and waits for results.
// Uses functions from userfuncs package.
func submitTasks() error {
	// Test 1: goAdd(10, 20)
	fmt.Println("\nTest 1: goAdd(10, 20)")
	resultRef1, err := api.Remote[int](userfuncs.GetGoAdd()).Call(10, 20)
	if err != nil {
		return fmt.Errorf("failed to submit goAdd task: %w", err)
	}
	result1, err := resultRef1.Get()
	if err != nil {
		return fmt.Errorf("failed to get goAdd result: %w", err)
	}
	fmt.Printf("  Result: %d (expected: 30)\n", result1)
	if result1 != 30 {
		return fmt.Errorf("goAdd(10, 20) = %d, want 30", result1)
	}

	// Test 2: goMultiply(5, 7)
	fmt.Println("\nTest 2: goMultiply(5, 7)")
	resultRef2, err := api.Remote[int](userfuncs.GetGoMultiply()).Call(5, 7)
	if err != nil {
		return fmt.Errorf("failed to submit goMultiply task: %w", err)
	}
	result2, err := resultRef2.Get()
	if err != nil {
		return fmt.Errorf("failed to get goMultiply result: %w", err)
	}
	fmt.Printf("  Result: %d (expected: 35)\n", result2)
	if result2 != 35 {
		return fmt.Errorf("goMultiply(5, 7) = %d, want 35", result2)
	}

	// Test 3: goConcat("Hello, ", "World!")
	fmt.Println("\nTest 3: goConcat(\"Hello, \", \"World!\")")
	resultRef3, err := api.Remote[string](userfuncs.GetGoConcat()).Call("Hello, ", "World!")
	if err != nil {
		return fmt.Errorf("failed to submit goConcat task: %w", err)
	}
	result3, err := resultRef3.Get()
	if err != nil {
		return fmt.Errorf("failed to get goConcat result: %w", err)
	}
	fmt.Printf("  Result: %q (expected: \"Hello, World!\")\n", result3)
	if result3 != "Hello, World!" {
		return fmt.Errorf("goConcat() = %q, want \"Hello, World!\"", result3)
	}

	// Test 4: goCompute(2, 3, 4)
	fmt.Println("\nTest 4: goCompute(2, 3, 4)")
	resultRef4, err := api.Remote[int](userfuncs.GetGoCompute()).Call(2, 3, 4)
	if err != nil {
		return fmt.Errorf("failed to submit goCompute task: %w", err)
	}
	result4, err := resultRef4.Get()
	if err != nil {
		return fmt.Errorf("failed to get goCompute result: %w", err)
	}
	fmt.Printf("  Result: %d (expected: 10)\n", result4)
	if result4 != 10 {
		return fmt.Errorf("goCompute(2, 3, 4) = %d, want 10", result4)
	}

	return nil
}

// submitTasksWithWait submits multiple remote tasks and demonstrates the use of Wait.
// This shows how to use Ray's Wait operation to wait for multiple objects to become available.
//
// Wait is useful when:
// 1. You want to process results as soon as they become available (streaming)
// 2. You want to set a timeout for waiting on objects
// 3. You want to wait for only a subset of objects before proceeding
//
// This is analogous to Python Ray's ray.wait() and Java Ray's Ray.wait().
func submitTasksWithWait() error {
	fmt.Println("\n=== Test 5: Demonstrating Wait API ===")

	// Submit 5 tasks concurrently
	fmt.Println("\nSubmitting 5 goAdd tasks concurrently...")

	// Task 1: goAdd(1, 2) = 3
	ref1, err := api.Remote[int](userfuncs.GetGoAdd()).Call(1, 2)
	if err != nil {
		return fmt.Errorf("failed to submit task 1: %w", err)
	}

	// Task 2: goAdd(10, 20) = 30
	ref2, err := api.Remote[int](userfuncs.GetGoAdd()).Call(10, 20)
	if err != nil {
		return fmt.Errorf("failed to submit task 2: %w", err)
	}

	// Task 3: goAdd(100, 200) = 300
	ref3, err := api.Remote[int](userfuncs.GetGoAdd()).Call(100, 200)
	if err != nil {
		return fmt.Errorf("failed to submit task 3: %w", err)
	}

	// Task 4: goAdd(1000, 2000) = 3000
	ref4, err := api.Remote[int](userfuncs.GetGoAdd()).Call(1000, 2000)
	if err != nil {
		return fmt.Errorf("failed to submit task 4: %w", err)
	}

	// Task 5: goAdd(10000, 20000) = 30000
	ref5, err := api.Remote[int](userfuncs.GetGoAdd()).Call(10000, 20000)
	if err != nil {
		return fmt.Errorf("failed to submit task 5: %w", err)
	}

	// Collect all references into a slice
	allRefs := []*api.ObjectRef[int]{ref1, ref2, ref3, ref4, ref5}
	expectedResults := []int{3, 30, 300, 3000, 30000}

	// ========================================================================
	// Wait API Demonstration
	// ========================================================================

	// Example 1: Wait for 3 objects to be ready (numReturns=3)
	// This returns as soon as 3 objects are available, without waiting for all 5
	fmt.Println("\n--- Wait Example 1: Wait for 3 objects to be ready ---")
	fmt.Println("  Calling api.Wait(refs, numReturns=3, timeoutMs=10000, fetchLocal=false)")

	waitResult1, err := api.Wait[int](allRefs, 3, 10000, false)
	if err != nil {
		return fmt.Errorf("Wait failed: %w", err)
	}

	fmt.Printf("  Ready count: %d\n", len(waitResult1.Ready()))
	fmt.Printf("  Unready count: %d\n", len(waitResult1.Unready()))

	// Get results from ready objects
	fmt.Println("\n  Processing ready objects:")
	for i, ref := range waitResult1.Ready() {
		result, err := ref.Get()
		if err != nil {
			return fmt.Errorf("failed to get result %d: %w", i, err)
		}
		fmt.Printf("    Object %d: result = %d\n", i+1, result)
	}

	// Example 2: Wait for all remaining objects with a timeout
	fmt.Println("\n--- Wait Example 2: Wait for all remaining objects ---")
	remainingRefs := waitResult1.Unready()

	// Initialize waitResult2 to handle the case when there are no remaining refs
	var waitResult2 *api.WaitResult[int]

	if len(remainingRefs) > 0 {
		fmt.Printf("  Waiting for %d remaining objects with 5 second timeout...\n", len(remainingRefs))

		waitResult2, err = api.Wait[int](remainingRefs, len(remainingRefs), 5000, false)
		if err != nil {
			return fmt.Errorf("Wait for remaining failed: %w", err)
		}

		fmt.Printf("  Ready count: %d\n", len(waitResult2.Ready()))
		fmt.Printf("  Unready count: %d\n", len(waitResult2.Unready()))

		// Process remaining ready objects
		if len(waitResult2.Ready()) > 0 {
			fmt.Println("\n  Processing remaining ready objects:")
			for i, ref := range waitResult2.Ready() {
				result, err := ref.Get()
				if err != nil {
					return fmt.Errorf("failed to get remaining result %d: %w", i, err)
				}
				fmt.Printf("    Object %d: result = %d\n", i+1, result)
			}
		}
	} else {
		// All objects were ready in the first wait
		fmt.Println("  All 5 objects were ready in the first wait, no remaining objects")
		// Create an empty WaitResult for consistency
		waitResult2 = api.NewWaitResult([]*api.ObjectRef[int]{}, []*api.ObjectRef[int]{})
	}

	// Example 3: Wait with fetchLocal=true (fetches objects to local node)
	fmt.Println("\n--- Wait Example 3: Wait with fetchLocal=true ---")
	// Submit new tasks for this demonstration
	ref6, err := api.Remote[int](userfuncs.GetGoAdd()).Call(5, 5)
	if err != nil {
		return fmt.Errorf("failed to submit task 6: %w", err)
	}
	ref7, err := api.Remote[int](userfuncs.GetGoAdd()).Call(10, 10)
	if err != nil {
		return fmt.Errorf("failed to submit task 7: %w", err)
	}

	fetchLocalRefs := []*api.ObjectRef[int]{ref6, ref7}
	fmt.Println("  Calling api.Wait(refs, numReturns=2, timeoutMs=10000, fetchLocal=true)")
	waitResult3, err := api.Wait[int](fetchLocalRefs, 2, 10000, true)
	if err != nil {
		return fmt.Errorf("Wait with fetchLocal failed: %w", err)
	}

	fmt.Printf("  Ready count: %d (objects fetched to local node)\n", len(waitResult3.Ready()))

	// Get results
	for i, ref := range waitResult3.Ready() {
		result, err := ref.Get()
		if err != nil {
			return fmt.Errorf("failed to get fetchLocal result %d: %w", i, err)
		}
		fmt.Printf("    Object %d: result = %d\n", i+1, result)
	}

	// Example 4: WaitWithTimeout - convenience method for waiting with timeout
	fmt.Println("\n--- Wait Example 4: Using WaitWithTimeout ---")
	ref8, err := api.Remote[int](userfuncs.GetGoAdd()).Call(100, 100)
	if err != nil {
		return fmt.Errorf("failed to submit task 8: %w", err)
	}

	singleRef := []*api.ObjectRef[int]{ref8}
	fmt.Println("  Calling api.WaitWithTimeout(refs, timeoutMs=5000) - waits for at least 1 object")
	waitResult4, err := api.WaitWithTimeout[int](singleRef, 5000)
	if err != nil {
		return fmt.Errorf("WaitWithTimeout failed: %w", err)
	}

	fmt.Printf("  Ready count: %d\n", len(waitResult4.Ready()))
	if len(waitResult4.Ready()) > 0 {
		result, err := waitResult4.Ready()[0].Get()
		if err != nil {
			return fmt.Errorf("failed to get WaitWithTimeout result: %w", err)
		}
		fmt.Printf("    Result: %d\n", result)
	}

	// Example 5: WaitWithNumReturns - wait for specific number of objects
	fmt.Println("\n--- Wait Example 5: Using WaitWithNumReturns ---")
	ref9, err := api.Remote[int](userfuncs.GetGoAdd()).Call(1, 1)
	if err != nil {
		return fmt.Errorf("failed to submit task 9: %w", err)
	}
	ref10, err := api.Remote[int](userfuncs.GetGoAdd()).Call(2, 2)
	if err != nil {
		return fmt.Errorf("failed to submit task 10: %w", err)
	}

	numReturnsRefs := []*api.ObjectRef[int]{ref9, ref10}
	fmt.Println("  Calling api.WaitWithNumReturns(refs, numReturns=2, timeoutMs=5000)")
	waitResult5, err := api.WaitWithNumReturns[int](numReturnsRefs, 2, 5000)
	if err != nil {
		return fmt.Errorf("WaitWithNumReturns failed: %w", err)
	}

	fmt.Printf("  Ready count: %d\n", len(waitResult5.Ready()))
	fmt.Printf("  Unready count: %d\n", len(waitResult5.Unready()))

	// Get results from waitResult5
	if len(waitResult5.Ready()) > 0 {
		fmt.Println("\n  Processing WaitWithNumReturns results:")
		for i, ref := range waitResult5.Ready() {
			result, err := ref.Get()
			if err != nil {
				return fmt.Errorf("failed to get WaitWithNumReturns result %d: %w", i, err)
			}
			fmt.Printf("    Object %d: result = %d\n", i+1, result)
		}
	}

	// Verify all expected results from the first 5 tasks
	fmt.Println("\n  Verifying all results from first 5 tasks...")

	// Collect all ready refs from waitResult1 and waitResult2
	allReadyRefs := append([]*api.ObjectRef[int]{}, waitResult1.Ready()...)
	if waitResult2 != nil {
		allReadyRefs = append(allReadyRefs, waitResult2.Ready()...)
	}

	if len(allReadyRefs) != 5 {
		return fmt.Errorf("expected 5 ready refs, got %d", len(allReadyRefs))
	}

	// Collect and verify results
	actualResults := make([]int, 0, 5)
	for _, ref := range allReadyRefs {
		result, err := ref.Get()
		if err != nil {
			return fmt.Errorf("failed to get final result: %w", err)
		}
		actualResults = append(actualResults, result)
	}

	// Verify all expected results are present (order may vary since Wait returns as objects become ready)
	for _, expected := range expectedResults {
		found := false
		for _, actual := range actualResults {
			if actual == expected {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("expected result %d not found in actual results %v", expected, actualResults)
		}
	}
	fmt.Println("  All expected results verified successfully!")

	fmt.Println("\n✅ Wait API demonstration completed successfully!")
	return nil
}

// submitTasksConcurrently submits multiple tasks concurrently and waits for all results.
// This demonstrates the concurrent submission pattern, which is more efficient than
// sequential submission when tasks are independent.
//
// Concurrent submission pattern:
// 1. Submit all tasks first (without waiting)
// 2. Collect all ObjectRef handles
// 3. Wait for all tasks to complete
// 4. Process results
//
// This is analogous to Python Ray's:
//
//	refs = [remote_function.remote(i) for i in range(5)]
//	results = ray.get(refs)
//
// And Java Ray's:
//
//	List<ObjectRef<Integer>> refs = new ArrayList<>();
//	for (int i = 0; i < 5; i++) {
//	    refs.add(taskSubmitter.submitTask(...));
//	}
//	List<Integer> results = Ray.get(refs);
func submitTasksConcurrently() error {
	fmt.Println("\n=== Test 6: Concurrent Task Submission ===")

	// ========================================================================
	// Phase 1: Submit all tasks concurrently (without waiting)
	// ========================================================================
	fmt.Println("\nPhase 1: Submitting 10 tasks concurrently (without waiting for results)...")

	// Submit 10 goAdd tasks with different inputs
	// Each task: goAdd(i, i) = i + i
	const numTasks = 10
	refs := make([]*api.ObjectRef[int], 0, numTasks)

	for i := 1; i <= numTasks; i++ {
		x := i
		y := i

		// Submit task and get ObjectRef immediately
		// Don't wait for result - just collect the reference
		ref, err := api.Remote[int](userfuncs.GetGoAdd()).Call(x, y)
		if err != nil {
			return fmt.Errorf("failed to submit task %d: %w", i, err)
		}

		refs = append(refs, ref)
		fmt.Printf("  Submitted task %d: goAdd(%d, %d)\n", i, x, y)
	}

	fmt.Printf("\n✅ Successfully submitted %d tasks concurrently\n", numTasks)
	fmt.Println("  (Tasks are now executing in parallel on workers)")

	// ========================================================================
	// Phase 2: Wait for all tasks to complete
	// ========================================================================
	fmt.Println("\nPhase 2: Waiting for all tasks to complete...")

	// Use Wait API to wait for all tasks
	// numReturns = numTasks means wait for all objects to be ready
	waitResult, err := api.Wait[int](refs, numTasks, 30000, false)
	if err != nil {
		return fmt.Errorf("Wait failed: %w", err)
	}

	fmt.Printf("  All %d tasks completed!\n", len(waitResult.Ready()))

	if len(waitResult.Unready()) > 0 {
		return fmt.Errorf("unexpected: %d tasks still unready after Wait", len(waitResult.Unready()))
	}

	// ========================================================================
	// Phase 3: Get all results
	// ========================================================================
	fmt.Println("\nPhase 3: Getting all results...")

	results := make([]int, 0, numTasks)
	for i, ref := range waitResult.Ready() {
		result, err := ref.Get()
		if err != nil {
			return fmt.Errorf("failed to get result %d: %w", i, err)
		}
		results = append(results, result)
		fmt.Printf("  Task %d: goAdd(%d, %d) = %d\n", i+1, (i + 1), (i + 1), result)
	}

	// ========================================================================
	// Phase 4: Verify results
	// ========================================================================
	fmt.Println("\nPhase 4: Verifying results...")

	// Expected results: [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
	// (goAdd(1,1)=2, goAdd(2,2)=4, ..., goAdd(10,10)=20)
	expectedResults := make([]int, numTasks)
	for i := 1; i <= numTasks; i++ {
		expectedResults[i-1] = i + i
	}

	// Check if all expected results are present
	// Note: Results may arrive in different order due to parallel execution
	resultMap := make(map[int]bool)
	for _, result := range results {
		resultMap[result] = true
	}

	allMatch := true
	for _, expected := range expectedResults {
		if !resultMap[expected] {
			fmt.Printf("  ERROR: expected result %d not found\n", expected)
			allMatch = false
		}
	}

	if !allMatch {
		return fmt.Errorf("result verification failed: got %v, want %v", results, expectedResults)
	}

	fmt.Println("  All results verified successfully!")

	// ========================================================================
	// Performance comparison
	// ========================================================================
	fmt.Println("\n=== Performance Comparison ===")
	fmt.Println("Sequential submission:")
	fmt.Println("  - Submit task 1 → Wait for result → Submit task 2 → Wait for result → ...")
	fmt.Println("  - Total time = sum of all task execution times")
	fmt.Println("  - Poor parallelism")
	fmt.Println("")
	fmt.Println("Concurrent submission:")
	fmt.Println("  - Submit all tasks → Wait for all → Get all results")
	fmt.Println("  - Total time = max of all task execution times (plus overhead)")
	fmt.Println("  - Excellent parallelism - tasks execute in parallel on workers")
	fmt.Println("")
	fmt.Println("For 10 independent tasks, concurrent submission can be up to 10x faster!")

	fmt.Println("\n✅ Concurrent task submission demonstration completed successfully!")
	return nil
}

// demonstrateKillActor verifies the driver-side KillActor API: an actor is
// created, confirmed alive and callable, then killed with noRestart=true so
// later method calls fail instead of executing.
func demonstrateKillActor() error {
	handle, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(100)
	if err != nil {
		return fmt.Errorf("create kill actor failed: %w", err)
	}
	typed := api.NewActorHandleImpl[int](handle.ID())

	// Confirm the actor is alive and callable before killing.
	ref, err := typed.Task((*userfuncs.Counter).Value).Remote()
	if err != nil {
		return fmt.Errorf("Value before kill failed: %w", err)
	}
	if v, err := ref.Get(); err != nil || v != 100 {
		return fmt.Errorf("Value before kill = %v (err %v), want 100", v, err)
	}
	fmt.Println("  Counter.Value() before kill = 100 ✓")

	// Kill the actor from the driver side with noRestart=true.
	if err := api.KillActor(handle, true); err != nil {
		return fmt.Errorf("KillActor failed: %w", err)
	}

	// CoreWorker::KillActor is asynchronous (GCS -> raylet -> worker SIGKILL),
	// so poll a method call until the worker has died and the call fails,
	// proving both the kill and the no-restart semantics.
	deadline := time.Now().Add(5 * time.Second)
	for {
		ref, err = typed.Task((*userfuncs.Counter).Value).Remote()
		if err == nil {
			_, err = ref.Get()
		}
		if err != nil {
			break // worker is dead: submission or execution failed
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("expected actor to be unavailable after kill")
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Println("  Counter killed via KillActor(noRestart=true), later calls fail ✓")
	return nil
}

// demonstrateZeroCopyGet verifies the cross-process zero-copy Get path
// end-to-end. A large value (well above the pass-by-value threshold) is stored
// in plasma as a SharedMemoryBuffer; GetRaw then serves the data via
// PlasmaBufferView without an intermediate copy. Enable verbose driver logging
// to observe the "GetRaw" V(1) log line from nativeGetView.
func demonstrateZeroCopyGet() error {
	// ~4 MB payload, far above the 100KB pass-by-value threshold, so it is
	// promoted to plasma and read back as a SharedMemoryBuffer.
	const size = 4 * 1024 * 1024
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i * 31)
	}

	fmt.Printf("  Putting %d-byte payload into the object store...\n", size)
	ref, err := api.Put(payload, nil)
	if err != nil {
		return fmt.Errorf("failed to put large object: %w", err)
	}
	fmt.Printf("  Put object: %s\n", ref.ObjectID().Hex())

	got, err := ref.Get()
	if err != nil {
		return fmt.Errorf("failed to get large object: %w", err)
	}

	if len(got) != size {
		return fmt.Errorf("got %d bytes, want %d", len(got), size)
	}
	for i := range got {
		if got[i] != payload[i] {
			return fmt.Errorf("payload mismatch at byte %d: got %d, want %d", i, got[i], payload[i])
		}
	}
	fmt.Printf("  Verified %d bytes match exactly (zero-copy Get path) ✓\n", len(got))
	return nil
}

// demonstratePassByRefArg verifies the pass-by-reference task-argument path:
// a payload well above the 100KB pass-by-value threshold is handed to a remote
// echoBytes task. convertArgToFunctionArg promotes it to the object store via
// the explicit-ID zero-copy Put path (PutRawWithID -> CreateExisting/WriteData/
// SealExisting), and the worker returns the same bytes which the driver reads
// back. This exercises both the explicit-ID Put and the cross-process Get.
func demonstratePassByRefArg() error {
	const size = 4 * 1024 * 1024
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte((i*131 + 7) % 251)
	}

	fmt.Printf("  Calling echoBytes with %d-byte argument (pass-by-reference)...\n", size)
	retRef, err := api.Remote[[]byte](userfuncs.GetEchoBytes()).Call(payload)
	if err != nil {
		return fmt.Errorf("remote echoBytes call failed: %w", err)
	}
	got, err := retRef.Get()
	if err != nil {
		return fmt.Errorf("failed to get echoBytes result: %w", err)
	}

	if len(got) != size {
		return fmt.Errorf("got %d bytes, want %d", len(got), size)
	}
	for i := range got {
		if got[i] != payload[i] {
			return fmt.Errorf("payload mismatch at byte %d: got %d, want %d", i, got[i], payload[i])
		}
	}
	fmt.Printf("  echoBytes returned %d bytes, all match exactly (explicit-ID zero-copy Put + pass-by-reference) ✓\n", len(got))
	return nil
}
