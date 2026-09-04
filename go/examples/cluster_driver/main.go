/*
Copyright 2026 The Ray Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// cluster_driver is a minimal driver executable that connects to a real local
// Ray cluster through the CGO CoreWorker bridge. It registers the native
// (cluster-mode) runtime initializer, initializes a driver CoreWorker, performs
// an in-process Put/Get round-trip, and submits a remote task that must
// actually execute on a GO worker.
//
// The remote task function lives in the shared userfuncs package (not in this
// main package), and the job code_search_path points at the compiled
// userfuncs.so plugin so worker processes can resolve the same function symbol.
package main

import (
	"fmt"
	"os"

	_ "github.com/ray-project/ray/go/internal/runtime/native"
	"github.com/ray-project/ray/go/examples/userfuncs"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

func main() {
	gcsAddress := os.Getenv("RAY_ADDRESS")
	if gcsAddress == "" {
		gcsAddress = "127.0.0.1:6379"
	}
	jobID := os.Getenv("RAY_JOB_ID")
	if jobID == "" {
		jobID = "01000000"
	}
	funcSo := os.Getenv("RAY_GO_USERFUNC_SO")
	if funcSo == "" {
		funcSo = "bazel-bin/go/examples/userfuncs/plugin/userfuncs.so"
	}

	// Register the user functions in the driver (done by importing userfuncs),
	// and build a JobConfig that tells worker processes where to load the same
	// .so so the function is available for execution on the worker side.
	jobOpts, err := options.NewJobConfigBuilder().
		WithCodeSearchPath(funcSo).
		BuildToJobOptions()
	if err != nil {
		fmt.Fprintf(os.Stderr, "JOB CONFIG BUILD FAILED: %v\n", err)
		os.Exit(1)
	}
	jobOpts.JobID = jobID

	initOpts := &options.InitializeOptions{
		WorkerType: options.WorkerTypeDriver,
		Network:    options.NetworkOptions{GcsAddress: gcsAddress},
		Job:        jobOpts,
	}
	if err := api.InitWithOptions(initOpts); err != nil {
		fmt.Fprintf(os.Stderr, "INIT FAILED: %v\n", err)
		os.Exit(1)
	}
	defer api.Instance().Shutdown()
	fmt.Printf("INIT OK: native/CoreWorker driver initialized (gcs=%s, funcSo=%s)\n", gcsAddress, funcSo)

	// Put/Get round-trip inside the driver process (no worker required).
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	ref, err := api.Instance().Put(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "PUT FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("PUT OK: %v\n", ref.ObjectID())

	result, err := api.Instance().Get(ref)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GET FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("GET OK: %v\n", result)

	// Submit a remote task that must actually execute on a GO worker.
	rref, err := api.Instance().Remote(userfuncs.Add).Call(1, 2)
	if err != nil {
		fmt.Fprintf(os.Stderr, "REMOTE CALL FAILED: %v\n", err)
		os.Exit(1)
	} else {
		fmt.Printf("REMOTE SUBMIT OK: %v\n", rref.ObjectID())
	}

	// Verify the remote task actually completed on a real worker.
	result2, err := api.Instance().Get(rref)
	if err != nil {
		fmt.Fprintf(os.Stderr, "REMOTE GET FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("REMOTE GET OK: %v\n", result2)

	fmt.Println("CLUSTER_DRIVER_DONE")
}
