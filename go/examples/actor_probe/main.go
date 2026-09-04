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

// Minimal cluster-mode actor probe for the Ray Go runtime.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/ray-project/ray/go/examples/userfuncs"
	_ "github.com/ray-project/ray/go/internal/runtime/native"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

func main() {
	fmt.Println("PROBE START")
	fmt.Println("gcs=", os.Getenv("RAY_GCS_ADDRESS"), "so=", os.Getenv("RAY_GO_USERFUNC_SO"))
	funcSo := os.Getenv("RAY_GO_USERFUNC_SO")
	jobOpts, err := options.NewJobConfigBuilder().WithCodeSearchPath(funcSo).WithNamespace("comparison").BuildToJobOptions()
	if err != nil {
		log.Log.Error(err, "jobopts")
		os.Exit(1)
	}
	// A fresh job ID per run avoids GCS JobConfig caching / raylet finished-job reuse.
	if jobOpts.JobID == "" {
		var id [4]byte
		if _, err := rand.Read(id[:]); err != nil {
			log.Log.Error(err, "rand")
			os.Exit(1)
		}
		jobOpts.JobID = hex.EncodeToString(id[:])
	}
	fmt.Println("STEP: build init options")
	initOpts := &options.InitializeOptions{
		WorkerType: options.WorkerTypeDriver,
		Network: options.NetworkOptions{
			GcsAddress:    os.Getenv("RAY_GCS_ADDRESS"),
			NodeIPAddress: os.Getenv("RAY_NODE_IP_ADDRESS"),
		},
		Runtime: options.RuntimeOptions{
			RayletSocket: os.Getenv("RAY_RAYLET_SOCKET"),
			StoreSocket:  os.Getenv("RAY_STORE_SOCKET"),
		},
		Job: jobOpts,
	}
	fmt.Println("STEP: InitWithOptions")
	if err := api.InitWithOptions(initOpts); err != nil {
		log.Log.Error(err, "init")
		os.Exit(1)
	}
	defer api.Instance().Shutdown()

	fmt.Println("STEP: create actor")
	// The Counter class is derived implicitly on both the driver and the
	// worker from the registered (*Counter).Inc method value.
	actorHand, err := api.Instance().Actor(&userfuncs.Counter{}).Create()
	if err != nil {
		log.Log.Error(err, "create actor")
		os.Exit(1)
	}
	fmt.Println("actor created:", actorHand.ID().Hex())

	fmt.Println("STEP: submit actor tasks")
	for i := 1; i <= 3; i++ {
		ref, err := actorHand.Task((*userfuncs.Counter).Inc).Remote()
		if err != nil {
			fmt.Println("SUBMIT ERR:", err)
			log.Log.Error(err, "submit actor task")
			os.Exit(1)
		}
		fmt.Println("STEP: get actor result", i)
		val, err := api.Instance().Get(ref)
		if err != nil {
			fmt.Println("GET ERR:", err)
			os.Exit(1)
		}
		fmt.Printf("actor Inc call %d returned: %v\n", i, val)
		if fmt.Sprintf("%v", val) != fmt.Sprintf("%d", i) {
			fmt.Printf("STATE MISMATCH: expected %d, got %v\n", i, val)
			os.Exit(1)
		}
	}
	fmt.Println("ACTOR E2E OK")
}
