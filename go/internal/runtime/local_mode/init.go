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

package local_mode

import (
	"github.com/ray-project/ray/go/internal/runtime/base"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
)

// init registers the local-mode runtime with the public API layer so that
// api.Init / api.InitWithOptions with WorkerTypeLocal routes here through the
// dependency-inversion entry point (api.RegisterInitializer) instead of
// loading the go_runtime.so plugin. Driver and worker processes do not use
// this registration: they load go_runtime.so, whose own native init()
// registers the initializer for the driver/worker worker types.
//
// This package is pure Go (it does not depend on the CGO object store), so
// statically linking it into a driver does not pull in the C++ core worker or
// gRPC, avoiding the "linked both statically and dynamically" plugin conflict.
func init() {
	api.RegisterInitializer(options.WorkerTypeLocal, func(opts *options.InitializeOptions) (contract.RuntimeHandle, error) {
		if opts == nil {
			opts = &options.InitializeOptions{}
		}
		// api.Init() passes zero-value options, where WorkerType is
		// WorkerTypeWorker; force local mode so option validation and the
		// resulting runtime handle agree with the registered worker type.
		opts.WorkerType = options.WorkerTypeLocal
		// Local mode has no cluster: default the node IP for consumers that
		// expect a usable address.
		if opts.Network.NodeIPAddress == "" {
			opts.Network.NodeIPAddress = "127.0.0.1"
		}
		baseOpts, err := base.InitializeOptionsFromAPI(*opts)
		if err != nil {
			return nil, err
		}
		runtime, err := NewLocalModeRuntime(baseOpts)
		if err != nil {
			return nil, err
		}
		if err := runtime.Start(); err != nil {
			return nil, err
		}
		return base.NewRuntimeHandle[contract.Runtime](runtime), nil
	})
}
