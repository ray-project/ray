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

// Package local registers a pure-Go LocalModeRuntime as the Ray runtime
// initializer so that api.Instance().Init() works without a Ray cluster and
// without the native/CGO CoreWorker bridge.
//
// This is the open-source minimal-runtime entry point: importing this package
// makes "github.com/ray-project/ray/go/pkg/runtime/api" initialize a local
// in-memory runtime instead of failing with "no runtime initializer".
package local

import (
	"github.com/ray-project/ray/go/internal/runtime/base"
	"github.com/ray-project/ray/go/internal/runtime/local_mode"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
)

func init() {
	api.RegisterInitializer(options.WorkerTypeLocal, func(opts *options.InitializeOptions) (contract.RuntimeHandle, error) {
		if opts == nil {
			opts = &options.InitializeOptions{}
		}
		opts.WorkerType = options.WorkerTypeLocal
		// Local mode has no real network: default the node IP so option
		// validation inside InitializeOptionsFromAPI passes.
		if opts.Network.NodeIPAddress == "" {
			opts.Network.NodeIPAddress = "127.0.0.1"
		}
		baseOpts, err := base.InitializeOptionsFromAPI(*opts)
		if err != nil {
			return nil, err
		}
		runtime, err := local_mode.NewLocalModeRuntime(baseOpts)
		if err != nil {
			return nil, err
		}
		if err := runtime.Start(); err != nil {
			return nil, err
		}
		return base.NewRuntimeHandle[contract.Runtime](runtime), nil
	})
}
