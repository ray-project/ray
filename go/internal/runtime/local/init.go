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

// Package local is the open-source entry point for the pure-Go local mode
// runtime: importing it makes "github.com/ray-project/ray/go/pkg/runtime/api"
// initialize an in-memory LocalModeRuntime, so api.Instance().Init() works
// without a Ray cluster and without the native/CGO CoreWorker bridge.
//
// This package is a thin adapter only. The implementation lives in
// go/internal/runtime/local_mode, whose init() registers the WorkerTypeLocal
// initializer with pkg/runtime/api; the blank import below makes that
// registration reachable through a stable import path while keeping the public
// api layer free of internal imports (dependency inversion). Registering a
// second initializer for the same worker type here would be redundant.
//
// The registration is pure Go, so linking it into a driver pulls in neither the
// C++ core worker nor gRPC and cannot hit the "linked both statically and
// dynamically" plugin conflict.
package local

import (
	// Registration side effect: local_mode.init() registers the WorkerTypeLocal
	// initializer with pkg/runtime/api.
	_ "github.com/ray-project/ray/go/internal/runtime/local_mode"
)
