// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"fmt"
	"sync"

	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
)

// InitFunc is the type of the initialization function.
// It accepts options and returns a RuntimeHandle.
type InitFunc func(*options.InitializeOptions) (contract.RuntimeHandle, error)

// initFuncs maps an execution mode to its registered initializer.
var initFuncs = map[options.WorkerType]InitFunc{}

var initFuncsMu sync.Mutex

// RegisterInitializer registers an initializer for a specific worker type.
func RegisterInitializer(workerType options.WorkerType, fn InitFunc) {
	initFuncsMu.Lock()
	defer initFuncsMu.Unlock()
	initFuncs[workerType] = fn
}

// getInitFunc returns the initializer for the given worker type (Local → local
// mode; Driver/Worker → native). Falls back to any single registration for
// backward compatibility.
func getInitFunc(workerType options.WorkerType) InitFunc {
	initFuncsMu.Lock()
	defer initFuncsMu.Unlock()
	if len(initFuncs) == 1 {
		for _, fn := range initFuncs {
			return fn
		}
	}
	return initFuncs[workerType]
}

// defaultFactory is a fallback factory that returns an error if no initializer is registered.
type defaultFactory struct{}

func (f *defaultFactory) Initialize(opts *options.InitializeOptions) (contract.RuntimeHandle, error) {
	if fn := getInitFunc(opts.WorkerType); fn != nil {
		return fn(opts)
	}
	return nil, fmt.Errorf("no runtime initializer registered. " +
		"Make sure to import github.com/ray-project/ray/go/internal/runtime/native")
}

// globalFactory is the global factory instance.
var globalFactory = &defaultFactory{}

// getFactory returns the global factory instance.
func getFactory() Factory {
	return globalFactory
}

// Factory defines the interface for creating runtime instances.
type Factory interface {
	Initialize(opts *options.InitializeOptions) (contract.RuntimeHandle, error)
}
