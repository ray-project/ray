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

// Package userfuncs provides example user functions for Ray Go.
//
// The task functions in this package are registered manually through
// RegisterFunctions (see funcs.go). That is the mechanism both the driver (via
// a package import) and the worker (via the userfuncs.so plugin) use to
// populate the global function registry.
//
// The checked-in generated_wrappers.go and generated_registry.go files were
// produced by the raygen code generator, which is not part of this repository,
// and are maintained by hand since its removal — edit them freely. The
// //go:ray:task markers on the functions in funcs.go are inert documentation
// of that history: nothing in this tree scans them.
//
// # Example
//
// See e2e/driver_app/main.go for complete examples of submitting tasks through
// the registered functions.
package userfuncs
