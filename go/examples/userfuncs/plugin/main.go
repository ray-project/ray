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

// Package plugin is the entry point for the userfuncs plugin. Importing the
// userfuncs package is what registers the user functions: its package init
// runs when the .so is loaded by a worker. This main package must not call
// RegisterFunctions itself — that would register everything a second time on
// top of the import's init.
package main

import (
	_ "github.com/ray-project/ray/go/examples/userfuncs"
)

func main() {
	// Plugin, not an executable; the userfuncs package init does the
	// registration.
}
