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

package default_worker

import (
	"testing"
)

// TestWorkerCmd_UnknownFlagsTolerated verifies that the worker command tolerates
// unknown flags. The raylet appends shared flags (e.g. --ray-debugger-external)
// to every worker command regardless of language, so the Go worker must ignore
// unknown flags instead of failing to start. This matches the Java/C++ workers.
func TestWorkerCmd_UnknownFlagsTolerated(t *testing.T) {
	cmd := GetDefaultWorkerCmd()
	if !cmd.FParseErrWhitelist.UnknownFlags {
		t.Error("WorkerCmd.FParseErrWhitelist.UnknownFlags should be true to tolerate raylet-appended flags")
	}

	// Parsing a list that contains unknown flags must not report an unknown flag error.
	args := []string{"--ray-debugger-external", "--startup-token=123", "extra-arg"}
	cmd.SetArgs(args)
	if err := cmd.ParseFlags(args); err != nil {
		t.Errorf("ParseFlags returned error for unknown flag: %v", err)
	}
}
