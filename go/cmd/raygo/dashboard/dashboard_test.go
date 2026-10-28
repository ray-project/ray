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

package dashboard

import (
	"testing"

	"github.com/spf13/cobra"
)

// TestGetDashboardCmdParsesClusterFlags verifies that GetDashboardCmd parses
// the cluster flags exactly as ray/_private/services.py passes them to the Go
// dashboard head, and that the parsed values are available on the command.
func TestGetDashboardCmdParsesClusterFlags(t *testing.T) {
	cmd := GetDashboardCmd()
	cmd.SetArgs([]string{
		"--host=127.0.0.1",
		"--port=8265",
		"--port-retries=3",
		"--temp-dir=/tmp/ray/tmp",
		"--log-dir=/tmp/ray/logs",
		"--session-dir=/tmp/ray/session",
		"--logging-rotate-bytes=536870912",
		"--logging-rotate-backup-count=5",
		"--gcs-address=127.0.0.1:6379",
		"--cluster-id-hex=0123456789abcdef0123456789abcdef0123456789abcdef01234567",
		"--node-ip-address=127.0.0.1",
	})

	// Disable actual execution by setting a no-op RunE
	var executed bool
	origRunE := cmd.RunE
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		executed = true
		return nil
	}
	defer func() { cmd.RunE = origRunE }()

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if !executed {
		t.Fatal("command did not execute")
	}

	for flag, want := range map[string]string{
		"gcs-address":    "127.0.0.1:6379",
		"cluster-id-hex": "0123456789abcdef0123456789abcdef0123456789abcdef01234567",
		"session-dir":    "/tmp/ray/session",
		"port-retries":   "3",
	} {
		if got := cmd.Flags().Lookup(flag); got == nil || got.Value.String() != want {
			t.Fatalf("flag %s = %v, want %s", flag, got, want)
		}
	}
}
