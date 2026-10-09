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

// Package version provides the raygo `version` subcommand, printing the Ray
// version and source commit linked into the binary, aligned with the Python
// `ray --version` output (ray, version <VERSION>).
package version

import (
	"fmt"

	"github.com/ray-project/ray/go/pkg/version"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the Ray version and commit",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("raygo, version %s, commit %s\n", version.RayVersion, version.RayCommit)
		return nil
	},
}

// GetVersionCmd returns the `version` subcommand.
func GetVersionCmd() *cobra.Command {
	return versionCmd
}
