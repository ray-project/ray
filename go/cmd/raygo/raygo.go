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

package main

import (
	"os"

	"github.com/ray-project/ray/go/cmd/raygo/default_worker"
	"github.com/ray-project/ray/go/cmd/raygo/setup_worker"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/log/zap"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "raygo",
	Short: "Ray Go CLI tool",
	Long:  "Ray Go CLI tool for interacting with Ray from Go",
	Run: func(cmd *cobra.Command, args []string) {
		log.WithName("cli").Info("started", "version", "1.0.0", "args", len(args))
	},
}

func init() {
	// Register worker subcommand.
	rootCmd.AddCommand(default_worker.GetDefaultWorkerCmd())
	// Register setup-worker subcommand.
	rootCmd.AddCommand(setup_worker.GetSetupWorkerCmd())
}

func main() {
	// Initialize logger (development mode)
	if err := zap.SetupDefaultLogger(); err != nil {
		os.Exit(1)
	}

	cliLog := log.WithName("raygo")
	cliLog.Info("initializing")

	if err := rootCmd.Execute(); err != nil {
		cliLog.Error(err, "command failed")
		os.Exit(1)
	}

	cliLog.Info("completed successfully")
}
