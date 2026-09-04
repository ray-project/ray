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

// Package setup_worker provides the setup-worker command for Ray.
package setup_worker

import (
	"context"
	"fmt"
	"strings"

	"github.com/ray-project/ray/go/internal/common"
	runtime_env "github.com/ray-project/ray/go/internal/runtime_env"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/log/zap"
	"github.com/ray-project/ray/go/proto"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap/zapcore"
)

// SetupWorkerConfig holds the config options for the setup-worker command.
type SetupWorkerConfig struct {
	SerializedRuntimeEnvContext string
	Language                    string
}

var setupWorkerCfg SetupWorkerConfig

var setupWorkerCmd = &cobra.Command{
	Use:            "setup_worker",
	Short:          "Set up the environment for a Ray worker and launch the worker",
	Long:           "Set up the environment for a Ray worker by deserializing the runtime environment context and executing the appropriate worker process based on the specified language (PYTHON, JAVA, CPP, or GO).",
	DisableFlagParsing: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSetupWorker(cmd.Context(), args, &setupWorkerCfg)
	},
}

// parseKnownArgs mimics Python argparse's parse_known_args() behavior.
// It returns the remaining args after parsing known flags.
func parseKnownArgs(flagSet *pflag.FlagSet, args []string, cfg *SetupWorkerConfig) ([]string, error) {
	// Allow unknown flags without erroring.
	flagSet.ParseErrorsWhitelist.UnknownFlags = true

	// Parse the flags.
	if err := flagSet.Parse(args); err != nil {
		return nil, err
	}

	// Manually walk the original args to find unknown flags and their values.
	var remainingArgs []string

	i := 0
	for i < len(args) {
		arg := args[i]

		if !strings.HasPrefix(arg, "-") {
			// Non-flag argument, keep it in the remaining args.
			remainingArgs = append(remainingArgs, arg)
			i++
		} else {
			// This is a flag.
			var flagName string
			if strings.HasPrefix(arg, "--") {
				flagName = strings.TrimPrefix(arg, "--")
				if eqIdx := strings.Index(flagName, "="); eqIdx != -1 {
					flagName = flagName[:eqIdx]
				}
			} else if strings.HasPrefix(arg, "-") && len(arg) > 1 {
				flagName = string(arg[1]) // first character of a short flag
			}

			// Check whether this flag is registered.
			f := flagSet.Lookup(flagName)
			if f == nil {
				// Unknown flag, keep it in the remaining args.
				remainingArgs = append(remainingArgs, arg)
				// If it is not of the form --key=value, skip the next arg (the value).
				if !strings.Contains(arg, "=") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
					remainingArgs = append(remainingArgs, args[i+1])
					i += 2
				} else {
					i++
				}
			} else {
				// Known flag, skip it.
				if !strings.Contains(arg, "=") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
					i += 2
				} else {
					i++
				}
			}
		}
	}

	return remainingArgs, nil
}

func init() {}

func GetSetupWorkerCmd() *cobra.Command {
	return setupWorkerCmd
}

func runSetupWorker(ctx context.Context, args []string, cfg *SetupWorkerConfig) error {
	level, err := zapcore.ParseLevel(common.LoggerLevel)
	if err != nil {
		level = zapcore.InfoLevel
	}
	if err := zap.SetupDefaultLogger(zap.WithLevel(level)); err != nil {
		return fmt.Errorf("failed to initialize logger: %v\n", err)
	}

	logger := log.WithName("setup_worker")
	logger.Info("starting worker setup")

	// Create a separate pflag.FlagSet to parse known flags.
	flagSet := pflag.NewFlagSet("setup-worker", pflag.ContinueOnError)
	flagSet.SortFlags = false

	// Register the known flags.
	flagSet.StringVar(
		&cfg.SerializedRuntimeEnvContext,
		"serialized-runtime-env-context",
		"",
		"the serialized runtime env context",
	)

	flagSet.StringVar(
		&cfg.Language,
		"language",
		"",
		"the language type of the worker (PYTHON, JAVA, CPP, or GO)",
	)

	// Use parseKnownArgs to achieve behavior similar to Python's parse_known_args().
	remainingArgs, err := parseKnownArgs(flagSet, args, cfg)
	if err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	ctxStr := cfg.SerializedRuntimeEnvContext
	if ctxStr == "" {
		ctxStr = "{}"
	}
	runtimeEnvCtx, err := runtime_env.DeserializeContext(ctxStr)
	if err != nil {
		return fmt.Errorf("failed to deserialize runtime env context: %w", err)
	}

	logger.Info("runtime env context loaded",
		"command_prefix_len", len(runtimeEnvCtx.CommandPrefix),
		"env_vars_count", len(runtimeEnvCtx.EnvVars),
		"java_jars_count", len(runtimeEnvCtx.JavaJars),
		"has_override_entrypoint", runtimeEnvCtx.OverrideWorkerEntrypoint != "",
	)

	logger.Info("executing worker")

	// Check whether language was specified.
	if cfg.Language == "" {
		return fmt.Errorf("--language is required")
	}

	langVal, ok := proto.Language_value[strings.ToUpper(cfg.Language)]
	if !ok {
		validLanguages := make([]string, 0, len(proto.Language_value))
		for k := range proto.Language_value {
			validLanguages = append(validLanguages, k)
		}
		return fmt.Errorf("invalid language %q, must be one of: %s", cfg.Language, strings.Join(validLanguages, ", "))
	}
	if err := runtimeEnvCtx.ExecWorker(remainingArgs, proto.Language(langVal)); err != nil {
		return fmt.Errorf("failed to exec worker: %w", err)
	}

	return nil
}
