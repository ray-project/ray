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

package runtime_env

import (
	"context"
	"fmt"
	"github.com/ray-project/ray/go/pkg/log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

var nsightDefaultConfig = map[string]string{
	"t":            "cuda,cudnn,cublas,nvtx",
	"o":            "worker_process_%p",
	"stop-on-exit": "true",
}

type NsightPlugin struct {
	nsightCmd   []string
	nsightDir   string
	nsightMutex sync.RWMutex
}

func NewNsightPlugin(resourcesDir string) *NsightPlugin {
	sessionDir := filepath.Dir(resourcesDir)
	nsightDir := filepath.Join(sessionDir, "logs", "nsight")
	if err := os.MkdirAll(nsightDir, 0750); err != nil {
		log.Log.Info("Failed to create Nsight directory", "dir", nsightDir, "error", err)
	}
	return &NsightPlugin{
		nsightCmd: nil,
		nsightDir: nsightDir,
	}
}

func (p *NsightPlugin) Name() string {
	return FieldNsight
}

func (p *NsightPlugin) Priority() int {
	return RayRuntimeEnvPluginDefaultPriority
}

func (p *NsightPlugin) Validate(runtimeEnv *RuntimeEnv) error {
	nsightConfig := (*runtimeEnv)[FieldNsight]
	if nsightConfig == nil {
		return nil
	}
	switch v := nsightConfig.(type) {
	case string:
		if v != "default" {
			return fmt.Errorf("validate nsight config: unsupported nsight config: %s", v)
		}
		return nil
	case map[string]interface{}:
		for k, val := range v {
			if _, isStr := val.(string); !isStr {
				return fmt.Errorf("validate nsight config: value for key '%s' must be a string, got: %T", k, val)
			}
		}
		return nil
	default:
		return fmt.Errorf("validate nsight config: must be a map[string]interface{} or string 'default', got: %T", nsightConfig)
	}
}

func (p *NsightPlugin) GetURIs(runtimeEnv *RuntimeEnv) []string {
	return nil
}

func (p *NsightPlugin) Create(ctx context.Context, uri string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) (int64, error) {
	nsightConfig := (*runtimeEnv)[FieldNsight]
	if nsightConfig == nil {
		return 0, nil
	}

	if runtime.GOOS != "linux" {
		return 0, fmt.Errorf("create nsight plugin: nsight CLI is only available on Linux, current platform: %s", runtime.GOOS)
	}

	// Validate the config first.
	if err := p.Validate(runtimeEnv); err != nil {
		return 0, fmt.Errorf("validate nsight config: %w", err)
	}

	var configMap map[string]string
	switch v := nsightConfig.(type) {
	case string:
		configMap = make(map[string]string)
		for k, val := range nsightDefaultConfig {
			configMap[k] = val
		}
	case map[string]interface{}:
		configMap = make(map[string]string)
		for k, val := range v {
			configMap[k] = val.(string)
		}
	}

	isValid, errMsg := p.checkNsightScript(ctx, configMap)
	if !isValid {
		log.Log.Info("Nsight validation failed",
			"config", configMap,
			"error", errMsg,
			"platform", runtime.GOOS,
		)
		return 0, fmt.Errorf("create nsight plugin: nsight profile failed to run with the following error message:\n%s", errMsg)
	}

	configMap["o"] = filepath.Join(p.nsightDir, configMap["o"])
	parsedCmd := p.parseNsightConfig(configMap)
	p.nsightMutex.Lock()
	p.nsightCmd = parsedCmd
	p.nsightMutex.Unlock()
	log.Log.V(1).Info("Nsight plugin created successfully",
		"config", configMap,
		"output_dir", p.nsightDir,
		"cmd", strings.Join(p.nsightCmd, " "),
	)
	return 0, nil
}

func (p *NsightPlugin) ModifyContext(uris []string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) error {
	p.nsightMutex.RLock()
	nsightCmd := p.nsightCmd
	p.nsightMutex.RUnlock()
	if len(nsightCmd) == 0 {
		log.Log.V(1).Info("Nsight plugin: no config, skipping context modification")
		return nil
	}
	log.Log.Info("Running nsight profiler",
		"cmd", strings.Join(nsightCmd, " "),
		"py_executable", context.PyExecutable,
	)
	context.PyExecutable = strings.Join(nsightCmd, " ") + " python"
	return nil
}

func (p *NsightPlugin) DeleteURI(uri string) int64 {
	return 0
}

func (p *NsightPlugin) checkNsightScript(ctx context.Context, nsightConfig map[string]string) (bool, string) {
	testOutput := filepath.Join(p.nsightDir, "empty")
	configCopy := make(map[string]string)
	for k, v := range nsightConfig {
		configCopy[k] = v
	}
	configCopy["o"] = testOutput
	nsightCmd := p.parseNsightConfig(configCopy)
	nsightCmd = append(nsightCmd, sysExecutable(), "-c", "\"\"")
	log.Log.V(1).Info("Running nsight validation command",
		"cmd", strings.Join(nsightCmd, " "),
		"test_output", testOutput,
	)
	process := exec.CommandContext(ctx, nsightCmd[0], nsightCmd[1:]...)
	output, err := process.CombinedOutput()
	cleanUpCmd := exec.Command("rm", testOutput+".nsys-rep")
	_ = cleanUpCmd.Run()
	if err != nil {
		errorMsg := strings.TrimSpace(string(output))
		if errorMsg == "" {
			errorMsg = err.Error()
		}
		log.Log.Info("Nsight validation command failed",
			"cmd", strings.Join(nsightCmd, " "),
			"error", errorMsg,
		)
		return false, errorMsg
	}
	log.Log.V(1).Info("Nsight validation command succeeded")
	return true, ""
}

func sysExecutable() string {
	executable, err := os.Executable()
	if err != nil {
		return "python"
	}
	return executable
}

func (p *NsightPlugin) parseNsightConfig(nsightConfig map[string]string) []string {
	nsightCmd := []string{"nsys", "profile"}
	for option, optionVal := range nsightConfig {
		if len(option) > 1 {
			nsightCmd = append(nsightCmd, fmt.Sprintf("--%s=%s", option, optionVal))
		} else {
			nsightCmd = append(nsightCmd, "-"+option, optionVal)
		}
	}
	return nsightCmd
}
