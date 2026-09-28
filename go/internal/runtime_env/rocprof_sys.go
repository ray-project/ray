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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/ray-project/ray/go/pkg/log"
)

var rocProfSysDefaultConfig = map[string]interface{}{
	"env": map[string]string{
		"ROCPROFSYS_TIME_OUTPUT":   "false",
		"ROCPROFSYS_OUTPUT_PREFIX": "worker_process_%p",
	},
	"args": map[string]string{
		"F": "true",
	},
}

type RocProfSysPlugin struct {
	rocprofSysCmd   []string
	rocprofSysEnv   map[string]string
	rocprofSysDir   string
	rocprofSysMutex sync.RWMutex
}

func NewRocProfSysPlugin(resourcesDir string) *RocProfSysPlugin {
	sessionDir := filepath.Dir(resourcesDir)
	rocprofSysDir := filepath.Join(sessionDir, "logs", "rocprof_sys")
	if err := os.MkdirAll(rocprofSysDir, 0750); err != nil {
		log.Log.Info("Failed to create rocprof_sys directory", "dir", rocprofSysDir, "error", err)
	}
	return &RocProfSysPlugin{
		rocprofSysCmd: nil,
		rocprofSysEnv: nil,
		rocprofSysDir: rocprofSysDir,
	}
}

func (p *RocProfSysPlugin) Name() string {
	return FieldRocprofSys
}

func (p *RocProfSysPlugin) Priority() int {
	return RayRuntimeEnvPluginDefaultPriority
}

func (p *RocProfSysPlugin) Validate(runtimeEnv *RuntimeEnv) error {
	rocprofSysConfig := (*runtimeEnv)[FieldRocprofSys]
	if rocprofSysConfig == nil {
		return nil
	}
	switch v := rocprofSysConfig.(type) {
	case string:
		if v != "default" {
			return fmt.Errorf("validate rocprof_sys config: unsupported rocprof_sys config: %s", v)
		}
		return nil
	case map[string]interface{}:
		if argsRaw, ok := v["args"]; ok {
			switch argsVal := argsRaw.(type) {
			case map[string]string:
			case map[string]interface{}:
				for k, val := range argsVal {
					if _, isStr := val.(string); !isStr {
						return fmt.Errorf("validate rocprof_sys config: args value for key '%s' must be a string, got: %T", k, val)
					}
				}
			default:
				return fmt.Errorf("validate rocprof_sys config: args must be a map, got: %T", argsRaw)
			}
		}
		if envRaw, ok := v["env"]; ok {
			switch envVal := envRaw.(type) {
			case map[string]string:
			case map[string]interface{}:
				for k, val := range envVal {
					if _, isStr := val.(string); !isStr {
						return fmt.Errorf("validate rocprof_sys config: env value for key '%s' must be a string, got: %T", k, val)
					}
				}
			default:
				return fmt.Errorf("validate rocprof_sys config: env must be a map, got: %T", envRaw)
			}
		}
		return nil
	default:
		return fmt.Errorf("validate rocprof_sys config: must be a map[string]interface{} or string 'default', got: %T", rocprofSysConfig)
	}
}

func (p *RocProfSysPlugin) GetURIs(runtimeEnv *RuntimeEnv) []string {
	return nil
}

func (p *RocProfSysPlugin) Create(ctx context.Context, uri string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) (int64, error) {
	rocprofSysConfig := (*runtimeEnv)[FieldRocprofSys]
	if rocprofSysConfig == nil {
		return 0, nil
	}

	if runtime.GOOS != "linux" {
		return 0, fmt.Errorf("create rocprof_sys plugin: rocprof-sys CLI is only available on Linux, current platform: %s", runtime.GOOS)
	}

	if err := p.Validate(runtimeEnv); err != nil {
		return 0, fmt.Errorf("validate rocprof_sys config: %w", err)
	}

	var configMap map[string]interface{}
	switch v := rocprofSysConfig.(type) {
	case string:
		configMap = rocProfSysDefaultConfig
	case map[string]interface{}:
		configMap = v
	}

	isValid, errMsg := p.checkRocProfSysScript(ctx, configMap)
	if !isValid {
		log.Log.Info("Rocprof_sys validation failed",
			"config", configMap,
			"error", errMsg,
			"platform", runtime.GOOS,
		)
		return 0, fmt.Errorf("create rocprof_sys plugin: rocprof-sys profile failed to run with the following error message:\n%s", errMsg)
	}

	// Add output path to env
	if _, ok := configMap["env"]; !ok {
		configMap["env"] = make(map[string]interface{})
	}
	if envMap, ok := configMap["env"].(map[string]interface{}); ok {
		envMap["ROCPROFSYS_OUTPUT_PATH"] = p.rocprofSysDir
	}

	parsedCmd, parsedEnv := p.parseRocProfSysConfig(configMap)
	p.rocprofSysMutex.Lock()
	p.rocprofSysCmd = parsedCmd
	p.rocprofSysEnv = parsedEnv
	p.rocprofSysMutex.Unlock()

	log.Log.V(1).Info("Rocprof_sys plugin created successfully",
		"config", configMap,
		"output_dir", p.rocprofSysDir,
		"cmd", strings.Join(p.rocprofSysCmd, " "),
	)
	return 0, nil
}

func (p *RocProfSysPlugin) ModifyContext(uris []string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) error {
	p.rocprofSysMutex.RLock()
	rocprofSysCmd := p.rocprofSysCmd
	p.rocprofSysMutex.RUnlock()

	if len(rocprofSysCmd) == 0 {
		log.Log.V(1).Info("Rocprof_sys plugin: no config, skipping context modification")
		return nil
	}

	log.Log.Info("Running rocprof-sys profiler",
		"cmd", strings.Join(rocprofSysCmd, " "),
		"py_executable", context.PyExecutable,
	)

	context.PyExecutable = strings.Join(rocprofSysCmd, " ") + " " + context.PyExecutable

	for k, v := range p.rocprofSysEnv {
		context.EnvVars[k] = v
	}

	return nil
}

func (p *RocProfSysPlugin) DeleteURI(uri string) int64 {
	return 0
}

func (p *RocProfSysPlugin) checkRocProfSysScript(ctx context.Context, configMap map[string]interface{}) (bool, string) {
	testFolder := filepath.Join(p.rocprofSysDir, "test")

	configCopy := make(map[string]interface{})
	for k, v := range configMap {
		configCopy[k] = v
	}

	if _, ok := configCopy["env"]; !ok {
		configCopy["env"] = make(map[string]interface{})
	}
	if envMap, ok := configCopy["env"].(map[string]interface{}); ok {
		envMap["ROCPROFSYS_OUTPUT_PATH"] = testFolder
	}

	rocprofSysCmd, rocprofSysEnv := p.parseRocProfSysConfig(configCopy)

	err := os.MkdirAll(testFolder, 0o750)
	if err != nil {
		return false, fmt.Sprintf("failed to create test folder: %v", err)
	}

	testFilePath := filepath.Join(testFolder, "test.py")
	err = os.WriteFile(testFilePath, []byte("import time\n"), 0o640)
	if err != nil {
		cleanupErr := os.RemoveAll(testFolder)
		if cleanupErr != nil {
			log.Log.V(1).Info("Failed to cleanup test folder after file creation error",
				"folder", testFolder,
				"cleanup_error", cleanupErr,
			)
		}
		return false, fmt.Sprintf("failed to create test file: %v", err)
	}

	testCmd := append(rocprofSysCmd, testFilePath)

	envMap := make(map[string]string)
	for k, v := range rocprofSysEnv {
		envMap[k] = v
	}
	for _, v := range os.Environ() {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}

	process := exec.CommandContext(ctx, testCmd[0], testCmd[1:]...)
	process.Env = make([]string, 0, len(envMap))
	for k, v := range envMap {
		process.Env = append(process.Env, fmt.Sprintf("%s=%s", k, v))
	}
	output, err := process.CombinedOutput()

	log.Log.V(1).Info("Running rocprof_sys validation command",
		"cmd", strings.Join(testCmd, " "),
		"test_folder", testFolder,
	)

	cleanUpCmd := exec.Command("rm", "-r", testFolder)
	_ = cleanUpCmd.Run()

	if err != nil || process.ProcessState.ExitCode() != 0 {
		errorMsg := strings.TrimSpace(string(output))
		if errorMsg == "" {
			if err != nil {
				errorMsg = err.Error()
			} else {
				errorMsg = fmt.Sprintf("process exited with code %d", process.ProcessState.ExitCode())
			}
		}
		log.Log.Info("Rocprof_sys validation command failed",
			"cmd", strings.Join(rocprofSysCmd, " "),
			"error", errorMsg,
		)
		return false, errorMsg
	}

	log.Log.V(1).Info("Rocprof_sys validation command succeeded")
	return true, ""
}

func (p *RocProfSysPlugin) parseRocProfSysConfig(configMap map[string]interface{}) ([]string, map[string]string) {
	rocprofSysCmd := []string{"rocprof-sys-python"}
	rocprofSysEnv := make(map[string]string)

	// Parse rocprof-sys arg options from "args" key
	if argsRaw, ok := configMap["args"]; ok {
		var argsMap map[string]string
		switch v := argsRaw.(type) {
		case map[string]string:
			argsMap = v
		case map[string]interface{}:
			argsMap = make(map[string]string)
			for k, val := range v {
				if strVal, ok := val.(string); ok {
					argsMap[k] = strVal
				}
			}
		}
		// Parse rocprof-sys arg options
		for option, optionVal := range argsMap {
			// option standard based on
			// https://www.gnu.org/software/libc/manual/html_node/Argument-Syntax.html
			if len(option) > 1 {
				rocprofSysCmd = append(rocprofSysCmd, fmt.Sprintf("--%s=%s", option, optionVal))
			} else {
				rocprofSysCmd = append(rocprofSysCmd, "-"+option, optionVal)
			}
		}
	}

	// Parse rocprof-sys env options from "env" key
	if envRaw, ok := configMap["env"]; ok {
		var envMap map[string]string
		switch v := envRaw.(type) {
		case map[string]string:
			envMap = v
		case map[string]interface{}:
			envMap = make(map[string]string)
			for k, val := range v {
				if strVal, ok := val.(string); ok {
					envMap[k] = strVal
				}
			}
		}
		rocprofSysEnv = envMap
	}

	rocprofSysCmd = append(rocprofSysCmd, "--")
	return rocprofSysCmd, rocprofSysEnv
}
