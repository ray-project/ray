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

package logmonitor

import (
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	defaultMaxFilesOpen        = 200
	defaultMaxLinesPerRead     = 1000
	defaultManyFilesThreshold  = 1000
	defaultNameUpdateInterval  = 500 * time.Millisecond
	defaultIdleSleep           = 100 * time.Millisecond
	initialScanDelay           = 150 * time.Millisecond
	workerOutLogPattern        = "worker*.out"
	workerErrLogPattern        = "worker*.err"
	logPrefixActorName         = ":actor_name:"
	logPrefixTaskName          = ":task_name:"
	logPrefixJobID             = ":job_id:"
	windowsAccessViolationLine = "Windows fatal exception: access violation"
)

var (
	workerLogPattern       = regexp.MustCompile(`.*worker.*-([0-9a-f]+)-(\d+)`)
	runtimeEnvSetupPattern = regexp.MustCompile(`.*runtime_env_setup-(\d+)\.log`)
)

func parseWorkerFilename(path string) (string, *int, bool) {
	match := workerLogPattern.FindStringSubmatch(path)
	if match == nil {
		return "", nil, false
	}

	pidValue, err := strconv.Atoi(match[2])
	if err != nil {
		return "", nil, false
	}

	pid := pidValue
	return match[1], &pid, true
}

func parseRuntimeEnvFilename(path string) (string, bool) {
	match := runtimeEnvSetupPattern.FindStringSubmatch(path)
	if match == nil {
		return "", false
	}
	return match[1], true
}

func orderedLogPaths(logsDir string, autoscalerV2Enabled bool, runtimeEnvToDriver bool, tpuDirExists bool) ([]string, error) {
	entries, err := readdirPaths(logsDir)
	if err != nil {
		return nil, err
	}

	candidates := make([]string, 0, len(entries))
	appendMatches := func(pattern string) {
		for _, entry := range entries {
			if matchesPath(pattern, filepath.Base(entry)) {
				candidates = append(candidates, entry)
			}
		}
	}
	appendMatches("worker*[.out|.err]")
	appendMatches("java-worker*.log")
	appendMatches("raylet*.err")
	if autoscalerV2Enabled {
		candidates = append(candidates, filepath.Join(logsDir, "events", "event_AUTOSCALER.log"))
	} else {
		candidates = append(candidates, filepath.Join(logsDir, "monitor.log"))
	}
	appendMatches("gcs_server*.err")
	if tpuDirExists {
		tpuEntries, err := readdirPaths(filepath.Join(logsDir, "tpu_logs"))
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, tpuEntries...)
	}
	if runtimeEnvToDriver {
		appendMatches("runtime_env*.log")
	}
	return candidates, nil
}

func matchesPath(pattern, name string) bool {
	ok, err := path.Match(pattern, name)
	return err == nil && ok
}

func readdirPaths(dir string) ([]string, error) {
	handle, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer handle.Close()

	infos, err := handle.Readdir(-1)
	if err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(infos))
	for _, info := range infos {
		paths = append(paths, filepath.Join(dir, info.Name()))
	}
	return paths, nil
}

func isErrFile(path string) bool {
	return strings.HasSuffix(path, ".err")
}

func logPatterns(autoscalerV2Enabled bool, runtimeEnvToDriver bool) []string {
	patterns := []string{
		workerOutLogPattern,
		workerErrLogPattern,
		"java-worker*.log",
		"raylet*.err",
		"gcs_server*.err",
	}
	if autoscalerV2Enabled {
		patterns = append(patterns, filepath.Join("events", "event_AUTOSCALER.log"))
	} else {
		patterns = append(patterns, "monitor.log")
	}
	if runtimeEnvToDriver {
		patterns = append(patterns, "runtime_env*.log")
	}
	return patterns
}

func classifyComponentPID(path string) string {
	normalized := strings.ReplaceAll(path, "\\", "/")
	switch {
	case strings.Contains(normalized, "/raylet"):
		return "raylet"
	case strings.Contains(normalized, "/gcs_server"):
		return "gcs_server"
	case strings.Contains(normalized, "/monitor") || strings.Contains(normalized, "event_AUTOSCALER"):
		return "autoscaler"
	case strings.Contains(normalized, "/runtime_env"):
		return "runtime_env"
	default:
		return ""
	}
}
