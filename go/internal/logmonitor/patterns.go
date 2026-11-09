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
	defaultNumLinesToRead      = 1000
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
	defaultMaxLineSize         = 1 << 20  // 1 MiB; lines longer than this are truncated.
	defaultReadBufferSize      = 16 << 10 // 16 KiB; per-open-file reader buffer.
	defaultMaxBatchBytes       = 16 << 20 // 16 MiB; a pending batch larger than this is flushed early.
	publishErrorLogInterval    = 10 * time.Second
)

var (
	workerLogPattern       = regexp.MustCompile(`.*worker.*-([0-9a-f]+)-(\d+)`)
	runtimeEnvSetupPattern = regexp.MustCompile(`.*runtime_env_setup-(\d+)\.log`)
)

// DefaultMaxLineSize returns the default per-line size cap applied by the
// monitor when RuntimeConfig.MaxLineSize is unset.
func DefaultMaxLineSize() int { return defaultMaxLineSize }

// DefaultMaxFilesOpen returns the default cap on concurrently open log files
// applied when RuntimeConfig.MaxFilesOpen is unset.
func DefaultMaxFilesOpen() int { return defaultMaxFilesOpen }

// DefaultManyFilesThreshold returns the default threshold above which the
// filename scan interval is throttled, applied when
// RuntimeConfig.ManyFilesThreshold is unset.
func DefaultManyFilesThreshold() int { return defaultManyFilesThreshold }

// DefaultNumLinesToRead returns the default per-file line cap per scan cycle
// applied when RuntimeConfig.NumLinesToRead is unset.
func DefaultNumLinesToRead() int { return defaultNumLinesToRead }

// DefaultNameUpdateInterval returns the default interval between filename list
// updates applied when RuntimeConfig.NameUpdateInterval is unset.
func DefaultNameUpdateInterval() time.Duration { return defaultNameUpdateInterval }

func parseWorkerFilename(path string) (string, *int, bool) {
	match := workerLogPattern.FindStringSubmatch(path)
	if match == nil {
		return "", nil, false
	}

	pidValue, err := strconv.Atoi(match[2])
	if err != nil {
		return "", nil, false
	}
	if pidValue <= 0 || pidValue >= maxValidGCPID {
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
		tpuEntries, err := readdirPathsRecursive(filepath.Join(logsDir, "tpu_logs"), 32)
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

// readdirPathsRecursive returns the paths of all regular files reachable from
// dir, bounded by maxDepth levels of directory descent. Symlinks are followed
// (stat semantics): a symlink to a directory is descended into, a symlink to a
// regular file is collected, and a broken symlink or symlink loop (stat
// returns ELOOP) is skipped. A nonexistent dir returns an error, matching
// readdirPaths.
//
// maxDepth is the only bound on a cycle that does not trip ELOOP — a link
// pointing at an ancestor directory resolves fine and would recurse forever
// without it, and the same underlying file can then be reached through more
// than one path. The depth bound keeps that finite; it does not deduplicate.
func readdirPathsRecursive(dir string, maxDepth int) ([]string, error) {
	return readdirPathsRecursiveDepth(dir, 0, maxDepth)
}

func readdirPathsRecursiveDepth(dir string, depth, maxDepth int) ([]string, error) {
	if depth >= maxDepth {
		return []string{}, nil
	}

	// os.ReadDir builds entries from d_type where available, avoiding the
	// per-entry stat that a Readdir+os.Stat pair would otherwise perform.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		fullPath := filepath.Join(dir, entry.Name())
		switch {
		case entry.Type().IsRegular():
			// Regular file: collected directly without a stat.
			paths = append(paths, fullPath)
		case entry.Type().IsDir():
			// Directory: recurse.
			sub, err := readdirPathsRecursiveDepth(fullPath, depth+1, maxDepth)
			if err == nil {
				paths = append(paths, sub...)
			}
		default:
			// Symlink or unknown type: stat (follows the link) to decide
			// whether the target is a directory or a regular file. A broken
			// symlink or symlink loop (stat returns ELOOP) is skipped.
			st, err := os.Stat(fullPath)
			if err != nil {
				continue
			}
			if st.IsDir() {
				sub, err := readdirPathsRecursiveDepth(fullPath, depth+1, maxDepth)
				if err == nil {
					paths = append(paths, sub...)
				}
			} else if st.Mode().IsRegular() {
				paths = append(paths, fullPath)
			}
		}
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
