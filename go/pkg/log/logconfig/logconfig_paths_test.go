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

package logconfig

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/log/lumberjack"
	"github.com/stretchr/testify/assert"
)

// TestOutputPathsWithStderrOutput verifies that WithStderrOutput adds the
// stderr marker to both the Debug/Info core paths and the Warn/Error core
// paths, so every level is also visible on the process's stderr.
func TestOutputPathsWithStderrOutput(t *testing.T) {
	opt := NewLogOption(
		WithLogsDir("/tmp/logs"),
		WithLoggingFilename("monitor.log"),
		WithStderrOutput(),
	)

	logPaths, errPaths := opt.outputPaths()
	assert.Equal(t, []string{"/tmp/logs/monitor.log", lumberjack.StderrMarker}, logPaths)
	assert.Equal(t, []string{"/tmp/logs/monitor.log", lumberjack.StderrMarker}, errPaths)
}

// TestOutputPathsWithoutStderrOutput verifies that the default logger only
// writes to the configured log file and does not touch stderr.
func TestOutputPathsWithoutStderrOutput(t *testing.T) {
	opt := NewLogOption(
		WithLogsDir("/tmp/logs"),
		WithLoggingFilename("monitor.log"),
	)

	logPaths, errPaths := opt.outputPaths()
	assert.Equal(t, []string{"/tmp/logs/monitor.log"}, logPaths)
	assert.Equal(t, []string{"/tmp/logs/monitor.log"}, errPaths)
}

// TestOutputPathsWithStderrOutputNoLogsDir verifies that the stderr marker is
// the only output when no log file is configured.
func TestOutputPathsWithStderrOutputNoLogsDir(t *testing.T) {
	opt := NewLogOption(WithStderrOutput())

	logPaths, errPaths := opt.outputPaths()
	assert.Equal(t, []string{lumberjack.StderrMarker}, logPaths)
	assert.Equal(t, []string{lumberjack.StderrMarker}, errPaths)
}
