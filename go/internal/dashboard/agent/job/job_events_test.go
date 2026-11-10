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

package job

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJobEventLoggerNilReceiver covers the "event sink unavailable" contract:
// Python leaves the logger as None when creation fails, so every emit helper
// must tolerate a nil receiver instead of breaking job submission or stop.
func TestJobEventLoggerNilReceiver(t *testing.T) {
	var logger *jobEventLogger
	assert.NotPanics(t, func() { logger.submissionStarted("sub1") })
	assert.NotPanics(t, func() { logger.submissionCompleted("sub1", JobStatusFailed, "boom") })
	assert.NotPanics(t, func() { logger.submissionCompleted("sub1", JobStatusSucceeded, "") })
}

// TestNewJobEventLoggerSinkFailure covers the constructor failure path: a
// logs directory that cannot be created yields a nil logger, and later emit
// calls stay no-ops. The failure branch must be reached before any successful
// JOBS logger is cached in this process (event.GetEventLogger keeps a
// per-source singleton), hence the failure case runs first in this test.
func TestNewJobEventLoggerSinkFailure(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocked")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o644))

	logger := newJobEventLogger(blocker)
	assert.Nil(t, logger)
	assert.NotPanics(t, func() { logger.submissionStarted("sub1") })
	assert.NotPanics(t, func() { logger.submissionCompleted("sub1", JobStatusFailed, "boom") })

	// A writable directory yields a usable logger.
	okDir := t.TempDir()
	ok := newJobEventLogger(okDir)
	require.NotNil(t, ok)
	assert.NotPanics(t, func() { ok.submissionStarted("sub1") })
	assert.NotPanics(t, func() { ok.submissionCompleted("sub1", JobStatusSucceeded, "") })

	// The lifecycle events landed in the JOBS event log.
	entries, err := os.ReadDir(filepath.Join(okDir, "events"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "event_JOBS.log", entries[0].Name())
	content, err := os.ReadFile(filepath.Join(okDir, "events", entries[0].Name()))
	require.NoError(t, err)
	assert.Contains(t, string(content), "Started a ray job sub1.")
	assert.Contains(t, string(content), "Completed a ray job sub1 with a status SUCCEEDED.")
}
