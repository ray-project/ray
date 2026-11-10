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
	"context"
	"os"
	"path/filepath"
)

// JobLogStorageClient provides disk storage for the stdout/stderr of driver
// script logs.
type JobLogStorageClient struct {
	logsDir string
}

func NewJobLogStorageClient(logsDir string) *JobLogStorageClient {
	return &JobLogStorageClient{logsDir: logsDir}
}

const (
	numLogLinesOnError = 10
	maxLogSize         = 20000
)

// getLogs returns the whole driver log, or an empty string when the file does
// not exist.
func (c *JobLogStorageClient) getLogs(jobID string) string {
	content, err := os.ReadFile(c.getLogFilePath(jobID))
	if err != nil {
		return ""
	}
	return string(content)
}

// tailLogs streams the driver log as it grows. The returned channels are
// closed when ctx is cancelled.
func (c *JobLogStorageClient) tailLogs(ctx context.Context, jobID string) (<-chan []string, <-chan error) {
	return fileTailIterator(ctx, c.getLogFilePath(jobID))
}

// getLastNLogLines returns the tail of the driver log, defaulting to
// numLogLinesOnError lines when numLogLines is 0.
func (c *JobLogStorageClient) getLastNLogLines(ctx context.Context, jobID string, numLogLines int) string {
	if numLogLines == 0 {
		numLogLines = numLogLinesOnError
	}
	return fastTailLastNLines(c.getLogFilePath(jobID), numLogLines, maxLogSize)
}

func (c *JobLogStorageClient) getLogFilePath(jobID string) string {
	return filepath.Join(c.logsDir, jobLogsPath(jobID))
}
