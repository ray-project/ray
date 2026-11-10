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
	"github.com/ray-project/ray/go/internal/event"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// jobEventLogger reports job lifecycle events to the JOBS event log. Python
// wraps event logger creation in try/except and leaves the handle as None on
// failure, so a broken event sink never breaks job submission.
// newJobEventLogger reproduces that: it returns nil rather than an error, and
// every emit method is nil-safe.
type jobEventLogger struct {
	logger *event.EventLoggerAdapter
}

// newJobEventLogger creates the JOBS event logger, returning nil when the sink
// cannot be initialized.
func newJobEventLogger(logsDir string) *jobEventLogger {
	l, err := event.GetEventLogger(proto.Event_JOBS, logsDir)
	if err != nil {
		log.Log.Error(err, "failed to initialize the JOBS event logger")
		return nil
	}
	return &jobEventLogger{logger: l}
}

// submissionStarted emits the job submission start event.
func (e *jobEventLogger) submissionStarted(submissionID string) {
	if e == nil {
		return
	}
	e.logger.Info("Started a ray job "+submissionID+".", map[string]string{
		"submission_id": submissionID,
	})
}

// submissionCompleted emits the job completion event, at ERROR severity when
// the job failed with a message.
func (e *jobEventLogger) submissionCompleted(jobID string, status JobStatus, errorMessage string) {
	if e == nil {
		return
	}
	message := "Completed a ray job " + jobID + " with a status " + string(status) + "."
	fields := map[string]string{"submission_id": jobID}
	if errorMessage != "" {
		e.logger.Error(message+" "+errorMessage, fields)
		return
	}
	e.logger.Info(message, fields)
}
