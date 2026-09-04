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


// Package gcs provides the Go client for Ray Global Control Store (GCS).
package gcs

import (
	"fmt"
	"time"

	"github.com/ray-project/ray/go/pkg/ids"
)

// ErrorData is the error data structure in the format used by the Python
// GcsErrorSubscriber.
type ErrorData struct {
	// JobID is the related job ID.
	JobID ids.JobID
	// Type is the error type.
	Type string
	// ErrorMessage is the detailed error description.
	ErrorMessage string
	// Timestamp is the Unix timestamp in milliseconds.
	Timestamp int64
}

// String implements fmt.Stringer.
func (e ErrorData) String() string {
	return fmt.Sprintf("ErrorData{JobID: %s, Type: %s, Message: %s, Time: %s}",
		e.JobID.Hex(), e.Type, e.ErrorMessage, time.UnixMilli(e.Timestamp))
}

// LogData is the log data structure in the format used by the Python
// GcsLogSubscriber.
type LogData struct {
	// IP is the source IP address.
	IP string
	// PID is the process ID.
	PID uint32
	// JobID is the related job ID.
	JobID ids.JobID
	// IsError indicates whether this is an error log.
	IsError bool
	// ActorName is the actor name, if applicable.
	ActorName string
	// TaskName is the task name, if applicable.
	TaskName string
	// Lines are the log text lines.
	Lines []string
}

// LogBatchPayload is the publish-side payload for a log batch.
// It mirrors the Python GcsClient.publish_logs input schema.
type LogBatchPayload struct {
	IP        string
	PID       string
	JobID     string
	IsError   bool
	Lines     []string
	ActorName string
	TaskName  string
}

// String implements fmt.Stringer.
func (l LogData) String() string {
	return fmt.Sprintf("LogData{IP: %s, PID: %d, JobID: %s, IsError: %v, Lines: %d}",
		l.IP, l.PID, l.JobID.Hex(), l.IsError, len(l.Lines))
}
