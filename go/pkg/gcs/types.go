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
