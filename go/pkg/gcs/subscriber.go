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
	"context"
	"io"
)

// ErrorSubscriber subscribes to the GCS error stream, corresponding to the
// Python GcsErrorSubscriber.
type ErrorSubscriber interface {
	io.Closer
	// Subscribe subscribes to the error stream.
	Subscribe() error
	// Poll polls the next error.
	Poll(ctx context.Context) (errorID []byte, errorData *ErrorData, err error)
}

// LogSubscriber subscribes to the GCS log stream, corresponding to the Python
// GcsLogSubscriber.
type LogSubscriber interface {
	io.Closer
	// Subscribe subscribes to the log stream.
	Subscribe() error
	// Poll polls the next log entry.
	Poll(ctx context.Context) (*LogData, error)
}
