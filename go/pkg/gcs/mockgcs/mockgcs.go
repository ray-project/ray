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

// Package mockgcs provides a gcs.Client test double shared by several test
// files.
package mockgcs

import "github.com/ray-project/ray/go/pkg/gcs"

// Client is a test double for the gcs.Client interface.
//
// It embeds the gcs.Client interface so that only the methods a test cares
// about (Close and IsClosed) need to be overridden. Every other method keeps
// the embedded nil interface and panics when called, which surfaces unexpected
// GCS access early in tests and avoids maintaining a full stub in several files
// whenever the interface grows.
type Client struct {
	gcs.Client
	closed bool
}

// Close records that the client was closed and returns nil.
func (m *Client) Close() error {
	m.closed = true
	return nil
}

// IsClosed reports whether the client has been closed.
func (m *Client) IsClosed() bool {
	return m.closed
}
