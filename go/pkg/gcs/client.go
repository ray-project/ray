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
	"errors"
	"sync"

	"github.com/ray-project/ray/go/pkg/ids"
)

// ErrNotImplemented indicates that a feature has not been implemented yet.
var ErrNotImplemented = errors.New("not implemented")

// ErrKeyNotFound indicates that the key does not exist (distinct from a key
// that exists but holds an empty value).
var ErrKeyNotFound = errors.New("key not found")

// Global singleton client.
var (
	clientInstance Client
	clientMu       sync.RWMutex
	clientOnce     sync.Once
)

// Client is the primary GCS client interface, composed of sub-interfaces that
// cover all GCS functionality.
type Client interface {
	InternalKVInterface
	NodeInfoInterface
	NodeResourceInterface
	ActorInfoInterface
	JobInfoInterface
	WorkerInfoInterface
	PlacementGroupInterface
	AutoscalerInterface

	// Address returns the GCS server address (host:port).
	Address() string

	// ClusterID returns the cluster ID.
	ClusterID() ids.ClusterID

	// Close disconnects from GCS.
	Close() error

	// IsClosed reports whether the client has been closed.
	// This method is used to check if a cached client is still usable.
	IsClosed() bool

	ReportAutoscalingState(autoscalingState string) error
}

// SetClient sets the global GCS client instance. It is called by
// implementation packages (e.g., go/internal/gcs/native) during
// initialization. sync.Once ensures it is set only once, and this injection
// lets external packages provide the client without creating a circular
// dependency.
func SetClient(client Client) {
	clientOnce.Do(func() {
		clientMu.Lock()
		defer clientMu.Unlock()
		clientInstance = client
	})
}

// GetClient returns the global GCS client instance, or (nil, ErrNotImplemented)
// if it has not been initialized yet.
func GetClient() (Client, error) {
	clientMu.RLock()
	defer clientMu.RUnlock()
	if clientInstance == nil {
		return nil, ErrNotImplemented
	}
	return clientInstance, nil
}

// ClearClient clears the global GCS client instance so that a closed client is
// not returned afterwards.
func ClearClient() {
	clientMu.Lock()
	defer clientMu.Unlock()
	clientInstance = nil
	// Reset sync.Once to allow the client to be set again.
	clientOnce = sync.Once{}
}
