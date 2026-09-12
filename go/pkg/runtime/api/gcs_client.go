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

package api

import (
	"context"
	"sync"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/proto"
)

// ============================================================================
// GCS Client Interface and Factory (Dependency Inversion)
// ============================================================================

// GCSClientFactory defines the interface for creating GCS clients.
// This follows the Dependency Inversion Principle:
// - High-level module (api) defines the abstraction
// - Low-level module (internal/gcs/native) implements the abstraction
type GCSClientFactory interface {
	// CreateClient creates a new GCS client with the given options.
	CreateClient(opts gcs.ClientOptions) (GCSClient, error)
}

// GCSClient defines the interface for GCS client operations.
// This interface abstracts away the concrete implementation details,
// allowing the api package to depend only on abstractions rather than
// concrete implementations in go/internal/gcs/native.
type GCSClient interface {
	// GetNodeToConnect fetches node connection info for a driver node.
	// This corresponds to Java's GcsClient.getNodeToConnectForDriver().
	GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error)

	// NextJobID fetches the next available JobID from GCS.
	// This corresponds to Java's GcsClient.nextJobId().
	// Returns the hex-encoded JobID string (4 bytes = 8 hex characters).
	NextJobID(ctx context.Context) (string, error)

	// Close closes the GCS client connection.
	Close() error

	// IsClosed reports whether the client has been closed.
	// This method is used by the cache to check if a cached client is still usable.
	IsClosed() bool
}

// globalGCSClientFactory is the registered GCS client factory.
// This is set by go_runtime.so during initialization via RegisterGCSClientFactory().
var (
	globalGCSClientFactory GCSClientFactory
	gcsFactoryMu           sync.RWMutex
)

// RegisterGCSClientFactory registers the GCS client factory.
// This function is called by go_runtime.so during initialization to provide
// the concrete implementation of GCSClientFactory.
//
// The factory pattern allows go_runtime.so to inject the concrete implementation
// (from go/internal/gcs/native) into the api package without the api package
// directly importing go/internal/gcs/native.
func RegisterGCSClientFactory(factory GCSClientFactory) {
	gcsFactoryMu.Lock()
	defer gcsFactoryMu.Unlock()
	globalGCSClientFactory = factory
}

// getGCSClientFactory returns the registered GCS client factory.
// Returns nil if no factory has been registered.
func getGCSClientFactory() GCSClientFactory {
	gcsFactoryMu.RLock()
	defer gcsFactoryMu.RUnlock()
	return globalGCSClientFactory
}

// createGCSClient creates a GCS client using the registered factory.
// Returns an error if no factory has been registered.
func createGCSClient(opts gcs.ClientOptions) (GCSClient, error) {
	factory := getGCSClientFactory()
	if factory == nil {
		return nil, ErrGCSClientFactoryNotRegistered
	}
	return factory.CreateClient(opts)
}

// ErrGCSClientFactoryNotRegistered is returned when trying to create a GCS client
// before the factory has been registered by go_runtime.so.
var ErrGCSClientFactoryNotRegistered = &gcsClientFactoryNotRegisteredError{}

type gcsClientFactoryNotRegisteredError struct{}

func (e *gcsClientFactoryNotRegisteredError) Error() string {
	return "GCS client factory not registered - go_runtime.so must call RegisterGCSClientFactory() during initialization"
}

// ============================================================================
// GCS Client Cache (for reusing clients across multiple operations)
// ============================================================================

// gcsClientCache is a global cache for GCS clients.
var gcsClientCache sync.Map

// getOrCreateCachedClient gets a cached GCS client or creates a new one.
func getOrCreateCachedClient(gcsAddress string, opts gcs.ClientOptions) (GCSClient, error) {
	// Try to get existing client from cache
	if cached, ok := gcsClientCache.Load(gcsAddress); ok {
		client := cached.(GCSClient)
		// Check if client is still usable (not closed)
		if !client.IsClosed() {
			return client, nil
		}
		// Client was closed, remove from cache and create a new one
		gcsClientCache.Delete(gcsAddress)
	}

	// Create new client
	client, err := createGCSClient(opts)
	if err != nil {
		return nil, err
	}

	gcsClientCache.Store(gcsAddress, client)
	return client, nil
}

// WithCachedClient executes a function with a cached GCS client.
// This is the recommended way to perform multiple GCS operations efficiently.
func WithCachedClient(gcsAddress string, opts gcs.ClientOptions, fn func(GCSClient) error) error {
	client, err := getOrCreateCachedClient(gcsAddress, opts)
	if err != nil {
		return err
	}
	return fn(client)
}

// clearCachedClient removes a cached GCS client.
func clearCachedClient(gcsAddress string) {
	if cached, ok := gcsClientCache.Load(gcsAddress); ok {
		client := cached.(GCSClient)
		_ = client.Close()
		gcsClientCache.Delete(gcsAddress)
	}
}

// ClearAllCachedClients removes all cached GCS clients.
func ClearAllCachedClients() {
	gcsClientCache.Range(func(key, value interface{}) bool {
		client := value.(GCSClient)
		_ = client.Close()
		gcsClientCache.Delete(key)
		return true
	})
}
