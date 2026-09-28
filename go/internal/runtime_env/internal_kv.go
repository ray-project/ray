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

package runtime_env

import (
	"context"
	"fmt"

	"github.com/ray-project/ray/go/pkg/gcs"
)

var (
	// globalGCSClient is the global GCS client instance used for the
	// internal KV store.
	globalGCSClient gcs.Client
)

// InternalKVReset clears the global GCS client instance.
func InternalKVReset() {
	globalGCSClient = nil
}

// InternalKVGetGCSClient returns the global GCS client instance.
func InternalKVGetGCSClient() gcs.Client {
	return globalGCSClient
}

// InitializeInternalKV sets the global GCS client instance.
func InitializeInternalKV(gcsClient gcs.Client) error {
	if gcsClient == nil {
		return fmt.Errorf("GCS client is nil")
	}
	globalGCSClient = gcsClient
	return nil
}

// InternalKVInitialized reports whether the global GCS client is set.
func InternalKVInitialized() bool {
	return globalGCSClient != nil
}

// InternalKVGet gets the value stored under the given key and namespace.
func InternalKVGet(ctx context.Context, key string, namespace string) ([]byte, error) {
	if globalGCSClient == nil {
		return nil, fmt.Errorf("GCS client is not initialized")
	}

	return globalGCSClient.Get(ctx, namespace, key)
}

// InternalKVExists reports whether the given key exists in the namespace.
func InternalKVExists(ctx context.Context, key string, namespace string) (bool, error) {
	if globalGCSClient == nil {
		return false, fmt.Errorf("GCS client is not initialized")
	}

	return globalGCSClient.Exists(ctx, namespace, key)
}

// PinRuntimeEnvURICall pins a runtime env URI reference in the GCS with the
// given expiration (in seconds).
func PinRuntimeEnvURICall(ctx context.Context, uri string, expirationS int) error {
	if globalGCSClient == nil {
		return fmt.Errorf("GCS client is not initialized")
	}

	// The GCS client's PinRuntimeEnvURI method is not implemented yet.
	// Report the gap instead of pretending success: callers treat a nil
	// error as having acquired the pin, which leaves uploaded packages
	// exposed to the GCS runtime-env garbage collector.
	return fmt.Errorf("PinRuntimeEnvURI is not implemented by the GCS bridge")
}

// InternalKVPut puts a value under the given key and namespace, returning
// whether the key was created.
func InternalKVPut(ctx context.Context, key string, value []byte, overwrite bool, namespace string) (bool, error) {
	if globalGCSClient == nil {
		return false, fmt.Errorf("GCS client is not initialized")
	}

	return globalGCSClient.Put(ctx, namespace, key, value, overwrite)
}

// InternalKVDel deletes the given key (or all keys with the prefix) from the
// namespace, returning the number of keys deleted.
func InternalKVDel(ctx context.Context, key string, delByPrefix bool, namespace string) (int, error) {
	if globalGCSClient == nil {
		return 0, fmt.Errorf("GCS client is not initialized")
	}

	return globalGCSClient.Del(ctx, namespace, key, delByPrefix)
}

// InternalKVList returns all values stored under the given prefix in the
// namespace.
func InternalKVList(ctx context.Context, prefix string, namespace string) ([][]byte, error) {
	if globalGCSClient == nil {
		return nil, fmt.Errorf("GCS client is not initialized")
	}

	keys, err := globalGCSClient.Keys(ctx, namespace, prefix)
	if err != nil {
		return nil, err
	}

	result := make([][]byte, len(keys))
	for i, key := range keys {
		result[i] = []byte(key)
	}

	return result, nil
}
