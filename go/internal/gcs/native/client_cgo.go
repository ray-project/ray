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

//go:build cgo

package native

/*
#include <stdlib.h>
#include "ray/core_worker/lib/go/gcs_client_bridge.h"
#include "ray/core_worker/lib/go/gcs_memory.h"
*/
import "C"

import (
	"context"
	"encoding/hex"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
	protolib "google.golang.org/protobuf/proto"
)

const (
	maxProtoMessageSize = 100 << 20 // 100MB
	maxListResultCount  = 10000     // Max list result count.
	clusterIDHexLength  = 56        // ClusterID hex length.
)

// validateProtoMessageSize validates the protobuf message size.
func validateProtoMessageSize(cSize C.int) error {
	if cSize < 0 {
		return fmt.Errorf("invalid message size: %d (negative)", cSize)
	}
	if cSize > maxProtoMessageSize {
		return fmt.Errorf("message size %d exceeds maximum %d", cSize, maxProtoMessageSize)
	}
	return nil
}

// cgoClient is the CGO-backed GCS client implementation.
type cgoClient struct {
	mu     sync.RWMutex
	closed atomic.Bool // closed guards concurrent calls.
	ptr    *C.CGcsClient
}

// getPtr safely returns the C client pointer.
//
// Deprecated for new CGO call sites: the returned pointer is only safe as long
// as the caller keeps the shared lock, and getPtr releases it before returning.
// Use withPtr instead so a concurrent Close (write lock) cannot destroy the C
// client mid-call.
func (c *cgoClient) getPtr() *C.CGcsClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ptr
}

// withPtr runs f with the client pointer while holding the shared lock, so a
// concurrent Close (write lock) cannot destroy the C client mid-call.
func (c *cgoClient) withPtr(f func(ptr *C.CGcsClient) error) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() || c.ptr == nil {
		return fmt.Errorf("gcs client is closed")
	}
	return f(c.ptr)
}

// ConnectClient creates and connects a GCS client.
func ConnectClient(opts gcs.ClientOptions) (gcs.Client, error) {
	if opts.TimeoutMs < 0 {
		return nil, fmt.Errorf("timeout must be non-negative")
	}
	if opts.Address == "" {
		return nil, fmt.Errorf("address must not be empty")
	}
	// Note: Allow nil ClusterID for operations like NextJobID and FetchNodeInfoFromGCS.
	// The C++ end will handle nil ClusterID by fetching it from GCS if needed.
	// This matches Java's behavior: ToGcsClientOptions passes NilClusterID with
	// allow_cluster_id_nil=true, fetch_cluster_id_if_nil=false.
	if opts.ClusterID.IsNil() {
		// Use a temporary nil ClusterID - C++ will handle it
		// The C++ GcsClientOptions constructor will be called with allow_cluster_id_nil=true
	}
	// Validate ClusterID length (only when non-nil).
	if !opts.ClusterID.IsNil() && len(opts.ClusterID.Hex()) != clusterIDHexLength {
		return nil, fmt.Errorf("invalid cluster ID length: must be %d hex characters", clusterIDHexLength)
	}
	// Validate hex format (only when non-nil).
	if !opts.ClusterID.IsNil() {
		hexID := opts.ClusterID.Hex()
		if _, err := hex.DecodeString(hexID); err != nil {
			return nil, fmt.Errorf("invalid cluster ID format: must be hex characters [0-9a-fA-F]: %w", err)
		}
	}

	var cErr *C.char
	cAddress := C.CString(opts.Address)
	defer C.free(unsafe.Pointer(cAddress))

	// Pass the ClusterID (even if nil); the C++ side handles a nil ClusterID.
	var cClusterID *C.char
	if opts.ClusterID.IsNil() {
		// Pass empty string for nil ClusterID.
		// C++ will receive an empty string and create a NilClusterID.
		cClusterID = C.CString("")
	} else {
		cClusterID = C.CString(opts.ClusterID.Hex())
	}
	defer C.free(unsafe.Pointer(cClusterID))

	cPtr := C.ray_gcs_client_create(cAddress, cClusterID, C.int64_t(opts.TimeoutMs), &cErr)
	if cPtr == nil {
		return nil, fmt.Errorf("failed to create GCS client: %s", C.GoString(cErr))
	}

	client := &cgoClient{
		ptr: cPtr,
	}

	// Set the global singleton.
	gcs.SetClient(client)

	// Register the synchronous GlobalStateAccessor singleton (used by common.GlobalState etc.)
	gcs.SetGlobalStateAccessor(newStateCgoClient(client))

	return client, nil
}

// Address returns the GCS server address.
func (c *cgoClient) Address() string {
	var addr string
	_ = c.withPtr(func(ptr *C.CGcsClient) error {
		cAddr := C.ray_gcs_client_address(ptr)
		if cAddr != nil {
			defer C.free(unsafe.Pointer(cAddr))
			addr = C.GoString(cAddr)
		}
		return nil
	})
	return addr
}

// ClusterID returns the cluster ID.
func (c *cgoClient) ClusterID() ids.ClusterID {
	clusterID := ids.NilClusterID()
	_ = c.withPtr(func(ptr *C.CGcsClient) error {
		cClusterID := C.ray_gcs_client_cluster_id(ptr)
		if cClusterID != nil {
			defer C.free(unsafe.Pointer(cClusterID))
			if parsed, err := ids.ClusterIDFromHex(C.GoString(cClusterID)); err == nil {
				clusterID = parsed
			}
		}
		return nil
	})
	return clusterID
}

// IsClosed reports whether the client has been closed.
// This method is used by the api package to check if a cached client is still usable.
func (c *cgoClient) IsClosed() bool {
	return c.closed.Load()
}

// Close disconnects from GCS.
func (c *cgoClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ptr == nil {
		return nil // Already closed, idempotent.
	}
	C.ray_gcs_client_destroy(c.ptr)
	c.ptr = nil
	c.closed.Store(true) // Set the closed flag.

	// Clear the global singleton reference so a closed client cannot be reused.
	gcs.ClearClient()

	// Symmetrically clear the synchronous GlobalStateAccessor singleton to avoid
	// returning a stale instance wrapping the closed client
	gcs.ClearGlobalStateAccessor()

	return nil
}

// =============================================================================
// InternalKVInterface - InternalKV storage interface.
// =============================================================================

// Get returns the KV value for the given key.
func (c *cgoClient) Get(ctx context.Context, ns, key string) ([]byte, error) {
	// Wrap the blocking CGO call in runAsync.
	resultCh := runAsync(ctx, func() ([]byte, error) {
		var cErr *C.char
		var cData unsafe.Pointer
		var cSize C.size_t
		var out []byte

		cNS := C.CString(ns)
		defer C.free(unsafe.Pointer(cNS))
		cKey := C.CString(key)
		defer C.free(unsafe.Pointer(cKey))

		err := c.withPtr(func(ptr *C.CGcsClient) error {
			result := C.ray_gcs_client_kv_get(ptr, cNS, cKey, &cData, &cSize, &cErr)
			if result != 0 {
				defer C.free(unsafe.Pointer(cErr))
				errMsg := C.GoString(cErr)
				if errMsg == "Key not found" {
					return gcs.ErrKeyNotFound
				}
				return fmt.Errorf("kv get failed: %s", errMsg)
			}

			if cData == nil || cSize == 0 {
				C.free(cData)
				out = []byte{}
				return nil
			}

			out = C.GoBytes(cData, C.int(cSize))
			C.free(cData)
			return nil
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	})

	// Wait for the result, honouring context cancellation.
	result, err := waitContext(ctx, resultCh)
	return result, err
}

// MultiGet performs a batched KV get.
func (c *cgoClient) MultiGet(ctx context.Context, ns string, keys []string) (map[string][]byte, error) {
	if len(keys) == 0 {
		return make(map[string][]byte), nil
	}

	// Pre-allocate and release all strings in a single defer to avoid a loop of defers.
	cKeys := make([]*C.char, len(keys))
	for i, k := range keys {
		cKeys[i] = C.CString(k)
	}
	defer func() {
		// Release all input key strings.
		for _, k := range cKeys {
			if k != nil {
				C.free(unsafe.Pointer(k))
			}
		}
	}()

	var cKeysOut **C.char
	var cValuesOut *unsafe.Pointer
	var cSizesOut *C.size_t
	var cCountOut C.int

	cNS := C.CString(ns)
	defer C.free(unsafe.Pointer(cNS))

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		result := C.ray_gcs_client_kv_multi_get(
			ptr,
			cNS,
			&cKeys[0],
			C.int(len(keys)),
			&cKeysOut,
			&cValuesOut,
			&cSizesOut,
			&cCountOut,
			nil,
		)
		if result != 0 {
			return fmt.Errorf("kv multi get failed: result=%d", result)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	resultMap := make(map[string][]byte)

	// Distinguish count==0 from nil pointers for better error handling.
	if cCountOut == 0 {
		return resultMap, nil
	}
	if cKeysOut == nil || cValuesOut == nil || cSizesOut == nil {
		// count>0 but some pointers are nil, meaning C returned an abnormal state.
		return nil, fmt.Errorf("internal error: multi-get returned count=%d but result pointers are nil (keysOut=%v, valuesOut=%v, sizesOut=%v)",
			cCountOut, cKeysOut == nil, cValuesOut == nil, cSizesOut == nil)
	}

	keysArray := unsafe.Slice(cKeysOut, cCountOut)
	valuesArray := unsafe.Slice(cValuesOut, cCountOut)
	sizesArray := unsafe.Slice(cSizesOut, cCountOut)

	for i := 0; i < int(cCountOut); i++ {
		// Skip nil keys to avoid empty string in result map
		if keysArray[i] == nil {
			continue
		}
		key := C.GoString(keysArray[i])
		size := sizesArray[i]
		if size > 0 {
			resultMap[key] = C.GoBytes(valuesArray[i], C.int(size))
		}
	}

	// Release the C memory for the output arrays.
	for i := 0; i < int(cCountOut); i++ {
		C.free(unsafe.Pointer(keysArray[i]))
		C.free(valuesArray[i])
	}
	C.free(unsafe.Pointer(cKeysOut))
	C.free(unsafe.Pointer(cValuesOut))
	C.free(unsafe.Pointer(cSizesOut))

	return resultMap, nil
}

// Put stores a KV value.
func (c *cgoClient) Put(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
	var cErr *C.char
	var cSuccess C.int

	cNS := C.CString(ns)
	defer C.free(unsafe.Pointer(cNS))
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	// Pin the Go memory with runtime.Pinner to avoid the large overhead of
	// C.CBytes, which allocates new C memory and copies the data so large values
	// consume twice the memory. Pinner keeps the Go memory from being moved by
	// the GC during the CGO call.
	var cValue unsafe.Pointer
	var cSize C.size_t
	if len(value) > 0 {
		var p runtime.Pinner
		p.Pin(&value[0])
		defer p.Unpin()
		cValue = unsafe.Pointer(&value[0])
		cSize = C.size_t(len(value))
	} else {
		cValue = nil
		cSize = 0
	}

	cOverwrite := 0
	if overwrite {
		cOverwrite = 1
	}

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		result := C.ray_gcs_client_kv_put(ptr, cNS, cKey, cValue, cSize, C.int(cOverwrite), &cSuccess, &cErr)
		if result != 0 {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("kv put failed: %s", C.GoString(cErr))
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	return cSuccess != 0, nil
}

// Del deletes a KV value.
func (c *cgoClient) Del(ctx context.Context, ns, key string, delByPrefix bool) (int, error) {
	var cErr *C.char
	var cCount C.int

	cNS := C.CString(ns)
	defer C.free(unsafe.Pointer(cNS))
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cDelByPrefix := 0
	if delByPrefix {
		cDelByPrefix = 1
	}

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		result := C.ray_gcs_client_kv_del(ptr, cNS, cKey, C.int(cDelByPrefix), &cCount, &cErr)
		if result != 0 {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("kv del failed: %s", C.GoString(cErr))
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return int(cCount), nil
}

// Keys returns the list of keys matching the prefix.
func (c *cgoClient) Keys(ctx context.Context, ns, prefix string) ([]string, error) {
	var cErr *C.char
	var cKeys **C.char
	var cCount C.int

	cNS := C.CString(ns)
	defer C.free(unsafe.Pointer(cNS))
	cPrefix := C.CString(prefix)
	defer C.free(unsafe.Pointer(cPrefix))

	// Use defer to ensure all C memory is released even on panic.
	defer func() {
		if cKeys != nil && cCount > 0 {
			keyArray := (*[1 << 30]*C.char)(unsafe.Pointer(cKeys))
			for i := 0; i < int(cCount); i++ {
				C.free(unsafe.Pointer(keyArray[i]))
			}
			C.free(unsafe.Pointer(cKeys))
		}
	}()

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		result := C.ray_gcs_client_kv_keys(ptr, cNS, cPrefix, &cKeys, &cCount, &cErr)
		if result != 0 {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("kv keys failed: %s", C.GoString(cErr))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Convert the C string array into a Go string slice.
	keys := make([]string, cCount)
	keyArray := (*[1 << 30]*C.char)(unsafe.Pointer(cKeys))
	for i := 0; i < int(cCount); i++ {
		keys[i] = C.GoString(keyArray[i])
	}

	return keys, nil
}

// Exists reports whether a key exists.
func (c *cgoClient) Exists(ctx context.Context, ns, key string) (bool, error) {
	var cErr *C.char
	var cExists C.int

	cNS := C.CString(ns)
	defer C.free(unsafe.Pointer(cNS))
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		result := C.ray_gcs_client_kv_exists(ptr, cNS, cKey, &cExists, &cErr)
		if result != 0 {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("kv exists failed: %s", C.GoString(cErr))
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	return cExists != 0, nil
}

// =============================================================================
// NodeInfoInterface - node info interface.
// =============================================================================

// GetNodeToConnect returns the node to connect for the given IP address.
func (c *cgoClient) GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	resultCh := runAsync(ctx, func() (*proto.GcsNodeInfo, error) {
		var cErr *C.char
		var cSerialized *C.char
		var cSize C.int

		cNodeIpAddress := C.CString(nodeIpAddress)
		defer C.free(unsafe.Pointer(cNodeIpAddress))

		err := c.withPtr(func(ptr *C.CGcsClient) error {
			ok := C.ray_gcs_client_nodes_get_node_to_connect(ptr, cNodeIpAddress, &cSerialized, &cSize, &cErr)
			if cErr != nil {
				defer C.free(unsafe.Pointer(cErr))
				return fmt.Errorf("get node to connect failed: %s", C.GoString(cErr))
			}
			if ok == 0 || cSerialized == nil || cSize == 0 {
				return fmt.Errorf("no node found for IP address: %s", nodeIpAddress)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}

		// Validate the message size.
		if err := validateProtoMessageSize(cSize); err != nil {
			C.free(unsafe.Pointer(cSerialized))
			return nil, err
		}

		defer C.free(unsafe.Pointer(cSerialized))
		serialized := C.GoBytes(unsafe.Pointer(cSerialized), cSize)

		var result proto.GcsNodeInfo
		if err := protolib.Unmarshal(serialized, &result); err != nil {
			return nil, fmt.Errorf("unmarshal node info failed: %w", err)
		}
		return &result, nil
	})

	result, err := waitContext(ctx, resultCh)
	return result, err
}

// =============================================================================
// NodeResourceInterface - node resource interface.
// =============================================================================

// GetAvailableResources returns the available resources.
func (c *cgoClient) GetAvailableResources(ctx context.Context, nodeID ids.NodeID) (*proto.AvailableResources, error) {
	cNodeID := C.CString(nodeID.Hex())
	defer C.free(unsafe.Pointer(cNodeID))

	var cSerialized *C.char
	var cSize C.int
	var cErr *C.char
	var unavailable bool

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_node_resources_get_available(ptr, cNodeID, &cSerialized, &cSize, &cErr)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get available resources failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cSize == 0 {
			unavailable = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if unavailable {
		return nil, nil
	}

	// Validate the message size.
	if err := validateProtoMessageSize(cSize); err != nil {
		C.free(unsafe.Pointer(cSerialized))
		return nil, err
	}

	defer C.free(unsafe.Pointer(cSerialized))
	// Use GoBytes rather than GoString because the protobuf data is binary.
	serialized := C.GoBytes(unsafe.Pointer(cSerialized), cSize)

	// Deserialize the proto data.
	var result proto.AvailableResources
	if err := protolib.Unmarshal(serialized, &result); err != nil {
		return nil, fmt.Errorf("unmarshal available resources failed: %w", err)
	}
	return &result, nil
}

// GetTotalResources returns the total resources.
func (c *cgoClient) GetTotalResources(ctx context.Context, nodeID ids.NodeID) (*proto.TotalResources, error) {
	cNodeID := C.CString(nodeID.Hex())
	defer C.free(unsafe.Pointer(cNodeID))

	var cSerialized *C.char
	var cSize C.int
	var cErr *C.char
	var unavailable bool

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_node_resources_get_total(ptr, cNodeID, &cSerialized, &cSize, &cErr)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get total resources failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cSize == 0 {
			unavailable = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if unavailable {
		return nil, nil
	}

	// Validate the message size.
	if err := validateProtoMessageSize(cSize); err != nil {
		C.free(unsafe.Pointer(cSerialized))
		return nil, err
	}

	defer C.free(unsafe.Pointer(cSerialized))
	// Use GoBytes rather than GoString because the protobuf data is binary.
	serialized := C.GoBytes(unsafe.Pointer(cSerialized), cSize)

	// Deserialize the proto data.
	var result proto.TotalResources
	if err := protolib.Unmarshal(serialized, &result); err != nil {
		return nil, fmt.Errorf("unmarshal total resources failed: %w", err)
	}
	return &result, nil
}

// =============================================================================
// ActorInfoInterface - actor info interface.
// =============================================================================

// GetActorInfo returns the actor info.
func (c *cgoClient) GetActorInfo(ctx context.Context, actorID ids.ActorID) (*proto.ActorTableData, error) {
	cActorID := C.CString(actorID.Hex())
	defer C.free(unsafe.Pointer(cActorID))

	var cSerialized *C.char
	var cSize C.int
	var cErr *C.char
	var unavailable bool

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_actors_get_actor_info(ptr, cActorID, &cSerialized, &cSize, &cErr)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get actor info failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cSize == 0 {
			unavailable = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if unavailable {
		return nil, nil
	}

	// Validate the message size.
	if err := validateProtoMessageSize(cSize); err != nil {
		C.free(unsafe.Pointer(cSerialized))
		return nil, err
	}

	defer C.free(unsafe.Pointer(cSerialized))
	serialized := C.GoBytes(unsafe.Pointer(cSerialized), cSize)

	var result proto.ActorTableData
	if err := protolib.Unmarshal(serialized, &result); err != nil {
		return nil, fmt.Errorf("unmarshal actor info failed: %w", err)
	}
	return &result, nil
}

// ListActors lists all actors, optionally filtered by job ID.
func (c *cgoClient) ListActors(ctx context.Context, jobID *ids.JobID) ([]*proto.ActorTableData, error) {
	return c.ListActorsByFilter(ctx, jobID, nil)
}

// ListActorsByFilter lists actors filtered by job and state.
// A nil or empty actorStateName means no state filtering is applied.
func (c *cgoClient) ListActorsByFilter(ctx context.Context, jobID *ids.JobID, actorStateName *gcs.ActorStateName) ([]*proto.ActorTableData, error) {
	var cJobID *C.char
	if jobID != nil {
		cJobID = C.CString(jobID.Hex())
		defer C.free(unsafe.Pointer(cJobID))
	}

	var cState *C.char
	if actorStateName != nil && *actorStateName != "" {
		cState = C.CString(string(*actorStateName))
		defer C.free(unsafe.Pointer(cState))
	}

	var cErr *C.char
	var cSerialized **C.char
	var cSizes *C.int
	var cCount C.int
	var empty bool

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_actors_get_all_actor_info(
			ptr,
			cJobID,
			cState,
			&cSerialized,
			&cSizes,
			&cCount,
			&cErr,
		)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("list actors failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cCount == 0 {
			empty = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if empty {
		return []*proto.ActorTableData{}, nil
	}

	// Validate that cCount is non-negative.
	if cCount < 0 {
		C.free(unsafe.Pointer(cSerialized))
		return nil, fmt.Errorf("invalid cCount: %d", cCount)
	}

	// Validate that cCount does not exceed the maximum.
	if cCount > maxListResultCount {
		C.free(unsafe.Pointer(cSerialized))
		C.free(unsafe.Pointer(cSizes))
		return nil, fmt.Errorf("result count %d exceeds maximum %d", cCount, maxListResultCount)
	}

	// Add a cSizes check.
	if cSizes == nil {
		C.free(unsafe.Pointer(cSerialized))
		return nil, fmt.Errorf("cSizes is nil")
	}

	defer C.free(unsafe.Pointer(cSerialized))
	defer C.free(unsafe.Pointer(cSizes))

	result := make([]*proto.ActorTableData, cCount)
	// Use a dynamic slice instead of a fixed-size array conversion.
	serializedSlice := unsafe.Slice(cSerialized, cCount)
	sizesSlice := unsafe.Slice(cSizes, cCount)
	for i := 0; i < int(cCount); i++ {
		size := int(sizesSlice[i])
		if size == 0 {
			continue
		}

		// Validate the message size.
		if err := validateProtoMessageSize(C.int(size)); err != nil {
			return nil, err
		}

		// Copy the data and release the C memory.
		serialized := C.GoBytes(unsafe.Pointer(serializedSlice[i]), C.int(size))
		C.free(unsafe.Pointer(serializedSlice[i]))

		var actorInfo proto.ActorTableData
		if err := protolib.Unmarshal(serialized, &actorInfo); err != nil {
			return nil, fmt.Errorf("unmarshal actor info failed: %w", err)
		}
		result[i] = &actorInfo
	}

	return result, nil
}

// =============================================================================
// JobInfoInterface - job info interface.
// =============================================================================

// GetJobInfo returns the job info.
func (c *cgoClient) GetJobInfo(ctx context.Context, jobID ids.JobID) (*proto.JobTableData, error) {
	cJobID := C.CString(jobID.Hex())
	defer C.free(unsafe.Pointer(cJobID))

	var cSerialized *C.char
	var cSize C.int
	var cErr *C.char
	var unavailable bool

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_jobs_get_job_info(ptr, cJobID, &cSerialized, &cSize, &cErr)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get job info failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cSize == 0 {
			unavailable = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if unavailable {
		return nil, nil
	}

	defer C.free(unsafe.Pointer(cSerialized))
	// Use GoBytes rather than GoString because the protobuf data is binary.
	serialized := C.GoBytes(unsafe.Pointer(cSerialized), cSize)

	// Deserialize the proto data.
	var result proto.JobTableData
	if err := protolib.Unmarshal(serialized, &result); err != nil {
		return nil, fmt.Errorf("unmarshal job info failed: %w", err)
	}
	return &result, nil
}

// ListJobs lists all jobs.
func (c *cgoClient) ListJobs(ctx context.Context) ([]*proto.JobTableData, error) {
	var cErr *C.char
	var cSerialized **C.char
	var cSizes *C.int
	var cCount C.int
	var empty bool

	// Fetch the full info by default (do not skip fields).
	skipSubmission := C.int(0)
	skipRunningTasks := C.int(0)

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_jobs_get_all_job_info(
			ptr,
			skipSubmission,
			skipRunningTasks,
			&cSerialized,
			&cSizes,
			&cCount,
			&cErr,
		)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("list jobs failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cCount == 0 {
			empty = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if empty {
		return []*proto.JobTableData{}, nil
	}

	// Validate that cCount is non-negative.
	if cCount < 0 {
		C.free(unsafe.Pointer(cSerialized))
		return nil, fmt.Errorf("invalid cCount: %d", cCount)
	}

	// Validate that cCount does not exceed the maximum.
	if cCount > maxListResultCount {
		C.free(unsafe.Pointer(cSerialized))
		C.free(unsafe.Pointer(cSizes))
		return nil, fmt.Errorf("result count %d exceeds maximum %d", cCount, maxListResultCount)
	}

	// Add a cSizes check.
	if cSizes == nil {
		C.free(unsafe.Pointer(cSerialized))
		return nil, fmt.Errorf("cSizes is nil")
	}

	defer C.free(unsafe.Pointer(cSerialized))
	defer C.free(unsafe.Pointer(cSizes))

	result := make([]*proto.JobTableData, cCount)
	// Use a dynamic slice instead of a fixed-size array conversion.
	serializedSlice := unsafe.Slice(cSerialized, cCount)
	sizesSlice := unsafe.Slice(cSizes, cCount)
	for i := 0; i < int(cCount); i++ {
		size := int(sizesSlice[i])
		if size == 0 {
			continue
		}

		// Copy the data and release the C memory.
		serialized := C.GoBytes(unsafe.Pointer(serializedSlice[i]), C.int(size))
		C.free(unsafe.Pointer(serializedSlice[i]))

		var jobInfo proto.JobTableData
		if err := protolib.Unmarshal(serialized, &jobInfo); err != nil {
			return nil, fmt.Errorf("unmarshal job info failed: %w", err)
		}
		result[i] = &jobInfo
	}

	return result, nil
}

// NextJobID returns the next job ID.
func (c *cgoClient) NextJobID(ctx context.Context) (ids.JobID, error) {
	resultCh := runAsync(ctx, func() (ids.JobID, error) {
		var cErr *C.char
		// JobID hex string is 8 characters (4 bytes) + null terminator
		// Using 65 bytes to be safe and match the C++ buffer size
		jobIdHexOut := make([]C.char, 65)

		err := c.withPtr(func(ptr *C.CGcsClient) error {
			ok := C.ray_gcs_client_jobs_get_next_job_id(ptr, &jobIdHexOut[0], &cErr)
			if cErr != nil {
				defer C.free(unsafe.Pointer(cErr))
				return fmt.Errorf("get next job id failed: %s", C.GoString(cErr))
			}
			if ok == 0 {
				return fmt.Errorf("failed to get next job id")
			}
			return nil
		})
		if err != nil {
			return ids.NilJobID(), err
		}

		jobIdHex := C.GoString(&jobIdHexOut[0])
		jobID, err := ids.JobIDFromHex(jobIdHex)
		if err != nil {
			return ids.NilJobID(), fmt.Errorf("invalid job id hex: %w", err)
		}
		return jobID, nil
	})

	result, err := waitContext(ctx, resultCh)
	return result, err
}

// =============================================================================
// WorkerInfoInterface - worker info interface.
// =============================================================================

// GetWorkerInfo returns the worker info.
func (c *cgoClient) GetWorkerInfo(ctx context.Context, workerID ids.WorkerID) (*proto.WorkerTableData, error) {
	cWorkerID := C.CString(workerID.Hex())
	defer C.free(unsafe.Pointer(cWorkerID))

	var cSerialized *C.char
	var cSize C.int
	var cErr *C.char
	var unavailable bool

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_workers_get_worker_info(ptr, cWorkerID, &cSerialized, &cSize, &cErr)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get worker info failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cSize == 0 {
			unavailable = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if unavailable {
		return nil, nil
	}

	defer C.free(unsafe.Pointer(cSerialized))
	serialized := C.GoBytes(unsafe.Pointer(cSerialized), cSize)

	var result proto.WorkerTableData
	if err := protolib.Unmarshal(serialized, &result); err != nil {
		return nil, fmt.Errorf("unmarshal worker info failed: %w", err)
	}
	return &result, nil
}

// ListWorkers lists all workers.
func (c *cgoClient) ListWorkers(ctx context.Context) ([]*proto.WorkerTableData, error) {
	var cErr *C.char
	var cSerialized **C.char
	var cSizes *C.int
	var cCount C.int
	var empty bool

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_workers_get_all_worker_info(
			ptr,
			&cSerialized,
			&cSizes,
			&cCount,
			&cErr,
		)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("list workers failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cCount == 0 {
			empty = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if empty {
		return []*proto.WorkerTableData{}, nil
	}

	// Validate that cCount is non-negative.
	if cCount < 0 {
		C.free(unsafe.Pointer(cSerialized))
		return nil, fmt.Errorf("invalid cCount: %d", cCount)
	}

	// Validate that cCount does not exceed the maximum.
	if cCount > maxListResultCount {
		C.free(unsafe.Pointer(cSerialized))
		C.free(unsafe.Pointer(cSizes))
		return nil, fmt.Errorf("result count %d exceeds maximum %d", cCount, maxListResultCount)
	}

	// Add a cSizes check.
	if cSizes == nil {
		C.free(unsafe.Pointer(cSerialized))
		return nil, fmt.Errorf("cSizes is nil")
	}

	defer C.free(unsafe.Pointer(cSerialized))
	defer C.free(unsafe.Pointer(cSizes))

	result := make([]*proto.WorkerTableData, cCount)
	// Use a dynamic slice instead of a fixed-size array conversion.
	serializedSlice := unsafe.Slice(cSerialized, cCount)
	sizesSlice := unsafe.Slice(cSizes, cCount)
	for i := 0; i < int(cCount); i++ {
		size := int(sizesSlice[i])
		if size == 0 {
			continue
		}

		// Copy the data and release the C memory.
		serialized := C.GoBytes(unsafe.Pointer(serializedSlice[i]), C.int(size))
		C.free(unsafe.Pointer(serializedSlice[i]))

		var workerInfo proto.WorkerTableData
		if err := protolib.Unmarshal(serialized, &workerInfo); err != nil {
			return nil, fmt.Errorf("unmarshal worker info failed: %w", err)
		}
		result[i] = &workerInfo
	}

	return result, nil
}

// =============================================================================
// PlacementGroupInterface - placement group interface.
// =============================================================================

// GetPlacementGroup returns the placement group info.
func (c *cgoClient) GetPlacementGroup(ctx context.Context, pgID ids.PlacementGroupID) (*proto.PlacementGroupTableData, error) {
	cPgID := C.CString(pgID.Hex())
	defer C.free(unsafe.Pointer(cPgID))

	var cSerialized *C.char
	var cSize C.int
	var cErr *C.char
	var unavailable bool

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_placement_groups_get_by_id(ptr, cPgID, &cSerialized, &cSize, &cErr)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get placement group failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cSize == 0 {
			unavailable = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if unavailable {
		return nil, nil
	}

	defer C.free(unsafe.Pointer(cSerialized))
	serialized := C.GoBytes(unsafe.Pointer(cSerialized), cSize)

	var result proto.PlacementGroupTableData
	if err := protolib.Unmarshal(serialized, &result); err != nil {
		return nil, fmt.Errorf("unmarshal placement group info failed: %w", err)
	}
	return &result, nil
}

// ListPlacementGroups lists all placement groups.
func (c *cgoClient) ListPlacementGroups(ctx context.Context) ([]*proto.PlacementGroupTableData, error) {
	var cErr *C.char
	var cSerialized **C.char
	var cSizes *C.int
	var cCount C.int
	var empty bool

	err := c.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_placement_groups_get_all(
			ptr,
			&cSerialized,
			&cSizes,
			&cCount,
			&cErr,
		)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("list placement groups failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cCount == 0 {
			empty = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if empty {
		return []*proto.PlacementGroupTableData{}, nil
	}

	// Validate that cCount is non-negative.
	if cCount < 0 {
		C.free(unsafe.Pointer(cSerialized))
		return nil, fmt.Errorf("invalid cCount: %d", cCount)
	}

	// Validate that cCount does not exceed the maximum.
	if cCount > maxListResultCount {
		C.free(unsafe.Pointer(cSerialized))
		C.free(unsafe.Pointer(cSizes))
		return nil, fmt.Errorf("result count %d exceeds maximum %d", cCount, maxListResultCount)
	}

	// Add a cSizes check.
	if cSizes == nil {
		C.free(unsafe.Pointer(cSerialized))
		return nil, fmt.Errorf("cSizes is nil")
	}

	defer C.free(unsafe.Pointer(cSerialized))
	defer C.free(unsafe.Pointer(cSizes))

	result := make([]*proto.PlacementGroupTableData, cCount)
	// Use a dynamic slice instead of a fixed-size array conversion.
	serializedSlice := unsafe.Slice(cSerialized, cCount)
	sizesSlice := unsafe.Slice(cSizes, cCount)
	for i := 0; i < int(cCount); i++ {
		size := int(sizesSlice[i])
		if size == 0 {
			continue
		}

		// Copy the data and release the C memory.
		serialized := C.GoBytes(unsafe.Pointer(serializedSlice[i]), C.int(size))
		C.free(unsafe.Pointer(serializedSlice[i]))

		var pgInfo proto.PlacementGroupTableData
		if err := protolib.Unmarshal(serialized, &pgInfo); err != nil {
			return nil, fmt.Errorf("unmarshal placement group info failed: %w", err)
		}
		result[i] = &pgInfo
	}

	return result, nil
}

// getPlacementGroupByNameC invokes the C bridge to look up a placement group by
// name and namespace, then validates and deserializes the result. The caller
// owns cName/cNamespace (passed as already-converted C strings; the caller must
// always pass a non-nil namespace). It returns (nil, nil) when no group matches.
func getPlacementGroupByNameC(cc *cgoClient, cName, cNamespace *C.char) (*proto.PlacementGroupTableData, error) {
	var cSerialized *C.char
	var cSize C.int
	var cErr *C.char
	var unavailable bool

	err := cc.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_placement_groups_get_by_name(
			ptr,
			cName,
			cNamespace,
			&cSerialized,
			&cSize,
			&cErr,
		)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get placement group by name failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cSize == 0 {
			unavailable = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if unavailable {
		return nil, nil
	}

	if err := validateProtoMessageSize(cSize); err != nil {
		C.free(unsafe.Pointer(cSerialized))
		return nil, err
	}

	defer C.free(unsafe.Pointer(cSerialized))
	serialized := C.GoBytes(unsafe.Pointer(cSerialized), cSize)

	var result proto.PlacementGroupTableData
	if err := protolib.Unmarshal(serialized, &result); err != nil {
		return nil, fmt.Errorf("unmarshal placement group info failed: %w", err)
	}
	return &result, nil
}

// GetPlacementGroupByName gets placement group info by name and namespace.
//
// The namespace is always passed to the C bridge as a non-nil C string, so the
// C++ "default" fallback never silently rewrites an explicit empty namespace
// (see the client_cgo.go contract on GetPlacementGroupByName).
func (c *cgoClient) GetPlacementGroupByName(ctx context.Context, name, namespace string) (*proto.PlacementGroupTableData, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("placement group name is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	// Always pass a non-nil namespace so the C++ "default" fallback never
	// silently rewrites an explicit empty namespace.
	cNamespace := C.CString(namespace)
	defer C.free(unsafe.Pointer(cNamespace))

	return getPlacementGroupByNameC(c, cName, cNamespace)
}
