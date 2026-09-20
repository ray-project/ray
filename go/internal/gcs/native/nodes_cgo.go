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

// Package node CGO bindings for the GCS node table.
package native

/*
#include <stdlib.h>
#include "ray/core_worker/lib/go/gcs_client_bridge.h"
#include "ray/core_worker/lib/go/gcs_memory.h"
*/
import "C"

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
	protolib "google.golang.org/protobuf/proto"
)

// CheckAlive reports whether each node is alive.
func (c *cgoClient) CheckAlive(ctx context.Context, nodeIDs []ids.NodeID) ([]bool, error) {
	if len(nodeIDs) == 0 {
		return []bool{}, nil
	}

	hexIDs := make([]*C.char, len(nodeIDs))
	for i, id := range nodeIDs {
		hexIDs[i] = C.CString(id.Hex())
	}
	defer func() {
		for _, s := range hexIDs {
			if s != nil {
				C.free(unsafe.Pointer(s))
			}
		}
	}()

	var cErr *C.char
	aliveOut := make([]C.int, len(nodeIDs))

	ok := C.ray_gcs_client_nodes_check_alive(
		c.ptr,
		&hexIDs[0],
		C.int(len(nodeIDs)),
		&aliveOut[0],
		&cErr,
	)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, fmt.Errorf("check alive failed: %s", C.GoString(cErr))
	}
	if ok == 0 {
		return nil, fmt.Errorf("check alive failed")
	}

	result := make([]bool, len(nodeIDs))
	for i := 0; i < len(nodeIDs); i++ {
		result[i] = aliveOut[i] != 0
	}

	return result, nil
}

// GetAll returns the node info for the requested nodes.
func (c *cgoClient) GetAll(ctx context.Context, nodeIDs []ids.NodeID) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	var hexIDs []*C.char
	var count C.int

	if len(nodeIDs) > 0 {
		hexIDs = make([]*C.char, len(nodeIDs))
		for i, id := range nodeIDs {
			hexIDs[i] = C.CString(id.Hex())
		}
		defer func() {
			for _, s := range hexIDs {
				if s != nil {
					C.free(unsafe.Pointer(s))
				}
			}
		}()
		count = C.int(len(nodeIDs))
	}

	var cErr *C.char
	var cSerialized **C.char
	var cSizes *C.int
	var cCount C.int

	// Pass nil when no node IDs are specified.
	var hexIDsPtr **C.char
	if len(hexIDs) > 0 {
		hexIDsPtr = &hexIDs[0]
	}

	ok := C.ray_gcs_client_nodes_get_all(
		c.ptr,
		hexIDsPtr,
		count,
		&cSerialized,
		&cSizes,
		&cCount,
		&cErr,
	)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, fmt.Errorf("get all nodes failed: %s", C.GoString(cErr))
	}
	if ok == 0 || cSerialized == nil || cCount == 0 {
		return make(map[ids.NodeID]*proto.GcsNodeInfo), nil
	}

	defer C.free(unsafe.Pointer(cSerialized))
	defer C.free(unsafe.Pointer(cSizes))

	result := make(map[ids.NodeID]*proto.GcsNodeInfo)
	serializedArray := (*[1 << 30]*C.char)(unsafe.Pointer(cSerialized))
	sizesArray := (*[1 << 30]C.int)(unsafe.Pointer(cSizes))
	for i := 0; i < int(cCount); i++ {
		size := int(sizesArray[i])
		if size == 0 {
			continue
		}

		// Copy the data with GoBytes, then release the C memory.
		serialized := C.GoBytes(unsafe.Pointer(serializedArray[i]), C.int(size))
		C.free(unsafe.Pointer(serializedArray[i]))

		var nodeInfo proto.GcsNodeInfo
		if err := protolib.Unmarshal(serialized, &nodeInfo); err != nil {
			return nil, fmt.Errorf("unmarshal node info failed: %w", err)
		}

		// Extract the NodeID from the proto.
		nodeID, err := ids.NodeIDFromBinary(nodeInfo.NodeId)
		if err != nil {
			return nil, fmt.Errorf("invalid node ID: %w", err)
		}
		result[nodeID] = &nodeInfo
	}

	return result, nil
}

// DrainNodes sets the given nodes to the drained state.
// It returns the list of node IDs that were successfully drained; nodes already
// in the drained state are not included in the result.
func (c *cgoClient) DrainNodes(ctx context.Context, nodeIDs []ids.NodeID) ([]ids.NodeID, error) {
	if len(nodeIDs) == 0 {
		return []ids.NodeID{}, nil
	}

	hexIDs := make([]*C.char, len(nodeIDs))
	for i, id := range nodeIDs {
		hexIDs[i] = C.CString(id.Hex())
	}
	defer func() {
		for _, s := range hexIDs {
			if s != nil {
				C.free(unsafe.Pointer(s))
			}
		}
	}()

	var cErr *C.char
	var cDrainedIDs **C.char
	var cDrainedCount C.int

	ok := C.ray_gcs_client_nodes_drain(
		c.ptr,
		&hexIDs[0],
		C.int(len(nodeIDs)),
		&cDrainedIDs,
		&cDrainedCount,
		&cErr,
	)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, fmt.Errorf("drain nodes failed: %s", C.GoString(cErr))
	}
	if ok == 0 || cDrainedIDs == nil || cDrainedCount == 0 {
		return []ids.NodeID{}, nil
	}

	defer C.free(unsafe.Pointer(cDrainedIDs))

	result := make([]ids.NodeID, cDrainedCount)
	// C++ returns a char** pointer array; parse it into a []*C.char slice.
	drainedArray := unsafe.Slice((**C.char)(unsafe.Pointer(cDrainedIDs)), int(cDrainedCount))
	for i := 0; i < int(cDrainedCount); i++ {
		hexID := C.GoString(drainedArray[i])
		nodeID, err := ids.NodeIDFromHex(hexID)
		if err != nil {
			// Release the remaining strings.
			for j := i; j < int(cDrainedCount); j++ {
				C.free(unsafe.Pointer(drainedArray[j]))
			}
			return nil, fmt.Errorf("invalid drained node ID '%s': %w", hexID, err)
		}
		result[i] = nodeID
		// Release each individually allocated string.
		C.free(unsafe.Pointer(drainedArray[i]))
	}

	return result, nil
}
