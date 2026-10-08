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
	"fmt"
	"runtime"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/log"
	protopb "github.com/ray-project/ray/go/proto"
	"google.golang.org/protobuf/proto"
)

// GetAutoscalerStatus returns the autoscaler status as a deserialized
// AutoscalingState object.
func (c *cgoClient) GetAutoscalerStatus(ctx context.Context) (*protopb.GetClusterStatusReply, error) {
	var cSerialized *C.char
	var cSize C.int
	var cErr *C.char

	log.Log.V(1).Info("Getting autoscaler status")
	ok := C.ray_gcs_client_autoscaler_get_status(c.getPtr(), &cSerialized, &cSize, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		if cSerialized != nil {
			C.free(unsafe.Pointer(cSerialized))
		}
		return nil, fmt.Errorf("get autoscaler status failed: %s", C.GoString(cErr))
	}
	if ok == 0 {
		if cSerialized != nil {
			C.free(unsafe.Pointer(cSerialized))
		}
		return nil, fmt.Errorf("get autoscaler status failed: C++ returned ok=0 with no error")
	}

	// Parse the protobuf payload.
	serialized := C.GoBytes(unsafe.Pointer(cSerialized), cSize)
	C.free(unsafe.Pointer(cSerialized))

	reply := &protopb.GetClusterStatusReply{}
	if err := proto.Unmarshal(serialized, reply); err != nil {
		return nil, fmt.Errorf("unmarshal autoscaler status failed: %w", err)
	}

	return reply, nil
}

// ReportAutoscalingState reports the autoscaling state to GCS.
// autoscalingState holds the protobuf-serialized AutoscalingState bytes.
func (c *cgoClient) ReportAutoscalingState(autoscalingState string) error {
	if c.closed.Load() {
		return fmt.Errorf("client is closed")
	}
	stateBytes := []byte(autoscalingState)
	if len(stateBytes) == 0 {
		return fmt.Errorf("autoscaling state must not be empty")
	}

	// Pin the Go memory with runtime.Pinner to avoid the overhead of C.CBytes,
	// which allocates new C memory and copies the data, so a large state would
	// consume twice the memory. Pinner keeps the Go memory from being moved by
	// the GC during the CGO call.
	var p runtime.Pinner
	p.Pin(&stateBytes[0])
	defer p.Unpin()

	var cErr *C.char
	ok := C.ray_gcs_client_autoscaler_report_state(
		c.getPtr(),
		(*C.char)(unsafe.Pointer(&stateBytes[0])),
		C.int32_t(len(stateBytes)),
		&cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		if ok == 0 {
			return fmt.Errorf("report autoscaling state failed: %s", C.GoString(cErr))
		}
	}
	if ok == 0 {
		return fmt.Errorf("report autoscaling state failed: C++ returned ok=0 with no error")
	}
	return nil
}
