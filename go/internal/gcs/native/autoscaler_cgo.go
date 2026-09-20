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
	"unsafe"

	"github.com/ray-project/ray/go/pkg/gcs"
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
// TODO: The C++ bridge does not yet expose report_autoscaling_state; once the
// interface is wired up this method should call it via CGO.
func (c *cgoClient) ReportAutoscalingState(autoscalingState string) error {
	return gcs.ErrNotImplemented
}
