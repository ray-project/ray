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
*/
import "C"

import (
	"context"
	"fmt"
	"time"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/gcs"
)

// LogBatchPublisher publishes log batches through the native GCS client bridge.
type LogBatchPublisher struct {
	client *cgoClient
}

// NewLogBatchPublisher creates a publish-side adapter from a native GCS client.
func NewLogBatchPublisher(client gcs.Client) (*LogBatchPublisher, error) {
	nativeClient, ok := client.(*cgoClient)
	if !ok {
		return nil, fmt.Errorf("unsupported gcs client type %T", client)
	}
	return &LogBatchPublisher{client: nativeClient}, nil
}

// PublishLogBatch publishes one log batch to GCS.
func (p *LogBatchPublisher) PublishLogBatch(ctx context.Context, payload gcs.LogBatchPayload) error {
	if p == nil || p.client == nil {
		return fmt.Errorf("log batch publisher is not initialized")
	}

	var cErr *C.char
	cKeyID := C.CString(payload.JobID)
	defer C.free(unsafe.Pointer(cKeyID))
	cIP := C.CString(payload.IP)
	defer C.free(unsafe.Pointer(cIP))
	cPID := C.CString(payload.PID)
	defer C.free(unsafe.Pointer(cPID))
	cJobID := C.CString(payload.JobID)
	defer C.free(unsafe.Pointer(cJobID))
	cActorName := C.CString(payload.ActorName)
	defer C.free(unsafe.Pointer(cActorName))
	cTaskName := C.CString(payload.TaskName)
	defer C.free(unsafe.Pointer(cTaskName))

	var cLines **C.char
	if len(payload.Lines) > 0 {
		linePtrs := make([]*C.char, len(payload.Lines))
		for i, line := range payload.Lines {
			linePtrs[i] = C.CString(line)
			defer C.free(unsafe.Pointer(linePtrs[i]))
		}
		cLines = (**C.char)(unsafe.Pointer(&linePtrs[0]))
	}

	timeoutMs := int64(-1)
	if deadline, ok := ctx.Deadline(); ok {
		timeoutMs = time.Until(deadline).Milliseconds()
		if timeoutMs < 0 {
			timeoutMs = 0
		}
	}

	ok := C.ray_gcs_client_publisher_publish_log_batch(
		p.client.getPtr(),
		cKeyID,
		cIP,
		cPID,
		cJobID,
		C.int(boolToInt(payload.IsError)),
		(**C.char)(unsafe.Pointer(cLines)),
		C.int(len(payload.Lines)),
		cActorName,
		cTaskName,
		C.int64_t(timeoutMs),
		&cErr,
	)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return fmt.Errorf("publish log batch failed: %s", C.GoString(cErr))
	}
	if ok == 0 {
		return fmt.Errorf("publish log batch failed")
	}
	return nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
