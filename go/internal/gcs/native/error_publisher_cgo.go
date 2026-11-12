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
	"strings"
	"time"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/gcs"
)

// Compile-time assertion: *cgoClient satisfies the pkg-level
// gcs.ErrorInfoPublisher interface.
var _ gcs.ErrorInfoPublisher = (*cgoClient)(nil)

// scrubNUL removes embedded NUL bytes. The bridge hands error_type and
// error_message to the C++ protobuf setters as NUL-terminated strings, so a
// NUL byte from the raw raylet.out tail — plausible exactly in the hard-crash
// scenarios this diagnostic targets — would silently truncate the published
// message mid-content. Removing the bytes keeps the whole tail readable. The
// binary-safe job id needs no such handling because it crosses the boundary
// with an explicit length.
func scrubNUL(s string) string {
	return strings.ReplaceAll(s, "\x00", "")
}

// PublishErrorInfo publishes one error info message through the native GCS
// client bridge, mirroring the Python agent's GcsClient.publish_error: the
// wrapper stamps the timestamp and the RPC timeout comes from the context
// deadline. A deadline-less ctx is rejected: timeout -1 makes the C++ retry
// queue wait unboundedly and this synchronous cgo call cannot observe ctx
// cancellation, so the contract on the interface is enforced here.
func (c *cgoClient) PublishErrorInfo(ctx context.Context, keyID string, payload gcs.ErrorInfoPayload) error {
	if c == nil {
		return fmt.Errorf("gcs client is not initialized")
	}

	if _, ok := ctx.Deadline(); !ok {
		return fmt.Errorf("PublishErrorInfo requires a context deadline: without one the synchronous cgo publish cannot observe cancellation and the underlying retry queue waits unboundedly")
	}

	timeoutMs := int64(-1)
	if deadline, ok := ctx.Deadline(); ok {
		timeoutMs = time.Until(deadline).Milliseconds()
		if timeoutMs < 0 {
			timeoutMs = 0
		}
	}

	var cErr *C.char
	cKeyID := C.CString(keyID)
	defer C.free(unsafe.Pointer(cKeyID))
	cType := C.CString(payload.Type)
	defer C.free(unsafe.Pointer(cType))
	cMessage := C.CString(scrubNUL(payload.Message))
	defer C.free(unsafe.Pointer(cMessage))

	var jobIDPtr *C.char
	if len(payload.JobID) > 0 {
		jobIDPtr = (*C.char)(unsafe.Pointer(&payload.JobID[0]))
	}

	return c.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_publisher_publish_error(
			ptr,
			cKeyID,
			jobIDPtr,
			C.int(len(payload.JobID)),
			cType,
			cMessage,
			C.double(float64(time.Now().UnixNano())/1e9),
			C.int64_t(timeoutMs),
			&cErr,
		)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("publish error info failed: %s", C.GoString(cErr))
		}
		if ok == 0 {
			return fmt.Errorf("publish error info failed")
		}
		return nil
	})
}
