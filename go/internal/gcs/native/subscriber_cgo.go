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
	"time"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
	protolib "google.golang.org/protobuf/proto"
)

// cgoErrorSubscriber is the Error Subscriber CGO implementation.
type cgoErrorSubscriber struct {
	ptr *C.CGcsErrorSubscriber
}

// cgoLogSubscriber is the Log Subscriber CGO implementation.
type cgoLogSubscriber struct {
	ptr *C.CGcsLogSubscriber
}

// CreateErrorSubscriber creates an error subscriber.
func CreateErrorSubscriber(address, workerID string) (*cgoErrorSubscriber, error) {
	cAddress := C.CString(address)
	defer C.free(unsafe.Pointer(cAddress))
	cWorkerID := C.CString(workerID)
	defer C.free(unsafe.Pointer(cWorkerID))

	var cErr *C.char
	cPtr := C.ray_gcs_error_subscriber_create(cAddress, cWorkerID, &cErr)
	if cPtr == nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, fmt.Errorf("failed to create error subscriber: %s", C.GoString(cErr))
	}

	return &cgoErrorSubscriber{ptr: cPtr}, nil
}

// Subscribe subscribes to the error stream.
func (s *cgoErrorSubscriber) Subscribe() error {
	var cErr *C.char
	ok := C.ray_gcs_error_subscriber_subscribe(s.ptr, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return fmt.Errorf("subscribe failed: %s", C.GoString(cErr))
	}
	if ok == 0 {
		return fmt.Errorf("subscribe failed")
	}
	return nil
}

// Poll polls for errors.
func (s *cgoErrorSubscriber) Poll(ctx context.Context) ([]byte, *gcs.ErrorData, error) {
	timeoutMs := 100
	if deadline, ok := ctx.Deadline(); ok {
		ms := time.Until(deadline).Milliseconds()
		if ms > 0 && ms <= 10000 {
			timeoutMs = int(ms)
		}
	}

	var cErr *C.char
	var cErrorID *C.char
	var cErrorData unsafe.Pointer
	var cErrorDataSize C.size_t

	ok := C.ray_gcs_error_subscriber_poll(
		s.ptr,
		C.int(timeoutMs),
		&cErrorID,
		&cErrorData,
		&cErrorDataSize,
		&cErr,
	)

	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, nil, fmt.Errorf("poll error failed: %s", C.GoString(cErr))
	}
	if ok == 0 {
		C.free(unsafe.Pointer(cErrorID))
		C.free(cErrorData)
		return nil, nil, nil // No data.
	}

	defer C.free(unsafe.Pointer(cErrorID))

	// Convert to Go data and release the C memory.
	errorDataBytes := C.GoBytes(cErrorData, C.int(cErrorDataSize))
	C.free(cErrorData)

	var errorProto proto.ErrorTableData
	if err := protolib.Unmarshal(errorDataBytes, &errorProto); err != nil {
		return nil, nil, fmt.Errorf("unmarshal error data failed: %w", err)
	}

	// Convert from the proto to gcs.ErrorData.
	jobID, err := ids.JobIDFromBinary(errorProto.GetJobId())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid job ID: %w", err)
	}

	errorData := &gcs.ErrorData{
		JobID:        jobID,
		Type:         errorProto.GetType(),
		ErrorMessage: errorProto.GetErrorMessage(),
		Timestamp:    int64(errorProto.GetTimestamp()),
	}

	return []byte(C.GoString(cErrorID)), errorData, nil
}

// Close closes the subscriber.
func (s *cgoErrorSubscriber) Close() error {
	C.ray_gcs_error_subscriber_close(s.ptr)
	return nil
}

// CreateLogSubscriber creates a log subscriber.
func CreateLogSubscriber(address, workerID string) (*cgoLogSubscriber, error) {
	cAddress := C.CString(address)
	defer C.free(unsafe.Pointer(cAddress))
	cWorkerID := C.CString(workerID)
	defer C.free(unsafe.Pointer(cWorkerID))

	var cErr *C.char
	cPtr := C.ray_gcs_log_subscriber_create(cAddress, cWorkerID, &cErr)
	if cPtr == nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, fmt.Errorf("failed to create log subscriber: %s", C.GoString(cErr))
	}

	return &cgoLogSubscriber{ptr: cPtr}, nil
}

// Subscribe subscribes to the log stream.
func (s *cgoLogSubscriber) Subscribe() error {
	var cErr *C.char
	ok := C.ray_gcs_log_subscriber_subscribe(s.ptr, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return fmt.Errorf("subscribe failed: %s", C.GoString(cErr))
	}
	if ok == 0 {
		return fmt.Errorf("subscribe failed")
	}
	return nil
}

// Poll polls for logs.
func (s *cgoLogSubscriber) Poll(ctx context.Context) (*gcs.LogData, error) {
	timeoutMs := 100
	if deadline, ok := ctx.Deadline(); ok {
		ms := time.Until(deadline).Milliseconds()
		if ms > 0 && ms <= 10000 {
			timeoutMs = int(ms)
		}
	}

	var cErr *C.char
	var cLogData unsafe.Pointer
	var cLogDataSize C.size_t

	ok := C.ray_gcs_log_subscriber_poll(
		s.ptr,
		C.int(timeoutMs),
		&cLogData,
		&cLogDataSize,
		&cErr,
	)

	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return nil, fmt.Errorf("poll log failed: %s", C.GoString(cErr))
	}
	if ok == 0 {
		C.free(cLogData)
		return nil, nil // No data.
	}

	// Convert to Go data and release the C memory.
	logDataBytes := C.GoBytes(cLogData, C.int(cLogDataSize))
	C.free(cLogData)

	// Return the raw log data as a single line.
	// TODO: implement full proto parsing.
	logData := &gcs.LogData{
		Lines: []string{string(logDataBytes)},
	}

	return logData, nil
}

// Close closes the subscriber.
func (s *cgoLogSubscriber) Close() error {
	C.ray_gcs_log_subscriber_close(s.ptr)
	return nil
}
