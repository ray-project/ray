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
	"unsafe"

	"github.com/ray-project/ray/go/pkg/gcs"
)

// Compile-time assertion: *cgoClient satisfies the pkg-level
// gcs.NodeResourceUsagePublisher interface, so the reporter module's
// resolvePublisher can select it as the publisher via the
// cfg.GCS.(gcs.NodeResourceUsagePublisher) type assertion.
var _ gcs.NodeResourceUsagePublisher = (*cgoClient)(nil)

// PublishNodeResourceUsage publishes a node resource usage report to GCS.
// keyID is the GCS key (with the RAY_REPORTER: prefix) under which the usage is
// stored, and jsonPayload is the JSON-serialized resource usage payload. It
// returns an error if the publication fails.
func (c *cgoClient) PublishNodeResourceUsage(ctx context.Context, keyID, jsonPayload string) error {
	if c == nil {
		return fmt.Errorf("gcs client is not initialized")
	}
	if strings.TrimSpace(keyID) == "" {
		return fmt.Errorf("node resource usage key must not be empty")
	}
	if strings.TrimSpace(jsonPayload) == "" {
		return fmt.Errorf("node resource usage payload must not be empty")
	}

	_, err := waitContext(ctx, runAsync(ctx, func() (struct{}, error) {
		// Allocate the C strings inside the goroutine closure so their
		// lifetime is bound to the goroutine that uses them: if ctx is
		// cancelled the outer call returns while this C call is still
		// running, and only this closure's defers may free the buffers.
		var cErr *C.char
		cKeyID := C.CString(keyID)
		defer C.free(unsafe.Pointer(cKeyID))
		cPayload := C.CString(jsonPayload)
		defer C.free(unsafe.Pointer(cPayload))

		err := c.withPtr(func(ptr *C.CGcsClient) error {
			ok := C.ray_gcs_client_publisher_publish_node_resource_usage(
				ptr,
				cKeyID,
				cPayload,
				&cErr,
			)
			if cErr != nil {
				defer C.free(unsafe.Pointer(cErr))
				return fmt.Errorf("publish node resource usage failed: %s", C.GoString(cErr))
			}
			if ok == 0 {
				return fmt.Errorf("publish node resource usage failed")
			}
			return nil
		})
		if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	}))
	if err != nil {
		return err
	}
	return nil
}
