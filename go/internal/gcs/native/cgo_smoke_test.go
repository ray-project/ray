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

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
)

// A ConnectClient case that dials a real GCS address is intentionally absent
// here: ray_gcs_client_create connects through the C++ GcsClient and blocks
// until its timeout, so it needs a live cluster. That coverage lives in the
// opt-in target under go/internal/gcs/integration (RAY_GCS_INTEGRATION_TESTS=1).

// Test_cgoSmoke checks that the CGO bindings compile and that ConnectClient
// rejects invalid options in Go before any C++ call.
func Test_cgoSmoke(t *testing.T) {
	t.Run("NegativeTimeout", func(t *testing.T) {
		_, err := ConnectClient(gcs.ClientOptions{
			TimeoutMs: -1000,
			Address:   "127.0.0.1:6379",
			ClusterID: ids.NewClusterID(),
		})
		if err == nil {
			t.Error("expected error for negative timeout")
		}
	})

	t.Run("EmptyAddress", func(t *testing.T) {
		_, err := ConnectClient(gcs.ClientOptions{
			TimeoutMs: 5000,
			Address:   "",
			ClusterID: ids.NewClusterID(),
		})
		if err == nil {
			t.Error("expected error for empty address")
		}
	})
}
