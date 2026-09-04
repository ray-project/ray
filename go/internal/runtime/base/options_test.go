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

package base

import (
	"strings"
	"testing"

	"github.com/ray-project/ray/go/pkg/options"
)

func TestInitializeOptions(t *testing.T) {
	opts := InitializeOptions{
		WorkerType: options.WorkerTypeDriver,
		Network: NetworkOptions{
			NodeIPAddress:   "127.0.0.1",
			NodeManagerPort: 12345,
			GcsAddress:      "127.0.0.1:6379",
		},
		Runtime: RuntimeOptions{
			StoreSocket:    "/tmp/ray/store.sock",
			RayletSocket:   "/tmp/ray/raylet.sock",
			LogDir:         "/tmp/ray/logs",
			StartupToken:   100,
			RuntimeEnvHash: 0,
		},
		Job: JobOptions{
			JobID:     []byte{0x01, 0x02, 0x03},
			JobConfig: []byte(`{"name": "test_job"}`),
		},
	}

	if opts.WorkerType != options.WorkerTypeDriver {
		t.Errorf("WorkerType = %v, expected %v", opts.WorkerType, options.WorkerTypeDriver)
	}
	if opts.Network.NodeIPAddress != "127.0.0.1" {
		t.Errorf("Network.NodeIPAddress = %v, expected %v", opts.Network.NodeIPAddress, "127.0.0.1")
	}
	if opts.Network.NodeManagerPort != 12345 {
		t.Errorf("Network.NodeManagerPort = %v, expected %v", opts.Network.NodeManagerPort, 12345)
	}
}

// TestValidateInitializeOptions tests the parameter validation function.
func TestValidateInitializeOptions(t *testing.T) {
	tests := []struct {
		name        string
		opts        InitializeOptions
		expectError bool
		errorMsg    string
	}{
		{
			name: "ValidOptions",
			opts: InitializeOptions{
				WorkerType: options.WorkerTypeDriver,
				Network: NetworkOptions{
					NodeIPAddress:   "127.0.0.1",
					NodeManagerPort: 12345,
					GcsAddress:      "127.0.0.1:6379",
				},
				Runtime: RuntimeOptions{
					StoreSocket:  "/tmp/ray/store.sock",
					RayletSocket: "/tmp/ray/raylet.sock",
					LogDir:       "/tmp/ray/logs",
				},
			},
			expectError: false,
		},
		{
			name: "InvalidWorkerType",
			opts: InitializeOptions{
				WorkerType: options.WorkerType(-1),
				Network: NetworkOptions{
					NodeIPAddress:   "127.0.0.1",
					NodeManagerPort: 12345,
					GcsAddress:      "127.0.0.1:6379",
				},
				Runtime: RuntimeOptions{
					StoreSocket:  "/tmp/ray/store.sock",
					RayletSocket: "/tmp/ray/raylet.sock",
					LogDir:       "/tmp/ray/logs",
				},
			},
			expectError: true,
			errorMsg:    "invalid value: -1",
		},
		{
			name: "EmptyNodeIPAddress",
			opts: InitializeOptions{
				WorkerType: options.WorkerTypeDriver,
				Network: NetworkOptions{
					NodeIPAddress:   "",
					NodeManagerPort: 12345,
					GcsAddress:      "127.0.0.1:6379",
				},
				Runtime: RuntimeOptions{
					StoreSocket:  "/tmp/ray/store.sock",
					RayletSocket: "/tmp/ray/raylet.sock",
					LogDir:       "/tmp/ray/logs",
				},
			},
			expectError: true,
			errorMsg:    "cannot be empty",
		},
		{
			name: "InvalidNodeIPAddress",
			opts: InitializeOptions{
				WorkerType: options.WorkerTypeDriver,
				Network: NetworkOptions{
					NodeIPAddress:   "invalid_ip",
					NodeManagerPort: 12345,
					GcsAddress:      "127.0.0.1:6379",
				},
				Runtime: RuntimeOptions{
					StoreSocket:  "/tmp/ray/store.sock",
					RayletSocket: "/tmp/ray/raylet.sock",
					LogDir:       "/tmp/ray/logs",
				},
			},
			expectError: true,
			errorMsg:    "invalid format: invalid_ip",
		},
		{
			name: "ValidNodeManagerPort_Zero",
			opts: InitializeOptions{
				WorkerType: options.WorkerTypeDriver,
				Network: NetworkOptions{
					NodeIPAddress:   "127.0.0.1",
					NodeManagerPort: 0,
					GcsAddress:      "127.0.0.1:6379",
				},
				Runtime: RuntimeOptions{
					StoreSocket:  "/tmp/ray/store.sock",
					RayletSocket: "/tmp/ray/raylet.sock",
					LogDir:       "/tmp/ray/logs",
				},
			},
			expectError: false,
		},
		{
			name: "InvalidNodeManagerPort_Negative",
			opts: InitializeOptions{
				WorkerType: options.WorkerTypeWorker,
				Network: NetworkOptions{
					NodeIPAddress:   "127.0.0.1",
					NodeManagerPort: -1,
					GcsAddress:      "127.0.0.1:6379",
				},
				Runtime: RuntimeOptions{
					StoreSocket:  "/tmp/ray/store.sock",
					RayletSocket: "/tmp/ray/raylet.sock",
					LogDir:       "/tmp/ray/logs",
				},
			},
			expectError: true,
			errorMsg:    "invalid value: -1",
		},
		{
			name: "InvalidNodeManagerPort_TooLarge",
			opts: InitializeOptions{
				WorkerType: options.WorkerTypeWorker,
				Network: NetworkOptions{
					NodeIPAddress:   "127.0.0.1",
					NodeManagerPort: 70000,
					GcsAddress:      "127.0.0.1:6379",
				},
				Runtime: RuntimeOptions{
					StoreSocket:  "/tmp/ray/store.sock",
					RayletSocket: "/tmp/ray/raylet.sock",
					LogDir:       "/tmp/ray/logs",
				},
			},
			expectError: true,
			errorMsg:    "invalid value: 70000",
		},
		{
			name: "EmptyStoreSocket",
			opts: InitializeOptions{
				WorkerType: options.WorkerTypeWorker,
				Network: NetworkOptions{
					NodeIPAddress:   "127.0.0.1",
					NodeManagerPort: 12345,
					GcsAddress:      "127.0.0.1:6379",
				},
				Runtime: RuntimeOptions{
					StoreSocket:  "",
					RayletSocket: "/tmp/ray/raylet.sock",
					LogDir:       "/tmp/ray/logs",
				},
			},
			expectError: true,
			errorMsg:    "cannot be empty",
		},
		{
			name: "EmptyRayletSocket",
			opts: InitializeOptions{
				WorkerType: options.WorkerTypeWorker,
				Network: NetworkOptions{
					NodeIPAddress:   "127.0.0.1",
					NodeManagerPort: 12345,
					GcsAddress:      "127.0.0.1:6379",
				},
				Runtime: RuntimeOptions{
					StoreSocket:  "/tmp/ray/store.sock",
					RayletSocket: "",
					LogDir:       "/tmp/ray/logs",
				},
			},
			expectError: true,
			errorMsg:    "cannot be empty",
		},
		{
			name: "EmptyGcsAddress",
			opts: InitializeOptions{
				WorkerType: options.WorkerTypeDriver,
				Network: NetworkOptions{
					NodeIPAddress:   "127.0.0.1",
					NodeManagerPort: 12345,
					GcsAddress:      "",
				},
				Runtime: RuntimeOptions{
					StoreSocket:  "/tmp/ray/store.sock",
					RayletSocket: "/tmp/ray/raylet.sock",
					LogDir:       "/tmp/ray/logs",
				},
			},
			expectError: true,
			errorMsg:    "cannot be empty",
		},
		{
			name: "EmptyLogDir",
			opts: InitializeOptions{
				WorkerType: options.WorkerTypeWorker,
				Network: NetworkOptions{
					NodeIPAddress:   "127.0.0.1",
					NodeManagerPort: 12345,
					GcsAddress:      "127.0.0.1:6379",
				},
				Runtime: RuntimeOptions{
					StoreSocket:  "/tmp/ray/store.sock",
					RayletSocket: "/tmp/ray/raylet.sock",
					LogDir:       "",
				},
			},
			expectError: true,
			errorMsg:    "cannot be empty",
		},
		{
			name: "ValidIPv6Address",
			opts: InitializeOptions{
				WorkerType: options.WorkerTypeDriver,
				Network: NetworkOptions{
					NodeIPAddress:   "::1",
					NodeManagerPort: 12345,
					GcsAddress:      "[::1]:6379",
				},
				Runtime: RuntimeOptions{
					StoreSocket:  "/tmp/ray/store.sock",
					RayletSocket: "/tmp/ray/raylet.sock",
					LogDir:       "/tmp/ray/logs",
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateInitializeOptions(tt.opts)
			if (err != nil) != tt.expectError {
				t.Errorf("validateInitializeOptions() error = %v, expectError = %v", err, tt.expectError)
			}
			if tt.expectError && tt.errorMsg != "" && err != nil {
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("validateInitializeOptions() error message = %v, expected to contain %v", err.Error(), tt.errorMsg)
				}
			}
		})
	}
}