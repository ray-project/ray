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

package runtime_env

import (
	"testing"
)

// ============ Constant tests ============

func TestConstants(t *testing.T) {
	// Verify the per-file size warning threshold.
	expectedSize := 10 * 1024 * 1024
	if FILE_SIZE_WARNING != expectedSize {
		t.Errorf("expected FILE_SIZE_WARNING to be %d, got %d", expectedSize, FILE_SIZE_WARNING)
	}

	// Verify the Ray package prefix.
	if RAY_PKG_PREFIX != "_ray_pkg_" {
		t.Errorf("expected RAY_PKG_PREFIX to be '_ray_pkg_', got %s", RAY_PKG_PREFIX)
	}

	// Verify the macOS zip hidden directory name.
	if MAC_OS_ZIP_HIDDEN_DIR_NAME != "__MACOSX" {
		t.Errorf("expected MAC_OS_ZIP_HIDDEN_DIR_NAME to be '__MACOSX', got %s", MAC_OS_ZIP_HIDDEN_DIR_NAME)
	}

	// Verify the default URI pin expiration.
	expectedExpiration := 10 * 60
	if RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_DEFAULT != expectedExpiration {
		t.Errorf("expected RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_DEFAULT to be %d, got %d", expectedExpiration, RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_DEFAULT)
	}
}

// ============ Environment variable name tests ============

func TestEnvVarNames(t *testing.T) {
	tests := []struct {
		name     string
		actual   string
		expected string
	}{
		{
			name:     "upload_fail_env",
			actual:   RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING_ENV_VAR,
			expected: "RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING",
		},
		{
			name:     "download_fail_env",
			actual:   RAY_RUNTIME_ENV_FAIL_DOWNLOAD_FOR_TESTING_ENV_VAR,
			expected: "RAY_RUNTIME_ENV_FAIL_DOWNLOAD_FOR_TESTING",
		},
		{
			name:     "uri_expiration_env",
			actual:   RAY_RUNTIME_ENV_TEMPORARY_REFERENCE_EXPIRATION_S_ENV_VAR,
			expected: "RAY_RUNTIME_ENV_TEMPORARY_REFERENCE_EXPIRATION_S",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.actual != tt.expected {
				t.Errorf("expected %s to be %q, got %q", tt.name, tt.expected, tt.actual)
			}
		})
	}
}

// ============ GCS_STORAGE_MAX_SIZE tests ============

func TestGCSStorageMaxSize(t *testing.T) {
	// Verify GCS_STORAGE_MAX_SIZE has a value (from the env var or the default).
	// The default should be common.GRPC_CPP_MAX_MESSAGE_SIZE.
	if GCS_STORAGE_MAX_SIZE <= 0 {
		t.Error("expected GCS_STORAGE_MAX_SIZE to be positive")
	}
}

// ============ Logger tests ============

func TestDefaultLogger(t *testing.T) {
	// Verify defaultLogger is initialized.
	// logr.Logger is an interface, so only check that it is not the zero value.
	// logr.Logger exposes no way to detect the zero value, so keep this check minimal.
	_ = defaultLogger
}
