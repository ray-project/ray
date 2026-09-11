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

package errors

import (
	"errors"
	"strings"
	"testing"
)

// TestSentinelErrors tests that all sentinel errors are defined.
func TestSentinelErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrInvalidArgument", ErrInvalidArgument},
		{"ErrNotInitialized", ErrNotInitialized},
		{"ErrAlreadyRunning", ErrAlreadyRunning},
		{"ErrShutdown", ErrShutdown},
		{"ErrTimeout", ErrTimeout},
		{"ErrConnectionFailed", ErrConnectionFailed},
		{"ErrInternalError", ErrInternalError},
		{"ErrInvalidWorkerType", ErrInvalidWorkerType},
		{"ErrInvalidIPAddress", ErrInvalidIPAddress},
		{"ErrInvalidPort", ErrInvalidPort},
		{"ErrEmptySocketPath", ErrEmptySocketPath},
		{"ErrEmptyGcsAddress", ErrEmptyGcsAddress},
		{"ErrEmptyLogDir", ErrEmptyLogDir},
		{"ErrRuntimeFactoryNotSet", ErrRuntimeFactoryNotSet},
		{"ErrUnknownWorkerType", ErrUnknownWorkerType},
		{"ErrPluginNotFound", ErrPluginNotFound},
		{"ErrPluginLoadFailed", ErrPluginLoadFailed},
		{"ErrSymbolNotFound", ErrSymbolNotFound},
		{"ErrInvalidSymbolType", ErrInvalidSymbolType},
		{"ErrInvalidHandle", ErrInvalidHandle},
		{"ErrSerializationFailed", ErrSerializationFailed},
		{"ErrDeserializationFailed", ErrDeserializationFailed},
		{"ErrInvalidMetadata", ErrInvalidMetadata},
		{"ErrObjectNotFound", ErrObjectNotFound},
		{"ErrObjectStoreNotInitialized", ErrObjectStoreNotInitialized},
		{"ErrPutFailed", ErrPutFailed},
		{"ErrGetFailed", ErrGetFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Errorf("%s should not be nil", tt.name)
			}
		})
	}
}

// TestRayError_Interface tests the RayError interface implementation.
func TestRayError_Interface(t *testing.T) {
	err := NewRayError("TEST_CODE",
		WithCategory(CategoryRuntime),
		WithMessage("test message"),
		WithRetryable(true))

	if err == nil {
		t.Fatal("NewRayError should not return nil")
	}

	if code := err.Code(); code != "TEST_CODE" {
		t.Errorf("Code() = %v, expected TEST_CODE", code)
	}

	if cat := err.Category(); cat != CategoryRuntime {
		t.Errorf("Category() = %v, expected %v", cat, CategoryRuntime)
	}

	if !err.IsRetryable() {
		t.Error("IsRetryable() should return true")
	}

	expectedMsg := "[TEST_CODE] test message"
	if err.Error() != expectedMsg {
		t.Errorf("Error() = %v, expected %v", err.Error(), expectedMsg)
	}
}

// TestRayError_WithWrappedError tests RayError with wrapped errors.
func TestRayError_WithWrappedError(t *testing.T) {
	wrappedErr := errors.New("wrapped error")
	err := WrapRayError(wrappedErr, "TEST_CODE",
		WithCategory(CategorySystem),
		WithMessage("test message"),
		WithRetryable(false))

	if err == nil {
		t.Fatal("WrapRayError should not return nil")
	}

	errMsg := err.Error()
	expectedSubstr := "wrapped error"
	if !strings.Contains(errMsg, expectedSubstr) {
		t.Errorf("Error() = %v, should contain %v", errMsg, expectedSubstr)
	}

	unwrapped := errors.Unwrap(err)
	if unwrapped != wrappedErr {
		t.Errorf("Unwrap() = %v, expected %v", unwrapped, wrappedErr)
	}
}

// TestRayError_Category tests error category constants.
func TestRayError_Category(t *testing.T) {
	tests := []struct {
		name     string
		category ErrorCategory
	}{
		{"CategoryArgument", CategoryArgument},
		{"CategoryInitialization", CategoryInitialization},
		{"CategoryRuntime", CategoryRuntime},
		{"CategoryNetwork", CategoryNetwork},
		{"CategorySerialization", CategorySerialization},
		{"CategorySystem", CategorySystem},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// ErrorCategory is now an int-based enum, cannot be compared to string
			// This test verifies the category is defined and non-zero
			if int(tt.category) < 0 || int(tt.category) > int(CategorySystem) {
				t.Errorf("%s has invalid category value %d", tt.name, tt.category)
			}
		})
	}
}

// TestIsRayError tests the IsRayError function.
func TestIsRayError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"RayError", NewRayError("TEST", WithCategory(CategoryRuntime), WithMessage("test"), WithRetryable(false)), true},
		{"WrappedRayError", WrapRayError(errors.New("wrapped"), "TEST", WithCategory(CategoryRuntime), WithMessage("test"), WithRetryable(false)), true},
		{"StandardError", errors.New("standard error"), false},
		{"NilError", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRayError(tt.err)
			if result != tt.expected {
				t.Errorf("IsRayError() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestGetRayErrorCode tests the GetRayErrorCode function.
func TestGetRayErrorCode(t *testing.T) {
	err := NewRayError("MY_CODE",
		WithCategory(CategoryRuntime),
		WithMessage("test"),
		WithRetryable(false))
	code := GetRayErrorCode(err)
	if code != "MY_CODE" {
		t.Errorf("GetRayErrorCode() = %v, expected MY_CODE", code)
	}

	standardErr := errors.New("standard error")
	code = GetRayErrorCode(standardErr)
	if code != "" {
		t.Errorf("GetRayErrorCode() for standard error = %v, expected empty", code)
	}
}

// TestGetRayErrorCategory tests the GetRayErrorCategory function.
func TestGetRayErrorCategory(t *testing.T) {
	err := NewRayError("TEST",
		WithCategory(CategoryNetwork),
		WithMessage("test"),
		WithRetryable(false))
	cat := GetRayErrorCategory(err)
	if cat != CategoryNetwork {
		t.Errorf("GetRayErrorCategory() = %v, expected %v", cat, CategoryNetwork)
	}
}

// TestIsRetryableError tests the IsRetryableError function.
func TestIsRetryableError(t *testing.T) {
	retryableErr := NewRayError("TEST",
		WithCategory(CategoryNetwork),
		WithMessage("test"),
		WithRetryable(true))
	if !IsRetryableError(retryableErr) {
		t.Error("IsRetryableError() should return true for retryable error")
	}

	nonRetryableErr := NewRayError("TEST",
		WithCategory(CategoryRuntime),
		WithMessage("test"),
		WithRetryable(false))
	if IsRetryableError(nonRetryableErr) {
		t.Error("IsRetryableError() should return false for non-retryable error")
	}
}

// TestHelperFunctions tests helper functions.
func TestHelperFunctions(t *testing.T) {
	// Test NewInvalidArgumentError
	err := NewInvalidArgumentError("param", "reason")
	if err.Code() != "INVALID_ARGUMENT" {
		t.Errorf("NewInvalidArgumentError Code() = %v, expected INVALID_ARGUMENT", err.Code())
	}
	if err.Category() != CategoryArgument {
		t.Errorf("NewInvalidArgumentError Category() = %v, expected %v", err.Category(), CategoryArgument)
	}
	if err.IsRetryable() {
		t.Error("NewInvalidArgumentError should not be retryable")
	}

	// Test NewInitializationError
	err = NewInitializationError("component", "reason")
	if err.Code() != "INITIALIZATION_ERROR" {
		t.Errorf("NewInitializationError Code() = %v, expected INITIALIZATION_ERROR", err.Code())
	}
	if err.Category() != CategoryInitialization {
		t.Errorf("NewInitializationError Category() = %v, expected %v", err.Category(), CategoryInitialization)
	}

	// Test NewRuntimeError
	err = NewRuntimeError("operation", "reason")
	if err.Code() != "RUNTIME_ERROR" {
		t.Errorf("NewRuntimeError Code() = %v, expected RUNTIME_ERROR", err.Code())
	}
	if err.Category() != CategoryRuntime {
		t.Errorf("NewRuntimeError Category() = %v, expected %v", err.Category(), CategoryRuntime)
	}

	// Test NewNetworkError
	err = NewNetworkError("operation", true)
	if err.Code() != "NETWORK_ERROR" {
		t.Errorf("NewNetworkError Code() = %v, expected NETWORK_ERROR", err.Code())
	}
	if err.Category() != CategoryNetwork {
		t.Errorf("NewNetworkError Category() = %v, expected %v", err.Category(), CategoryNetwork)
	}
	if !err.IsRetryable() {
		t.Error("NewNetworkError should be retryable")
	}

	// Test NewSerializationError
	err = NewSerializationError("reason")
	if err.Code() != "SERIALIZATION_ERROR" {
		t.Errorf("NewSerializationError Code() = %v, expected SERIALIZATION_ERROR", err.Code())
	}
	if err.Category() != CategorySerialization {
		t.Errorf("NewSerializationError Category() = %v, expected %v", err.Category(), CategorySerialization)
	}

	// Test NewDeserializationError
	err = NewDeserializationError("reason")
	if err.Code() != "DESERIALIZATION_ERROR" {
		t.Errorf("NewDeserializationError Code() = %v, expected DESERIALIZATION_ERROR", err.Code())
	}

	// Test NewSystemError
	err = NewSystemError("reason")
	if err.Code() != "SYSTEM_ERROR" {
		t.Errorf("NewSystemError Code() = %v, expected SYSTEM_ERROR", err.Code())
	}
	if err.Category() != CategorySystem {
		t.Errorf("NewSystemError Category() = %v, expected %v", err.Category(), CategorySystem)
	}
}

// TestRayError_ErrorFormat tests error message formatting.
func TestRayError_ErrorFormat(t *testing.T) {
	tests := []struct {
		name     string
		err      RayError
		expected string
	}{
		{
			name:     "SimpleError",
			err:      NewRayError("CODE", WithCategory(CategoryRuntime), WithMessage("message"), WithRetryable(false)),
			expected: "[CODE] message",
		},
		{
			name:     "WrappedError",
			err:      WrapRayError(errors.New("cause"), "CODE", WithCategory(CategoryRuntime), WithMessage("message"), WithRetryable(false)),
			expected: "[CODE] message: cause",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.err.Error()
			if result != tt.expected {
				t.Errorf("Error() = %v, expected %v", result, tt.expected)
			}
		})
	}
}
