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
	"testing"
)

// TestValidationError tests the ValidationError type.
func TestValidationError(t *testing.T) {
	t.Run("basic error", func(t *testing.T) {
		err := NewValidationError("port", "must be between 1 and 65535", 99999)

		if err == nil {
			t.Fatal("NewValidationError should not return nil")
		}

		expected := `invalid parameter "port": must be between 1 and 65535 (value: 99999)`
		if err.Error() != expected {
			t.Errorf("Error() = %v, expected %v", err.Error(), expected)
		}

		if err.Code() != "VALIDATION_ERROR" {
			t.Errorf("Code() = %v, expected VALIDATION_ERROR", err.Code())
		}

		if err.Retryable() {
			t.Error("ValidationError should not be retryable")
		}

		if err.Unwrap() != nil {
			t.Error("Unwrap() should return nil for unwrapped ValidationError")
		}
	})

	t.Run("without value", func(t *testing.T) {
		err := NewValidationError("name", "cannot be empty", nil)

		expected := `invalid parameter "name": cannot be empty`
		if err.Error() != expected {
			t.Errorf("Error() = %v, expected %v", err.Error(), expected)
		}
	})

	t.Run("wrapped error", func(t *testing.T) {
		wrappedErr := errors.New("underlying validation failed")
		err := WrapValidationError(wrappedErr, "config", "invalid format")

		if err.Unwrap() != wrappedErr {
			t.Error("Unwrap() should return the wrapped error")
		}

		// Verify Details returns the map
		details := err.Details()
		if details == nil {
			t.Error("Details() should return a non-nil map")
		}
	})
}

// TestRuntimeError tests the RuntimeError type.
func TestRuntimeError(t *testing.T) {
	t.Run("with state", func(t *testing.T) {
		err := NewRuntimeError("submit_task", "not_initialized")

		expected := `runtime operation "submit_task" failed: current state is "not_initialized"`
		if err.Error() != expected {
			t.Errorf("Error() = %v, expected %v", err.Error(), expected)
		}

		if err.Code() != "RUNTIME_ERROR" {
			t.Errorf("Code() = %v, expected RUNTIME_ERROR", err.Code())
		}
	})

	t.Run("without state", func(t *testing.T) {
		err := NewRuntimeError("get_actor", "")

		expected := `runtime operation "get_actor" failed`
		if err.Error() != expected {
			t.Errorf("Error() = %v, expected %v", err.Error(), expected)
		}
	})

	t.Run("retryable detection", func(t *testing.T) {
		connectErr := NewRuntimeError("connect_to_gcs", "")
		if !connectErr.Retryable() {
			t.Error("RuntimeError with 'connect' should be retryable")
		}

		timeoutErr := NewRuntimeError("timeout_operation", "")
		if !timeoutErr.Retryable() {
			t.Error("RuntimeError with 'timeout' should be retryable")
		}

		normalErr := NewRuntimeError("submit_task", "")
		if normalErr.Retryable() {
			t.Error("Normal RuntimeError should not be retryable")
		}
	})

	t.Run("wrapped error", func(t *testing.T) {
		wrappedErr := errors.New("task submission failed")
		err := WrapRuntimeError(wrappedErr, "submit_task")

		if err.Unwrap() != wrappedErr {
			t.Error("Unwrap() should return the wrapped error")
		}
	})
}

// TestInitializationError tests the InitializationError type.
func TestInitializationError(t *testing.T) {
	t.Run("basic error", func(t *testing.T) {
		err := NewInitializationError("Runtime", "configuration missing")

		expected := "failed to initialize Runtime: configuration missing"
		if err.Error() != expected {
			t.Errorf("Error() = %v, expected %v", err.Error(), expected)
		}

		if err.Code() != "INITIALIZATION_ERROR" {
			t.Errorf("Code() = %v, expected INITIALIZATION_ERROR", err.Code())
		}

		if err.Retryable() {
			t.Error("InitializationError should not be retryable")
		}
	})

	t.Run("wrapped error", func(t *testing.T) {
		wrappedErr := errors.New("failed to parse config")
		err := WrapInitializationError(wrappedErr, "GCSClient", "invalid configuration")

		if err.Unwrap() != wrappedErr {
			t.Error("Unwrap() should return the wrapped error")
		}

		// Verify Config is initialized
		config := err.Config
		if config == nil {
			t.Error("Config should be initialized")
		}
	})
}

// TestNetworkError tests the NetworkError type.
func TestNetworkError(t *testing.T) {
	t.Run("with endpoint", func(t *testing.T) {
		err := NewNetworkError("dial", "127.0.0.1:6379", 5000000000) // 5 seconds

		expected := "network dial to 127.0.0.1:6379 failed"
		if err.Error() != expected {
			t.Errorf("Error() = %v, expected %v", err.Error(), expected)
		}

		if err.Code() != "NETWORK_ERROR" {
			t.Errorf("Code() = %v, expected NETWORK_ERROR", err.Code())
		}

		if !err.Retryable() {
			t.Error("NetworkError should be retryable by default")
		}
	})

	t.Run("without endpoint", func(t *testing.T) {
		err := NewNetworkError("send", "", 0)

		expected := "network send failed"
		if err.Error() != expected {
			t.Errorf("Error() = %v, expected %v", err.Error(), expected)
		}
	})

	t.Run("wrapped error with custom retryable", func(t *testing.T) {
		wrappedErr := errors.New("connection refused")
		err := WrapNetworkError(wrappedErr, "dial", "localhost:6379")

		if err.Unwrap() != wrappedErr {
			t.Error("Unwrap() should return the wrapped error")
		}

		if !err.Retryable() {
			t.Error("WrappedNetworkError should be retryable by default")
		}
	})
}

// TestSerializationError tests the SerializationError type.
func TestSerializationError(t *testing.T) {
	t.Run("serialize error", func(t *testing.T) {
		err := NewSerializationError("TaskSpec")

		expected := "serialize of TaskSpec failed: "
		if err.Error()[:len(expected)] != expected {
			t.Errorf("Error() = %v, expected prefix %v", err.Error(), expected)
		}

		if err.Code() != "SERIALIZATION_ERROR" {
			t.Errorf("Code() = %v, expected SERIALIZATION_ERROR", err.Code())
		}

		if err.Retryable() {
			t.Error("SerializationError should not be retryable")
		}
	})

	t.Run("deserialize error", func(t *testing.T) {
		err := NewDeserializationError("ActorHandle")

		expected := "deserialize of ActorHandle failed: "
		if err.Error()[:len(expected)] != expected {
			t.Errorf("Error() = %v, expected prefix %v", err.Error(), expected)
		}

		if err.Code() != "SERIALIZATION_ERROR" {
			t.Errorf("Code() = %v, expected SERIALIZATION_ERROR", err.Code())
		}
	})

	t.Run("wrapped error", func(t *testing.T) {
		wrappedErr := errors.New("protobuf encoding failed")
		err := WrapSerializationError(wrappedErr, "ObjectRef", "serialize")

		if err.Unwrap() != wrappedErr {
			t.Error("Unwrap() should return the wrapped error")
		}
	})
}

// TestSystemError tests the SystemError type.
func TestSystemError(t *testing.T) {
	t.Run("basic error", func(t *testing.T) {
		err := NewSystemError("MemoryStore", "out of memory")

		expected := "system error in MemoryStore: out of memory"
		if err.Error() != expected {
			t.Errorf("Error() = %v, expected %v", err.Error(), expected)
		}

		if err.Code() != "SYSTEM_ERROR" {
			t.Errorf("Code() = %v, expected SYSTEM_ERROR", err.Code())
		}

		if err.Retryable() {
			t.Error("SystemError should not be retryable")
		}
	})

	t.Run("with suggestion", func(t *testing.T) {
		err := NewSystemError("Scheduler", "deadlock detected")
		err.WithSuggestion("Restart the runtime with increased resources")

		expected := "system error in Scheduler: deadlock detected. Restart the runtime with increased resources"
		if err.Error() != expected {
			t.Errorf("Error() = %v, expected %v", err.Error(), expected)
		}
	})

	t.Run("wrapped error", func(t *testing.T) {
		wrappedErr := errors.New("unexpected nil pointer")
		err := WrapSystemError(wrappedErr, "Dispatcher", "internal assertion failed")

		if err.Unwrap() != wrappedErr {
			t.Error("Unwrap() should return the wrapped error")
		}
	})
}

// TestRayError_Interface tests the RayError interface implementation across all types.
func TestRayError_Interface(t *testing.T) {
	testCases := []struct {
		name      string
		err       RayError
		code      string
		retryable bool
	}{
		{"ValidationError", NewValidationError("test", "reason", nil), "VALIDATION_ERROR", false},
		{"RuntimeError", NewRuntimeError("op", "state"), "RUNTIME_ERROR", false},
		{"InitializationError", NewInitializationError("comp", "reason"), "INITIALIZATION_ERROR", false},
		{"NetworkError", NewNetworkError("op", "endpoint", 0), "NETWORK_ERROR", true},
		{"SerializationError", NewSerializationError("type"), "SERIALIZATION_ERROR", false},
		{"SystemError", NewSystemError("comp", "reason"), "SYSTEM_ERROR", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err.Code() != tc.code {
				t.Errorf("Code() = %v, expected %v", tc.err.Code(), tc.code)
			}

			if tc.err.Retryable() != tc.retryable {
				t.Errorf("Retryable() = %v, expected %v", tc.err.Retryable(), tc.retryable)
			}

			// All should have Error() method
			if tc.err.Error() == "" {
				t.Error("Error() should not return empty string")
			}

			// All should have Message() method (same as Error())
			if tc.err.Message() != tc.err.Error() {
				t.Error("Message() should return same as Error()")
			}

			// All should have Details() method
			if tc.err.Details() == nil {
				t.Error("Details() should return a non-nil map")
			}
		})
	}
}

// TestErrorWrapping tests error wrapping and Unwrap functionality.
func TestErrorWrapping(t *testing.T) {
	baseErr := errors.New("base error")

	t.Run("ValidationError wrapping", func(t *testing.T) {
		err := WrapValidationError(baseErr, "param", "reason")
		unwrapped := errors.Unwrap(err)
		if unwrapped != baseErr {
			t.Errorf("Unwrap() = %v, expected %v", unwrapped, baseErr)
		}
	})

	t.Run("RuntimeError wrapping", func(t *testing.T) {
		err := WrapRuntimeError(baseErr, "operation")
		unwrapped := errors.Unwrap(err)
		if unwrapped != baseErr {
			t.Errorf("Unwrap() = %v, expected %v", unwrapped, baseErr)
		}
	})

	t.Run("InitializationError wrapping", func(t *testing.T) {
		err := WrapInitializationError(baseErr, "component", "reason")
		unwrapped := errors.Unwrap(err)
		if unwrapped != baseErr {
			t.Errorf("Unwrap() = %v, expected %v", unwrapped, baseErr)
		}
	})

	t.Run("NetworkError wrapping", func(t *testing.T) {
		err := WrapNetworkError(baseErr, "operation", "endpoint")
		unwrapped := errors.Unwrap(err)
		if unwrapped != baseErr {
			t.Errorf("Unwrap() = %v, expected %v", unwrapped, baseErr)
		}
	})

	t.Run("SerializationError wrapping", func(t *testing.T) {
		err := WrapSerializationError(baseErr, "dataType", "operation")
		unwrapped := errors.Unwrap(err)
		if unwrapped != baseErr {
			t.Errorf("Unwrap() = %v, expected %v", unwrapped, baseErr)
		}
	})

	t.Run("SystemError wrapping", func(t *testing.T) {
		err := WrapSystemError(baseErr, "component", "reason")
		unwrapped := errors.Unwrap(err)
		if unwrapped != baseErr {
			t.Errorf("Unwrap() = %v, expected %v", unwrapped, baseErr)
		}
	})
}

// TestErrorDetailsMap tests that Details maps are properly initialized.
func TestErrorDetailsMap(t *testing.T) {
	t.Run("ValidationError details", func(t *testing.T) {
		err := NewValidationError("test", "reason", nil)
		details := err.Details()
		if len(details) != 0 {
			t.Errorf("Details map should be empty but has %d entries", len(details))
		}
		// Should be able to add entries
		details["key"] = "value"
		if details["key"] != "value" {
			t.Error("Should be able to modify details map")
		}
	})

	t.Run("RuntimeError details", func(t *testing.T) {
		err := NewRuntimeError("op", "state")
		details := err.Details()
		if len(details) != 0 {
			t.Errorf("Detailsmap should be empty but has %d entries", len(details))
		}
	})

	t.Run("NetworkError details", func(t *testing.T) {
		err := NewNetworkError("op", "endpoint", 0)
		details := err.Details()
		if len(details) != 0 {
			t.Errorf("Detailsmap should be empty but has %d entries", len(details))
		}
	})
}

// TestErrorChaining tests error chain using errors.Is and errors.As.
func TestErrorChaining(t *testing.T) {
	t.Run("errors.As with ValidationError", func(t *testing.T) {
		baseErr := errors.New("validation failed")
		wrappedErr := WrapValidationError(baseErr, "port", "invalid")

		var valErr *ValidationError
		if !errors.As(wrappedErr, &valErr) {
			t.Error("errors.As should detect ValidationError")
		}

		if valErr.Parameter != "port" {
			t.Errorf("Parameter = %v, expected port", valErr.Parameter)
		}
	})

	t.Run("errors.As with RuntimeError", func(t *testing.T) {
		baseErr := errors.New("runtime failed")
		wrappedErr := WrapRuntimeError(baseErr, "submit")

		var runErr *RuntimeError
		if !errors.As(wrappedErr, &runErr) {
			t.Error("errors.As should detect RuntimeError")
		}

		if runErr.Operation != "submit" {
			t.Errorf("Operation = %v, expected submit", runErr.Operation)
		}
	})

	t.Run("errors.As with nested errors", func(t *testing.T) {
		baseErr := errors.New("deep error")
		middleErr := WrapRuntimeError(baseErr, "middle")
		outerErr := WrapSystemError(middleErr, "outer", "chained")

		var sysErr *SystemError
		if !errors.As(outerErr, &sysErr) {
			t.Error("errors.As should detect SystemError in chain")
		}

		if sysErr.Component != "outer" {
			t.Errorf("Component = %v, expected outer", sysErr.Component)
		}
	})
}
