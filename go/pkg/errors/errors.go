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

// Package errors provides public API error types for Ray Go Runtime.
//
// This package defines high-level error abstractions suitable for end users.
// Internal implementation details are hidden; users interact with clean,
// user-friendly error types that provide actionable guidance.
//
// Error Types:
//   - ValidationError: Invalid parameter provided by user
//   - RuntimeError: Runtime operation failed due to state issues
//   - InitializationError: Component initialization or configuration failed
//   - NetworkError: Network communication failure
//   - SerializationError: Data serialization/deserialization failed
//   - SystemError: Unexpected system error or internal inconsistency
//
// Usage Patterns:
//
//  1. Direct type assertion with errors.As():
//     var valErr *ValidationError
//     if errors.As(err, &valErr) {
//     log.Printf("Invalid parameter %s: %s", valErr.Parameter, valErr.Reason)
//     }
//
//  2. Using helper constructors:
//     return nil, NewValidationError("port", "must be between 1 and 65535", port)
//
//  3. Wrapping internal errors:
//     return nil, WrapRuntimeError(internalErr, "submit_task")
package errors

import (
	"fmt"
	"strings"
	"time"
)

// RayError is the root interface for all Ray public API errors.
type RayError interface {
	error
	// Code returns a machine-readable error code.
	Code() string
	// Message returns a human-readable error description.
	Message() string
	// Details returns optional structured context information.
	Details() map[string]interface{}
	// Unwrap returns the underlying error if present.
	Unwrap() error
	// Retryable indicates whether the error may be resolved by retrying.
	Retryable() bool
}

// ValidationError represents an invalid parameter provided by the user.
type ValidationError struct {
	Parameter string                 // Parameter name
	Value     interface{}            // Actual value (optional)
	Reason    string                 // Validation failure reason
	details   map[string]interface{} // Additional context
	err       error                  // Underlying error
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	if e.Value != nil {
		return fmt.Sprintf("invalid parameter %q: %v (value: %v)", e.Parameter, e.Reason, e.Value)
	}
	return fmt.Sprintf("invalid parameter %q: %s", e.Parameter, e.Reason)
}

// Code implements RayError.
func (e *ValidationError) Code() string { return "VALIDATION_ERROR" }

// Message implements RayError.
func (e *ValidationError) Message() string { return e.Error() }

// Details implements RayError.
func (e *ValidationError) Details() map[string]interface{} { return e.details }

// Unwrap implements RayError.
func (e *ValidationError) Unwrap() error { return e.err }

// Retryable implements RayError.
func (e *ValidationError) Retryable() bool { return false }

// NewValidationError creates a new ValidationError.
func NewValidationError(param, reason string, value interface{}) *ValidationError {
	return &ValidationError{
		Parameter: param,
		Reason:    reason,
		Value:     value,
		details:   make(map[string]interface{}),
	}
}

// WrapValidationError wraps an existing error as ValidationError.
func WrapValidationError(err error, param, reason string) *ValidationError {
	return &ValidationError{
		Parameter: param,
		Reason:    reason,
		err:       err,
		details:   make(map[string]interface{}),
	}
}

// NewValidationErrorWithDetails creates a ValidationError with internal details and wrapped error.
// This function is exported for use by the internal/errors package only.
// Most users should use NewValidationError or WrapValidationError instead.
func NewValidationErrorWithDetails(details map[string]interface{}, err error, param, reason string, value interface{}) *ValidationError {
	return &ValidationError{
		Parameter: param,
		Reason:    reason,
		Value:     value,
		details:   details,
		err:       err,
	}
}

// RuntimeError represents a runtime operation failure due to state issues.
type RuntimeError struct {
	Operation string // Failed operation (e.g., "submit_task", "get_actor")
	State     string // Current state (e.g., "not_initialized", "already_running")
	details   map[string]interface{}
	err       error
}

// Error implements the error interface.
func (e *RuntimeError) Error() string {
	if e.State != "" {
		return fmt.Sprintf("runtime operation %q failed: current state is %q", e.Operation, e.State)
	}
	return fmt.Sprintf("runtime operation %q failed", e.Operation)
}

// Code implements RayError.
func (e *RuntimeError) Code() string { return "RUNTIME_ERROR" }

// Message implements RayError.
func (e *RuntimeError) Message() string { return e.Error() }

// Details implements RayError.
func (e *RuntimeError) Details() map[string]interface{} { return e.details }

// Unwrap implements RayError.
func (e *RuntimeError) Unwrap() error { return e.err }

// Retryable implements RayError.
func (e *RuntimeError) Retryable() bool {
	// Some runtime errors may be retryable (e.g., temporary resource shortage)
	return strings.Contains(e.Operation, "connect") || strings.Contains(e.Operation, "timeout")
}

// NewRuntimeError creates a new RuntimeError.
func NewRuntimeError(operation, state string) *RuntimeError {
	return &RuntimeError{
		Operation: operation,
		State:     state,
		details:   make(map[string]interface{}),
	}
}

// WrapRuntimeError wraps an existing error as RuntimeError.
func WrapRuntimeError(err error, operation string) *RuntimeError {
	return &RuntimeError{
		Operation: operation,
		details:   make(map[string]interface{}),
		err:       err,
	}
}

// NewRuntimeErrorWithDetails creates a RuntimeError with internal details and wrapped error.
// This function is exported for use by the internal/errors package only.
// Most users should use NewRuntimeError or WrapRuntimeError instead.
func NewRuntimeErrorWithDetails(details map[string]interface{}, err error, operation, state string) *RuntimeError {
	return &RuntimeError{
		Operation: operation,
		State:     state,
		details:   details,
		err:       err,
	}
}

// InitializationError represents component initialization or configuration failure.
type InitializationError struct {
	Component string                 // Component name (e.g., "Runtime", "GCSClient")
	Reason    string                 // Failure reason
	Config    map[string]interface{} // Related configuration items (sanitized)
	details   map[string]interface{}
	err       error
}

// Error implements the error interface.
func (e *InitializationError) Error() string {
	return fmt.Sprintf("failed to initialize %s: %s", e.Component, e.Reason)
}

// Code implements RayError.
func (e *InitializationError) Code() string { return "INITIALIZATION_ERROR" }

// Message implements RayError.
func (e *InitializationError) Message() string { return e.Error() }

// Details implements RayError.
func (e *InitializationError) Details() map[string]interface{} { return e.details }

// Unwrap implements RayError.
func (e *InitializationError) Unwrap() error { return e.err }

// Retryable implements RayError.
func (e *InitializationError) Retryable() bool { return false }

// NewInitializationError creates a new InitializationError.
func NewInitializationError(component, reason string) *InitializationError {
	return &InitializationError{
		Component: component,
		Reason:    reason,
		Config:    make(map[string]interface{}),
		details:   make(map[string]interface{}),
	}
}

// WrapInitializationError wraps an existing error as InitializationError.
func WrapInitializationError(err error, component, reason string) *InitializationError {
	return &InitializationError{
		Component: component,
		Reason:    reason,
		Config:    make(map[string]interface{}),
		details:   make(map[string]interface{}),
		err:       err,
	}
}

// NewInitializationErrorWithDetails creates an InitializationError with internal details and wrapped error.
// This function is exported for use by the internal/errors package only.
// Most users should use NewInitializationError or WrapInitializationError instead.
func NewInitializationErrorWithDetails(details map[string]interface{}, err error, component, reason string, config map[string]interface{}) *InitializationError {
	return &InitializationError{
		Component: component,
		Reason:    reason,
		Config:    config,
		details:   details,
		err:       err,
	}
}

// NetworkError represents network communication failure.
type NetworkError struct {
	Operation string        // Network operation (e.g., "dial", "send", "receive")
	Endpoint  string        // Target endpoint
	Timeout   time.Duration // Timeout duration (if applicable)
	retryable bool          // Whether retryable
	details   map[string]interface{}
	err       error
}

// Error implements the error interface.
func (e *NetworkError) Error() string {
	if e.Endpoint != "" {
		return fmt.Sprintf("network %s to %s failed", e.Operation, e.Endpoint)
	}
	return fmt.Sprintf("network %s failed", e.Operation)
}

// Code implements RayError.
func (e *NetworkError) Code() string { return "NETWORK_ERROR" }

// Message implements RayError.
func (e *NetworkError) Message() string { return e.Error() }

// Details implements RayError.
func (e *NetworkError) Details() map[string]interface{} { return e.details }

// Unwrap implements RayError.
func (e *NetworkError) Unwrap() error { return e.err }

// Retryable implements RayError.
func (e *NetworkError) Retryable() bool { return e.retryable }

// NewNetworkError creates a new NetworkError.
func NewNetworkError(operation, endpoint string, timeout time.Duration) *NetworkError {
	return &NetworkError{
		Operation: operation,
		Endpoint:  endpoint,
		Timeout:   timeout,
		retryable: true, // Default: network errors are retryable
		details:   make(map[string]interface{}),
	}
}

// WrapNetworkError wraps an existing error as NetworkError.
func WrapNetworkError(err error, operation, endpoint string) *NetworkError {
	return &NetworkError{
		Operation: operation,
		Endpoint:  endpoint,
		retryable: true,
		details:   make(map[string]interface{}),
		err:       err,
	}
}

// NewNetworkErrorWithDetails creates a NetworkError with internal details and wrapped error.
// This function is exported for use by the internal/errors package only.
// Most users should use NewNetworkError or WrapNetworkError instead.
func NewNetworkErrorWithDetails(details map[string]interface{}, err error, operation, endpoint string, timeout time.Duration, retryable bool) *NetworkError {
	return &NetworkError{
		Operation: operation,
		Endpoint:  endpoint,
		Timeout:   timeout,
		retryable: retryable,
		details:   details,
		err:       err,
	}
}

// SerializationError represents data serialization/deserialization failure.
type SerializationError struct {
	DataType  string // Data type (e.g., "TaskSpec", "ActorHandle")
	Operation string // "serialize" or "deserialize"
	Reason    string // Failure reason
	details   map[string]interface{}
	err       error
}

// Error implements the error interface.
func (e *SerializationError) Error() string {
	return fmt.Sprintf("%s of %s failed: %s", e.Operation, e.DataType, e.Reason)
}

// Code implements RayError.
func (e *SerializationError) Code() string { return "SERIALIZATION_ERROR" }

// Message implements RayError.
func (e *SerializationError) Message() string { return e.Error() }

// Details implements RayError.
func (e *SerializationError) Details() map[string]interface{} { return e.details }

// Unwrap implements RayError.
func (e *SerializationError) Unwrap() error { return e.err }

// Retryable implements RayError.
func (e *SerializationError) Retryable() bool { return false }

// NewSerializationError creates a new SerializationError.
func NewSerializationError(dataType string) *SerializationError {
	return &SerializationError{
		DataType:  dataType,
		Operation: "serialize",
		details:   make(map[string]interface{}),
	}
}

// NewDeserializationError creates a new SerializationError for deserialization.
func NewDeserializationError(dataType string) *SerializationError {
	return &SerializationError{
		DataType:  dataType,
		Operation: "deserialize",
		details:   make(map[string]interface{}),
	}
}

// WrapSerializationError wraps an existing error as SerializationError.
func WrapSerializationError(err error, dataType, operation string) *SerializationError {
	return &SerializationError{
		DataType:  dataType,
		Operation: operation,
		details:   make(map[string]interface{}),
		err:       err,
	}
}

// NewSerializationErrorWithDetails creates a SerializationError with internal details and wrapped error.
// This function is exported for use by the internal/errors package only.
// Most users should use NewSerializationError or WrapSerializationError instead.
func NewSerializationErrorWithDetails(details map[string]interface{}, err error, dataType, operation, reason string) *SerializationError {
	return &SerializationError{
		DataType:  dataType,
		Operation: operation,
		Reason:    reason,
		details:   details,
		err:       err,
	}
}

// SystemError represents unexpected system error or internal inconsistency.
type SystemError struct {
	Component  string // Affected component
	Reason     string // Error description
	Suggestion string // Suggested resolution
	details    map[string]interface{}
	err        error
}

// Error implements the error interface.
func (e *SystemError) Error() string {
	msg := fmt.Sprintf("system error in %s: %s", e.Component, e.Reason)
	if e.Suggestion != "" {
		msg += ". " + e.Suggestion
	}
	return msg
}

// Code implements RayError.
func (e *SystemError) Code() string { return "SYSTEM_ERROR" }

// Message implements RayError.
func (e *SystemError) Message() string { return e.Error() }

// Details implements RayError.
func (e *SystemError) Details() map[string]interface{} { return e.details }

// Unwrap implements RayError.
func (e *SystemError) Unwrap() error { return e.err }

// Retryable implements RayError.
func (e *SystemError) Retryable() bool { return false }

// NewSystemError creates a new SystemError.
func NewSystemError(component, reason string) *SystemError {
	return &SystemError{
		Component: component,
		Reason:    reason,
		details:   make(map[string]interface{}),
	}
}

// WrapSystemError wraps an existing error as SystemError.
func WrapSystemError(err error, component, reason string) *SystemError {
	return &SystemError{
		Component: component,
		Reason:    reason,
		details:   make(map[string]interface{}),
		err:       err,
	}
}

// NewSystemErrorWithDetails creates a SystemError with internal details and wrapped error.
// This function is exported for use by the internal/errors package only.
// Most users should use NewSystemError or WrapSystemError instead.
func NewSystemErrorWithDetails(details map[string]interface{}, err error, component, reason, suggestion string) *SystemError {
	return &SystemError{
		Component:  component,
		Reason:     reason,
		Suggestion: suggestion,
		details:    details,
		err:        err,
	}
}

// WithSuggestion adds a suggestion to a SystemError.
func (e *SystemError) WithSuggestion(suggestion string) *SystemError {
	e.Suggestion = suggestion
	return e
}

// ActorExitError represents an actor exiting intentionally.
// This error is NOT serialized and propagated to the caller,
// similar to Java's RayIntentionalSystemExitException.
// It indicates that the actor is exiting normally, not due to an error.
type ActorExitError struct {
	errorMessage string // Exit message
	details      map[string]interface{}
}

// Error implements the error interface.
func (e *ActorExitError) Error() string {
	return fmt.Sprintf("actor exiting: %s", e.errorMessage)
}

// Code implements RayError.
func (e *ActorExitError) Code() string { return "ACTOR_EXIT" }

// Message implements RayError.
func (e *ActorExitError) Message() string { return e.Error() }

// Details implements RayError.
func (e *ActorExitError) Details() map[string]interface{} { return e.details }

// Unwrap implements RayError.
func (e *ActorExitError) Unwrap() error { return nil }

// Retryable implements RayError.
func (e *ActorExitError) Retryable() bool { return false }

// NewActorExitError creates a new ActorExitError.
func NewActorExitError(message string) *ActorExitError {
	return &ActorExitError{
		errorMessage: message,
		details:      make(map[string]interface{}),
	}
}

// ============================================================================
// Error Translator Interface (Dependency Inversion)
// ============================================================================

// ErrorTranslator defines the interface for converting internal errors to public API errors.
// This interface is defined in pkg/errors (high-level policy) and implemented by internal/errors (low-level details).
// This follows the Dependency Inversion Principle: high-level modules define abstractions,
// low-level modules implement concrete details.
type ErrorTranslator interface {
	// Translate converts an internal error to an appropriate public API error.
	// Returns nil if the translator cannot handle this error type.
	//
	// The translated error should:
	// - Be a concrete public error type (*ValidationError, *RuntimeError, etc.)
	// - Wrap the original error via Unwrap() for debugging
	// - Not expose sensitive internal details
	Translate(err error) error
}

// DefaultTranslator is the default error translator instance.
// This variable is initialized by internal/errors package via init().
// If nil, ConvertToPublic will wrap errors as SystemError.
var DefaultTranslator ErrorTranslator

// ConvertToPublic converts an internal error to an appropriate public API error type.
// This is the public API entry point for error translation.
//
// Conversion strategy:
// 1. Delegate to DefaultTranslator (which is an *InternalErrorTranslator).
// 2. If DefaultTranslator is nil, wrap as SystemError for graceful degradation.
//
// Examples:
//
//	// Simple usage
//	objRef, err := submitter.SubmitTask(ctx, task)
//	if err != nil {
//	    return nil, errors.ConvertToPublic(err)
//	}
//
//	// Custom translator for specific needs
//	customTranslator := &InternalErrorTranslator{}
//	publicErr := customTranslator.Translate(internalErr)
func ConvertToPublic(err error) error {
	if DefaultTranslator == nil {
		// Fallback: wrap as SystemError when no translator is available
		return WrapSystemError(err, "unknown", "no translator available")
	}
	return DefaultTranslator.Translate(err)
}

// ============================================================================
// Error Type Checking Helpers (for API consistency with Java)
// ============================================================================

// Note: These helper functions are provided for API consistency with Java Ray API.
// However, the recommended approach is to use Go's errors.As() for type-safe error inspection:
//
//	var valErr *ValidationError
//	if errors.As(err, &valErr) {
//	    // Handle validation error
//	}
//
// The following helpers are kept for backward compatibility and Java API parity.

// IsRayError checks if an error is a RayError (any public API error type).
// This function is provided for Java API consistency.
// Prefer using errors.As() for type-safe error inspection in Go code.
func IsRayError(err error) bool {
	_, ok := err.(RayError)
	return ok
}

// IsTimeout checks if an error indicates a timeout condition.
// This function is provided for Java API consistency.
// Prefer using errors.As() with specific error types for more precise checks.
func IsTimeout(err error) bool {
	if networkErr, ok := err.(*NetworkError); ok {
		return networkErr.Timeout > 0
	}
	// Also check RuntimeError for timeout-related states
	if runtimeErr, ok := err.(*RuntimeError); ok {
		return strings.Contains(runtimeErr.Operation, "timeout")
	}
	return false
}

// IsActorNotExists checks if an error indicates that an actor does not exist.
// This function is provided for Java API consistency.
func IsActorNotExists(err error) bool {
	if runtimeErr, ok := err.(*RuntimeError); ok {
		return runtimeErr.State == "actor_not_exists" ||
			strings.Contains(runtimeErr.Message(), "actor does not exist")
	}
	return false
}

// IsActorDead checks if an error indicates that an actor is dead.
// This function is provided for Java API consistency.
func IsActorDead(err error) bool {
	if runtimeErr, ok := err.(*RuntimeError); ok {
		return runtimeErr.State == "actor_dead" ||
			strings.Contains(runtimeErr.Message(), "actor is dead")
	}
	return false
}

// ============================================================================
// Predefined Error Constants (for API consistency with Java)
// ============================================================================

// Common error codes for API consistency
const (
	// ErrRuntimeNotInitializedCode indicates runtime is not initialized
	ErrRuntimeNotInitializedCode = "RUNTIME_NOT_INITIALIZED"

	// ErrInvalidArgumentCode indicates invalid argument
	ErrInvalidArgumentCode = "INVALID_ARGUMENT"

	// ErrIntentionalSystemExitCode indicates intentional system exit
	ErrIntentionalSystemExitCode = "INTENTIONAL_SYSTEM_EXIT"
)

// ErrRuntimeNotInitialized is a predefined error for runtime not initialized.
// This is used when API is called before Ray runtime is initialized.
var ErrRuntimeNotInitialized = &RuntimeError{
	Operation: "api_call",
	State:     "not_initialized",
	details:   make(map[string]interface{}),
}

// NewRayInvalidArgumentException creates a new validation error for invalid arguments.
// This function is provided for Java API consistency.
// Prefer using NewValidationError for clearer Go code.
func NewRayInvalidArgumentException(message string) *ValidationError {
	return NewValidationError("argument", message, nil)
}

// NewRayIntentionalSystemExitException creates a new intentional system exit error.
// This is used when an actor intentionally exits.
// This function is provided for Java API consistency.
func NewRayIntentionalSystemExitException(message string) *ActorExitError {
	return NewActorExitError(message)
}
