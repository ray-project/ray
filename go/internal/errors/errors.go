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

// Package errors provides unified error type definitions for Ray Go Runtime.
//
// Error handling patterns:
// 1. Sentinel errors (ErrInvalidArgument, etc.) - for errors.Is() comparisons and error wrapping
// 2. RayError interface with factory functions (NewInvalidArgumentError, etc.) - for structured errors with codes and categories
//
// When to use which:
//   - Use sentinel errors with errors.Is() for simple error type checks
//   - Use factory functions when you need error codes, categories, and retryable flags
//   - Sentinel errors can be wrapped: fmt.Errorf("%w: context", ErrInvalidArgument)
//   - Factory functions return RayError with rich metadata
//
// Design notes:
// 1. Unified cross-layer error types for easy error handling and classification.
// 2. Uses sentinel errors and error type interfaces.
// 3. Supports error wrapping and error chains.
package errors

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/ray-project/ray/go/pkg/ids"
)

// Common error types.

var (
	// ErrInvalidArgument invalid argument error.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrNotInitialized not initialized error.
	ErrNotInitialized = errors.New("not initialized")
	// ErrAlreadyRunning already running error.
	ErrAlreadyRunning = errors.New("already running")
	// ErrShutdown already shutdown error.
	ErrShutdown = errors.New("already shutdown")
	// ErrTimeout timeout error.
	ErrTimeout = errors.New("operation timed out")
	// ErrConnectionFailed connection failed error.
	ErrConnectionFailed = errors.New("connection failed")
	// ErrInternalError internal error.
	ErrInternalError = errors.New("internal error")
)

// Initialization related errors.

var (
	// ErrInvalidWorkerType invalid worker type.
	ErrInvalidWorkerType = errors.New("invalid worker type")
	// ErrInvalidIPAddress invalid IP address format.
	ErrInvalidIPAddress = errors.New("invalid IP address format")
	// ErrInvalidPort invalid port number.
	ErrInvalidPort = errors.New("invalid port number")
	// ErrEmptySocketPath socket path cannot be empty.
	ErrEmptySocketPath = errors.New("socket path cannot be empty")
	// ErrEmptyGcsAddress GCS address cannot be empty.
	ErrEmptyGcsAddress = errors.New("GCS address cannot be empty")
	// ErrEmptyLogDir log directory cannot be empty.
	ErrEmptyLogDir = errors.New("log directory cannot be empty")
	// ErrRuntimeFactoryNotSet runtime factory not set.
	ErrRuntimeFactoryNotSet = errors.New("runtime factory not set")
)

// Worker related errors.

var (
	// ErrUnknownWorkerType unknown worker type.
	ErrUnknownWorkerType = errors.New("unknown worker type")
	// ErrInvalidHandle invalid handle.
	ErrInvalidHandle = errors.New("invalid handle")
)

// Runtime initialization and state errors.

var (
	// ErrRuntimeAlreadyInitialized runtime already initialized error.
	ErrRuntimeAlreadyInitialized = errors.New("runtime already initialized")
	// ErrRuntimeNotInitialized runtime not initialized error.
	ErrRuntimeNotInitialized = errors.New("runtime not initialized")
)

// Plugin related errors (for plugin loading and validation).

var (
	// ErrPluginNotFound plugin file not found.
	ErrPluginNotFound = errors.New("plugin file not found")
	// ErrPluginLoadFailed failed to load plugin.
	ErrPluginLoadFailed = errors.New("failed to load plugin")
	// ErrSymbolNotFound symbol not found in plugin.
	ErrSymbolNotFound = errors.New("symbol not found in plugin")
	// ErrInvalidSymbolType plugin symbol has unexpected type.
	ErrInvalidSymbolType = errors.New("plugin symbol has unexpected type")
	// ErrPluginPathInvalid invalid plugin path.
	ErrPluginPathInvalid = errors.New("invalid plugin path")
	// ErrPluginTooLarge plugin file too large.
	ErrPluginTooLarge = errors.New("plugin file too large")
	// ErrPluginExtInvalid invalid plugin file extension.
	ErrPluginExtInvalid = errors.New("invalid plugin file extension")
	// ErrPluginPathTraversal path traversal detected.
	ErrPluginPathTraversal = errors.New("path traversal detected")
	// ErrPluginPathNotAllowed plugin path not in allowed whitelist.
	ErrPluginPathNotAllowed = errors.New("plugin path not in allowed whitelist")
	// ErrPluginChecksumMismatch plugin checksum mismatch.
	ErrPluginChecksumMismatch = errors.New("plugin checksum mismatch")
)

// Serialization related errors.

var (
	// ErrSerializationFailed serialization failed.
	ErrSerializationFailed = errors.New("serialization failed")
	// ErrDeserializationFailed deserialization failed.
	ErrDeserializationFailed = errors.New("deserialization failed")
	// ErrInvalidMetadata invalid metadata.
	ErrInvalidMetadata = errors.New("invalid metadata")
)

// Object store related errors.

var (
	// ErrObjectNotFound object not found.
	ErrObjectNotFound = errors.New("object not found")
	// ErrObjectStoreNotInitialized object store not initialized.
	ErrObjectStoreNotInitialized = errors.New("object store not initialized")
	// ErrPutFailed failed to put object.
	ErrPutFailed = errors.New("failed to put object")
	// ErrGetFailed failed to get object.
	ErrGetFailed = errors.New("failed to get object")
)

// RayError is the interface for Ray runtime errors.
//
// Design notes:
// 1. Provides error classification and error codes.
// 2. Facilitates error type judgment by upper-level code.
type RayError interface {
	error
	// Code returns the error code.
	Code() string
	// Category returns the error category.
	Category() ErrorCategory
	// IsRetryable returns whether the error is retryable.
	IsRetryable() bool
	// Details returns structured context information about the error.
	Details() map[string]interface{}
}

// ErrorCategory represents error classification using iota for type safety.
type ErrorCategory int

const (
	// CategoryArgument argument error.
	CategoryArgument ErrorCategory = iota
	// CategoryInitialization initialization error.
	CategoryInitialization
	// CategoryRuntime runtime error.
	CategoryRuntime
	// CategoryNetwork network error.
	CategoryNetwork
	// CategorySerialization serialization error.
	CategorySerialization
	// CategorySystem system error.
	CategorySystem
)

// String returns the string representation of ErrorCategory.
func (c ErrorCategory) String() string {
	switch c {
	case CategoryArgument:
		return "ARGUMENT"
	case CategoryInitialization:
		return "INITIALIZATION"
	case CategoryRuntime:
		return "RUNTIME"
	case CategoryNetwork:
		return "NETWORK"
	case CategorySerialization:
		return "SERIALIZATION"
	case CategorySystem:
		return "SYSTEM"
	default:
		return "UNKNOWN"
	}
}

// Error codes - use constants for type safety.
const (
	CodeInvalidArgument      = "INVALID_ARGUMENT"
	CodeInitializationError  = "INITIALIZATION_ERROR"
	CodeRuntimeError         = "RUNTIME_ERROR"
	CodeNetworkError         = "NETWORK_ERROR"
	CodeSerializationError   = "SERIALIZATION_ERROR"
	CodeDeserializationError = "DESERIALIZATION_ERROR"
	CodeSystemError          = "SYSTEM_ERROR"
)

// rayError implements the RayError interface.
type rayError struct {
	code      string
	category  ErrorCategory
	message   string
	retryable bool
	wrapped   error
	details   map[string]interface{}
}

// Error implements the error interface.
func (e *rayError) Error() string {
	if e.wrapped != nil {
		return fmt.Sprintf("[%s] %s: %v", e.code, e.message, e.wrapped)
	}
	return fmt.Sprintf("[%s] %s", e.code, e.message)
}

// Code returns the error code.
func (e *rayError) Code() string {
	return e.code
}

// Category returns the error category.
func (e *rayError) Category() ErrorCategory {
	return e.category
}

// IsRetryable returns whether the error is retryable.
func (e *rayError) IsRetryable() bool {
	return e.retryable
}

// Unwrap returns the wrapped error.
func (e *rayError) Unwrap() error {
	return e.wrapped
}

// Details returns structured context information about the error.
func (e *rayError) Details() map[string]interface{} {
	if e.details == nil {
		return make(map[string]interface{})
	}
	// Return a copy to prevent external modification
	copy := make(map[string]interface{}, len(e.details))
	for k, v := range e.details {
		copy[k] = v
	}
	return copy
}

// errorConfig is used to configure RayError creation.
type errorConfig struct {
	code      string
	category  ErrorCategory
	message   string
	retryable bool
	wrapped   error
	details   map[string]interface{}
}

// ErrorOption is a function type for creating errors with functional options.
type ErrorOption func(*errorConfig)

// WithCategory sets the error category.
func WithCategory(category ErrorCategory) ErrorOption {
	return func(c *errorConfig) {
		c.category = category
	}
}

// WithMessage sets the error message.
func WithMessage(message string) ErrorOption {
	return func(c *errorConfig) {
		c.message = message
	}
}

// WithRetryable sets whether the error is retryable.
func WithRetryable(retryable bool) ErrorOption {
	return func(c *errorConfig) {
		c.retryable = retryable
	}
}

// WithWrappedError sets the wrapped error.
func WithWrappedError(err error) ErrorOption {
	return func(c *errorConfig) {
		c.wrapped = err
	}
}

// WithDetails sets structured context information for the error.
func WithDetails(details map[string]interface{}) ErrorOption {
	return func(c *errorConfig) {
		c.details = details
	}
}

// NewRayError creates a new RayError using functional options pattern.
//
// Parameters:
//   - code: error code (e.g., "INVALID_ARGUMENT")
//   - opts: functional options to configure the error
//
// Example:
//
//	err := NewRayError(CodeInvalidArgument,
//	    WithCategory(CategoryArgument),
//	    WithMessage("invalid port number"),
//	    WithRetryable(false))
func NewRayError(code string, opts ...ErrorOption) RayError {
	cfg := &errorConfig{
		code:      code,
		category:  CategoryRuntime,
		message:   "",
		retryable: false,
		wrapped:   nil,
		details:   nil,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return &rayError{
		code:      cfg.code,
		category:  cfg.category,
		message:   cfg.message,
		retryable: cfg.retryable,
		wrapped:   cfg.wrapped,
		details:   cfg.details,
	}
}

// WrapRayError wraps an existing error as RayError using functional options pattern.
//
// Parameters:
//   - err: the wrapped error
//   - code: error code
//   - opts: functional options to configure the error
func WrapRayError(err error, code string, opts ...ErrorOption) RayError {
	cfg := &errorConfig{
		code:      code,
		category:  CategoryRuntime,
		message:   "",
		retryable: false,
		wrapped:   err,
		details:   nil,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return &rayError{
		code:      cfg.code,
		category:  cfg.category,
		message:   cfg.message,
		retryable: cfg.retryable,
		wrapped:   cfg.wrapped,
		details:   cfg.details,
	}
}

// IsRayError checks if the error is of RayError type.
func IsRayError(err error) bool {
	_, ok := err.(RayError)
	return ok
}

// GetRayErrorCode gets the error code of RayError.
func GetRayErrorCode(err error) string {
	if rayErr, ok := err.(RayError); ok {
		return rayErr.Code()
	}
	return ""
}

// GetRayErrorCategory gets the error category of RayError.
func GetRayErrorCategory(err error) ErrorCategory {
	if rayErr, ok := err.(RayError); ok {
		return rayErr.Category()
	}
	return CategoryRuntime // default to CategoryRuntime for non-RayError
}

// IsRetryableError checks if the error is retryable.
func IsRetryableError(err error) bool {
	if rayErr, ok := err.(RayError); ok {
		return rayErr.IsRetryable()
	}
	return false
}

// Helper functions: create specific types of errors using functional options.

// NewInvalidArgumentError creates an argument error with structured context.
func NewInvalidArgumentError(param, reason string) RayError {
	return NewRayError(CodeInvalidArgument,
		WithCategory(CategoryArgument),
		WithMessage(fmt.Sprintf("invalid argument %q: %s", param, reason)),
		WithRetryable(false),
		WithDetails(map[string]interface{}{
			"parameter": param,
			"reason":    reason,
		}))
}

// NewInitializationError creates an initialization error with structured context.
func NewInitializationError(component, reason string) RayError {
	return NewRayError(CodeInitializationError,
		WithCategory(CategoryInitialization),
		WithMessage(fmt.Sprintf("failed to initialize %s: %s", component, reason)),
		WithRetryable(false),
		WithDetails(map[string]interface{}{
			"component": component,
			"reason":    reason,
		}))
}

// NewRuntimeError creates a runtime error with structured context.
func NewRuntimeError(operation, reason string) RayError {
	return NewRayError(CodeRuntimeError,
		WithCategory(CategoryRuntime),
		WithMessage(fmt.Sprintf("runtime error during %s: %s", operation, reason)),
		WithRetryable(false),
		WithDetails(map[string]interface{}{
			"operation": operation,
			"reason":    reason,
			"state":     inferStateFromReason(reason),
		}))
}

// inferStateFromReason infers the runtime state from the error reason.
func inferStateFromReason(reason string) string {
	switch {
	case strings.Contains(reason, "not initialized"):
		return "not_initialized"
	case strings.Contains(reason, "already running"):
		return "already_running"
	case strings.Contains(reason, "already shutdown"):
		return "already_shutdown"
	default:
		return "unknown"
	}
}

// NewNetworkError creates a network error with structured context.
func NewNetworkError(operation string, retryable bool) RayError {
	return NewRayError(CodeNetworkError,
		WithCategory(CategoryNetwork),
		WithMessage(fmt.Sprintf("network error during %s", operation)),
		WithRetryable(retryable),
		WithDetails(map[string]interface{}{
			"operation": operation,
			"retryable": retryable,
		}))
}

// NewSerializationError creates a serialization error with structured context.
func NewSerializationError(reason string) RayError {
	return NewRayError(CodeSerializationError,
		WithCategory(CategorySerialization),
		WithMessage(fmt.Sprintf("serialization error: %s", reason)),
		WithRetryable(false),
		WithDetails(map[string]interface{}{
			"reason":    reason,
			"operation": "serialize",
		}))
}

// NewDeserializationError creates a deserialization error with structured context.
func NewDeserializationError(reason string) RayError {
	return NewRayError(CodeDeserializationError,
		WithCategory(CategorySerialization),
		WithMessage(fmt.Sprintf("deserialization error: %s", reason)),
		WithRetryable(false),
		WithDetails(map[string]interface{}{
			"reason":    reason,
			"operation": "deserialize",
		}))
}

// NewSystemError creates a system error with structured context.
func NewSystemError(reason string) RayError {
	return NewRayError(CodeSystemError,
		WithCategory(CategorySystem),
		WithMessage(fmt.Sprintf("system error: %s", reason)),
		WithRetryable(false),
		WithDetails(map[string]interface{}{
			"reason": reason,
		}))
}

// ============================================================================
// Java-serialization-specific errors
// ============================================================================
// These error types are serialization-compatible with Java's RayTaskException
// and are used for cross-language error propagation across the CGO boundary.

// SerializedRayError is the serialized form of a Ray error.
// Compatible with Java RayTaskException protobuf format.
type SerializedRayError struct {
	// ErrorCode is the error code (e.g., "TASK_EXECUTION_FAILED")
	ErrorCode string `json:"error_code"`
	// ErrorType is the error type name (e.g., "RayTaskException")
	ErrorType string `json:"error_type"`
	// ErrorMessage is the full error message including context
	ErrorMessage string `json:"error_message"`
	// CauseMessage is the underlying cause message
	CauseMessage string `json:"cause_message"`
	// StackTrace is the stack trace as a string
	StackTrace string `json:"stack_trace"`
	// PID is the process ID where the error occurred
	PID int `json:"pid"`
	// IPAddress is the IP address of the node where the error occurred
	IPAddress string `json:"ip_address"`
	// TaskID is the ID of the task where the error occurred (if applicable)
	TaskID string `json:"task_id,omitempty"`
	// ActorID is the ID of the actor where the error occurred (if applicable)
	ActorID string `json:"actor_id,omitempty"`
	// JobID is the ID of the job where the error occurred (if applicable)
	JobID string `json:"job_id,omitempty"`
}

// Error type constants - avoid hardcoding strings in multiple places.
const (
	RayTaskExceptionType        = "RayTaskException"
	RayActorExceptionType       = "RayActorException"
	RayActorMethodExceptionType = "RayActorMethodException"
)

// Error code constants - avoid hardcoding strings in multiple places.
const (
	TaskExecutionFailedCode = "TASK_EXECUTION_FAILED"
	ActorCreationFailedCode = "ACTOR_CREATION_FAILED"
	ActorMethodFailedCode   = "ACTOR_METHOD_FAILED"
)

// baseRayError contains common fields for all Ray error types.
// Using embedded struct to eliminate code duplication.
type baseRayError struct {
	message    string
	cause      error
	stackTrace string
	pid        int
	ipAddress  string
	jobID      ids.JobID
}

// Error implements the error interface.
func (b *baseRayError) Error() string {
	return fmt.Sprintf("(pid=%d, ip=%s, jobId=%s)", b.message, b.pid, b.ipAddress, b.jobID.Hex())
}

// CauseMessage implements RayError interface.
func (b *baseRayError) CauseMessage() string {
	if b.cause != nil {
		return b.cause.Error()
	}
	return ""
}

// StackTrace implements RayError interface.
func (b *baseRayError) StackTrace() string {
	return b.stackTrace
}

// ToSerializedFormBase creates the base part of SerializedRayError.
// Type-specific fields (taskID, actorID) should be added by the caller.
func (b *baseRayError) ToSerializedFormBase(errorType, errorCode, errorMessage string) *SerializedRayError {
	return &SerializedRayError{
		ErrorCode:    errorCode,
		ErrorType:    errorType,
		ErrorMessage: errorMessage,
		CauseMessage: b.CauseMessage(),
		StackTrace:   b.StackTrace(),
		PID:          b.pid,
		IPAddress:    b.ipAddress,
		JobID:        b.jobID.Hex(),
	}
}

// TaskExecutionError represents a task execution failure.
// Compatible with Java RayTaskException.
type TaskExecutionError struct {
	baseRayError
	taskID ids.TaskID
}

// NewTaskExecutionError creates a new TaskExecutionError.
func NewTaskExecutionError(
	taskID ids.TaskID,
	jobID ids.JobID,
	cause error,
	message string,
) *TaskExecutionError {
	return &TaskExecutionError{
		baseRayError: baseRayError{
			message:    message,
			cause:      cause,
			stackTrace: captureStackTrace(2),
			pid:        getPID(),
			ipAddress:  getIPAddress(),
			jobID:      jobID,
		},
		taskID: taskID,
	}
}

// Error implements the error interface.
func (e *TaskExecutionError) Error() string {
	return fmt.Sprintf("RayTaskException: %s (pid=%d, ip=%s, taskId=%s)",
		e.message, e.pid, e.ipAddress, e.taskID.Hex())
}

// ErrorCode implements RayError interface.
func (e *TaskExecutionError) ErrorCode() string {
	return TaskExecutionFailedCode
}

// ErrorType implements RayError interface.
func (e *TaskExecutionError) ErrorType() string {
	return RayTaskExceptionType
}

// CauseMessage implements RayError interface.
func (e *TaskExecutionError) CauseMessage() string {
	return e.baseRayError.CauseMessage()
}

// StackTrace implements RayError interface.
func (e *TaskExecutionError) StackTrace() string {
	return e.baseRayError.StackTrace()
}

// ToSerializedForm implements RayError interface.
func (e *TaskExecutionError) ToSerializedForm() *SerializedRayError {
	serialized := e.baseRayError.ToSerializedFormBase(RayTaskExceptionType, TaskExecutionFailedCode, e.Error())
	serialized.TaskID = e.taskID.Hex()
	return serialized
}

// Unwrap returns the underlying cause.
func (e *TaskExecutionError) Unwrap() error {
	return e.cause
}

// ActorCreationError represents an actor creation failure.
// Compatible with Java RayActorException.
type ActorCreationError struct {
	baseRayError
	actorID ids.ActorID
}

// NewActorCreationError creates a new ActorCreationError.
func NewActorCreationError(
	actorID ids.ActorID,
	jobID ids.JobID,
	cause error,
	message string,
) *ActorCreationError {
	return &ActorCreationError{
		baseRayError: baseRayError{
			message:    message,
			cause:      cause,
			stackTrace: captureStackTrace(2),
			pid:        getPID(),
			ipAddress:  getIPAddress(),
			jobID:      jobID,
		},
		actorID: actorID,
	}
}

// Error implements the error interface.
func (e *ActorCreationError) Error() string {
	return fmt.Sprintf("RayActorException: %s (pid=%d, ip=%s, actorId=%s)",
		e.message, e.pid, e.ipAddress, e.actorID.Hex())
}

// ErrorCode implements RayError interface.
func (e *ActorCreationError) ErrorCode() string {
	return ActorCreationFailedCode
}

// ErrorType implements RayError interface.
func (e *ActorCreationError) ErrorType() string {
	return RayActorExceptionType
}

// CauseMessage implements RayError interface.
func (e *ActorCreationError) CauseMessage() string {
	return e.baseRayError.CauseMessage()
}

// StackTrace implements RayError interface.
func (e *ActorCreationError) StackTrace() string {
	return e.baseRayError.StackTrace()
}

// ToSerializedForm implements RayError interface.
func (e *ActorCreationError) ToSerializedForm() *SerializedRayError {
	serialized := e.baseRayError.ToSerializedFormBase(RayActorExceptionType, ActorCreationFailedCode, e.Error())
	serialized.ActorID = e.actorID.Hex()
	return serialized
}

// Unwrap returns the underlying cause.
func (e *ActorCreationError) Unwrap() error {
	return e.cause
}

// ActorMethodError represents an actor method call failure.
// Compatible with Java RayTaskException for actor tasks.
type ActorMethodError struct {
	baseRayError
	actorID    ids.ActorID
	taskID     ids.TaskID
	methodName string
}

// ActorMethodErrorOptions configures NewActorMethodError creation.
// Using options pattern for better readability and maintainability.
type ActorMethodErrorOptions struct {
	// ActorID is the ID of the actor where the error occurred
	ActorID ids.ActorID
	// TaskID is the ID of the task where the error occurred
	TaskID ids.TaskID
	// JobID is the ID of the job
	JobID ids.JobID
	// MethodName is the name of the failed method
	MethodName string
	// Cause is the underlying error
	Cause error
	// Message is the error message
	Message string
}

// NewActorMethodError creates a new ActorMethodError using options pattern.
//
// Example:
//
//	err := NewActorMethodError(ActorMethodErrorOptions{
//	    ActorID:    actorID,
//	    TaskID:     taskID,
//	    JobID:      jobID,
//	    MethodName: "myMethod",
//	    Cause:      underlyingErr,
//	    Message:    "method failed",
//	})
func NewActorMethodError(opts ActorMethodErrorOptions) *ActorMethodError {
	return &ActorMethodError{
		baseRayError: baseRayError{
			message:    opts.Message,
			cause:      opts.Cause,
			stackTrace: captureStackTrace(2),
			pid:        getPID(),
			ipAddress:  getIPAddress(),
			jobID:      opts.JobID,
		},
		actorID:    opts.ActorID,
		taskID:     opts.TaskID,
		methodName: opts.MethodName,
	}
}

// Error implements the error interface.
func (e *ActorMethodError) Error() string {
	return fmt.Sprintf("RayTaskException: %s (pid=%d, ip=%s, actorId=%s, method=%s)",
		e.message, e.pid, e.ipAddress, e.actorID.Hex(), e.methodName)
}

// ErrorCode implements RayError interface.
func (e *ActorMethodError) ErrorCode() string {
	return ActorMethodFailedCode
}

// ErrorType implements RayError interface.
func (e *ActorMethodError) ErrorType() string {
	return RayActorMethodExceptionType
}

// CauseMessage implements RayError interface.
func (e *ActorMethodError) CauseMessage() string {
	return e.baseRayError.CauseMessage()
}

// StackTrace implements RayError interface.
func (e *ActorMethodError) StackTrace() string {
	return e.baseRayError.StackTrace()
}

// ToSerializedForm implements RayError interface.
func (e *ActorMethodError) ToSerializedForm() *SerializedRayError {
	serialized := e.baseRayError.ToSerializedFormBase(RayActorMethodExceptionType, ActorMethodFailedCode, e.Error())
	serialized.ActorID = e.actorID.Hex()
	serialized.TaskID = e.taskID.Hex()
	return serialized
}

// Unwrap returns the underlying cause.
func (e *ActorMethodError) Unwrap() error {
	return e.cause
}

// captureStackTrace captures the current stack trace as a string.
// skip is the number of stack frames to skip.
func captureStackTrace(skip int) string {
	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, false)
	lines := strings.Split(string(buf[:n]), "\n")

	// Skip the first few frames (runtime.Stack and captureStackTrace itself)
	if skip >= len(lines) {
		skip = len(lines) - 1
	}
	return strings.Join(lines[skip:], "\n")
}

// getPID returns the current process ID.
func getPID() int {
	return os.Getpid()
}

// getIPAddress returns the current node IP address.
// Returns empty string if IP address cannot be determined.
func getIPAddress() string {
	// TODO: Implement IP address detection
	// For now, return empty string
	return ""
}

// SerializeError serializes a TaskExecutionError to JSON bytes.
// Compatible with Java RayTaskException serialization.
// Note: This function is primarily used for testing. Production code uses
// ToSerializedForm() method directly on *TaskExecutionError.
func SerializeError(err *TaskExecutionError) ([]byte, error) {
	serialized := err.ToSerializedForm()
	return json.Marshal(serialized)
}

// DeserializeError deserializes JSON bytes to a TaskExecutionError.
// Compatible with Java RayTaskException deserialization.
// Note: This function is primarily used for testing. Production code uses
// json.Unmarshal directly on *SerializedRayError.
func DeserializeError(data []byte) (*TaskExecutionError, error) {
	var serialized SerializedRayError
	if err := json.Unmarshal(data, &serialized); err != nil {
		return nil, fmt.Errorf("failed to deserialize error: %w", err)
	}

	// Reconstruct TaskExecutionError from serialized form
	taskID, _ := ids.TaskIDFromHex(serialized.TaskID)
	jobID, _ := ids.JobIDFromHex(serialized.JobID)
	// Extract message from ErrorMessage (remove the RayTaskException prefix)
	message := serialized.ErrorMessage
	if idx := strings.Index(serialized.ErrorMessage, ": "); idx != -1 {
		message = serialized.ErrorMessage[idx+2:]
		// Remove the (pid=..., ip=..., taskId=...) suffix
		if parenIdx := strings.Index(message, " ("); parenIdx != -1 {
			message = message[:parenIdx]
		}
	}
	return &TaskExecutionError{
		baseRayError: baseRayError{
			message:    message,
			cause:      fmt.Errorf("%s", serialized.CauseMessage),
			stackTrace: serialized.StackTrace,
			pid:        serialized.PID,
			ipAddress:  serialized.IPAddress,
			jobID:      jobID,
		},
		taskID: taskID,
	}, nil
}
