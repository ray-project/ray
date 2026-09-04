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

package object

import (
	"encoding/json"
	"fmt"
)

// exceptionFactory is a function type for creating exceptions from raw data.
type exceptionFactory func(data map[string]interface{}) RayException

// exceptionRegistry maps error codes to their corresponding exception factories.
// Uses unified factory functions to reduce boilerplate code for ID-based exceptions.
var exceptionRegistry = map[int]exceptionFactory{
	// Simple exceptions with no extra fields
	ErrorCodeWorkerDied: simpleExceptionFactory(NewRayWorkerException),

	// ID-based exceptions - all use the same unified RayIDException type
	ErrorCodeActorDied:             idExceptionFactory(NewRayIDExceptionActorDied),
	ErrorCodeActorUnavailable:      idExceptionFactory(NewRayIDExceptionActorUnavailable),
	ErrorCodeObjectUnreconstructable: idExceptionFactory(NewRayIDExceptionUnreconstructable),
	ErrorCodeObjectLost:            idExceptionFactory(NewRayIDExceptionLost),
	ErrorCodeOwnerDied:             idExceptionFactory(NewRayIDExceptionOwnerDied),
	ErrorCodeObjectDeleted:         idExceptionFactory(NewRayIDExceptionDeleted),

	// Complex exceptions with multiple fields
	ErrorCodeTaskExecutionException: func(data map[string]interface{}) RayException {
		taskID, _ := data["task_id"].(string)
		message, _ := data["message"].(string)
		stackTrace, _ := data["stack_trace"].(string)
		return NewRayTaskExecutionException(taskID, fmt.Errorf("%s", message), stackTrace)
	},
}

// RayException is the base interface for Ray exceptions.
// All Ray exception types must implement this interface.
type RayException interface {
	error
	// ToBytes serializes the exception to a byte array.
	ToBytes() []byte
	// ErrorCode returns the error code.
	ErrorCode() int
}

// rayException is the base implementation of Ray exceptions.
type rayException struct {
	code    int
	message string
}

// Error implements the error interface.
func (e *rayException) Error() string {
	return e.message
}

// ErrorCode returns the error code.
func (e *rayException) ErrorCode() int {
	return e.code
}

// ToBytes serializes the exception to a byte array.
func (e *rayException) ToBytes() []byte {
	data, _ := json.Marshal(map[string]interface{}{
		"error_code": e.code,
		"message":    e.message,
	})
	return data
}

// rayIDException is a generic implementation for ID-based Ray exceptions.
// It embeds rayException and adds an ID field (can be actorID or objectID).
// This reduces code duplication for exceptions that only differ by ID type.
type rayIDException struct {
	rayException
	id string
}

// ID returns the ID associated with this exception.
func (e *rayIDException) ID() string {
	return e.id
}

// ToBytes serializes the exception to a byte array, including the ID.
func (e *rayIDException) ToBytes() []byte {
	data, _ := json.Marshal(map[string]interface{}{
		"error_code": e.code,
		"message":    e.message,
		"id":         e.id,
	})
	return data
}

// simpleExceptionFactory creates a factory function for exceptions with no extra fields.
// This is a generic helper to reduce boilerplate code for simple exception types.
func simpleExceptionFactory[T RayException](newFunc func() T) exceptionFactory {
	return func(data map[string]interface{}) RayException {
		return newFunc()
	}
}

// idExceptionFactory creates a factory function for exceptions with an ID field (actorID or objectID).
// This is a generic helper to reduce boilerplate code for ID-based exception types.
func idExceptionFactory[T RayException](newFunc func(id string) T) exceptionFactory {
	return func(data map[string]interface{}) RayException {
		// Try to extract actor_id or object_id from data
		id, _ := data["actor_id"].(string)
		if id == "" {
			id, _ = data["object_id"].(string)
		}
		return newFunc(id)
	}
}

// RayWorkerException is thrown when a worker process dies unexpectedly.
type RayWorkerException struct {
	rayException
}

// NewRayWorkerException creates a RayWorkerException.
func NewRayWorkerException() *RayWorkerException {
	return &RayWorkerException{
		rayException: rayException{
			code:    ErrorCodeWorkerDied,
			message: "RayWorkerException: Worker has died",
		},
	}
}

// RayIDException is a unified ID-based Ray exception for all actor/object related errors.
// This unified type replaces the previous separate types (RayActorException, RayActorUnavailableException, etc.)
// to reduce code duplication. The specific error type is identified by ErrorCode() and ErrorType().
type RayIDException struct {
	rayIDException
	errorType string // Error type identifier for distinguishing between different ID-based errors
}

// ErrorType returns the type of this ID exception (e.g., "ActorDied", "ObjectLost").
func (e *RayIDException) ErrorType() string {
	return e.errorType
}

// formatIDExceptionMessage formats the error message based on error type and ID.
func formatIDExceptionMessage(errorType string, id string, code int) string {
	switch code {
	case ErrorCodeActorDied:
		return fmt.Sprintf("RayActorException: Actor %s has died", id)
	case ErrorCodeActorUnavailable:
		return fmt.Sprintf("RayActorUnavailableException: Actor %s is unavailable", id)
	case ErrorCodeObjectUnreconstructable:
		return fmt.Sprintf("RayObjectUnreconstructableException: Object %s is unreconstructable", id)
	case ErrorCodeObjectLost:
		return fmt.Sprintf("RayObjectLostException: Object %s is lost", id)
	case ErrorCodeOwnerDied:
		return fmt.Sprintf("RayOwnerDiedException: Owner of object %s has died", id)
	case ErrorCodeObjectDeleted:
		return fmt.Sprintf("RayObjectDeletedException: Object %s has been deleted", id)
	default:
		return fmt.Sprintf("RayIDException: %s - ID: %s", errorType, id)
	}
}

// NewRayIDException creates a unified ID-based Ray exception.
// This is the base constructor for all ID-based exceptions.
func NewRayIDException(code int, errorType string, id string) *RayIDException {
	message := formatIDExceptionMessage(errorType, id, code)
	return &RayIDException{
		rayIDException: rayIDException{
			rayException: rayException{
				code:    code,
				message: message,
			},
			id: id,
		},
		errorType: errorType,
	}
}

// NewRayIDExceptionActorDied creates an ID exception for ErrorCodeActorDied.
func NewRayIDExceptionActorDied(id string) *RayIDException {
	return NewRayIDException(ErrorCodeActorDied, "ActorDied", id)
}

// NewRayIDExceptionActorUnavailable creates an ID exception for ErrorCodeActorUnavailable.
func NewRayIDExceptionActorUnavailable(id string) *RayIDException {
	return NewRayIDException(ErrorCodeActorUnavailable, "ActorUnavailable", id)
}

// NewRayIDExceptionUnreconstructable creates an ID exception for ErrorCodeObjectUnreconstructable.
func NewRayIDExceptionUnreconstructable(id string) *RayIDException {
	return NewRayIDException(ErrorCodeObjectUnreconstructable, "ObjectUnreconstructable", id)
}

// NewRayIDExceptionLost creates an ID exception for ErrorCodeObjectLost.
func NewRayIDExceptionLost(id string) *RayIDException {
	return NewRayIDException(ErrorCodeObjectLost, "ObjectLost", id)
}

// NewRayIDExceptionOwnerDied creates an ID exception for ErrorCodeOwnerDied.
func NewRayIDExceptionOwnerDied(id string) *RayIDException {
	return NewRayIDException(ErrorCodeOwnerDied, "OwnerDied", id)
}

// NewRayIDExceptionDeleted creates an ID exception for ErrorCodeObjectDeleted.
func NewRayIDExceptionDeleted(id string) *RayIDException {
	return NewRayIDException(ErrorCodeObjectDeleted, "ObjectDeleted", id)
}

// Legacy type aliases for backward compatibility (deprecated).
// These are provided for code that still uses the old type names.
// New code should use NewRayIDException* constructors directly.

// RayActorException is deprecated, use NewRayIDExceptionActorDied instead.
// Deprecated: Use RayIDException with ErrorType() == "ActorDied" instead.
type RayActorException = RayIDException

// NewRayActorException creates a RayActorException (deprecated).
// Deprecated: Use NewRayIDExceptionActorDied instead.
func NewRayActorException(actorID string) *RayIDException {
	return NewRayIDExceptionActorDied(actorID)
}

// RayActorUnavailableException is deprecated, use NewRayIDExceptionActorUnavailable instead.
// Deprecated: Use RayIDException with ErrorType() == "ActorUnavailable" instead.
type RayActorUnavailableException = RayIDException

// NewRayActorUnavailableException creates a RayActorUnavailableException (deprecated).
// Deprecated: Use NewRayIDExceptionActorUnavailable instead.
func NewRayActorUnavailableException(actorID string) *RayIDException {
	return NewRayIDExceptionActorUnavailable(actorID)
}

// RayObjectUnreconstructableException is deprecated, use NewRayIDExceptionUnreconstructable instead.
// Deprecated: Use RayIDException with ErrorType() == "ObjectUnreconstructable" instead.
type RayObjectUnreconstructableException = RayIDException

// NewRayObjectUnreconstructableException creates a RayObjectUnreconstructableException (deprecated).
// Deprecated: Use NewRayIDExceptionUnreconstructable instead.
func NewRayObjectUnreconstructableException(objectID string) *RayIDException {
	return NewRayIDExceptionUnreconstructable(objectID)
}

// RayObjectLostException is deprecated, use NewRayIDExceptionLost instead.
// Deprecated: Use RayIDException with ErrorType() == "ObjectLost" instead.
type RayObjectLostException = RayIDException

// NewRayObjectLostException creates a RayObjectLostException (deprecated).
// Deprecated: Use NewRayIDExceptionLost instead.
func NewRayObjectLostException(objectID string) *RayIDException {
	return NewRayIDExceptionLost(objectID)
}

// RayOwnerDiedException is deprecated, use NewRayIDExceptionOwnerDied instead.
// Deprecated: Use RayIDException with ErrorType() == "OwnerDied" instead.
type RayOwnerDiedException = RayIDException

// NewRayOwnerDiedException creates a RayOwnerDiedException (deprecated).
// Deprecated: Use NewRayIDExceptionOwnerDied instead.
func NewRayOwnerDiedException(objectID string) *RayIDException {
	return NewRayIDExceptionOwnerDied(objectID)
}

// RayObjectDeletedException is deprecated, use NewRayIDExceptionDeleted instead.
// Deprecated: Use RayIDException with ErrorType() == "ObjectDeleted" instead.
type RayObjectDeletedException = RayIDException

// NewRayObjectDeletedException creates a RayObjectDeletedException (deprecated).
// Deprecated: Use NewRayIDExceptionDeleted instead.
func NewRayObjectDeletedException(objectID string) *RayIDException {
	return NewRayIDExceptionDeleted(objectID)
}

// RayTaskExecutionException is thrown when an error occurs during task execution.
type RayTaskExecutionException struct {
	rayException
	taskID     string
	cause      error
	stackTrace string
}

// NewRayTaskExecutionException creates a RayTaskExecutionException.
func NewRayTaskExecutionException(taskID string, cause error, stackTrace string) *RayTaskExecutionException {
	return &RayTaskExecutionException{
		rayException: rayException{
			code:    ErrorCodeTaskExecutionException,
			message: fmt.Sprintf("RayTaskExecutionException: Task %s failed: %v", taskID, cause),
		},
		taskID:     taskID,
		cause:      cause,
		stackTrace: stackTrace,
	}
}

// Cause returns the root cause.
func (e *RayTaskExecutionException) Cause() error {
	return e.cause
}

// StackTrace returns the stack trace.
func (e *RayTaskExecutionException) StackTrace() string {
	return e.stackTrace
}

// ExceptionFromBytes deserializes an exception from a byte array.
// This is a utility function to recover exceptions from NativeRayObject.Data.
// Uses a registry pattern to avoid hard-coded exception type switching.
func ExceptionFromBytes(data []byte) (RayException, error) {
	if data == nil {
		return nil, nil
	}

	var rawData map[string]interface{}
	if err := json.Unmarshal(data, &rawData); err != nil {
		// If not JSON format, treat as plain string.
		return &rayException{
			code:    ErrorCodeTaskExecutionException,
			message: string(data),
		}, nil
	}

	errorCode, ok := rawData["error_code"].(float64)
	if !ok {
		return nil, fmt.Errorf("invalid exception data: missing error_code")
	}

	code := int(errorCode)
	// Use registry to create exception, falls back to generic exception if not found
	if factory, exists := exceptionRegistry[code]; exists {
		return factory(rawData), nil
	}

	// Unknown error code, return generic exception
	message, _ := rawData["message"].(string)
	return &rayException{
		code:    code,
		message: message,
	}, nil
}