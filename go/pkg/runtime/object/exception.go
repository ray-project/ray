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
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/ray-project/ray/go/internal/errors"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/encoding/protowire"
)

// exceptionFactory is a function type for creating exceptions from raw data.
type exceptionFactory func(data map[string]interface{}) RayException

// exceptionRegistry maps error codes to their corresponding exception factories.
// Uses unified factory functions to reduce boilerplate code for ID-based exceptions.
var exceptionRegistry = map[int]exceptionFactory{
	// Simple exceptions with no extra fields
	ErrorCodeWorkerDied: simpleExceptionFactory(NewRayWorkerException),

	// ID-based exceptions - all use the same unified RayIDException type
	ErrorCodeActorDied:               idExceptionFactory(NewRayIDExceptionActorDied),
	ErrorCodeActorUnavailable:        idExceptionFactory(NewRayIDExceptionActorUnavailable),
	ErrorCodeObjectUnreconstructable: idExceptionFactory(NewRayIDExceptionUnreconstructable),
	ErrorCodeObjectLost:              idExceptionFactory(NewRayIDExceptionLost),
	ErrorCodeOwnerDied:               idExceptionFactory(NewRayIDExceptionOwnerDied),
	ErrorCodeObjectDeleted:           idExceptionFactory(NewRayIDExceptionDeleted),

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

// errorTypeNames mirrors the C++ rpc::ErrorType enum (src/ray/protobuf/common.proto).
// An object whose metadata is the decimal string of one of these numbers is an error object
// (see C++ RayObject::IsException in src/ray/common/ray_object.cc). Number 2 is intentionally
// absent because no rpc::ErrorType value uses it.
var errorTypeNames = map[int]string{
	0:  "WORKER_DIED",
	1:  "ACTOR_DIED",
	3:  "TASK_EXECUTION_EXCEPTION",
	4:  "OBJECT_IN_PLASMA",
	5:  "TASK_CANCELLED",
	6:  "ACTOR_CREATION_FAILED",
	7:  "RUNTIME_ENV_SETUP_FAILED",
	8:  "OBJECT_LOST",
	9:  "OWNER_DIED",
	10: "OBJECT_DELETED",
	11: "DEPENDENCY_RESOLUTION_FAILED",
	12: "OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED",
	13: "OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED",
	14: "OBJECT_FETCH_TIMED_OUT",
	15: "LOCAL_RAYLET_DIED",
	16: "TASK_PLACEMENT_GROUP_REMOVED",
	17: "ACTOR_PLACEMENT_GROUP_REMOVED",
	18: "TASK_UNSCHEDULABLE_ERROR",
	19: "ACTOR_UNSCHEDULABLE_ERROR",
	20: "OUT_OF_DISK_ERROR",
	21: "OBJECT_FREED",
	22: "OUT_OF_MEMORY",
	23: "NODE_DIED",
	24: "END_OF_STREAMING_GENERATOR",
	25: "ACTOR_UNAVAILABLE",
	26: "GENERATOR_TASK_FAILED_FOR_OBJECT_RECONSTRUCTION",
	27: "OBJECT_UNRECONSTRUCTABLE_PUT",
	28: "OBJECT_UNRECONSTRUCTABLE_RETRIES_DISABLED",
	29: "OBJECT_UNRECONSTRUCTABLE_BORROWED",
	30: "OBJECT_UNRECONSTRUCTABLE_REF_NOT_FOUND",
	31: "OBJECT_UNRECONSTRUCTABLE_TASK_CANCELLED",
	32: "OBJECT_UNRECONSTRUCTABLE_LINEAGE_DISABLED",
	33: "WORKER_STARTUP_FAILED",
	34: "STREAMING_GENERATOR_REPLAY_INCONSISTENT",
}

// errorTypeName returns the readable name for a C++ error type number, falling back to a
// generated name for unknown numbers.
func errorTypeName(errorType int) string {
	if name, ok := errorTypeNames[errorType]; ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN_%d", errorType)
}

// ParseErrorType parses the object metadata as a Ray error type, following the C++
// RayObject::IsException convention (src/ray/common/ray_object.cc): a metadata of at most 2 bytes
// whose content is the canonical decimal string of an rpc::ErrorType number. Metadata longer than
// 2 bytes (e.g. "PYTHON") is never an error. It returns the parsed error type number and whether
// the metadata encodes one.
func ParseErrorType(metadata []byte) (int, bool) {
	// For performance, assume metadata of >2 chars (e.g. "PYTHON") is not an error.
	if len(metadata) == 0 || len(metadata) > 2 {
		return 0, false
	}
	// Parse the decimal value directly from the bytes to avoid a []byte->string
	// conversion (and allocation) on the hot path, which runs on every Get.
	errorType := 0
	for _, b := range metadata {
		if b < '0' || b > '9' {
			return 0, false
		}
		errorType = errorType*10 + int(b-'0')
	}
	// C++ compares the metadata string with std::to_string(error_type_number), so only the
	// canonical decimal representation without leading zeros matches ("03" != "3").
	if len(metadata) == 2 && metadata[0] == '0' {
		return 0, false
	}
	// Any canonical 1-2 digit decimal is treated as an error type. This keeps the check
	// forward-compatible with C++ ErrorType enum additions even when errorTypeNames has not
	// been updated; errorTypeName falls back to a generated name for unmapped numbers.
	return errorType, true
}

// goWorkerErrorMetadata is the metadata value the Go worker attaches to task execution error
// objects (see convertGoResultToC in go/internal/runtime/cgo/task_executor.go). Its data payload
// is a JSON object serialized from go/internal/errors.SerializedRayError.
const goWorkerErrorMetadata = `{"type":"error"}`

// Precomputed byte forms of the metadata values compared in ErrorObjectFromNative. Comparing
// bytes avoids a []byte->string conversion (and allocation) on every Get.
var goWorkerErrorMetadataBytes = []byte(goWorkerErrorMetadata)
var metadataTypeTaskExecutionExceptionBytes = []byte(MetadataTypeTaskExecutionException)

// taskExceptionFromGoWorkerError builds a readable task execution exception from a Go worker
// error object. The worker serializes the error payload via go/internal/errors.SerializedRayError
// (see convertGoResultToC in go/internal/runtime/cgo/task_executor.go), so the same type is reused
// here to avoid maintaining a second copy of the wire format. When the JSON payload cannot be
// parsed it still returns a readable exception carrying the raw data, so the driver never sees a
// silent/generic deserialization failure.
func taskExceptionFromGoWorkerError(data []byte) RayException {
	var payload errors.SerializedRayError
	message := ""
	stackTrace := ""
	if err := json.Unmarshal(data, &payload); err == nil {
		message = payload.ErrorMessage
		if message == "" {
			message = payload.CauseMessage
		}
		stackTrace = payload.StackTrace
	} else {
		message = string(data)
	}
	if message == "" {
		message = "task execution failed"
	}
	return NewRayTaskExecutionException("", fmt.Errorf("%s", message), stackTrace)
}

// errorTypeFactories maps the C++ rpc::ErrorType numbers (used as error-object metadata) to
// exception factories for the error types that have a dedicated Go exception class.
var errorTypeFactories = map[int]func(*NativeRayObject) RayException{
	RpcErrorTypeWorkerDied: func(*NativeRayObject) RayException {
		return NewRayWorkerException()
	},
	RpcErrorTypeActorDied: func(nativeObj *NativeRayObject) RayException {
		if message := extractErrorInfoMessage(nativeObj.Data); message != "" {
			// Both branches report the same ErrorCode(): keep the Go ErrorCodeActorDied constant
			// (2), not the raw rpc::ErrorType number (1), so callers switching on ErrorCode() see
			// a single value for ACTOR_DIED.
			exc := NewRayIDExceptionActorDied("")
			exc.message = message
			return exc
		}
		return NewRayIDExceptionActorDied("")
	},
	RpcErrorTypeTaskExecutionException: func(nativeObj *NativeRayObject) RayException {
		message := extractErrorInfoMessage(nativeObj.Data)
		if message == "" {
			message = "task execution failed"
		}
		return NewRayTaskExecutionException("", fmt.Errorf("%s", message), "")
	},
}

// ErrorObjectFromNative converts a NativeRayObject whose metadata encodes a Ray error type into a
// readable RayException. It returns (nil, false) when the object is not an error object.
//
// Note on ErrorCode() semantics: exceptions produced for error types with a dedicated Go exception
// class (WORKER_DIED, ACTOR_DIED, TASK_EXECUTION_EXCEPTION) return the Go ErrorCode* constant;
// all other error types (e.g. WORKER_STARTUP_FAILED) fall back to newRayErrorTypeException and
// return the raw C++ rpc::ErrorType number as ErrorCode(). Callers that switch on ErrorCode()
// should treat both as identifiers of the error kind, not as values from a single numbering scheme.
//
// Two metadata conventions are recognized:
//   - the C++ error-object convention: metadata is the decimal string of an rpc::ErrorType number
//     (e.g. "33" for WORKER_STARTUP_FAILED), following RayObject::IsException;
//   - the Go local-mode convention: metadata is MetadataTypeTaskExecutionException and Data holds
//     msgpack-encoded ExceptionData produced by RayExceptionSerializer.
func ErrorObjectFromNative(nativeObj *NativeRayObject) (RayException, bool) {
	if nativeObj == nil {
		return nil, false
	}
	// The Go worker encodes task execution errors with a {"type":"error"} metadata and a JSON
	// error payload (see convertGoResultToC in go/internal/runtime/cgo/task_executor.go).
	if bytes.Equal(nativeObj.Metadata, goWorkerErrorMetadataBytes) {
		return taskExceptionFromGoWorkerError(nativeObj.Data), true
	}
	// Go local mode encodes task execution exceptions with a string metadata and msgpack data.
	if bytes.Equal(nativeObj.Metadata, metadataTypeTaskExecutionExceptionBytes) {
		exc, err := (&RayExceptionSerializer{}).FromBytes(nativeObj.Data)
		if err == nil && exc != nil {
			return exc, true
		}
	}
	errorType, ok := ParseErrorType(nativeObj.Metadata)
	if !ok {
		return nil, false
	}
	if factory, exists := errorTypeFactories[errorType]; exists {
		return factory(nativeObj), true
	}
	return newRayErrorTypeException(errorType, errorTypeName(errorType), extractErrorInfoMessage(nativeObj.Data)), true
}

// newRayErrorTypeException builds a readable exception for an error type that has no dedicated Go
// exception class. The message includes the error type name and, when available, the error message
// carried by the object.
func newRayErrorTypeException(errorType int, name, message string) RayException {
	if message == "" {
		return &rayException{
			code:    errorType,
			message: fmt.Sprintf("RayException[%s]: object is a Ray error object (error_type=%d)", name, errorType),
		}
	}
	return &rayException{
		code:    errorType,
		message: fmt.Sprintf("RayException[%s]: %s", name, message),
	}
}

// extractErrorInfoMessage extracts the human-readable error message from a C++ error object's
// data. C++ RayObject error objects carry their rpc::RayErrorInfo serialized as a msgpack-wrapped
// protobuf (see MakeSerializedErrorBuffer in src/ray/common/ray_object.cc):
//
//	[msgpack int: wrapped size] [msgpack bin: serialized rpc::RayErrorInfo protobuf]
//
// rpc::RayErrorInfo.error_message is field 5 (string). Returns "" when the data cannot be parsed.
func extractErrorInfoMessage(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	if _, err := dec.DecodeInt64(); err != nil {
		return ""
	}
	var serialized []byte
	if err := dec.Decode(&serialized); err != nil {
		return ""
	}
	return extractProtoStringField(serialized, 5)
}

// extractProtoStringField scans a protobuf wire-format message for the given field number and
// returns its value when the field is a length-delimited string. It is a best-effort parser used
// only to surface readable error messages from a serialized rpc::RayErrorInfo. It uses
// google.golang.org/protobuf/encoding/protowire for bounds-checked wire-format decoding.
func extractProtoStringField(data []byte, fieldNum int) string {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return ""
		}
		data = data[n:]
		if int(num) == fieldNum && typ == protowire.BytesType {
			value, m := protowire.ConsumeBytes(data)
			if m < 0 {
				return ""
			}
			return string(value)
		}
		if n := protowire.ConsumeFieldValue(num, typ, data); n < 0 {
			return ""
		} else {
			data = data[n:]
		}
	}
	return ""
}
