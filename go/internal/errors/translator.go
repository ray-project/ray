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
// This package implements the ErrorTranslator interface defined in pkg/errors.
package errors

import (
	"errors"
	"regexp"
	"strings"
	"time"

	public "github.com/ray-project/ray/go/pkg/errors"
)

// Precompiled regular expressions for error message parsing.
// This avoids recompiling on every conversion call.
var (
	invalidArgRegex    = regexp.MustCompile(`invalid argument ["'](\w+)["']: (.+)$`)
	invalidParamRegex  = regexp.MustCompile(`invalid (\w+) (?:number|format|value): (.+)$`)
	cannotBeEmptyRegex = regexp.MustCompile(`["'](\w+)["'] cannot be (.+)$`)

	initErrorRegex   = regexp.MustCompile(`failed to initialize (\w+): (.+)$`)
	initOfErrorRegex = regexp.MustCompile(`initialization of (\w+) failed: (.+)$`)
	cannotInitRegex  = regexp.MustCompile(`cannot initialize (\w+): (.+)$`)

	runtimeErrorRegex    = regexp.MustCompile(`runtime error during (\w+): (.+)$`)
	operationFailedRegex = regexp.MustCompile(`operation ['"](\w+)['"] failed: (.+)$`)
	failedToRegex        = regexp.MustCompile(`failed to (\w+(?: \w+)*): (.+)$`)

	serializationOfTypeRegex = regexp.MustCompile(`(?:serialization|deserialization) of (\w+)`)
)

// InternalErrorTranslator implements `pkg/errors.ErrorTranslator` interface.
// It translates internal RayError types to public API errors.
type InternalErrorTranslator struct{}

// Translate implements `pkg/errors.ErrorTranslator.Translate`.
func (t *InternalErrorTranslator) Translate(err error) error {
	if err == nil {
		return nil
	}

	// Step 1: Check for sentinel errors (direct comparison)
	publicErr := convertSentinelError(err)
	if publicErr != nil {
		return publicErr
	}

	// Step 2: Check if it's a RayError (structured errors)
	var rayErr RayError
	if IsRayError(err) {
		rayErr = err.(RayError)
		return convertRayError(rayErr)
	}

	// Step 3: Check error chain for RayError
	var found RayError
	for cursor := err; cursor != nil; cursor = errors.Unwrap(cursor) {
		if r, ok := cursor.(RayError); ok {
			found = r
			break
		}
	}
	if found != nil {
		return convertRayError(found)
	}

	// Step 4: Unknown error type, wrap as SystemError for graceful degradation
	return public.WrapSystemError(err, "unknown", "unexpected error occurred")
}

// isInternalSentinel checks if an error is one of the known internal sentinel errors.
func isInternalSentinel(err error) bool {
	sentinels := []error{
		// Common errors
		ErrInvalidArgument,
		ErrNotInitialized,
		ErrAlreadyRunning,
		ErrShutdown,
		ErrTimeout,
		ErrConnectionFailed,
		ErrInternalError,

		// Initialization errors
		ErrInvalidWorkerType,
		ErrInvalidIPAddress,
		ErrInvalidPort,
		ErrEmptySocketPath,
		ErrEmptyGcsAddress,
		ErrEmptyLogDir,
		ErrRuntimeFactoryNotSet,

		// Worker errors
		ErrUnknownWorkerType,
		ErrInvalidHandle,

		// Runtime state errors
		ErrRuntimeAlreadyInitialized,
		ErrRuntimeNotInitialized,

		// Plugin errors
		ErrPluginNotFound,
		ErrPluginLoadFailed,
		ErrSymbolNotFound,
		ErrInvalidSymbolType,
		ErrPluginPathInvalid,
		ErrPluginTooLarge,
		ErrPluginExtInvalid,
		ErrPluginPathTraversal,
		ErrPluginPathNotAllowed,
		ErrPluginChecksumMismatch,

		// Serialization errors
		ErrSerializationFailed,
		ErrDeserializationFailed,
		ErrInvalidMetadata,

		// Object store errors
		ErrObjectNotFound,
		ErrObjectStoreNotInitialized,
		ErrPutFailed,
		ErrGetFailed,
	}

	for _, sentinel := range sentinels {
		if errors.Is(err, sentinel) {
			return true
		}
	}

	return false
}

// convertSentinelError handles direct conversion of sentinel errors.
func convertSentinelError(err error) error {
	// Common error types
	switch {
	case errors.Is(err, ErrInvalidArgument):
		return public.NewValidationError("unknown", "invalid argument provided", nil)

	case errors.Is(err, ErrNotInitialized):
		return public.NewRuntimeError("initialize", "not_initialized")

	case errors.Is(err, ErrAlreadyRunning):
		return public.NewRuntimeError("start", "already_running")

	case errors.Is(err, ErrShutdown):
		return public.NewRuntimeError("operate", "already_shutdown")

	case errors.Is(err, ErrTimeout):
		// Timeout errors are retryable by default in public.NewNetworkError
		return public.NewNetworkError("timeout", "", 0)

	case errors.Is(err, ErrConnectionFailed):
		// Connection failed errors are retryable by default
		return public.NewNetworkError("connect", "", 0)

	case errors.Is(err, ErrInternalError):
		return public.NewSystemError("internal", "an internal error occurred")
	}

	// Initialization related errors
	switch {
	case errors.Is(err, ErrInvalidWorkerType):
		return public.NewInitializationError("WorkerFactory", "invalid worker type specified")

	case errors.Is(err, ErrInvalidIPAddress):
		return public.NewValidationError("ip_address", "IP address format is invalid", nil)

	case errors.Is(err, ErrInvalidPort):
		return public.NewValidationError("port", "port number is invalid", nil)

	case errors.Is(err, ErrEmptySocketPath):
		return public.NewValidationError("socket_path", "socket path cannot be empty", "")

	case errors.Is(err, ErrEmptyGcsAddress):
		return public.NewInitializationError("GCSClient", "GCS address is required")

	case errors.Is(err, ErrEmptyLogDir):
		return public.NewInitializationError("Logger", "log directory is required")

	case errors.Is(err, ErrRuntimeFactoryNotSet):
		return public.NewInitializationError("RuntimeFactory", "runtime factory is not set")
	}

	// Worker related errors
	switch {
	case errors.Is(err, ErrUnknownWorkerType):
		return public.NewInitializationError("WorkerManager", "unknown worker type requested")

	case errors.Is(err, ErrInvalidHandle):
		return public.NewValidationError("handle", "handle is invalid", nil)
	}

	// Runtime state errors
	switch {
	case errors.Is(err, ErrRuntimeAlreadyInitialized):
		return public.NewRuntimeError("initialize", "runtime_already_initialized")

	case errors.Is(err, ErrRuntimeNotInitialized):
		return public.NewRuntimeError("operate", "runtime_not_initialized")
	}

	// Plugin related errors (hide details from public API)
	switch {
	case errors.Is(err, ErrPluginNotFound),
		errors.Is(err, ErrPluginLoadFailed),
		errors.Is(err, ErrSymbolNotFound),
		errors.Is(err, ErrInvalidSymbolType),
		errors.Is(err, ErrPluginPathInvalid),
		errors.Is(err, ErrPluginTooLarge),
		errors.Is(err, ErrPluginExtInvalid),
		errors.Is(err, ErrPluginPathTraversal),
		errors.Is(err, ErrPluginPathNotAllowed),
		errors.Is(err, ErrPluginChecksumMismatch):
		// All plugin errors uniformly converted to SystemError, hiding sensitive details
		return public.NewSystemError("plugin_system", "plugin operation failed")
	}

	// Serialization related errors
	switch {
	case errors.Is(err, ErrSerializationFailed):
		return public.NewSerializationError("unknown")

	case errors.Is(err, ErrDeserializationFailed):
		return public.NewDeserializationError("unknown")

	case errors.Is(err, ErrInvalidMetadata):
		return public.NewDeserializationError("invalid metadata")
	}

	// Object store related errors
	switch {
	case errors.Is(err, ErrObjectNotFound):
		// ObjectNotFound may mean user-requested object doesn't exist, convert to friendlier error
		return public.NewRuntimeError("get_object", "object_not_found")

	case errors.Is(err, ErrObjectStoreNotInitialized):
		return public.NewInitializationError("ObjectStore", "object store is not initialized")

	case errors.Is(err, ErrPutFailed):
		return public.NewRuntimeError("put_object", "put_operation_failed")

	case errors.Is(err, ErrGetFailed):
		return public.NewRuntimeError("get_object", "get_operation_failed")
	}

	// Unmatched sentinel error
	return nil
}

// convertRayError handles structured RayError conversion.
func convertRayError(rayErr RayError) error {
	// Convert based on error category
	switch rayErr.Category() {
	case CategoryArgument:
		return extractValidationError(rayErr)

	case CategoryInitialization:
		return extractInitializationError(rayErr)

	case CategoryRuntime:
		return extractRuntimeError(rayErr)

	case CategoryNetwork:
		return extractNetworkError(rayErr)

	case CategorySerialization:
		return extractSerializationError(rayErr)

	case CategorySystem:
		return extractSystemError(rayErr)

	default:
		// Unknown category, downgrade to SystemError
		return public.WrapSystemError(rayErr, "unknown_category",
			"uncategorized error: "+rayErr.Code())
	}
}

// extractValidationError extracts ValidationError from internal RayError.
func extractValidationError(rayErr RayError) *public.ValidationError {
	// Strategy 1: Try to get structured information from Details()
	details := rayErr.Details()
	if details != nil {
		param, _ := details["parameter"].(string)
		reason, _ := details["reason"].(string)
		value := details["value"]

		if param != "" && reason != "" {
			return public.NewValidationErrorWithDetails(details, rayErr, param, reason, value)
		}
	}

	// Strategy 2: Parse parameter name and reason from error message
	msg := rayErr.Error()
	param, reason := parseInvalidArgumentMessage(msg)

	if param != "" && reason != "" {
		return public.NewValidationErrorWithDetails(make(map[string]interface{}), rayErr, param, reason, nil)
	}

	// Strategy 3: Completely unable to parse, use default values
	return public.NewValidationError("unknown", msg, nil)
}

// parseInvalidArgumentMessage parses parameter name and reason from error message.
func parseInvalidArgumentMessage(msg string) (param, reason string) {
	// Try precompiled regex patterns first
	if matches := invalidArgRegex.FindStringSubmatch(msg); len(matches) >= 3 {
		return matches[1], matches[2]
	}

	if matches := invalidParamRegex.FindStringSubmatch(msg); len(matches) >= 3 {
		return matches[1], matches[2]
	}

	if matches := cannotBeEmptyRegex.FindStringSubmatch(msg); len(matches) >= 3 {
		return matches[1], matches[2]
	}

	return "", ""
}

// extractInitializationError extracts InitializationError from internal RayError.
func extractInitializationError(rayErr RayError) *public.InitializationError {
	// Strategy 1: Try to get structured information from Details()
	details := rayErr.Details()
	if details != nil {
		component, _ := details["component"].(string)
		reason, _ := details["reason"].(string)
		config := details["config"]

		if component != "" && reason != "" {
			var configMap map[string]interface{}
			if cm, ok := config.(map[string]interface{}); ok {
				configMap = cm
			} else {
				configMap = make(map[string]interface{})
			}

			return public.NewInitializationErrorWithDetails(details, rayErr, component, reason, configMap)
		}
	}

	// Strategy 2: Parse component name and reason from error message
	msg := rayErr.Error()
	component, reason := parseInitializationMessage(msg)

	if component != "" && reason != "" {
		return public.NewInitializationErrorWithDetails(make(map[string]interface{}), rayErr, component, reason, make(map[string]interface{}))
	}

	// Strategy 3: Use default values
	return public.NewInitializationError("unknown_component", msg)
}

// parseInitializationMessage parses component name and reason from error message.
func parseInitializationMessage(msg string) (component, reason string) {
	// Try precompiled regex patterns first
	if matches := initErrorRegex.FindStringSubmatch(msg); len(matches) >= 3 {
		return matches[1], matches[2]
	}

	if matches := initOfErrorRegex.FindStringSubmatch(msg); len(matches) >= 3 {
		return matches[1], matches[2]
	}

	if matches := cannotInitRegex.FindStringSubmatch(msg); len(matches) >= 3 {
		return matches[1], matches[2]
	}

	return "", ""
}

// extractRuntimeError extracts RuntimeError from internal RayError.
func extractRuntimeError(rayErr RayError) *public.RuntimeError {
	// Strategy 1: Try to get structured information from Details()
	details := rayErr.Details()
	if details != nil {
		operation, _ := details["operation"].(string)
		state, _ := details["state"].(string)

		if operation != "" {
			return public.NewRuntimeErrorWithDetails(details, rayErr, operation, state)
		}
	}

	// Strategy 2: Parse operation name and state from error message
	msg := rayErr.Error()
	operation, state := parseRuntimeErrorMessage(msg)

	if operation != "" {
		return public.NewRuntimeErrorWithDetails(make(map[string]interface{}), rayErr, operation, state)
	}

	// Strategy 3: Use default values
	return public.NewRuntimeError("unknown_operation", "unknown_state")
}

// parseRuntimeErrorMessage parses operation name and state from error message.
func parseRuntimeErrorMessage(msg string) (operation, state string) {
	// Try precompiled regex patterns first
	if matches := runtimeErrorRegex.FindStringSubmatch(msg); len(matches) >= 3 {
		return matches[1], matches[2]
	}

	if matches := operationFailedRegex.FindStringSubmatch(msg); len(matches) >= 3 {
		return matches[1], matches[2]
	}

	if matches := failedToRegex.FindStringSubmatch(msg); len(matches) >= 3 {
		return matches[1], matches[2]
	}

	return "", ""
}

// extractNetworkError extracts NetworkError from internal RayError.
func extractNetworkError(rayErr RayError) *public.NetworkError {
	// Strategy 1: Try to get structured information from Details()
	details := rayErr.Details()
	if details != nil {
		operation, _ := details["operation"].(string)
		endpoint, _ := details["endpoint"].(string)
		timeout, _ := details["timeout"].(time.Duration)
		retryable, _ := details["retryable"].(bool)

		return public.NewNetworkErrorWithDetails(details, rayErr, operation, endpoint, timeout, retryable || rayErr.IsRetryable())
	}

	// Strategy 2: Infer from error message
	msg := rayErr.Error()
	operation := "network_operation"
	retryable := rayErr.IsRetryable()

	// Detect timeout
	if strings.Contains(msg, "timed out") || strings.Contains(msg, "timeout") {
		operation = "timeout"
		retryable = true
	}

	// Detect connection failure
	if strings.Contains(msg, "connection") {
		operation = "connect"
		retryable = true
	}

	return public.NewNetworkErrorWithDetails(make(map[string]interface{}), rayErr, operation, "", 0, retryable)
}

// extractSerializationError extracts SerializationError from internal RayError.
func extractSerializationError(rayErr RayError) *public.SerializationError {
	// Strategy 1: Try to get structured information from Details()
	details := rayErr.Details()
	if details != nil {
		dataType, _ := details["data_type"].(string)
		operation, _ := details["operation"].(string)
		reason, _ := details["reason"].(string)

		if dataType != "" {
			op := "serialize"
			if operation != "" {
				op = operation
			}

			return public.NewSerializationErrorWithDetails(details, rayErr, dataType, op, reason)
		}
	}

	// Strategy 2: Infer from error message
	msg := rayErr.Error()
	dataType := "unknown_data"
	operation := "serialize"
	reason := msg

	// Detect deserialization
	if strings.Contains(msg, "deserialization") || strings.Contains(msg, "deserialize") {
		operation = "deserialize"
	}

	// Try to extract data type using precompiled regex
	if match := serializationOfTypeRegex.FindStringSubmatch(msg); len(match) > 1 {
		dataType = match[1]
	}

	return public.NewSerializationErrorWithDetails(make(map[string]interface{}), rayErr, dataType, operation, reason)
}

// extractSystemError extracts SystemError from internal RayError.
func extractSystemError(rayErr RayError) *public.SystemError {
	// Strategy 1: Try to get structured information from Details()
	details := rayErr.Details()
	if details != nil {
		component, _ := details["component"].(string)
		reason, _ := details["reason"].(string)
		suggestion, _ := details["suggestion"].(string)

		if component != "" && reason != "" {
			return public.NewSystemErrorWithDetails(details, rayErr, component, reason, suggestion)
		}
	}

	// Strategy 2: Use RayError's Code and Message
	return public.NewSystemError(strings.ToLower(rayErr.Code()), rayErr.Error())
}

// init registers the default translator
func init() {
	public.DefaultTranslator = &InternalErrorTranslator{}
}
