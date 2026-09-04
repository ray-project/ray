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
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// ExceptionData holds serialized exception data for cross-language compatibility.
// This structure matches Java's RayExceptionSerializer.ExceptionData for interoperability.
type ExceptionData struct {
	Language            string `msgpack:"language"`
	FormattedException  string `msgpack:"formatted_exception"`
	SerializedException []byte `msgpack:"serialized_exception"`
	ErrorCode           int    `msgpack:"error_code"`
	TaskID              string `msgpack:"task_id,omitempty"`
	ActorID             string `msgpack:"actor_id,omitempty"`
	ObjectID            string `msgpack:"object_id,omitempty"`
}

// RayExceptionSerializer handles cross-language exception serialization.
// Similar to Java's RayExceptionSerializer, this provides:
// - Complete stack trace propagation across languages
// - Language identification for proper deserialization
// - Error code preservation for error handling
type RayExceptionSerializer struct{}

// ToBytes serializes a RayException to bytes for cross-language transmission.
// The serialized format includes:
// - Language identifier (GO, JAVA, PYTHON, etc.)
// - Formatted exception string with stack trace
// - Serialized exception object for language-specific recovery
// - Error code for error classification
func (s *RayExceptionSerializer) ToBytes(exception RayException) ([]byte, error) {
	var formattedException string
	var taskID string

	// Extract additional information based on exception type
	switch e := exception.(type) {
	case *RayTaskExecutionException:
		taskID = e.taskID
		// For cross-language compatibility, include both message and stack trace
		formattedException = fmt.Sprintf("%v\n%s", e.cause, e.stackTrace)
	case *RayIDException:
		taskID = e.id
		if e.errorType == "ActorDied" || e.errorType == "ActorUnavailable" {
			formattedException = e.message
		} else {
			formattedException = e.message
		}
	default:
		formattedException = exception.Error()
	}

	data := &ExceptionData{
		Language:           LanguageGo,
		ErrorCode:          exception.ErrorCode(),
		FormattedException: formattedException,
		TaskID:             taskID,
	}

	// Serialize the exception object itself
	serialized, err := msgpack.Marshal(exception)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize exception: %w", err)
	}
	data.SerializedException = serialized

	// Marshal to final bytes
	result, err := msgpack.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal exception data: %w", err)
	}

	return result, nil
}

// FromBytes deserializes a RayException from bytes.
// Supports both Go exceptions and cross-language exceptions from Java/Python.
func (s *RayExceptionSerializer) FromBytes(data []byte) (RayException, error) {
	if data == nil {
		return nil, nil
	}

	var exceptionData ExceptionData
	if err := msgpack.Unmarshal(data, &exceptionData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal exception data: %w", err)
	}

	// Handle based on source language
	if exceptionData.Language == LanguageGo {
		// Go exception, deserialize SerializedException field using msgpack
		var rawData map[string]interface{}
		if err := msgpack.Unmarshal(exceptionData.SerializedException, &rawData); err != nil {
			// Fallback: create exception from formatted string
			return &rayException{
				code:    exceptionData.ErrorCode,
				message: exceptionData.FormattedException,
			}, nil
		}

		// Extract error code and use registry to create exception
		errorCodeFloat, ok := rawData["error_code"].(float64)
		if !ok {
			// Fallback: create exception from formatted string
			return &rayException{
				code:    exceptionData.ErrorCode,
				message: exceptionData.FormattedException,
			}, nil
		}

		code := int(errorCodeFloat)
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

	// Cross-language exception from Java/Python
	return &CrossLanguageException{
		SourceLanguage: exceptionData.Language,
		Message: fmt.Sprintf("An exception raised from %s:\n%s",
			exceptionData.Language, exceptionData.FormattedException),
		ErrorCodeValue: exceptionData.ErrorCode,
		TaskID:         exceptionData.TaskID,
		ActorID:        exceptionData.ActorID,
		ObjectID:       exceptionData.ObjectID,
	}, nil
}

// CrossLanguageException represents an exception from another language runtime.
// This enables proper error propagation across language boundaries.
type CrossLanguageException struct {
	SourceLanguage string
	Message        string
	ErrorCodeValue int
	TaskID         string
	ActorID        string
	ObjectID       string
}

// Error implements the error interface.
func (e *CrossLanguageException) Error() string {
	return e.Message
}

// ErrorCode returns the error code from the source language.
func (e *CrossLanguageException) ErrorCode() int {
	return e.ErrorCodeValue
}

// ToBytes serializes the cross-language exception.
func (e *CrossLanguageException) ToBytes() []byte {
	data := map[string]interface{}{
		"source_language": e.SourceLanguage,
		"message":         e.Message,
		"error_code":      e.ErrorCodeValue,
	}
	if e.TaskID != "" {
		data["task_id"] = e.TaskID
	}
	if e.ActorID != "" {
		data["actor_id"] = e.ActorID
	}
	if e.ObjectID != "" {
		data["object_id"] = e.ObjectID
	}

	result, _ := msgpack.Marshal(data)
	return result
}
