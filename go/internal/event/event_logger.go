// Copyright 2026 The Ray Authors.
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

package event

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

// Global logger used to record unexpected situations such as parse failures.
var globalLogger = logr.Discard()

// SetGlobalLogger sets the global logger.
func SetGlobalLogger(logger logr.Logger) {
	globalLogger = logger
}

// getEventID generates a unique 36-character hexadecimal event ID.
func getEventID() string {
	const hexDigits = "0123456789abcdef"
	result := make([]byte, 36)

	seed := time.Now().UnixNano()
	for i := range result {
		seed = (seed*1103515245 + 12345) & 0x7fffffff
		result[i] = hexDigits[int(seed)%len(hexDigits)]
	}
	return string(result)
}

// EventLoggerAdapter is the event logger adapter.
type EventLoggerAdapter struct {
	logger         logr.Logger            // Underlying logr logger
	source         proto.Event_SourceType // Event source type
	sourceHostname string                 // Source hostname (auto-detected)
	sourcePID      int32                  // Source process ID (auto-detected)
	lock           sync.Mutex             // Protects globalContext
	globalContext  map[string]string      // Global metadata context
}

// NewEventLoggerAdapter creates a new event logger adapter.
func NewEventLoggerAdapter(source proto.Event_SourceType, logger logr.Logger) *EventLoggerAdapter {
	hostname, _ := os.Hostname()
	return &EventLoggerAdapter{
		logger:         logger,
		source:         source,
		sourceHostname: hostname,
		sourcePID:      int32(os.Getpid()),
		globalContext:  make(map[string]string),
	}
}

// SetGlobalContext sets the global context; it is attached to every event
// emitted afterwards.
func (e *EventLoggerAdapter) SetGlobalContext(globalContext map[string]string) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if globalContext == nil {
		e.globalContext = make(map[string]string)
	} else {
		// Deep copy so external mutations cannot affect the internal state.
		e.globalContext = make(map[string]string)
		for k, v := range globalContext {
			e.globalContext[k] = v
		}
	}
}

// Trace emits an event with TRACE severity.
func (e *EventLoggerAdapter) Trace(message string, kwargs ...map[string]string) {
	e.emit(proto.Event_TRACE, message, kwargs...)
}

// Debug emits an event with DEBUG severity.
func (e *EventLoggerAdapter) Debug(message string, kwargs ...map[string]string) {
	e.emit(proto.Event_DEBUG, message, kwargs...)
}

// Info emits an event with INFO severity.
func (e *EventLoggerAdapter) Info(message string, kwargs ...map[string]string) {
	e.emit(proto.Event_INFO, message, kwargs...)
}

// Warning emits an event with WARNING severity.
func (e *EventLoggerAdapter) Warning(message string, kwargs ...map[string]string) {
	e.emit(proto.Event_WARNING, message, kwargs...)
}

// Error emits an event with ERROR severity.
func (e *EventLoggerAdapter) Error(message string, kwargs ...map[string]string) {
	e.emit(proto.Event_ERROR, message, kwargs...)
}

// Fatal emits an event with FATAL severity.
func (e *EventLoggerAdapter) Fatal(message string, kwargs ...map[string]string) {
	e.emit(proto.Event_FATAL, message, kwargs...)
}

// emit is the core event emission method.
func (e *EventLoggerAdapter) emit(severity proto.Event_Severity, message string, kwargs ...map[string]string) {
	event := &proto.Event{
		EventId:        getEventID(),
		Timestamp:      time.Now().Unix(),
		Message:        message,
		Severity:       severity,
		Label:          "", // TODO: support event types and schema in the future.
		SourceType:     e.source,
		SourceHostname: e.sourceHostname,
		SourcePid:      e.sourcePID,
		CustomFields:   make(map[string]string),
	}

	// Attach the global context.
	e.lock.Lock()
	for k, v := range e.globalContext {
		if v != "" && k != "" {
			event.CustomFields[k] = v
		}
	}
	e.lock.Unlock()

	// Attach the custom fields of this call.
	if len(kwargs) > 0 && kwargs[0] != nil {
		for k, v := range kwargs[0] {
			if v != "" && k != "" {
				event.CustomFields[k] = v
			}
		}
	}

	// Serialize the protobuf message as JSON.
	// Options:
	// - UseProtoNames=true: keep the field names from the proto definition
	//   (instead of camelCase).
	// - UseEnumNumbers=false: render enum values as strings, not integers.
	// - EmitUnpopulated=true: emit fields even when they hold no value.
	marshalOptions := protojson.MarshalOptions{
		UseProtoNames:   true,
		UseEnumNumbers:  false,
		EmitUnpopulated: true,
	}

	jsonBytes, err := marshalOptions.Marshal(event)
	if err != nil {
		e.logger.Error(err, "Failed to marshal event")
		return
	}

	// Severity order: TRACE < DEBUG < INFO < WARNING < ERROR < FATAL.
	switch severity {
	case proto.Event_TRACE:
		e.logger.V(2).Info(string(jsonBytes))
	case proto.Event_DEBUG:
		e.logger.V(1).Info(string(jsonBytes))
	case proto.Event_INFO:
		e.logger.V(0).Info(string(jsonBytes))
	case proto.Event_WARNING:
		e.logger.V(1).Info(string(jsonBytes), "LEVEL", "WARNING")
	case proto.Event_ERROR:
		e.logger.Error(nil, string(jsonBytes))
	case proto.Event_FATAL:
		e.logger.Error(nil, string(jsonBytes))
	default:
		e.logger.V(0).Info(string(jsonBytes))
	}
}

// buildEventFileLogger builds a file-backed event logger.
func buildEventFileLogger(source proto.Event_SourceType, sinkDir string) (logr.Logger, error) {
	dirPath := filepath.Join(sinkDir, "events")
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return logr.Discard(), fmt.Errorf("failed to create events directory: %w", err)
	}

	// Log file path: sink_dir/events/event_SOURCE.log
	filePath := filepath.Join(dirPath, fmt.Sprintf("event_%s.log", source.String()))

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return logr.Discard(), fmt.Errorf("failed to open log file: %w", err)
	}

	logger := NewFileLogger(file)
	return logger, nil
}

// Global event logger registry. Every (source_name, process) pair shares one
// EventLoggerAdapter instance.
var (
	eventLoggerLock sync.Mutex
	eventLoggerMap  = make(map[string]*EventLoggerAdapter)
)

// GetEventLogger returns the event logger of the current process (singleton).
// Only one logger instance is created for a given (source, process) pair.
func GetEventLogger(source proto.Event_SourceType, sinkDir string) (*EventLoggerAdapter, error) {
	eventLoggerLock.Lock()
	defer eventLoggerLock.Unlock()

	sourceName := source.String()

	if adapter, exists := eventLoggerMap[sourceName]; exists {
		return adapter, nil
	}

	logger, err := buildEventFileLogger(source, sinkDir)
	if err != nil {
		return nil, fmt.Errorf("failed to build event file logger: %w", err)
	}

	adapter := NewEventLoggerAdapter(source, logger)
	eventLoggerMap[sourceName] = adapter

	return adapter, nil
}

// ParseEvent parses an Event object from a JSON string.
func ParseEvent(eventStr string) (*proto.Event, error) {
	var event proto.Event
	err := json.Unmarshal([]byte(eventStr), &event)
	if err != nil {
		globalLogger.Error(err, "Failed to parse event", "event_str", eventStr)
		return nil, err
	}
	return &event, nil
}

// FilterEventByLevel keeps only the events whose severity is below the given
// level when querying or displaying them.
// Severity order (lower number means lower severity):
// TRACE (0) < DEBUG (1) < INFO (2) < WARNING (3) < ERROR (4) < FATAL (5).
func FilterEventByLevel(event *proto.Event, filterEventLevel string) bool {
	eventLevels := map[proto.Event_Severity]int{
		proto.Event_TRACE:   0,
		proto.Event_DEBUG:   1,
		proto.Event_INFO:    2,
		proto.Event_WARNING: 3,
		proto.Event_ERROR:   4,
		proto.Event_FATAL:   5,
	}

	filterEventLevel = strings.ToUpper(filterEventLevel)

	filterSeverityInt, exist := proto.Event_Severity_value[filterEventLevel]
	if !exist {
		return false
	}
	filterSeverity := proto.Event_Severity(filterSeverityInt)

	if eventLevels[event.Severity] < eventLevels[filterSeverity] {
		return true
	}

	return false
}
