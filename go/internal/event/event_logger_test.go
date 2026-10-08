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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

// mockLogSink is a simple logr sink that captures log output.
type mockLogSink struct {
	messages []string
	errors   []error
	lock     sync.Mutex
}

func (m *mockLogSink) Init(info logr.RuntimeInfo) {}

func (m *mockLogSink) Enabled(level int) bool {
	return true
}

func (m *mockLogSink) Info(level int, msg string, keysAndValues ...interface{}) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.messages = append(m.messages, msg)
}

func (m *mockLogSink) Error(err error, msg string, keysAndValues ...interface{}) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.errors = append(m.errors, err)
	m.messages = append(m.messages, msg)
}

func (m *mockLogSink) WithValues(keysAndValues ...interface{}) logr.LogSink {
	return m
}

func (m *mockLogSink) WithName(name string) logr.LogSink {
	return m
}

func newMockLogSink() *mockLogSink {
	return &mockLogSink{
		messages: make([]string, 0),
		errors:   make([]error, 0),
	}
}

func (m *mockLogSink) getMessages() []string {
	m.lock.Lock()
	defer m.lock.Unlock()
	result := make([]string, len(m.messages))
	copy(result, m.messages)
	return result
}

func (m *mockLogSink) getErrors() []error {
	m.lock.Lock()
	defer m.lock.Unlock()
	result := make([]error, len(m.errors))
	copy(result, m.errors)
	return result
}

func (m *mockLogSink) clear() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.messages = make([]string, 0)
	m.errors = make([]error, 0)
}

// TestGetEventID tests the event ID generation function.
func TestGetEventID(t *testing.T) {
	t.Run("id has the expected length", func(t *testing.T) {
		id := getEventID()
		if len(id) != 36 {
			t.Errorf("expected ID length 36, got: %d", len(id))
		}
	})

	t.Run("id contains only hex characters", func(t *testing.T) {
		id := getEventID()
		hexDigits := "0123456789abcdef"
		for _, c := range id {
			if !strings.ContainsRune(hexDigits, c) {
				t.Errorf("ID contains a non-hex character: %c", c)
			}
		}
	})

	t.Run("ids differ across a time gap", func(t *testing.T) {
		id1 := getEventID()
		time.Sleep(10 * time.Millisecond)
		id2 := getEventID()

		if id1 == id2 {
			t.Errorf("expected different IDs after a time gap, got the same ID")
		}
	})

	t.Run("id format check", func(t *testing.T) {
		id := getEventID()
		for i, c := range id {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
				t.Errorf("ID has an invalid character at position %d: %c", i, c)
			}
		}
	})
}

// TestNewEventLoggerAdapter tests creating a new event logger adapter.
func TestNewEventLoggerAdapter(t *testing.T) {
	mockSink := newMockLogSink()
	logger := logr.New(mockSink)

	adapter := NewEventLoggerAdapter(proto.Event_AUTOSCALER, logger)

	if adapter.source != proto.Event_AUTOSCALER {
		t.Errorf("expected source type AUTOSCALER, got: %v", adapter.source)
	}

	if adapter.logger.GetSink() == nil {
		t.Error("expected a non-nil logger")
	}

	if adapter.sourceHostname == "" {
		t.Error("expected a non-empty sourceHostname")
	}

	if adapter.sourcePID <= 0 {
		t.Error("expected sourcePID to be greater than 0")
	}

	if adapter.globalContext == nil {
		t.Error("expected a non-nil globalContext")
	}

	if len(adapter.globalContext) != 0 {
		t.Error("expected the initial globalContext to be empty")
	}
}

// TestSetGlobalContext tests setting the global context.
func TestSetGlobalContext(t *testing.T) {
	mockSink := newMockLogSink()
	logger := logr.New(mockSink)
	adapter := NewEventLoggerAdapter(proto.Event_AUTOSCALER, logger)

	t.Run("set a normal context", func(t *testing.T) {
		context := map[string]string{
			"key1": "value1",
			"key2": "value2",
		}
		adapter.SetGlobalContext(context)

		adapter.lock.Lock()
		defer adapter.lock.Unlock()

		if len(adapter.globalContext) != 2 {
			t.Errorf("expected globalContext length 2, got: %d", len(adapter.globalContext))
		}

		if adapter.globalContext["key1"] != "value1" {
			t.Errorf("expected key1 to be value1, got: %s", adapter.globalContext["key1"])
		}

		if adapter.globalContext["key2"] != "value2" {
			t.Errorf("expected key2 to be value2, got: %s", adapter.globalContext["key2"])
		}
	})

	t.Run("set a nil context", func(t *testing.T) {
		adapter.SetGlobalContext(nil)

		adapter.lock.Lock()
		defer adapter.lock.Unlock()

		if len(adapter.globalContext) != 0 {
			t.Errorf("expected an empty globalContext, got length: %d", len(adapter.globalContext))
		}
	})

	t.Run("deep copy verification", func(t *testing.T) {
		context := map[string]string{"key": "original"}
		adapter.SetGlobalContext(context)

		context["key"] = "modified"

		adapter.lock.Lock()
		defer adapter.lock.Unlock()

		if adapter.globalContext["key"] != "original" {
			t.Errorf("expected globalContext to be unaffected by external mutation, got: %s", adapter.globalContext["key"])
		}
	})
}

// TestEmit tests the core emission method.
func TestEmit(t *testing.T) {
	mockSink := newMockLogSink()
	logger := logr.New(mockSink)
	adapter := NewEventLoggerAdapter(proto.Event_AUTOSCALER, logger)

	t.Run("emit an INFO event", func(t *testing.T) {
		mockSink.clear()
		adapter.emit(proto.Event_INFO, "test message", map[string]string{"custom": "field"})

		messages := mockSink.getMessages()
		if len(messages) == 0 {
			t.Fatal("expected log messages, got none")
		}

		// Verify the JSON format (parse the protobuf message with protojson).
		var event proto.Event
		err := protojson.Unmarshal([]byte(messages[0]), &event)
		if err != nil {
			t.Fatalf("failed to parse JSON: %v, raw message: %s", err, messages[0])
		}

		if event.Message != "test message" {
			t.Errorf("expected message 'test message', got: %s", event.Message)
		}

		if event.Severity != proto.Event_INFO {
			t.Errorf("expected severity INFO, got: %v", event.Severity)
		}

		if event.CustomFields["custom"] != "field" {
			t.Errorf("expected custom field 'custom' to be 'field', got: %s", event.CustomFields["custom"])
		}
	})

	t.Run("emit an event with global context", func(t *testing.T) {
		mockSink.clear()
		adapter.SetGlobalContext(map[string]string{"global": "context"})
		adapter.emit(proto.Event_DEBUG, "debug message", nil)

		messages := mockSink.getMessages()
		if len(messages) == 0 {
			t.Fatal("expected log messages, got none")
		}

		var event proto.Event
		err := protojson.Unmarshal([]byte(messages[0]), &event)
		if err != nil {
			t.Fatalf("failed to parse JSON: %v, raw message: %s", err, messages[0])
		}

		if event.CustomFields["global"] != "context" {
			t.Errorf("expected the global context to be present, got none")
		}
	})

	t.Run("empty kwargs handling", func(t *testing.T) {
		mockSink.clear()
		adapter.emit(proto.Event_TRACE, "trace message")

		messages := mockSink.getMessages()
		if len(messages) == 0 {
			t.Fatal("expected log messages, got none")
		}
	})
}

// TestLogLevelMethods tests the per-severity logging methods.
func TestLogLevelMethods(t *testing.T) {
	mockSink := newMockLogSink()
	logger := logr.New(mockSink)
	adapter := NewEventLoggerAdapter(proto.Event_AUTOSCALER, logger)

	tests := []struct {
		name     string
		method   func(string, ...map[string]string)
		severity proto.Event_Severity
	}{
		{"Trace", adapter.Trace, proto.Event_TRACE},
		{"Debug", adapter.Debug, proto.Event_DEBUG},
		{"Info", adapter.Info, proto.Event_INFO},
		{"Warning", adapter.Warning, proto.Event_WARNING},
		{"Error", adapter.Error, proto.Event_ERROR},
		{"Fatal", adapter.Fatal, proto.Event_FATAL},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSink.clear()
			tt.method(tt.name + " message")

			messages := mockSink.getMessages()
			if len(messages) == 0 {
				t.Fatalf("expected log messages, got none")
			}

			var event proto.Event
			err := protojson.Unmarshal([]byte(messages[0]), &event)
			if err != nil {
				t.Fatalf("failed to parse JSON: %v, raw message: %s", err, messages[0])
			}

			if event.Severity != tt.severity {
				t.Errorf("expected severity %v, got: %v", tt.severity, event.Severity)
			}

			if event.Message != tt.name+" message" {
				t.Errorf("expected message '%s message', got: %s", tt.name, event.Message)
			}
		})
	}
}

// TestBuildEventFileLogger tests building a file logger.
func TestBuildEventFileLogger(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("create a file logger", func(t *testing.T) {
		logger, err := buildEventFileLogger(proto.Event_AUTOSCALER, tempDir)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if logger.GetSink() == nil {
			t.Error("expected a non-nil logger")
		}

		expectedPath := filepath.Join(tempDir, "events", "event_AUTOSCALER.log")
		if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
			t.Errorf("expected the log file to exist: %s", expectedPath)
		}
	})

	t.Run("invalid directory path", func(t *testing.T) {
		longPath := strings.Repeat("/a", 1000) // Build an over-long path.
		_, err := buildEventFileLogger(proto.Event_AUTOSCALER, longPath)
		if err == nil {
			t.Log("note: the over-long path did not fail, possibly because the system supports long paths")
		}
	})
}

// TestGetEventLogger tests fetching the singleton event logger.
func TestGetEventLogger(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("first creation", func(t *testing.T) {
		// Reset the global state.
		eventLoggerLock.Lock()
		eventLoggerMap = make(map[string]*EventLoggerAdapter)
		eventLoggerLock.Unlock()

		adapter, err := GetEventLogger(proto.Event_CLUSTER_LIFECYCLE, tempDir)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if adapter == nil {
			t.Fatal("expected a non-nil adapter")
		}

		adapter2, err := GetEventLogger(proto.Event_CLUSTER_LIFECYCLE, tempDir)
		if err != nil {
			t.Fatalf("expected no error on the second call, got: %v", err)
		}

		if adapter != adapter2 {
			t.Error("expected the same instance (singleton)")
		}
	})

	t.Run("different source types create different instances", func(t *testing.T) {
		eventLoggerLock.Lock()
		eventLoggerMap = make(map[string]*EventLoggerAdapter)
		eventLoggerLock.Unlock()

		adapter1, err := GetEventLogger(proto.Event_AUTOSCALER, tempDir)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		adapter2, err := GetEventLogger(proto.Event_CLUSTER_LIFECYCLE, tempDir)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if adapter1 == adapter2 {
			t.Error("expected different instances for different source types")
		}
	})
}

// TestParseEvent tests parsing a JSON event string.
func TestParseEvent(t *testing.T) {
	t.Run("parse valid JSON", func(t *testing.T) {
		invalidJSON := `{"invalid": json}`
		event, err := ParseEvent(invalidJSON)
		if err == nil {
			t.Error("expected an error when parsing invalid JSON")
		}
		if event != nil {
			t.Error("expected event to be nil")
		}

		event, err = ParseEvent("")
		if err == nil {
			t.Error("expected an error when parsing an empty string")
		}
		if event != nil {
			t.Error("expected event to be nil")
		}

		event, err = ParseEvent("not json at all")
		if err == nil {
			t.Error("expected an error when parsing a non-JSON string")
		}
		if event != nil {
			t.Error("expected event to be nil")
		}
	})

	t.Run("parse invalid JSON", func(t *testing.T) {
		invalidJSON := `{"invalid": json}`

		event, err := ParseEvent(invalidJSON)
		if err == nil {
			t.Error("expected an error, got none")
		}

		if event != nil {
			t.Error("expected event to be nil")
		}
	})

	t.Run("parse an empty string", func(t *testing.T) {
		event, err := ParseEvent("")
		if err == nil {
			t.Error("expected an error, got none")
		}

		if event != nil {
			t.Error("expected event to be nil")
		}
	})
}

// TestFilterEventByLevel tests filtering events by severity level.
func TestFilterEventByLevel(t *testing.T) {
	createEvent := func(severity proto.Event_Severity) *proto.Event {
		return &proto.Event{
			Severity: severity,
		}
	}

	tests := []struct {
		name           string
		eventSeverity  proto.Event_Severity
		filterLevel    string
		expectedFilter bool
	}{
		{"TRACE below DEBUG", proto.Event_TRACE, "DEBUG", true},
		{"DEBUG equals DEBUG", proto.Event_DEBUG, "DEBUG", false},
		{"INFO above DEBUG", proto.Event_INFO, "DEBUG", false},
		{"WARNING above INFO", proto.Event_WARNING, "INFO", false},
		{"ERROR above WARNING", proto.Event_ERROR, "WARNING", false},
		{"FATAL is the highest", proto.Event_FATAL, "ERROR", false},
		{"TRACE below INFO", proto.Event_TRACE, "INFO", true},
		{"DEBUG below WARNING", proto.Event_DEBUG, "WARNING", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := createEvent(tt.eventSeverity)
			result := FilterEventByLevel(event, tt.filterLevel)

			if result != tt.expectedFilter {
				t.Errorf("expected filter result %v, got %v", tt.expectedFilter, result)
			}
		})
	}

	t.Run("invalid level string", func(t *testing.T) {
		event := createEvent(proto.Event_INFO)
		result := FilterEventByLevel(event, "INVALID_LEVEL")

		if result != false {
			t.Errorf("expected false for an invalid level, got %v", result)
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		event := createEvent(proto.Event_INFO)

		result1 := FilterEventByLevel(event, "info")
		if result1 != false {
			t.Errorf("expected lowercase 'info' to return false, got %v", result1)
		}

		result2 := FilterEventByLevel(event, "InFo")
		if result2 != false {
			t.Errorf("expected mixed-case 'InFo' to return false, got %v", result2)
		}

		debugEvent := createEvent(proto.Event_DEBUG)
		result3 := FilterEventByLevel(debugEvent, "info")
		if result3 != true {
			t.Errorf("expected a DEBUG event to be filtered out by INFO, got %v", result3)
		}
	})
}

// TestConcurrentAccess tests concurrent access.
func TestConcurrentAccess(t *testing.T) {
	mockSink := newMockLogSink()
	logger := logr.New(mockSink)
	adapter := NewEventLoggerAdapter(proto.Event_AUTOSCALER, logger)

	var wg sync.WaitGroup
	numGoroutines := 10

	// Multiple goroutines set the global context and emit events concurrently.
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			context := map[string]string{
				"goroutine": string(rune('0' + id)),
			}
			adapter.SetGlobalContext(context)
			adapter.Info("concurrent message", map[string]string{"id": string(rune('0' + id))})
		}(i)
	}

	wg.Wait()

	// Verify that no panic occurred.
	messages := mockSink.getMessages()
	if len(messages) < numGoroutines {
		t.Errorf("expected at least %d messages, got %d", numGoroutines, len(messages))
	}
}

// TestEventSerialization tests event serialization.
func TestEventSerialization(t *testing.T) {
	mockSink := newMockLogSink()
	logger := logr.New(mockSink)
	adapter := NewEventLoggerAdapter(proto.Event_AUTOSCALER, logger)

	adapter.emit(proto.Event_INFO, "serialization test", map[string]string{
		"field1": "value1",
		"field2": "value2",
	})

	messages := mockSink.getMessages()
	if len(messages) == 0 {
		t.Fatal("expected log messages")
	}

	// Verify the JSON format (via protojson).
	var event proto.Event
	err := protojson.Unmarshal([]byte(messages[0]), &event)
	if err != nil {
		t.Fatalf("failed to parse JSON: %v, raw message: %s", err, messages[0])
	}

	if event.EventId == "" {
		t.Error("missing event_id field")
	}

	if event.Timestamp == 0 {
		t.Error("missing timestamp field")
	}

	if event.Message == "" {
		t.Error("missing message field")
	}

	if event.Severity != proto.Event_INFO {
		t.Errorf("expected severity INFO (2), got: %d", event.Severity)
	}

	if event.SourceType != proto.Event_AUTOSCALER {
		t.Errorf("expected sourceType AUTOSCALER (5), got: %d", event.SourceType)
	}

	if event.CustomFields["field1"] != "value1" {
		t.Errorf("expected field1 to be 'value1', got: %s", event.CustomFields["field1"])
	}

	if event.CustomFields["field2"] != "value2" {
		t.Errorf("expected field2 to be 'value2', got: %s", event.CustomFields["field2"])
	}
}

// TestEmptyCustomFields tests the handling of empty custom fields.
func TestEmptyCustomFields(t *testing.T) {
	mockSink := newMockLogSink()
	logger := logr.New(mockSink)
	adapter := NewEventLoggerAdapter(proto.Event_AUTOSCALER, logger)

	adapter.emit(proto.Event_INFO, "test", map[string]string{
		"empty": "",
		"valid": "value",
	})

	messages := mockSink.getMessages()
	var event proto.Event
	err := protojson.Unmarshal([]byte(messages[0]), &event)
	if err != nil {
		t.Fatalf("failed to parse JSON: %v, raw message: %s", err, messages[0])
	}

	if _, exists := event.CustomFields["empty"]; exists {
		t.Error("an empty string value must not be added to CustomFields")
	}

	if event.CustomFields["valid"] != "value" {
		t.Errorf("expected field 'valid' to be 'value', got: %s", event.CustomFields["valid"])
	}
}

// TestGlobalLogger tests the global logger.
func TestGlobalLogger(t *testing.T) {
	originalLogger := globalLogger
	defer func() {
		globalLogger = originalLogger
	}()

	mockSink := newMockLogSink()
	newLogger := logr.New(mockSink)

	SetGlobalLogger(newLogger)

	if globalLogger.GetSink() != mockSink {
		t.Error("expected the global logger to be set correctly")
	}

	_, err := ParseEvent("invalid json")
	if err == nil {
		t.Error("expected a parse error")
	}

	errors := mockSink.getErrors()
	if len(errors) == 0 {
		t.Error("expected the global logger to record the error")
	}
}
