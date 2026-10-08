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
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
)

// TestNewFileLogger tests creating a file-backed logr logger.
func TestNewFileLogger(t *testing.T) {
	t.Run("create with a real file", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "test_logger_*.log")
		if err != nil {
			t.Fatalf("failed to create a temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())

		logger := NewFileLogger(tempFile)
		if logger.GetSink() == nil {
			t.Error("expected a non-nil logger")
		}

		tempFile.Close()
	})
}

// TestFileLogSink_Info tests the Info method.
func TestFileLogSink_Info(t *testing.T) {
	t.Run("write a simple message to a real file", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "test_info_*.log")
		if err != nil {
			t.Fatalf("failed to create a temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())

		sink := &fileLogSink{file: tempFile}

		message := `{"event": "test", "level": "info"}`
		sink.Info(0, message)

		tempFile.Close()

		// Read the file back to verify.
		content, err := os.ReadFile(tempFile.Name())
		if err != nil {
			t.Fatalf("failed to read the file: %v", err)
		}

		expected := message + "\n"
		if string(content) != expected {
			t.Errorf("expected '%s' to be written, got '%s'", expected, string(content))
		}
	})

	t.Run("multiple writes", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "test_multi_*.log")
		if err != nil {
			t.Fatalf("failed to create a temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())

		sink := &fileLogSink{file: tempFile}

		messages := []string{
			`{"event": "first"}`,
			`{"event": "second"}`,
			`{"event": "third"}`,
		}

		for _, msg := range messages {
			sink.Info(0, msg)
		}

		tempFile.Close()

		content, err := os.ReadFile(tempFile.Name())
		if err != nil {
			t.Fatalf("failed to read the file: %v", err)
		}

		lines := strings.Split(strings.TrimSpace(string(content)), "\n")
		if len(lines) != len(messages) {
			t.Errorf("expected %d log lines, got %d", len(messages), len(lines))
		}
	})
}

// TestFileLogSink_Error tests the Error method.
func TestFileLogSink_Error(t *testing.T) {
	t.Run("write an error message", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "test_error_*.log")
		if err != nil {
			t.Fatalf("failed to create a temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())

		sink := &fileLogSink{file: tempFile}

		testErr := &testError{"test error"}
		message := `{"event": "error", "error": "test"}`
		sink.Error(testErr, message)

		tempFile.Close()

		content, err := os.ReadFile(tempFile.Name())
		if err != nil {
			t.Fatalf("failed to read the file: %v", err)
		}

		expected := message + "\n"
		if string(content) != expected {
			t.Errorf("expected '%s' to be written, got '%s'", expected, string(content))
		}
	})

	t.Run("write with a nil error", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "test_nilerr_*.log")
		if err != nil {
			t.Fatalf("failed to create a temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())

		sink := &fileLogSink{file: tempFile}

		message := `{"event": "error"}`
		sink.Error(nil, message)

		tempFile.Close()

		content, err := os.ReadFile(tempFile.Name())
		if err != nil {
			t.Fatalf("failed to read the file: %v", err)
		}

		expected := message + "\n"
		if string(content) != expected {
			t.Errorf("expected '%s' to be written, got '%s'", expected, string(content))
		}
	})
}

// TestFileLogSink_WithValues tests the WithValues method.
func TestFileLogSink_WithValues(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test_withvalues_*.log")
	if err != nil {
		t.Fatalf("failed to create a temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	sink := &fileLogSink{file: tempFile}

	t.Run("add key-value pairs", func(t *testing.T) {
		newSink := sink.WithValues("key1", "value1", "key2", "value2")

		// WithValues must return the receiver (trivial implementation).
		if newSink != sink {
			t.Error("expected WithValues to return the receiver")
		}

		// Verify the original sink still works.
		sink.Info(0, `{"test": "original"}`)
		tempFile.Close()

		content, err := os.ReadFile(tempFile.Name())
		if err != nil {
			t.Fatalf("failed to read the file: %v", err)
		}

		if !strings.Contains(string(content), "original") {
			t.Error("expected the original sink to keep working")
		}
	})
}

// TestFileLogSink_WithName tests the WithName method.
func TestFileLogSink_WithName(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test_withname_*.log")
	if err != nil {
		t.Fatalf("failed to create a temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	sink := &fileLogSink{file: tempFile}

	t.Run("add a name", func(t *testing.T) {
		newSink := sink.WithName("test_logger")

		// WithName must return the receiver (trivial implementation).
		if newSink != sink {
			t.Error("expected WithName to return the receiver")
		}

		// Verify the original sink still works.
		sink.Info(0, `{"test": "original"}`)
		tempFile.Close()

		content, err := os.ReadFile(tempFile.Name())
		if err != nil {
			t.Fatalf("failed to read the file: %v", err)
		}

		if !strings.Contains(string(content), "original") {
			t.Error("expected the original sink to keep working")
		}
	})
}

// TestFileLogSink_Integration is an integration test.
func TestFileLogSink_Integration(t *testing.T) {
	t.Run("full logging flow", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "integration_*.log")
		if err != nil {
			t.Fatalf("failed to create a temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())

		logger := NewFileLogger(tempFile)

		// Log entries of various kinds.
		logger.GetSink().Info(0, `{"type": "info", "message": "application started"}`)
		logger.GetSink().Info(1, `{"type": "debug", "message": "loading config"}`)
		logger.GetSink().Error(nil, `{"type": "error", "message": "connection failed"}`)

		// Use WithValues and WithName.
		logger.GetSink().WithValues("component", "database").Info(0, `{"type": "info", "message": "db connected"}`)
		logger.GetSink().WithName("auth").Error(nil, `{"type": "error", "message": "auth failed"}`)

		tempFile.Close()

		content, err := os.ReadFile(tempFile.Name())
		if err != nil {
			t.Fatalf("failed to read the file: %v", err)
		}

		lines := strings.Split(strings.TrimSpace(string(content)), "\n")
		expectedCount := 5 // 5 log messages.

		if len(lines) != expectedCount {
			t.Errorf("expected %d log lines, got %d", expectedCount, len(lines))
		}
	})

	t.Run("verify real file writes", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "verify_*.log")
		if err != nil {
			t.Fatalf("failed to create a temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())

		logger := NewFileLogger(tempFile)

		testMessages := []string{
			`{"id": 1, "msg": "first"}`,
			`{"id": 2, "msg": "second"}`,
			`{"id": 3, "msg": "third"}`,
		}

		for _, msg := range testMessages {
			logger.GetSink().Info(0, msg)
		}

		tempFile.Close()

		// Read the file back to verify.
		content, err := os.ReadFile(tempFile.Name())
		if err != nil {
			t.Fatalf("failed to read the file: %v", err)
		}

		lines := strings.Split(strings.TrimSpace(string(content)), "\n")
		if len(lines) != len(testMessages) {
			t.Errorf("expected %d log lines, got %d", len(testMessages), len(lines))
		}

		for i, expected := range testMessages {
			if i < len(lines) && lines[i] != expected {
				t.Errorf("line %d: expected '%s', got '%s'", i+1, expected, lines[i])
			}
		}
	})
}

// TestFileLogSink_SyncBehavior tests the Sync behavior.
func TestFileLogSink_SyncBehavior(t *testing.T) {
	t.Run("every write syncs", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "sync_*.log")
		if err != nil {
			t.Fatalf("failed to create a temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())

		sink := &fileLogSink{file: tempFile}

		// First write.
		sink.Info(0, `{"msg": "first"}`)

		// Second write.
		sink.Info(0, `{"msg": "second"}`)

		// Error must sync too.
		sink.Error(nil, `{"msg": "error"}`)

		tempFile.Close()

		// Verify everything was written.
		content, err := os.ReadFile(tempFile.Name())
		if err != nil {
			t.Fatalf("failed to read the file: %v", err)
		}

		lineCount := strings.Count(string(content), "\n")
		if lineCount != 3 {
			t.Errorf("expected 3 log lines, got %d", lineCount)
		}
	})
}

// TestFileLogSink_ConcurrentAccess tests concurrent access.
func TestFileLogSink_ConcurrentAccess(t *testing.T) {
	tempFile, err := os.CreateTemp("", "concurrent_*.log")
	if err != nil {
		t.Fatalf("failed to create a temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	sink := &fileLogSink{file: tempFile}
	logger := logr.New(sink)

	var wg sync.WaitGroup

	// Start multiple goroutines writing concurrently.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				logger.GetSink().Info(0, `{"goroutine": `+string(rune('0'+id))+`, "iteration": `+string(rune('0'+j))+"}")
			}
		}(i)
	}

	wg.Wait()
	tempFile.Close()

	// Read the file back to verify.
	content, err := os.ReadFile(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to read the file: %v", err)
	}

	lineCount := strings.Count(string(content), "\n")
	if lineCount != 100 {
		t.Errorf("expected 100 log lines, got %d", lineCount)
	}
}

// testError is a simple error implementation used by the tests.
type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

// TestFileLogSink_EmptyMessage tests the handling of empty messages.
func TestFileLogSink_EmptyMessage(t *testing.T) {
	tempFile, err := os.CreateTemp("", "empty_*.log")
	if err != nil {
		t.Fatalf("failed to create a temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	sink := &fileLogSink{file: tempFile}

	t.Run("empty string message", func(t *testing.T) {
		sink.Info(0, "")

		tempFile.Close()

		content, err := os.ReadFile(tempFile.Name())
		if err != nil {
			t.Fatalf("failed to read the file: %v", err)
		}

		expected := "\n"
		if string(content) != expected {
			t.Errorf("expected '%s' to be written, got '%s'", expected, string(content))
		}
	})
}

// TestFileLogSink_LargeMessage tests the handling of large messages.
func TestFileLogSink_LargeMessage(t *testing.T) {
	tempFile, err := os.CreateTemp("", "large_*.log")
	if err != nil {
		t.Fatalf("failed to create a temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	sink := &fileLogSink{file: tempFile}

	// Build a large JSON message.
	largeMsg := `{"data": "` + strings.Repeat("x", 10000) + `"}`

	sink.Info(0, largeMsg)

	tempFile.Close()

	content, err := os.ReadFile(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to read the file: %v", err)
	}

	expected := largeMsg + "\n"
	if string(content) != expected {
		t.Errorf("expected the large message to be written, got length %d, want length %d", len(content), len(expected))
	}
}

// TestFileLogSink_UTF8 tests UTF-8 support.
func TestFileLogSink_UTF8(t *testing.T) {
	tempFile, err := os.CreateTemp("", "utf8_*.log")
	if err != nil {
		t.Fatalf("failed to create a temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	sink := &fileLogSink{file: tempFile}

	utf8Msg := `{"unicode": "héllo wörld🌍Привет"}`

	sink.Info(0, utf8Msg)

	tempFile.Close()

	content, err := os.ReadFile(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to read the file: %v", err)
	}

	expected := utf8Msg + "\n"
	if string(content) != expected {
		t.Errorf("expected the UTF-8 content to be written verbatim")
	}
}

// TestFileLogSink_InterfaceCompliance verifies the interface implementation.
func TestFileLogSink_InterfaceCompliance(t *testing.T) {
	// Compile-time check that fileLogSink implements logr.LogSink.
	var _ logr.LogSink = (*fileLogSink)(nil)

	// This test exists to guarantee interface compatibility: compilation
	// fails if fileLogSink misses any required method.
}

// TestFileLogSink_Enabled tests the Enabled method.
func TestFileLogSink_Enabled(t *testing.T) {
	tempFile, err := os.CreateTemp("", "enabled_*.log")
	if err != nil {
		t.Fatalf("failed to create a temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	sink := &fileLogSink{file: tempFile}

	t.Run("level 0 is enabled", func(t *testing.T) {
		if !sink.Enabled(0) {
			t.Error("expected level 0 to be enabled")
		}
	})

	t.Run("level 1 is enabled", func(t *testing.T) {
		if !sink.Enabled(1) {
			t.Error("expected level 1 to be enabled")
		}
	})

	t.Run("negative levels are enabled", func(t *testing.T) {
		if !sink.Enabled(-1) {
			t.Error("expected a negative level to be enabled")
		}
	})

	t.Run("large levels are enabled", func(t *testing.T) {
		if !sink.Enabled(100) {
			t.Error("expected a large level to be enabled")
		}
	})
}

// TestFileLogSink_Init tests the Init method.
func TestFileLogSink_Init(t *testing.T) {
	tempFile, err := os.CreateTemp("", "init_*.log")
	if err != nil {
		t.Fatalf("failed to create a temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	sink := &fileLogSink{file: tempFile}

	// Init must not panic or fail.
	info := logr.RuntimeInfo{
		CallDepth: 0,
	}
	sink.Init(info)

	// Init is a no-op, so there is no state change to verify.
	tempFile.Close()
}
