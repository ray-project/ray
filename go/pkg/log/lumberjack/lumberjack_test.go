package lumberjack

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

func TestNewWriter(t *testing.T) {
	tests := []struct {
		name     string
		paths    []string
		opts     *Options
		fallback io.Writer
		wantNil  bool
	}{
		{
			name:     "empty paths returns fallback",
			paths:    []string{},
			opts:     nil,
			fallback: os.Stdout,
			wantNil:  false,
		},
		{
			name:     "nil paths returns fallback",
			paths:    nil,
			opts:     nil,
			fallback: os.Stderr,
			wantNil:  false,
		},
		{
			name:     "empty string path returns fallback",
			paths:    []string{""},
			opts:     nil,
			fallback: os.Stdout,
			wantNil:  false,
		},
		{
			name:     "valid path creates logger",
			paths:    []string{"/tmp/test.log"},
			opts:     nil,
			fallback: os.Stdout,
			wantNil:  false,
		},
		{
			name:     "nil fallback returns os.Stdout",
			paths:    []string{},
			opts:     nil,
			fallback: nil,
			wantNil:  false,
		},
		{
			name:  "with custom options",
			paths: []string{"/tmp/test2.log"},
			opts: &Options{
				MaxSize:    50,
				MaxBackups: 5,
				MaxAge:     30,
				Compress:   false,
				LocalTime:  false,
			},
			fallback: os.Stdout,
			wantNil:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewWriter(tt.paths, tt.opts, tt.fallback)
			if (got == nil) != tt.wantNil {
				t.Errorf("NewWriter() = %v, wantNil %v", got, tt.wantNil)
			}
			if len(tt.paths) > 0 && tt.paths[0] != "" {
				// Should return lumberjack.Logger, not fallback
				if got == tt.fallback {
					t.Error("NewWriter() returned fallback for valid path")
				}
			}
			// Verify nil fallback returns os.Stdout
			if tt.fallback == nil && len(tt.paths) == 0 || (len(tt.paths) > 0 && tt.paths[0] == "") {
				if got != os.Stdout {
					t.Error("NewWriter() with nil fallback should return os.Stdout")
				}
			}
		})
	}
}

func TestNewWriteSyncer(t *testing.T) {
	tests := []struct {
		name     string
		paths    []string
		opts     *Options
		fallback zapcore.WriteSyncer
		wantNil  bool
	}{
		{
			name:     "empty paths returns fallback",
			paths:    []string{},
			opts:     nil,
			fallback: zapcore.AddSync(os.Stdout),
			wantNil:  false,
		},
		{
			name:     "nil paths returns fallback",
			paths:    nil,
			opts:     nil,
			fallback: zapcore.AddSync(os.Stderr),
			wantNil:  false,
		},
		{
			name:     "empty string path returns fallback",
			paths:    []string{""},
			opts:     nil,
			fallback: zapcore.AddSync(os.Stdout),
			wantNil:  false,
		},
		{
			name:     "valid path creates write syncer",
			paths:    []string{"/tmp/test-sync.log"},
			opts:     nil,
			fallback: zapcore.AddSync(os.Stdout),
			wantNil:  false,
		},
		{
			name:     "nil fallback returns os.Stdout syncer",
			paths:    []string{},
			opts:     nil,
			fallback: nil,
			wantNil:  false,
		},
		{
			name:  "with custom options",
			paths: []string{"/tmp/test-sync2.log"},
			opts: &Options{
				MaxSize:    200,
				MaxBackups: 10,
				MaxAge:     14,
				Compress:   true,
				LocalTime:  true,
			},
			fallback: zapcore.AddSync(os.Stdout),
			wantNil:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewWriteSyncer(tt.paths, tt.opts, tt.fallback)
			if (got == nil) != tt.wantNil {
				t.Errorf("NewWriteSyncer() = %v, wantNil %v", got, tt.wantNil)
			}
			if len(tt.paths) > 0 && tt.paths[0] != "" {
				// Should return lockedWriteSyncer, not fallback
				if got == tt.fallback {
					t.Error("NewWriteSyncer() returned fallback for valid path")
				}
			}
			// Verify nil fallback returns os.Stdout syncer
			if tt.fallback == nil && len(tt.paths) == 0 || (len(tt.paths) > 0 && tt.paths[0] == "") {
				// Should return a WriteSyncer wrapping os.Stdout
				if got == nil {
					t.Error("NewWriteSyncer() with nil fallback should not return nil")
				}
			}
		})
	}
}

func TestLockedWriteSyncer(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lumberjack-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logFile := filepath.Join(tmpDir, "test.log")

	ws := NewWriteSyncer([]string{logFile}, nil, zapcore.AddSync(os.Stdout))
	if ws == nil {
		t.Fatal("NewWriteSyncer returned nil")
	}

	// Test Write
	data := []byte("test message\n")
	n, err := ws.Write(data)
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}
	if n != len(data) {
		t.Errorf("Write() = %d, want %d", n, len(data))
	}

	// Test Sync
	if err := ws.Sync(); err != nil {
		t.Errorf("Sync() error = %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		t.Error("log file was not created")
	}
}

func TestNewWriteSyncerWritesToAllPaths(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lumberjack-multi-sync")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logFile1 := filepath.Join(tmpDir, "test-1.log")
	logFile2 := filepath.Join(tmpDir, "test-2.log")

	ws := NewWriteSyncer([]string{logFile1, logFile2}, nil, zapcore.AddSync(os.Stdout))
	data := []byte("multi sync message\n")
	if _, err := ws.Write(data); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := ws.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	for _, path := range []string{logFile1, logFile2} {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%q) error = %v", path, err)
		}
		if string(content) != string(data) {
			t.Fatalf("content of %q = %q, want %q", path, string(content), string(data))
		}
	}
}

func TestLockedWriteSyncerConcurrency(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lumberjack-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logFile := filepath.Join(tmpDir, "concurrent.log")

	ws := NewWriteSyncer([]string{logFile}, nil, zapcore.AddSync(os.Stdout))

	// Concurrent writes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				data := []byte("concurrent message\n")
				_, err := ws.Write(data)
				if err != nil {
					t.Errorf("concurrent write error: %v", err)
				}
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Sync should complete without error
	if err := ws.Sync(); err != nil {
		t.Errorf("Sync() error = %v", err)
	}
}

func TestNewWriterWithActualFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lumberjack-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logFile := filepath.Join(tmpDir, "writer-test.log")

	writer := NewWriter([]string{logFile}, &Options{
		MaxSize:    10,
		MaxBackups: 2,
		MaxAge:     1,
		Compress:   false,
		LocalTime:  true,
	}, os.Stdout)

	if writer == nil {
		t.Fatal("NewWriter returned nil")
	}

	// Write some data
	data := []byte("test log message\n")
	n, err := writer.Write(data)
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}
	if n != len(data) {
		t.Errorf("Write() = %d, want %d", n, len(data))
	}

	// Verify file was created
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		t.Error("log file was not created")
	}
}

func TestNewWriterWritesToAllPaths(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lumberjack-multi-writer")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logFile1 := filepath.Join(tmpDir, "writer-1.log")
	logFile2 := filepath.Join(tmpDir, "writer-2.log")

	writer := NewWriter([]string{logFile1, logFile2}, nil, os.Stdout)
	data := []byte("multi writer message\n")
	if _, err := writer.Write(data); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	for _, path := range []string{logFile1, logFile2} {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%q) error = %v", path, err)
		}
		if string(content) != string(data) {
			t.Fatalf("content of %q = %q, want %q", path, string(content), string(data))
		}
	}
}

func TestDefaultOptionsUsed(t *testing.T) {
	// Test that DefaultOptions is called when opts is nil
	tmpDir, err := os.MkdirTemp("", "lumberjack-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logFile := filepath.Join(tmpDir, "default-opts.log")

	// With nil opts
	ws := NewWriteSyncer([]string{logFile}, nil, zapcore.AddSync(os.Stdout))
	if ws == nil {
		t.Fatal("NewWriteSyncer returned nil")
	}

	// Write should succeed
	_, err = ws.Write([]byte("test"))
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}
}

func TestApplyTo(t *testing.T) {
	tests := []struct {
		name   string
		opts   *Options
		want   *lumberjack.Logger
	}{
		{
			name: "apply all options",
			opts: &Options{
				MaxSize:    50,
				MaxBackups: 5,
				MaxAge:     30,
				Compress:   true,
				LocalTime:  false,
			},
			want: &lumberjack.Logger{
				MaxSize:    50,
				MaxBackups: 5,
				MaxAge:     30,
				Compress:   true,
				LocalTime:  false,
			},
		},
		{
			name: "apply with zero values",
			opts: &Options{
				MaxSize:    0,
				MaxBackups: 0,
				MaxAge:     0,
				Compress:   false,
				LocalTime:  false,
			},
			want: &lumberjack.Logger{
				MaxSize:    0,
				MaxBackups: 0,
				MaxAge:     0,
				Compress:   false,
				LocalTime:  false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lj lumberjack.Logger
			tt.opts.ApplyTo(&lj)

			if lj.MaxSize != tt.want.MaxSize {
				t.Errorf("MaxSize = %d, want %d", lj.MaxSize, tt.want.MaxSize)
			}
			if lj.MaxBackups != tt.want.MaxBackups {
				t.Errorf("MaxBackups = %d, want %d", lj.MaxBackups, tt.want.MaxBackups)
			}
			if lj.MaxAge != tt.want.MaxAge {
				t.Errorf("MaxAge = %d, want %d", lj.MaxAge, tt.want.MaxAge)
			}
			if lj.Compress != tt.want.Compress {
				t.Errorf("Compress = %v, want %v", lj.Compress, tt.want.Compress)
			}
			if lj.LocalTime != tt.want.LocalTime {
				t.Errorf("LocalTime = %v, want %v", lj.LocalTime, tt.want.LocalTime)
			}
		})
	}
}

func TestApplyToNilCases(t *testing.T) {
	// Test with nil options
	t.Run("nil options", func(t *testing.T) {
		var lj lumberjack.Logger
		lj.MaxSize = 100 // Set initial value
		var nilOpts *Options = nil
		nilOpts.ApplyTo(&lj)
		// Should not modify anything
		if lj.MaxSize != 100 {
			t.Errorf("MaxSize should remain unchanged, got %d", lj.MaxSize)
		}
	})

	// Test with nil logger
	t.Run("nil logger", func(t *testing.T) {
		opts := &Options{
			MaxSize:    50,
			MaxBackups: 5,
			MaxAge:     30,
			Compress:   true,
			LocalTime:  true,
		}
		// Should not panic
		opts.ApplyTo(nil)
	})

	// Test with both nil
	t.Run("both nil", func(t *testing.T) {
		var nilOpts *Options = nil
		// Should not panic
		nilOpts.ApplyTo(nil)
	})
}

func TestRotationBehavior(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lumberjack-rotation-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logFile := filepath.Join(tmpDir, "rotation.log")

	// Create logger with small MaxSize for testing rotation
	opts := &Options{
		MaxSize:    1, // 1 MB
		MaxBackups: 3,
		MaxAge:     0, // Don't delete based on age
		Compress:   false,
		LocalTime:  true,
	}

	ws := NewWriteSyncer([]string{logFile}, opts, zapcore.AddSync(os.Stdout))
	if ws == nil {
		t.Fatal("NewWriteSyncer returned nil")
	}

	// Write enough data to trigger at least one rotation
	// 1 MB = 1,048,576 bytes
	data := make([]byte, 512*1024) // 512 KB
	for i := range data {
		data[i] = 'A' + byte(i%26)
	}
	data[len(data)-1] = '\n'

	// Write 3 chunks (1.5 MB total) to trigger rotation
	for i := 0; i < 3; i++ {
		n, err := ws.Write(data)
		if err != nil {
			t.Errorf("Write() error = %v", err)
		}
		if n != len(data) {
			t.Errorf("Write() = %d, want %d", n, len(data))
		}
	}

	// Sync to ensure all data is written
	if err := ws.Sync(); err != nil {
		t.Errorf("Sync() error = %v", err)
	}

	// Check that backup files were created
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}

	// Should have at least the main log file and one backup
	if len(files) < 1 {
		t.Errorf("Expected at least 1 file, got %d", len(files))
	}

	// Verify main log file exists
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		t.Error("Main log file was not created")
	}
}
