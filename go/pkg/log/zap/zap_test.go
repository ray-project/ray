package zap

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/ray-project/ray/go/pkg/log/lumberjack"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		opts    *Options
		wantErr bool
	}{
		{
			name:    "nil options uses defaults",
			opts:    nil,
			wantErr: false,
		},
		{
			name: "development mode",
			opts: &Options{
				Development: true,
				Level:       zapcore.DebugLevel,
			},
			wantErr: false,
		},
		{
			name: "production mode",
			opts: &Options{
				Development: false,
				Level:       zapcore.InfoLevel,
				Encoder:     JSONEncoder,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := New(tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && logger == logr.Discard() {
				t.Error("New() returned Discard logger")
			}
		})
	}
}

func TestNewRaw(t *testing.T) {
	opts := DefaultOptions()
	logger, err := NewRaw(opts)
	if err != nil {
		t.Fatalf("NewRaw() error = %v", err)
	}
	if logger == nil {
		t.Error("NewRaw() returned nil logger")
	}
}

func TestTeeOutputSeparation(t *testing.T) {
	// Use two observers to capture stdout and stderr level logs separately.
	// observer.New returns a Core that can be used directly in a Tee.
	stdoutCore, stdoutLogs := observer.New(zap.DebugLevel)
	stderrCore, stderrLogs := observer.New(zap.WarnLevel)

	// stdout allows Debug and Info (not Warn); stderr allows Warn and Error.
	stdoutLevel := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
		return l < zapcore.WarnLevel
	})
	stderrLevel := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
		return l >= zapcore.WarnLevel
	})

	// Wrap the observer cores to apply the level filtering.
	wrappedStdout := &levelFilteredCore{inner: stdoutCore, enabler: stdoutLevel}
	wrappedStderr := &levelFilteredCore{inner: stderrCore, enabler: stderrLevel}

	teeCore := zapcore.NewTee(wrappedStdout, wrappedStderr)
	logger := zap.New(teeCore)

	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")

	// Verify stdout only receives Debug and Info.
	stdoutEntries := stdoutLogs.All()
	if len(stdoutEntries) != 2 {
		t.Errorf("stdout expected 2 entries, got %d", len(stdoutEntries))
		for _, e := range stdoutEntries {
			t.Logf("stdout entry: %v", e)
		}
	} else {
		if stdoutEntries[0].Level != zapcore.DebugLevel {
			t.Errorf("stdout[0] expected Debug, got %v", stdoutEntries[0].Level)
		}
		if stdoutEntries[1].Level != zapcore.InfoLevel {
			t.Errorf("stdout[1] expected Info, got %v", stdoutEntries[1].Level)
		}
	}

	// Verify stderr only receives Warn and Error.
	stderrEntries := stderrLogs.All()
	if len(stderrEntries) != 2 {
		t.Errorf("stderr expected 2 entries, got %d", len(stderrEntries))
		for _, e := range stderrEntries {
			t.Logf("stderr entry: %v", e)
		}
	} else {
		if stderrEntries[0].Level != zapcore.WarnLevel {
			t.Errorf("stderr[0] expected Warn, got %v", stderrEntries[0].Level)
		}
		if stderrEntries[1].Level != zapcore.ErrorLevel {
			t.Errorf("stderr[1] expected Error, got %v", stderrEntries[1].Level)
		}
	}
}

// levelFilteredCore wraps a Core to apply additional level filtering.
type levelFilteredCore struct {
	inner   zapcore.Core
	enabler zapcore.LevelEnabler
}

func (c *levelFilteredCore) Enabled(level zapcore.Level) bool {
	return c.enabler.Enabled(level) && c.inner.Enabled(level)
}

func (c *levelFilteredCore) With(fields []zapcore.Field) zapcore.Core {
	return &levelFilteredCore{
		inner:   c.inner.With(fields),
		enabler: c.enabler,
	}
}

func (c *levelFilteredCore) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return ce.AddCore(entry, c)
	}
	return ce
}

func (c *levelFilteredCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	return c.inner.Write(entry, fields)
}

func (c *levelFilteredCore) Sync() error {
	return c.inner.Sync()
}

func TestLevelFiltering(t *testing.T) {
	tests := []struct {
		name       string
		minLevel   zapcore.Level
		logLevels  []zapcore.Level
		wantStdout int
		wantStderr int
	}{
		{
			name:       "Debug level captures all",
			minLevel:   zapcore.DebugLevel,
			logLevels:  []zapcore.Level{zapcore.DebugLevel, zapcore.InfoLevel, zapcore.WarnLevel, zapcore.ErrorLevel},
			wantStdout: 2, // Debug, Info
			wantStderr: 2, // Warn, Error
		},
		{
			name:       "Info level filters Debug",
			minLevel:   zapcore.InfoLevel,
			logLevels:  []zapcore.Level{zapcore.DebugLevel, zapcore.InfoLevel, zapcore.WarnLevel, zapcore.ErrorLevel},
			wantStdout: 1, // Info
			wantStderr: 2, // Warn, Error
		},
		{
			name:       "Warn level filters Debug and Info",
			minLevel:   zapcore.WarnLevel,
			logLevels:  []zapcore.Level{zapcore.DebugLevel, zapcore.InfoLevel, zapcore.WarnLevel, zapcore.ErrorLevel},
			wantStdout: 0, // nothing below Warn
			wantStderr: 2, // Warn, Error
		},
		{
			name:       "Error level only captures Error",
			minLevel:   zapcore.ErrorLevel,
			logLevels:  []zapcore.Level{zapcore.DebugLevel, zapcore.InfoLevel, zapcore.WarnLevel, zapcore.ErrorLevel},
			wantStdout: 0,
			wantStderr: 1, // Error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdoutCore, stdoutLogs := observer.New(zap.DebugLevel)
			stderrCore, stderrLogs := observer.New(zap.WarnLevel)

			// Create level enablers filtered by minLevel.
			stdoutLevel := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
				return l >= tt.minLevel && l < zapcore.WarnLevel
			})
			stderrLevel := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
				return l >= zapcore.WarnLevel && l >= tt.minLevel
			})

			wrappedStdout := &levelFilteredCore{inner: stdoutCore, enabler: stdoutLevel}
			wrappedStderr := &levelFilteredCore{inner: stderrCore, enabler: stderrLevel}

			teeCore := zapcore.NewTee(wrappedStdout, wrappedStderr)
			logger := zap.New(teeCore)

			for _, level := range tt.logLevels {
				switch level {
				case zapcore.DebugLevel:
					logger.Debug("debug")
				case zapcore.InfoLevel:
					logger.Info("info")
				case zapcore.WarnLevel:
					logger.Warn("warn")
				case zapcore.ErrorLevel:
					logger.Error("error")
				}
			}

			if got := len(stdoutLogs.All()); got != tt.wantStdout {
				t.Errorf("stdout entries = %d, want %d", got, tt.wantStdout)
			}
			if got := len(stderrLogs.All()); got != tt.wantStderr {
				t.Errorf("stderr entries = %d, want %d", got, tt.wantStderr)
			}
		})
	}
}

func TestZapError(t *testing.T) {
	tests := []struct {
		name string
		err  *ZapError
		want string
	}{
		{
			name: "error with path",
			err: &ZapError{
				Op:   "open",
				Path: "/var/log/test.log",
				Err:  os.ErrPermission,
			},
			want: "zap open: /var/log/test.log: permission denied",
		},
		{
			name: "error without path",
			err: &ZapError{
				Op:  "config",
				Err: os.ErrInvalid,
			},
			want: "zap config: invalid argument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSetupDefaultLogger(t *testing.T) {
	tests := []struct {
		name    string
		opts    []Option
		wantErr bool
	}{
		{
			name:    "default options",
			opts:    nil,
			wantErr: false,
		},
		{
			name:    "development mode",
			opts:    []Option{WithDevelopment(true)},
			wantErr: false,
		},
		{
			name:    "production mode",
			opts:    []Option{WithDevelopment(false)},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SetupDefaultLogger(tt.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetupDefaultLogger() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBuildZapLogger(t *testing.T) {
	tests := []struct {
		name string
		opts *Options
	}{
		{
			name: "development mode",
			opts: &Options{
				Development: true,
				Level:       zapcore.DebugLevel,
			},
		},
		{
			name: "production mode",
			opts: &Options{
				Development: false,
				Level:       zapcore.InfoLevel,
				Encoder:     JSONEncoder,
			},
		},
		{
			name: "with rotation options",
			opts: &Options{
				Development:      true,
				Level:            zapcore.InfoLevel,
				OutputPaths:      []string{"/tmp/test.log"},
				ErrorOutputPaths: []string{"/tmp/test-error.log"},
				Rotation: &lumberjack.Options{
					MaxSize:    50,
					MaxBackups: 5,
					MaxAge:     30,
					Compress:   true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := buildZapLogger(tt.opts)
			if err != nil {
				t.Fatalf("buildZapLogger() error = %v", err)
			}
			if logger == nil {
				t.Error("buildZapLogger() returned nil logger")
			}

			logger.Info("test message")
			logger.Sync()
		})
	}
}

func TestConsoleEncoder(t *testing.T) {
	var buf bytes.Buffer
	encoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
	core := zapcore.NewCore(encoder, zapcore.AddSync(&buf), zapcore.InfoLevel)
	logger := zap.New(core)

	logger.Info("test info message")

	output := buf.String()
	if !strings.Contains(output, "INFO") {
		t.Errorf("expected INFO in output, got: %s", output)
	}
	if !strings.Contains(output, "test info message") {
		t.Errorf("expected message in output, got: %s", output)
	}
}

func TestJSONEncoder(t *testing.T) {
	var buf bytes.Buffer
	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewCore(encoder, zapcore.AddSync(&buf), zapcore.InfoLevel)
	logger := zap.New(core)

	logger.Info("test json message")

	output := buf.String()
	if !strings.Contains(output, `"msg":"test json message"`) {
		t.Errorf("expected JSON message in output, got: %s", output)
	}
	if !strings.Contains(output, `"level":"info"`) {
		t.Errorf("expected level in output, got: %s", output)
	}
}

func TestNewEncoder(t *testing.T) {
	tests := []struct {
		name         string
		opts         *Options
		wantConsole  bool
	}{
		{
			name:         "nil options uses defaults (console)",
			opts:         nil,
			wantConsole:  true,
		},
		{
			name: "development mode with default encoder",
			opts: &Options{
				Development: true,
				Encoder:     ConsoleEncoder,
			},
			wantConsole: true,
		},
		{
			name: "development mode with JSON encoder override",
			opts: &Options{
				Development: true,
				Encoder:     JSONEncoder,
			},
			wantConsole: true, // Development=true forces console
		},
		{
			name: "production mode with JSON encoder",
			opts: &Options{
				Development: false,
				Encoder:     JSONEncoder,
			},
			wantConsole: false,
		},
		{
			name: "production mode with console encoder override",
			opts: &Options{
				Development: false,
				Encoder:     ConsoleEncoder,
			},
			wantConsole: true, // ConsoleEncoder forces console
		},
		{
			name: "production mode with default encoder (zero value = JSON)",
			opts: &Options{
				Development: false,
				Encoder:     JSONEncoder,
			},
			wantConsole: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoder := newEncoder(tt.opts)

			// Write a test message to verify encoder type
			var buf bytes.Buffer
			core := zapcore.NewCore(encoder, zapcore.AddSync(&buf), zapcore.InfoLevel)
			logger := zap.New(core)
			logger.Info("test message")

			output := buf.String()

			if tt.wantConsole {
				// Console encoder should have colored output
				if !strings.Contains(output, "INFO") {
					t.Errorf("expected console output with INFO, got: %s", output)
				}
			} else {
				// JSON encoder should have JSON format
				if !strings.Contains(output, `"level":"info"`) {
					t.Errorf("expected JSON output with level field, got: %s", output)
				}
			}
		})
	}
}

func TestEncoderConfigFunctions(t *testing.T) {
	// Test rayDevEncoderConfig returns valid config
	devConfig := rayDevEncoderConfig()
	if devConfig.TimeKey != "ts" {
		t.Errorf("rayDevEncoderConfig TimeKey = %s, want ts", devConfig.TimeKey)
	}
	if devConfig.EncodeLevel == nil {
		t.Error("rayDevEncoderConfig EncodeLevel should not be nil")
	}

	// Test productionEncoderConfig returns valid config
	prodConfig := productionEncoderConfig()
	if prodConfig.TimeKey != "ts" {
		t.Errorf("productionEncoderConfig TimeKey = %s, want ts", prodConfig.TimeKey)
	}
	if prodConfig.EncodeLevel == nil {
		t.Error("productionEncoderConfig EncodeLevel should not be nil")
	}
}

func TestOptionsDefaults(t *testing.T) {
	opts := DefaultOptions()

	if !opts.Development {
		t.Error("DefaultOptions Development should be true")
	}
	if opts.Level != zapcore.InfoLevel {
		t.Errorf("DefaultOptions Level = %v, want InfoLevel", opts.Level)
	}
	if opts.Encoder != ConsoleEncoder {
		t.Errorf("DefaultOptions Encoder = %v, want ConsoleEncoder", opts.Encoder)
	}
	if opts.OutputPaths != nil && len(opts.OutputPaths) != 0 {
		t.Errorf("DefaultOptions OutputPaths should be empty, got %v", opts.OutputPaths)
	}
	if opts.ErrorOutputPaths != nil && len(opts.ErrorOutputPaths) != 0 {
		t.Errorf("DefaultOptions ErrorOutputPaths should be empty, got %v", opts.ErrorOutputPaths)
	}
}

func TestLumberjackOptionsIntegration(t *testing.T) {
	// Test that lumberjack Options is properly integrated
	tmpDir, err := os.MkdirTemp("", "zap-lumberjack-integration")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logFile := filepath.Join(tmpDir, "test.log")

	opts := &Options{
		Development: true,
		Level:       zapcore.InfoLevel,
		OutputPaths: []string{logFile},
		Rotation: &lumberjack.Options{
			MaxSize:    10,
			MaxBackups: 2,
			MaxAge:     1,
			Compress:   false,
			LocalTime:  true,
		},
	}

	logger, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Log a message
	logger.Info("test integration message")

	// Verify file was created
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		t.Error("log file was not created")
	}
}