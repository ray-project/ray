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

package lumberjack

import (
	"io"
	"os"

	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

// Special path markers mapped to the process's standard streams. When a path
// in the output list equals one of these, the corresponding os.Stdout/os.Stderr
// is used instead of a file, so a component logger can write to a log file and
// the pod's stdout/stderr at the same time.
const (
	StdoutMarker = "stdout"
	StderrMarker = "stderr"
)

// streamWriteSyncer writes to an os.File standard stream and treats Sync as a
// no-op: stdout/stderr are unbuffered at the process level and their fsync
// returns EINVAL on Linux, which would otherwise surface as spurious log
// errors.
type streamWriteSyncer struct {
	file *os.File
}

func (s *streamWriteSyncer) Write(p []byte) (int, error) {
	return s.file.Write(p)
}

func (s *streamWriteSyncer) Sync() error {
	return nil
}

func buildWriters(paths []string, opts *Options) []io.Writer {
	writers := make([]io.Writer, 0, len(paths))
	for _, path := range paths {
		if path == "" {
			continue
		}
		switch path {
		case StdoutMarker:
			writers = append(writers, os.Stdout)
			continue
		case StderrMarker:
			writers = append(writers, os.Stderr)
			continue
		}
		writers = append(writers, &lumberjack.Logger{
			Filename:   path,
			MaxSize:    opts.MaxSize,
			MaxBackups: opts.MaxBackups,
			MaxAge:     opts.MaxAge,
			Compress:   opts.Compress,
			LocalTime:  opts.LocalTime,
		})
	}
	return writers
}

func buildWriteSyncers(paths []string, opts *Options) []zapcore.WriteSyncer {
	syncers := make([]zapcore.WriteSyncer, 0, len(paths))
	for _, path := range paths {
		if path == "" {
			continue
		}
		switch path {
		case StdoutMarker:
			syncers = append(syncers, zapcore.Lock(&streamWriteSyncer{file: os.Stdout}))
		case StderrMarker:
			syncers = append(syncers, zapcore.Lock(&streamWriteSyncer{file: os.Stderr}))
		default:
			syncers = append(syncers, zapcore.Lock(zapcore.AddSync(&lumberjack.Logger{
				Filename:   path,
				MaxSize:    opts.MaxSize,
				MaxBackups: opts.MaxBackups,
				MaxAge:     opts.MaxAge,
				Compress:   opts.Compress,
				LocalTime:  opts.LocalTime,
			})))
		}
	}
	return syncers
}

// NewWriter creates an io.Writer with rotation support.
// If paths is empty, returns the specified fallback Writer.
// If fallback is nil, defaults to os.Stdout.
func NewWriter(paths []string, opts *Options, fallback io.Writer) io.Writer {
	if len(paths) == 0 || paths[0] == "" {
		if fallback == nil {
			return os.Stdout
		}
		return fallback
	}

	if opts == nil {
		opts = DefaultOptions()
	}

	writers := buildWriters(paths, opts)
	if len(writers) == 0 {
		if fallback == nil {
			return os.Stdout
		}
		return fallback
	}
	return io.MultiWriter(writers...)
}

// NewWriteSyncer creates a zapcore.WriteSyncer.
// If paths is empty, returns the specified fallback WriteSyncer.
// If fallback is nil, defaults to os.Stdout wrapped as WriteSyncer.
func NewWriteSyncer(paths []string, opts *Options, fallback zapcore.WriteSyncer) zapcore.WriteSyncer {
	if len(paths) == 0 || paths[0] == "" {
		if fallback == nil {
			return zapcore.Lock(zapcore.AddSync(os.Stdout))
		}
		return fallback
	}

	if opts == nil {
		opts = DefaultOptions()
	}

	syncers := buildWriteSyncers(paths, opts)
	if len(syncers) == 0 {
		if fallback == nil {
			return zapcore.Lock(zapcore.AddSync(os.Stdout))
		}
		return fallback
	}
	return zapcore.NewMultiWriteSyncer(syncers...)
}
