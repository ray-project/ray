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


// Package zap provides Zap as the default logr implementation.
package zap

import (
	"go.uber.org/zap/zapcore"

	"github.com/ray-project/ray/go/pkg/log/lumberjack"
)

// EncoderType defines the encoder type.
type EncoderType int

const (
	// JSONEncoder is a JSON format encoder (production mode).
	JSONEncoder EncoderType = iota
	// ConsoleEncoder is a Console format encoder (development mode).
	ConsoleEncoder
)

// Option is a configuration option function for SetupDefaultLogger.
type Option func(*Options)

// WithDevelopment sets development mode.
// true: Console format output; false: JSON format output.
func WithDevelopment(dev bool) Option {
	return func(o *Options) {
		o.Development = dev
	}
}

// WithLevel sets the minimum log level.
func WithLevel(level zapcore.Level) Option {
	return func(o *Options) {
		o.Level = level
	}
}

// WithEncoder sets the encoder type.
func WithEncoder(encoder EncoderType) Option {
	return func(o *Options) {
		o.Encoder = encoder
	}
}

// WithOutputPaths sets stdout log paths (Info/Debug level).
func WithOutputPaths(paths ...string) Option {
	return func(o *Options) {
		o.OutputPaths = paths
	}
}

// WithErrorOutputPaths sets stderr log paths (Error/Warn level).
func WithErrorOutputPaths(paths ...string) Option {
	return func(o *Options) {
		o.ErrorOutputPaths = paths
	}
}

// WithRotation sets log rotation configuration.
func WithRotation(opts *lumberjack.Options) Option {
	return func(o *Options) {
		o.Rotation = opts
	}
}

// Options holds Zap configuration options.
type Options struct {
	// Development mode flag.
	// true: Console format with caller; false: JSON format
	Development bool

	// Level is the minimum log level.
	Level zapcore.Level

	// Encoder type.
	Encoder EncoderType

	// OutputPaths are stdout log paths (Info/Debug level).
	// Empty means output to os.Stdout.
	OutputPaths []string

	// ErrorOutputPaths are stderr log paths (Error/Warn level).
	// Empty means output to os.Stderr.
	ErrorOutputPaths []string

	// Rotation configuration.
	Rotation *lumberjack.Options

	// TimeEncoder for custom time formatting.
	TimeEncoder zapcore.TimeEncoder
}

// DefaultOptions returns default configuration (development mode).
func DefaultOptions() *Options {
	return &Options{
		Development:      true,
		Level:            zapcore.InfoLevel,
		Encoder:          ConsoleEncoder,
		OutputPaths:      []string{},
		ErrorOutputPaths: []string{},
	}
}

// applyOptions applies option functions to default configuration.
func applyOptions(opts ...Option) *Options {
	options := DefaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	return options
}