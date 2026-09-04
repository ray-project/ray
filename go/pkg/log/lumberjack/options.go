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


// Package lumberjack provides log file rotation Writer wrapper.
// Based on gopkg.in/natefinch/lumberjack.v2.
package lumberjack

import "gopkg.in/natefinch/lumberjack.v2"

// Options holds log rotation configuration.
type Options struct {
	// MaxSize is the maximum size per file in MB.
	// Default is 100 MB.
	MaxSize int

	// MaxBackups is the maximum number of old files to retain.
	// Default is 3.
	MaxBackups int

	// MaxAge is the maximum number of days to retain logs.
	// Default is 7 days (0 means no age-based cleanup).
	MaxAge int

	// Compress determines whether to compress old files.
	// Default is true.
	Compress bool

	// LocalTime determines whether to use local time for file naming.
	// Default is true.
	LocalTime bool
}

// DefaultOptions returns default rotation configuration.
func DefaultOptions() *Options {
	return &Options{
		MaxSize:    100, // 100MB
		MaxBackups: 3,
		MaxAge:     7,
		Compress:   true,
		LocalTime:  true,
	}
}

// ApplyTo applies configuration to a lumberjack.Logger.
func (o *Options) ApplyTo(l *lumberjack.Logger) {
	if o == nil || l == nil {
		return
	}
	l.MaxSize = o.MaxSize
	l.MaxBackups = o.MaxBackups
	l.MaxAge = o.MaxAge
	l.Compress = o.Compress
	l.LocalTime = o.LocalTime
}