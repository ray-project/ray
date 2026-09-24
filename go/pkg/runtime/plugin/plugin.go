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

// Package plugin provides Ray Runtime plugin loading functionality.
// This package encapsulates the Go plugin API loading and reflection call logic
// to avoid duplicate implementations in multiple places.
package plugin

import (
	"errors"
)

// Plugin related errors (for plugin loading and validation).
var (
	// ErrPluginNotFound plugin file not found.
	ErrPluginNotFound = errors.New("plugin file not found")
	// ErrPluginLoadFailed failed to load plugin.
	ErrPluginLoadFailed = errors.New("failed to load plugin")
	// ErrSymbolNotFound symbol not found in plugin.
	ErrSymbolNotFound = errors.New("symbol not found in plugin")
	// ErrInvalidSymbolType plugin symbol has unexpected type.
	ErrInvalidSymbolType = errors.New("plugin symbol has unexpected type")
	// ErrPluginPathInvalid invalid plugin path.
	ErrPluginPathInvalid = errors.New("invalid plugin path")
	// ErrPluginTooLarge plugin file too large.
	ErrPluginTooLarge = errors.New("plugin file too large")
	// ErrPluginExtInvalid invalid plugin file extension.
	ErrPluginExtInvalid = errors.New("invalid plugin file extension")
	// ErrPluginPathTraversal path traversal detected.
	ErrPluginPathTraversal = errors.New("path traversal detected")
	// ErrPluginPathNotAllowed plugin path not in allowed whitelist.
	ErrPluginPathNotAllowed = errors.New("plugin path not in allowed whitelist")
	// ErrPluginChecksumMismatch plugin checksum mismatch.
	ErrPluginChecksumMismatch = errors.New("plugin checksum mismatch")
	// ErrInvalidHandle invalid handle.
	ErrInvalidHandle = errors.New("invalid handle")
)
