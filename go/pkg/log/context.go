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


package log

import (
	"context"

	"github.com/go-logr/logr"
)

// loggerKey is the key type for context storage.
type loggerKey struct{}

// FromContext retrieves a Logger from context.
// If no Logger is found in context, returns the global Log.
func FromContext(ctx context.Context) logr.Logger {
	if l, ok := ctx.Value(loggerKey{}).(logr.Logger); ok {
		return l
	}
	return Log
}

// IntoContext stores a Logger into context.
func IntoContext(ctx context.Context, l logr.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// FromContextOrDiscard retrieves a Logger from context.
// If no Logger is found, returns a Discard Logger (no output).
func FromContextOrDiscard(ctx context.Context) logr.Logger {
	if l, ok := ctx.Value(loggerKey{}).(logr.Logger); ok {
		return l
	}
	return logr.Discard()
}