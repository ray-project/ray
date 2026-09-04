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
	"testing"

	"github.com/go-logr/logr"
)

func TestFromContext_NoLogger(t *testing.T) {
	// Reset Log to its initial state.
	Log = logr.New(NewDelegatingLogSink())

	ctx := context.Background()
	logger := FromContext(ctx)
	// Should return the global Log when no logger is in the context.
	if logger.GetSink() == nil {
		t.Error("FromContext should return global Log when no logger in context")
	}
}

func TestFromContext_WithLogger(t *testing.T) {
	ctx := context.Background()
	testSink := &testLogSink{}
	testLogger := logr.New(testSink)

	ctxWithLogger := IntoContext(ctx, testLogger)
	retrievedLogger := FromContext(ctxWithLogger)
	retrievedLogger.Info("test message", "key", "value")

	if testSink.lastMsg != "test message" {
		t.Errorf("expected 'test message', got '%s'", testSink.lastMsg)
	}
}

func TestFromContextOrDiscard_NoLogger(t *testing.T) {
	ctx := context.Background()
	logger := FromContextOrDiscard(ctx)
	logger.Info("test message")
}

func TestFromContextOrDiscard_WithLogger(t *testing.T) {
	ctx := context.Background()
	testSink := &testLogSink{}
	testLogger := logr.New(testSink)

	ctxWithLogger := IntoContext(ctx, testLogger)
	retrievedLogger := FromContextOrDiscard(ctxWithLogger)
	retrievedLogger.Info("test message", "key", "value")

	if testSink.lastMsg != "test message" {
		t.Errorf("expected 'test message', got '%s'", testSink.lastMsg)
	}
}

func TestIntoContext(t *testing.T) {
	ctx := context.Background()
	testSink := &testLogSink{}
	testLogger := logr.New(testSink)

	newCtx := IntoContext(ctx, testLogger)
	if newCtx == ctx {
		t.Error("IntoContext should return new context")
	}

	retrievedLogger, ok := newCtx.Value(loggerKey{}).(logr.Logger)
	if !ok {
		t.Error("context should contain logger")
	}
	if retrievedLogger.GetSink() != testLogger.GetSink() {
		t.Error("retrieved logger should match stored logger")
	}
}

func TestFromContext_ReturnsGlobalLog(t *testing.T) {
	// Reset Log to its initial state.
	Log = logr.New(NewDelegatingLogSink())

	ctx := context.Background()
	logger := FromContext(ctx)

	// Should return the global Log's sink.
	if logger.GetSink() != Log.GetSink() {
		t.Error("FromContext should return global Log when context has no logger")
	}
}

func TestIntoContext_Overwrite(t *testing.T) {
	ctx := context.Background()

	testSink1 := &testLogSink{}
	testLogger1 := logr.New(testSink1)

	testSink2 := &testLogSink{}
	testLogger2 := logr.New(testSink2)

	// First set.
	ctx1 := IntoContext(ctx, testLogger1)
	// Second set, overwriting the first.
	ctx2 := IntoContext(ctx1, testLogger2)

	retrievedLogger := FromContext(ctx2)
	retrievedLogger.Info("test")

	if testSink2.lastMsg != "test" {
		t.Error("expected second logger to be used")
	}
}
