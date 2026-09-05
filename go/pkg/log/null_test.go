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
	"errors"
	"testing"

	"github.com/go-logr/logr"
)

func TestNullLogSink_Info(t *testing.T) {
	sink := &NullLogSink{}
	sink.Info(0, "test message", "key", "value")
}

func TestNullLogSink_Error(t *testing.T) {
	sink := &NullLogSink{}
	sink.Error(errors.New("test error"), "test message", "key", "value")
}

func TestNullLogSink_Enabled(t *testing.T) {
	sink := &NullLogSink{}
	if sink.Enabled(0) {
		t.Error("NullLogSink.Enabled should return false")
	}
	if sink.Enabled(1) {
		t.Error("NullLogSink.Enabled should return false for any level")
	}
}

func TestNullLogSink_WithName_ReturnsSelf(t *testing.T) {
	sink := &NullLogSink{}
	result := sink.WithName("test")
	if result != sink {
		t.Error("WithName should return self")
	}
}

func TestNullLogSink_WithValues_ReturnsSelf(t *testing.T) {
	sink := &NullLogSink{}
	result := sink.WithValues("key", "value")
	if result != sink {
		t.Error("WithValues should return self")
	}
}

func TestNullLogSink_Init(t *testing.T) {
	sink := &NullLogSink{}
	sink.Init(logr.RuntimeInfo{})
}
