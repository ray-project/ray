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

package logmonitor

import (
	"testing"
	"time"
)

func TestNewWithConfigAppliesManyFilesThresholdOverride(t *testing.T) {
	logsDir := t.TempDir()
	m, err := NewWithConfig("127.0.0.1", logsDir, &recordingPublisher{}, RuntimeConfig{
		ManyFilesThreshold: 1234,
	})
	if err != nil {
		t.Fatalf("NewWithConfig() error = %v", err)
	}
	if m.manyFilesThreshold != 1234 {
		t.Fatalf("manyFilesThreshold = %d, want 1234", m.manyFilesThreshold)
	}
}

func TestNewWithConfigAppliesNumLinesToReadOverride(t *testing.T) {
	logsDir := t.TempDir()
	m, err := NewWithConfig("127.0.0.1", logsDir, &recordingPublisher{}, RuntimeConfig{
		NumLinesToRead: 2345,
	})
	if err != nil {
		t.Fatalf("NewWithConfig() error = %v", err)
	}
	if m.numLinesToRead != 2345 {
		t.Fatalf("numLinesToRead = %d, want 2345", m.numLinesToRead)
	}
}

func TestNewWithConfigAppliesNameUpdateIntervalOverride(t *testing.T) {
	logsDir := t.TempDir()
	m, err := NewWithConfig("127.0.0.1", logsDir, &recordingPublisher{}, RuntimeConfig{
		NameUpdateInterval: 1500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWithConfig() error = %v", err)
	}
	if m.nameUpdateInterval != 1500*time.Millisecond {
		t.Fatalf("nameUpdateInterval = %v, want 1.5s", m.nameUpdateInterval)
	}
}

func TestNewWithConfigKeepsDefaultsForNonPositiveOverrides(t *testing.T) {
	logsDir := t.TempDir()
	m, err := NewWithConfig("127.0.0.1", logsDir, &recordingPublisher{}, RuntimeConfig{
		ManyFilesThreshold: 0,
		NumLinesToRead:     -1,
		NameUpdateInterval: 0,
	})
	if err != nil {
		t.Fatalf("NewWithConfig() error = %v", err)
	}
	if m.manyFilesThreshold != defaultManyFilesThreshold {
		t.Fatalf("manyFilesThreshold = %d, want default %d", m.manyFilesThreshold, defaultManyFilesThreshold)
	}
	if m.numLinesToRead != defaultNumLinesToRead {
		t.Fatalf("numLinesToRead = %d, want default %d", m.numLinesToRead, defaultNumLinesToRead)
	}
	if m.nameUpdateInterval != defaultNameUpdateInterval {
		t.Fatalf("nameUpdateInterval = %v, want default %v", m.nameUpdateInterval, defaultNameUpdateInterval)
	}
}

func TestNewWithConfigKeepsApplyingMaxFilesOpenAndMaxLineSizeOverrides(t *testing.T) {
	logsDir := t.TempDir()
	m, err := NewWithConfig("127.0.0.1", logsDir, &recordingPublisher{}, RuntimeConfig{
		MaxFilesOpen: 321,
		MaxLineSize:  654,
	})
	if err != nil {
		t.Fatalf("NewWithConfig() error = %v", err)
	}
	if m.maxFilesOpen != 321 {
		t.Fatalf("maxFilesOpen = %d, want 321", m.maxFilesOpen)
	}
	if m.maxLineSize != 654 {
		t.Fatalf("maxLineSize = %d, want 654", m.maxLineSize)
	}
}
