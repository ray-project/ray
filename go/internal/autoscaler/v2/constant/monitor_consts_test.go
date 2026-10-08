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

package constant

import (
	"math"
	"testing"
)

// TestEnvInteger verifies that env var integer parsing aligns with the Python
// env_integer semantics.
func TestEnvInteger(t *testing.T) {
	t.Run("unset env var returns the default value", func(t *testing.T) {
		got := envInteger("TEST_ENV_INTEGER_KEY_UNSET", 5)
		if got != 5 {
			t.Fatalf("expected default 5, got %d", got)
		}
	})

	t.Run("empty env var falls back to the default value", func(t *testing.T) {
		t.Setenv("TEST_ENV_INTEGER_KEY", "")
		got := envInteger("TEST_ENV_INTEGER_KEY", 5)
		if got != 5 {
			t.Fatalf("expected fallback 5, got %d", got)
		}
	})

	t.Run("valid integer env var", func(t *testing.T) {
		t.Setenv("TEST_ENV_INTEGER_KEY", "42")
		got := envInteger("TEST_ENV_INTEGER_KEY", 5)
		if got != 42 {
			t.Fatalf("expected 42, got %d", got)
		}
	})

	t.Run("inf env var returns MaxInt", func(t *testing.T) {
		t.Setenv("TEST_ENV_INTEGER_KEY", "inf")
		got := envInteger("TEST_ENV_INTEGER_KEY", 5)
		if got != math.MaxInt {
			t.Fatalf("expected MaxInt, got %d", got)
		}
	})

	t.Run("invalid env var falls back to the default value", func(t *testing.T) {
		t.Setenv("TEST_ENV_INTEGER_KEY", "not-a-number")
		got := envInteger("TEST_ENV_INTEGER_KEY", 5)
		if got != 5 {
			t.Fatalf("expected fallback 5, got %d", got)
		}
	})
}

// TestAutoscalerDefaults verifies the defaults align with the Python constants
// (AUTOSCALER_METRIC_PORT=44217, AUTOSCALER_UPDATE_INTERVAL_S=5).
func TestAutoscalerDefaults(t *testing.T) {
	// t.Setenv restores the environment after the test, but the package-level
	// vars were already read at init time. Clear the env vars here and verify
	// the package-level defaults were not polluted by the external environment.
	if AUTOSCALER_METRIC_PORT != 44217 {
		t.Errorf("expected default AUTOSCALER_METRIC_PORT 44217, got %d", AUTOSCALER_METRIC_PORT)
	}
	if AUTOSCALER_UPDATE_INTERVAL_S != 5 {
		t.Errorf("expected default AUTOSCALER_UPDATE_INTERVAL_S 5, got %d", AUTOSCALER_UPDATE_INTERVAL_S)
	}
}
