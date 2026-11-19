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

package job

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildActorResources(t *testing.T) {
	assert.Equal(t, map[string]float64{"CPU": 0}, buildActorResources(0, 0, 0, nil))

	got := buildActorResources(2, 1, 4096, map[string]float64{"custom": 3})
	assert.Equal(t, map[string]float64{
		"CPU":    2,
		"GPU":    1,
		"memory": 4096,
		"custom": 3,
	}, got)

	// A negative CPU request is clamped to zero rather than dropped, while the
	// other non-positive quantities are still dropped.
	got = buildActorResources(-1, 0, -5, nil)
	assert.Equal(t, map[string]float64{"CPU": 0}, got)
}

// TestBuildActorResourcesKeepsTheCPUBudgetExplicit pins why a zero request is
// still materialized. An empty map asks the C++ actor bridge for its defaults,
// which charge one CPU for placement, and the job supervisor is pinned to a head
// node that has a single CPU to give; forwarding the zero instead makes the CPU
// requirement explicit and zero on both maps, as the reference implementation
// does for a job that names no resources.
func TestBuildActorResourcesKeepsTheCPUBudgetExplicit(t *testing.T) {
	got := buildActorResources(0, 0, 0, nil)
	cpus, ok := got["CPU"]
	require.True(t, ok, "the CPU key must be present for a zero request, or the bridge falls back to a placement requirement of 1 CPU")
	assert.Equal(t, float64(0), cpus)

	// A custom resource must not resurrect the default either: the zero CPU is
	// what selects the specified-resources branch of the bridge.
	got = buildActorResources(0, 0, 0, map[string]float64{"custom": 2})
	assert.Equal(t, float64(0), got["CPU"])
	assert.Equal(t, float64(2), got["custom"])
}

func TestNewActorError(t *testing.T) {
	assert.IsType(t, &runtimeEnvSetupError{}, newActorError("runtime_env_setup", errors.New("x")))
	assert.IsType(t, &actorUnschedulableError{}, newActorError("unschedulable", errors.New("x")))
	assert.IsType(t, &actorDiedError{}, newActorError("died", errors.New("x")))
	assert.IsType(t, &actorError{}, newActorError("unknown", errors.New("x")))
	assert.Contains(t, newActorError("died", errors.New("boom")).Error(), "boom")
}
