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
)

func TestBuildActorResources(t *testing.T) {
	assert.Equal(t, map[string]float64{}, buildActorResources(0, 0, 0, nil))

	got := buildActorResources(2, 1, 4096, map[string]float64{"custom": 3})
	assert.Equal(t, map[string]float64{
		"CPU":    2,
		"GPU":    1,
		"memory": 4096,
		"custom": 3,
	}, got)

	// Only positive values are added.
	got = buildActorResources(-1, 0, -5, nil)
	assert.Equal(t, map[string]float64{}, got)
}

func TestNewActorError(t *testing.T) {
	assert.IsType(t, &runtimeEnvSetupError{}, newActorError("runtime_env_setup", errors.New("x")))
	assert.IsType(t, &actorUnschedulableError{}, newActorError("unschedulable", errors.New("x")))
	assert.IsType(t, &actorDiedError{}, newActorError("died", errors.New("x")))
	assert.IsType(t, &actorError{}, newActorError("unknown", errors.New("x")))
	assert.Contains(t, newActorError("died", errors.New("boom")).Error(), "boom")
}
