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

package api

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/stretchr/testify/assert"
)

// TestObjectIDGenerator_GenerateObjectIDIndexRange verifies that
// GenerateObjectID never produces an index of 0, which would be rejected by
// ids.ObjectIDFromIndex (panic "invalid object index"). This regressed when
// api.Put was first exercised end-to-end against a real cluster.
func TestObjectIDGenerator_GenerateObjectIDIndexRange(t *testing.T) {
	g := NewObjectIDGenerator(ids.JobID{})

	// The first generated index must be >= 1; ObjectIDFromIndex panics on 0.
	oid, err := g.GenerateObjectID()
	assert.NoError(t, err)
	assert.NotEqual(t, ids.NilObjectID(), *oid)

	// Multiple calls must keep producing valid, distinct object IDs.
	// ObjectIDFromIndex supports indices 1..MaxObjectIndex; the index occupies
	// 32 bits of the ObjectID (ObjectIDIndexSize=32 bits, aligned with C++
	// kObjectIdIndexSize). The first call above already consumed index 1, so the
	// loop can generate up to MaxObjectIndex-1 more distinct indices without
	// hitting the 32-bit upper bound.
	seen := make(map[ids.ObjectID]bool)
	for i := 0; i < 100; i++ {
		oid, err := g.GenerateObjectID()
		assert.NoError(t, err)
		assert.NotEqual(t, ids.NilObjectID(), *oid)
		seen[*oid] = true
	}
	assert.Equal(t, 100, len(seen), "each generated ObjectID must be unique")
}
