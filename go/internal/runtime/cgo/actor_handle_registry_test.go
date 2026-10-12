//go:build cgo
// +build cgo

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

package cgo

import (
	"fmt"
	"sync"
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
)

func TestActorHandleRegistryAddContains(t *testing.T) {
	registry := &createdActorIDs{}
	id := newTestActorID(t, "0102030405060708090a0b0c0d0e0f10")
	if registry.contains(id) {
		t.Fatal("fresh registry should not contain any actor ID")
	}
	registry.add(id)
	if !registry.contains(id) {
		t.Fatal("registry should contain actor ID after add")
	}
}

func TestActorHandleRegistryDistinctIDs(t *testing.T) {
	registry := &createdActorIDs{}
	idA := newTestActorID(t, "1112131415161718191a1b1c1d1e1f20")
	idB := newTestActorID(t, "2122232425262728292a2b2c2d2e2f30")
	registry.add(idA)
	if registry.contains(idB) {
		t.Fatal("registry should not conflate distinct actor IDs")
	}
}

func TestActorHandleRegistryZeroValue(t *testing.T) {
	// createdActorIDSet is a package-level zero-value singleton; the zero value of
	// the embedded sync.Map is ready to use.
	registry := &createdActorIDs{}
	id := newTestActorID(t, "3132333435363738393a3b3c3d3e3f40")
	registry.add(id)
	if !registry.contains(id) {
		t.Fatal("zero-value registry should accept add/contains")
	}
}

func TestActorHandleRegistryConcurrent(t *testing.T) {
	registry := &createdActorIDs{}
	const goroutines = 32
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := newTestActorID(t, fmt.Sprintf("%032x", i))
			registry.add(id)
			if !registry.contains(id) {
				t.Errorf("concurrent add/contains lost actor %d", i)
			}
		}(i)
	}
	wg.Wait()
}

func newTestActorID(t *testing.T, hexStr string) ids.ActorID {
	t.Helper()
	id, err := ids.ActorIDFromHex(hexStr)
	if err != nil {
		t.Fatalf("ActorIDFromHex(%q) failed: %v", hexStr, err)
	}
	return id
}
