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

package serializer

import (
	"sync"
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
)

// RandomObjectID generates a new random ObjectID for testing.
func RandomObjectID() ids.ObjectID {
	return ids.NewObjectID()
}

// TestSerializationContext_Creation tests context creation.
func TestSerializationContext_Creation(t *testing.T) {
	ctx := NewSerializationContext()
	if ctx == nil {
		t.Fatal("NewSerializationContext() returned nil")
	}
	if ctx.containedObjectIDs == nil {
		t.Error("containedObjectIDs map should be initialized")
	}
}

// TestSerializationContext_Clear tests clearing context.
func TestSerializationContext_Clear(t *testing.T) {
	ctx := NewSerializationContext()

	// Add some test data
	oid := RandomObjectID()
	ctx.containedObjectIDs[oid] = true
	ctx.outerObjectID = oid

	ctx.Clear()

	if len(ctx.containedObjectIDs) != 0 {
		t.Error("Cleared context should have empty containedObjectIDs")
	}
	if !ctx.outerObjectID.IsNil() {
		t.Error("Cleared context should have nil outerObjectID")
	}
}

// TestContextManager_GetContext tests getting context.
func TestContextManager_GetContext(t *testing.T) {
	cm := NewContextManager()
	ctx := cm.GetContext()
	if ctx == nil {
		t.Fatal("GetContext() returned nil")
	}

	// Should return same context for same goroutine
	ctx2 := cm.GetContext()
	if ctx != ctx2 {
		t.Error("GetContext() should return same context for same goroutine")
	}
}

// TestContextManager_ReturnContext tests returning context.
func TestContextManager_ReturnContext(t *testing.T) {
	cm := NewContextManager()
	ctx := cm.GetContext()
	cm.ReturnContext(ctx)
	// Should not panic
}

// TestContextManager_ConcurrentAccess tests concurrent context access.
func TestContextManager_ConcurrentAccess(t *testing.T) {
	cm := NewContextManager()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ctx := cm.GetContext()
				if ctx == nil {
					t.Error("GetContext() returned nil")
				}
				cm.ReturnContext(ctx)
			}
		}()
	}
	wg.Wait()
}

// TestGetGoroutineID tests goroutine ID extraction.
func TestGetGoroutineID(t *testing.T) {
	id := getGoroutineID()
	if id == 0 {
		t.Error("getGoroutineID() should return non-zero ID")
	}
}

// BenchmarkContextManager_GetContext benchmarks context retrieval.
func BenchmarkContextManager_GetContext(b *testing.B) {
	cm := NewContextManager()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := cm.GetContext()
		cm.ReturnContext(ctx)
	}
}
