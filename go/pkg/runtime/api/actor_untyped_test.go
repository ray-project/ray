//go:build apiuntyped

// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Regression tests for the untyped actor convenience API (Ray.Actor /
// api.Instance().Actor with T = interface{}). The typed migration of Actor[T]
// derived the "<init>" descriptor from the type parameter T; for the untyped
// entry point T is interface{} and would degrade the descriptor to "unknown",
// so the actor was created but never constructed. These tests pin the restored
// behavior: the untyped path derives the descriptor from the actorClass
// argument (OSS's original behavior) and constructs a real instance.
//
// The tests run the real local-mode runtime, so they are gated behind the
// "apiuntyped" build tag because InitLocal mutates package-global handle state
// that the default api tests assume is unset; run them in isolation with:
//
//	go test -tags apiuntyped ./pkg/runtime/api/ -run UntypedActor
package api_test

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	_ "github.com/ray-project/ray/go/pkg/runtime/local" // register the local-mode initializer
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

// untypedAuditCounter is a stateful actor type used by the untyped-path tests.
type untypedAuditCounter struct {
	value int
}

// Inc increments the counter and returns the new value.
func (c *untypedAuditCounter) Inc() int {
	c.value++
	return c.value
}

// newUntypedAuditCounter is the 1-arg constructor factory, registered
// explicitly like the OSS examples register their actor constructors.
func newUntypedAuditCounter(initial int) *untypedAuditCounter {
	return &untypedAuditCounter{value: initial}
}

// assertInstanceConstructed verifies via the local-mode submitter that a live
// actor instance exists for id. GetActorHandle only returns a handle when the
// instance was actually constructed.
func assertInstanceConstructed(t *testing.T, id ids.ActorID) {
	t.Helper()
	rt := api.Internal().Runtime()
	if rt == nil {
		t.Fatal("runtime is nil after InitLocal")
	}
	ts := rt.GetTaskSubmitter()
	if ts == nil {
		t.Fatal("task submitter is nil after InitLocal")
	}
	sub, ok := ts.(submitter.TaskSubmitter)
	if !ok {
		t.Fatalf("unexpected submitter type %T", ts)
	}
	ah, err := sub.GetActorHandle(id)
	if err != nil {
		t.Fatalf("actor instance was not constructed: %v", err)
	}
	if ah == nil {
		t.Fatal("actor handle is nil even though instance exists")
	}
}

// TestUntypedActorConstructsInstance verifies that the untyped convenience path
// (Ray.Actor / api.Instance().Actor with T = interface{}) constructs a real
// actor instance when the constructor is registered explicitly via
// api.RegisterActorClass, matching OSS's original behavior.
func TestUntypedActorConstructsInstance(t *testing.T) {
	if err := api.InitLocal(); err != nil {
		t.Fatalf("InitLocal failed: %v", err)
	}
	defer api.Shutdown()

	// Mirror the OSS examples (e.g. actor_probe): the constructor is registered
	// explicitly, and the untyped convenience path must resolve it.
	if err := api.RegisterActorClass((*untypedAuditCounter)(nil), newUntypedAuditCounter); err != nil {
		t.Fatalf("RegisterActorClass failed: %v", err)
	}

	h, err := api.Instance().Actor(&untypedAuditCounter{}).Create(42)
	if err != nil {
		t.Fatalf("Instance().Actor(&untypedAuditCounter{}).Create(42) failed: %v", err)
	}
	if h == nil {
		t.Fatal("untyped Actor create returned nil handle")
	}

	assertInstanceConstructed(t, h.ID())
}

// TestTypedActorStillConstructsInstance guards the INT-aligned typed path: it
// must keep constructing the instance after the untyped shim is introduced.
func TestTypedActorStillConstructsInstance(t *testing.T) {
	if err := api.InitLocal(); err != nil {
		t.Fatalf("InitLocal failed: %v", err)
	}
	defer api.Shutdown()

	if err := api.RegisterActorClass((*untypedAuditCounter)(nil), newUntypedAuditCounter); err != nil {
		t.Fatalf("RegisterActorClass failed: %v", err)
	}

	h, err := api.Actor[*untypedAuditCounter]((*untypedAuditCounter)(nil)).Create(42)
	if err != nil {
		t.Fatalf("Actor[*untypedAuditCounter](nil).Create(42) failed: %v", err)
	}
	if h == nil {
		t.Fatal("typed Actor create returned nil handle")
	}

	assertInstanceConstructed(t, h.ID())
}
