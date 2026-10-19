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

// Unit tests for the actor task arity alignment: ActorHandleImpl.Task derives
// numReturns from the method signature, so void and error-only methods submit
// zero returns instead of writing one dummy object per call.
package api

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

// Noop is a void Counter method used to verify that an actor task with no
// return value submits zero returns.
func (c *Counter) Noop() {}

// Fail is an error-only Counter method: its single return is the trailing
// error, so the non-error return count is zero.
func (c *Counter) Fail() error { return nil }

// arityRecordingSubmitter records the last actor task submission so the test
// can assert the numReturns that ActorHandleImpl.Task derived. It returns no
// object IDs for zero-return submissions, mirroring the submitter contract.
type arityRecordingSubmitter struct {
	submitter.TaskSubmitter

	lastActorID         ids.ActorID
	lastActorNumReturns int
}

func (s *arityRecordingSubmitter) SubmitActorTask(actorID ids.ActorID, functionDescriptor function.FunctionDescriptor, args []function.FunctionArg, numReturns int, options *submitter.TaskOptions) ([]ids.ObjectID, error) {
	s.lastActorID = actorID
	s.lastActorNumReturns = numReturns
	if numReturns == 0 {
		return nil, nil
	}
	return []ids.ObjectID{ids.NilObjectID()}, nil
}

// initArityRuntime installs a recordingHandle whose runtime serves the given
// submitter, so submitTaskRef reaches it. It reuses mockRuntime (and therefore
// recordingRuntime) so the stored handle shares the concrete type already
// installed by the package's other handle-injecting tests: currentHandle is an
// atomic.Value, and storing a second concrete type in the same test binary
// panics.
func initArityRuntime(t *testing.T, sub submitter.TaskSubmitter) {
	t.Helper()
	object.SetSerializer(testSerializer{})
	setHandle(recordingHandle{rt: mockRuntime{sub: sub}})

	// Leave the runtime logically uninitialized for the tests that run after
	// this one, but keep the same concrete handle value installed: clearing
	// currentHandle would re-arm the atomic.Value for a conflicting store by a
	// later test. The package's uninitialized-precondition tests (e.g.
	// TestObjectRefGet_ReturnsErrorObjectException) reset the value themselves.
	t.Cleanup(func() { initialized.Store(false) })
}

// actorTestID is a fixed actor id for handle-based tests that never touch a
// real actor.
func actorTestID(t *testing.T) ids.ActorID {
	t.Helper()
	id, err := ids.ActorIDFromHex("0102030405060708090a0b0c0d0e0f10")
	if err != nil {
		t.Fatalf("failed to build actor id: %v", err)
	}
	return id
}

// TestActorTaskVoidMethodSubmitsZeroReturns verifies that an actor task for a
// void method derives numReturns 0 from the method signature and yields a nil
// result ref, instead of hard-coding 1.
func TestActorTaskVoidMethodSubmitsZeroReturns(t *testing.T) {
	sub := &arityRecordingSubmitter{}
	initArityRuntime(t, sub)

	handle := NewActorHandleImpl[*Counter](actorTestID(t))
	ref, err := handle.Task((*Counter).Noop).Remote()
	if err != nil {
		t.Fatalf("Remote: %v", err)
	}
	if ref != nil {
		t.Fatalf("void actor method should yield a nil result ref, got %v", ref)
	}
	if sub.lastActorNumReturns != 0 {
		t.Fatalf("SubmitActorTask numReturns = %d, want 0 for a void method", sub.lastActorNumReturns)
	}
}

// TestActorTaskErrorOnlyMethodSubmitsZeroReturns verifies the same derivation
// for an error-only method (trailing error counts as zero non-error returns).
func TestActorTaskErrorOnlyMethodSubmitsZeroReturns(t *testing.T) {
	sub := &arityRecordingSubmitter{}
	initArityRuntime(t, sub)

	handle := NewActorHandleImpl[*Counter](actorTestID(t))
	ref, err := handle.Task((*Counter).Fail).Remote()
	if err != nil {
		t.Fatalf("Remote: %v", err)
	}
	if ref != nil {
		t.Fatalf("error-only actor method should yield a nil result ref, got %v", ref)
	}
	if sub.lastActorNumReturns != 0 {
		t.Fatalf("SubmitActorTask numReturns = %d, want 0 for an error-only method", sub.lastActorNumReturns)
	}
}

// TestActorTaskValueMethodSubmitsOneReturn is the positive control: a value
// method still submits exactly one return.
func TestActorTaskValueMethodSubmitsOneReturn(t *testing.T) {
	sub := &arityRecordingSubmitter{}
	initArityRuntime(t, sub)

	handle := NewActorHandleImpl[*Counter](actorTestID(t))
	ref, err := handle.Task((*Counter).Add, 10).Remote()
	if err != nil {
		t.Fatalf("Remote: %v", err)
	}
	if ref == nil {
		t.Fatal("value actor method should yield a result ref")
	}
	if sub.lastActorNumReturns != 1 {
		t.Fatalf("SubmitActorTask numReturns = %d, want 1 for a value method", sub.lastActorNumReturns)
	}
}
