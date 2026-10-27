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

// driver_call_cpp is a Ray Go driver that verifies Go -> C++ cross-language
// calls and the C++ function descriptor. It covers:
//
//	Go -> C++ calls: RemoteCpp / RemoteCppVoid / CppTaskCaller for
//	    module-level functions, and RemoteCppActor / CppActorCreator /
//	    CppActorHandle / CppActorTask / CppActorTaskCaller for actors.
//	C++ function descriptor: CppFunctionDescriptor construction,
//	    FunctionDescriptorFromListWithLanguage(LanguageCpp, ...) parsing,
//	    ToList() (3-element), GetLanguage(), and Hash().
//
// The C++ worker must load cpp_xlang_worker.so (which registers "add",
// "log_event", "concat", "echo_list", "echo_map", "echo_student", and the
// "Calculator" actor via "create_actor"); the job's code_search_path must point
// at the directory containing the .so.
//
// Build & submit as a Ray job:
//
//	bazel build //go/e2e/driver_call_cpp:driver_call_cpp
//	bazel build //go/e2e/driver_call_cpp:cpp_xlang_worker.so
//	mkdir -p job_dir
//	cp bazel-bin/go/e2e/driver_call_cpp_/driver_call_cpp job_dir/
//	cp bazel-bin/go/e2e/driver_call_cpp/cpp_xlang_worker.so job_dir/
//	cd job_dir && ray job submit --working-dir . -- ./driver_call_cpp
package main

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/log/zap"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/function"
)

// errGetTimeout marks a Get that did not complete within its budget; it is
// distinct from a task failure, so deadline loops treat it as "not yet
// decided" rather than as evidence of death.
var errGetTimeout = errors.New("Get timed out")

// getWithTimeout bounds a blocking ObjectRef.Get: a Get on a ref whose worker
// hangs (dying but not dead — task neither completes nor errors) never
// returns, which would block a deadline loop past its wall-clock bound. The
// abandoned goroutine leaks until process exit, acceptable in a one-shot
// driver binary.
func getWithTimeout[T any](ref *api.ObjectRef[T], budget time.Duration) (T, error) {
	type result struct {
		val T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := ref.Get()
		ch <- result{v, err}
	}()
	select {
	case r := <-ch:
		return r.val, r.err
	case <-time.After(budget):
		var zero T
		return zero, errGetTimeout
	}
}

// maxE2ETimeout caps the scaled e2e deadline so a huge
// RAY_GO_E2E_TIMEOUT_SCALE cannot overflow time.Duration into a negative
// (shorter) deadline.
const maxE2ETimeout = 24 * time.Hour

// e2eTimeout scales a base deadline by RAY_GO_E2E_TIMEOUT_SCALE (default 1.0,
// floor 1.0): shared clusters export a larger value (e.g. 3) instead of
// editing per-driver deadline constants when waits flake under load. The
// scaled result is capped at maxE2ETimeout so the helper only ever loosens a
// deadline.
func e2eTimeout(base time.Duration) time.Duration {
	s := os.Getenv("RAY_GO_E2E_TIMEOUT_SCALE")
	if s == "" {
		return base
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 1.0 {
		return base
	}
	scaled := float64(base) * f
	if scaled > float64(maxE2ETimeout) {
		return maxE2ETimeout
	}
	return time.Duration(scaled)
}

// Student mirrors the C++ Student struct (name, age) registered in
// cpp_xlang_worker.cc. The Go runtime encodes a fully-exported (cross-language)
// struct as a positional msgpack array to match the C++ MSGPACK_DEFINE(name,
// age) layout, so the field order must match the C++ declaration. The msgpack
// tags only name the fields for the Go-internal map-encoding fallback.
type Student struct {
	Name string `msgpack:"name"`
	Age  int    `msgpack:"age"`
}

func main() {
	if err := zap.SetupDefaultLogger(); err != nil {
		fmt.Printf("ERROR: failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	logger := log.WithName("driver_call_cpp")

	// The C++ worker loads the registered .so files from the job's
	// code_search_path (raylet passes it through as --ray_code_search_path to
	// the C++ default worker). Ship cpp_xlang_worker.so in the --working-dir
	// and point the search path at it so the C++ worker can resolve "add",
	// "log_event", "Calculator", etc.
	cwd, err := os.Getwd()
	if err != nil {
		logger.Error(err, "failed to get current working directory")
		os.Exit(1)
	}
	jobConfig := options.NewJobConfigBuilder().WithCodeSearchPath(cwd)
	if err := api.Instance().InitializeWithJobConfig(jobConfig); err != nil {
		logger.Error(err, "failed to initialize Ray")
		os.Exit(1)
	}
	defer api.Shutdown()

	logger.Info("Ray initialized, running Go->C++ cross-language tests")

	// Module-level functions.
	if err := testCppFunctionDescriptor(); err != nil {
		logger.Error(err, "C++ function descriptor test failed")
		os.Exit(1)
	}
	if err := testAdd(); err != nil {
		logger.Error(err, "add test failed")
		os.Exit(1)
	}
	if err := testVoid(); err != nil {
		logger.Error(err, "log_event void test failed")
		os.Exit(1)
	}
	if err := testCrossLanguageTypes(); err != nil {
		logger.Error(err, "cross-language arg types test failed")
		os.Exit(1)
	}

	// Actor calls.
	if err := testActor(); err != nil {
		logger.Error(err, "Calculator actor test failed")
		os.Exit(1)
	}
	if err := testActorConcurrencyGroup(); err != nil {
		logger.Error(err, "P2 (Calculator actor concurrency group) failed")
		os.Exit(1)
	}

	fmt.Println("\n=== All Go->C++ cross-language tests passed! ===")
}

// testCppFunctionDescriptor verifies CppFunctionDescriptor
// construction, FromListWithLanguage(LanguageCpp) parsing, ToList (3
// elements), GetLanguage and Hash.
func testCppFunctionDescriptor() error {
	// Parse a 3-element C++ descriptor list.
	parsedDesc, err := function.FunctionDescriptorFromListWithLanguage(
		function.LanguageCpp, []string{"add", "GO", ""})
	if err != nil {
		return fmt.Errorf("FunctionDescriptorFromListWithLanguage(LanguageCpp) failed: %w", err)
	}
	cppDesc, ok := parsedDesc.(*function.CppFunctionDescriptor)
	if !ok {
		return fmt.Errorf("expected *CppFunctionDescriptor, got %T", parsedDesc)
	}
	if lang := cppDesc.GetLanguage(); lang != function.LanguageCpp {
		return fmt.Errorf("GetLanguage() = %v, want LanguageCpp", lang)
	}
	fmt.Printf("parsed CppFunctionDescriptor: %+v, language=%v\n", cppDesc, cppDesc.GetLanguage())

	// ToList returns the 3-element [functionName, caller, className].
	list := cppDesc.ToList()
	if len(list) != 3 {
		return fmt.Errorf("ToList() has %d elements, want 3: %v", len(list), list)
	}
	if list[0] != "add" || list[1] != "GO" || list[2] != "" {
		return fmt.Errorf("ToList() = %v, want [add GO ]", list)
	}
	fmt.Printf("ToList() = %v (3 elements, matches Java CppFunctionDescriptor.toList())\n", list)

	// Hash is stable and consistent with the Go hashStrings scheme.
	h1 := cppDesc.Hash()
	h2 := cppDesc.Hash()
	if h1 != h2 {
		return fmt.Errorf("Hash() not stable: %d vs %d", h1, h2)
	}
	// Same fields must hash identically when constructed independently.
	desc2, err := function.NewCppFunctionDescriptor("add", "GO", "")
	if err != nil {
		return fmt.Errorf("NewCppFunctionDescriptor failed: %w", err)
	}
	if h1 != desc2.Hash() {
		return fmt.Errorf("Hash() differs across equivalent descriptors: %d vs %d", h1, desc2.Hash())
	}
	fmt.Printf("Hash() = %d (stable)\n", h1)

	// NewCppFunctionDescriptorOrUnknown must never fail and substitutes
	// "unknown" for an invalid function name. C++ function names are more
	// permissive than Go identifiers (qualified names, template args etc. are
	// legal), so only a whitespace-bearing or empty name is invalid here. The
	// caller is NOT substituted: an empty caller is a valid value that
	// identifies a native C++ function descriptor (see NewCppFunctionDescriptor
	// and TestNewCppFunctionDescriptorOrUnknown), so it must pass through
	// unchanged rather than being corrupted to "unknown".
	desc := function.NewCppFunctionDescriptorOrUnknown("bad name", "", "X")
	if desc.CreateFunctionName != "unknown" {
		return fmt.Errorf("OrUnknown function-name substitution mismatch: %+v", desc)
	}
	if desc.Caller != "" {
		return fmt.Errorf("OrUnknown corrupted a valid empty caller: %+v", desc)
	}

	// Reject malformed input: a 4-element list is invalid for C++ descriptors.
	if _, err := function.FunctionDescriptorFromListWithLanguage(
		function.LanguageCpp, []string{"add", "GO", "", "extra"}); err == nil {
		return fmt.Errorf("expected error for 4-element C++ descriptor list")
	}

	// NewCppFunctionDescriptor validates the function name (required) but
	// accepts an empty caller: an empty caller is the native C++ descriptor
	// form (see TestNewCppFunctionDescriptor_EmptyCaller), not an invalid field.
	if _, err := function.NewCppFunctionDescriptor("", "GO", ""); err == nil {
		return fmt.Errorf("expected error for empty function name")
	}
	if desc, err := function.NewCppFunctionDescriptor("add", "", ""); err != nil {
		return fmt.Errorf("empty caller must be accepted as a native C++ descriptor, got error: %w", err)
	} else if desc.Caller != "" {
		return fmt.Errorf("empty caller must pass through unchanged: %+v", desc)
	}
	return nil
}

// testAdd verifies that Go calls the C++ module-level "add" function with two
// ints and receives 30.
func testAdd() error {
	caller := api.RemoteCpp[int]("add", "")
	ref, err := caller.Call(10, 20)
	if err != nil {
		return fmt.Errorf("call add failed: %w", err)
	}
	result, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("get add result failed: %w", err)
	}
	fmt.Printf("add(10, 20) = %d (expected 30)\n", result)
	if result != 30 {
		return fmt.Errorf("add(10, 20) = %d, want 30", result)
	}
	return nil
}

// testVoid verifies that a void C++ function executes successfully and the
// returned ObjectRef is nil with no error.
func testVoid() error {
	caller := api.RemoteCppVoid("log_event", "")
	ref, err := caller.Call("task-started")
	if err != nil {
		return fmt.Errorf("call log_event failed: %w", err)
	}
	// Compare ref to nil directly; do not %v-print the possibly-nil
	// *ObjectRef[struct{}] value, which exercises String() on a ref whose
	// objectID may be an invalid zero value.
	fmt.Printf("log_event returned ref==nil: %v\n", ref == nil)
	if ref != nil {
		return fmt.Errorf("log_event ObjectRef != nil, want nil")
	}
	return nil
}

// testCrossLanguageTypes verifies cross-language serializable types
// (string, int slice, string->int map, fully-exported struct) round-trip
// through the C++ worker.
func testCrossLanguageTypes() error {
	// string round-trip.
	concatRef, err := api.RemoteCpp[string]("concat", "").Call("foo", "bar")
	if err != nil {
		return fmt.Errorf("call concat failed: %w", err)
	}
	concatResult, err := api.Get(concatRef)
	if err != nil {
		return fmt.Errorf("get concat result failed: %w", err)
	}
	fmt.Printf("concat(foo, bar) = %q (expected foobar)\n", concatResult)
	if concatResult != "foobar" {
		return fmt.Errorf("concat = %q, want foobar", concatResult)
	}

	// int slice round-trip.
	listRef, err := api.RemoteCpp[[]int]("echo_list", "").Call([]int{1, 2, 3})
	if err != nil {
		return fmt.Errorf("call echo_list failed: %w", err)
	}
	listResult, err := api.Get(listRef)
	if err != nil {
		return fmt.Errorf("get echo_list result failed: %w", err)
	}
	fmt.Printf("echo_list([1 2 3]) = %v\n", listResult)
	if len(listResult) != 3 || listResult[0] != 1 || listResult[1] != 2 || listResult[2] != 3 {
		return fmt.Errorf("echo_list = %v, want [1 2 3]", listResult)
	}

	// string -> int map round-trip.
	mapRef, err := api.RemoteCpp[map[string]int]("echo_map", "").
		Call(map[string]int{"a": 1, "b": 2})
	if err != nil {
		return fmt.Errorf("call echo_map failed: %w", err)
	}
	mapResult, err := api.Get(mapRef)
	if err != nil {
		return fmt.Errorf("get echo_map result failed: %w", err)
	}
	fmt.Printf("echo_map({a:1 b:2}) = %v\n", mapResult)
	if mapResult["a"] != 1 || mapResult["b"] != 2 {
		return fmt.Errorf("echo_map = %v, want {a:1 b:2}", mapResult)
	}

	// fully-exported struct round-trip.
	studentRef, err := api.RemoteCpp[Student]("echo_student", "").
		Call(Student{Name: "tom", Age: 20})
	if err != nil {
		return fmt.Errorf("call echo_student failed: %w", err)
	}
	studentResult, err := api.Get(studentRef)
	if err != nil {
		return fmt.Errorf("get echo_student result failed: %w", err)
	}
	fmt.Printf("echo_student({tom 20}) = %+v\n", studentResult)
	if studentResult.Name != "tom" || studentResult.Age != 20 {
		return fmt.Errorf("echo_student = %+v, want {tom 20}", studentResult)
	}
	return nil
}

// testActor verifies the C++ "Calculator" actor: create it, call its "add"
// method, then exercise the handle's Kill / KillWithNoRestart.
func testActor() error {
	// Create the actor; the handle ID must be non-empty.
	handle, err := api.RemoteCppActor("create_actor", "Calculator").Create(0)
	if err != nil {
		return fmt.Errorf("create Calculator actor failed: %w", err)
	}
	defer func() { _ = api.KillActor(handle, true, true) }()
	if handle.ID().IsNil() {
		return fmt.Errorf("Calculator actor handle ID is nil")
	}
	fmt.Printf("created Calculator actor, ID=%v\n", handle.ID())

	// Call the "add" actor method.
	ref, err := api.CppActorTask[int](handle, "add", 5).Remote()
	if err != nil {
		return fmt.Errorf("call Calculator.add failed: %w", err)
	}
	result, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("get Calculator.add result failed: %w", err)
	}
	fmt.Printf("Calculator.add(5) = %d (expected 5)\n", result)
	if result != 5 {
		return fmt.Errorf("Calculator.add(5) = %d, want 5", result)
	}

	// A second call must observe accumulated state (value 5 + 10 = 15).
	ref2, err := api.CppActorTask[int](handle, "add", 10).Remote()
	if err != nil {
		return fmt.Errorf("second Calculator.add failed: %w", err)
	}
	result2, err := api.Get(ref2)
	if err != nil {
		return fmt.Errorf("get second Calculator.add result failed: %w", err)
	}
	fmt.Printf("Calculator.add(10) after add(5) = %d (expected 15)\n", result2)
	if result2 != 15 {
		return fmt.Errorf("Calculator.add(10) after add(5) = %d, want 15", result2)
	}

	// KillWithNoRestart terminates the actor; subsequent calls must fail.
	if err := handle.KillWithNoRestart(); err != nil {
		return fmt.Errorf("Calculator.KillWithNoRestart failed: %w", err)
	}

	// CoreWorker::KillActor is asynchronous (GCS -> raylet -> worker SIGKILL),
	// so poll until a method call fails, proving both the kill and no-restart
	// semantics. A call succeeding while the worker is still dying is fine.
	deadline := time.Now().Add(e2eTimeout(5 * time.Second))
	for {
		deadRef, err := api.CppActorTask[int](handle, "get_value").Remote()
		if err == nil {
			_, err = getWithTimeout(deadRef, time.Until(deadline))
		}
		if err != nil && !errors.Is(err, errGetTimeout) {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("expected C++ actor to be unavailable after KillWithNoRestart()")
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Printf("call after KillWithNoRestart correctly failed: %v\n", err)
	return nil
}

// testActorConcurrencyGroup verifies the P2 fix for the C++ actor concurrency
// group path:
//
//  1. CppActorCreator.WithConcurrencyGroups declares a named group on a C++ actor;
//     the group (name + max_concurrency) is carried through the creation options
//     and registered on the C++ worker's ConcurrencyGroupManager.
//  2. CppActorTaskCaller.WithConcurrencyGroup routes a method call into that
//     group by name (C++ actor methods cannot auto-match a group via their
//     descriptor, so explicit by-name routing is the supported path).
//
// Two concurrent sleep(1500) calls in a MaxCalls=1 group must serialize:
// ~3s total instead of ~1.5s. A missing group registration would instead fail
// the call (C++ RAY_CHECK in ConcurrencyGroupManager::GetExecutor).
func testActorConcurrencyGroup() error {
	// 1. Declare the group at creation time (Methods intentionally omitted:
	// C++ actor tasks are routed by name, never by method descriptor).
	handle, err := api.RemoteCppActor("create_actor", "Calculator").
		WithConcurrencyGroups(api.ConcurrencyGroup{Name: "cg_sleep", MaxCalls: 1}).
		Create(0)
	if err != nil {
		return fmt.Errorf("create Calculator actor with concurrency group failed: %w", err)
	}
	defer func() { _ = api.KillActor(handle, true, true) }()
	fmt.Printf("P2 created Calculator actor with group cg_sleep (MaxCalls=1)\n")

	// 2. Route two sleeping calls into the group by name and submit them
	// back-to-back so they overlap on the client side.
	start := time.Now()
	refA, err := api.CppActorTask[int](handle, "sleep", 1500).WithConcurrencyGroup("cg_sleep").Remote()
	if err != nil {
		return fmt.Errorf("submit sleep A failed: %w", err)
	}
	refB, err := api.CppActorTask[int](handle, "sleep", 1500).WithConcurrencyGroup("cg_sleep").Remote()
	if err != nil {
		return fmt.Errorf("submit sleep B failed: %w", err)
	}
	if _, err := api.Get(refA); err != nil {
		return fmt.Errorf("get sleep A failed: %w", err)
	}
	if _, err := api.Get(refB); err != nil {
		return fmt.Errorf("get sleep B failed: %w", err)
	}
	elapsed := time.Since(start)
	fmt.Printf("P2 cg_sleep: two concurrent sleep(1500ms) took %v (serialized if >= 2.2s)\n", elapsed)
	if elapsed < 2200*time.Millisecond {
		return fmt.Errorf("cg_sleep did not serialize: two sleep(1500) finished in %v, want >= 2.2s", elapsed)
	}

	// A call outside any declared group (no WithConcurrencyGroup) must still
	// succeed on the default executor, so the group declaration does not break
	// the ordinary C++ actor path.
	ref, err := api.CppActorTask[int](handle, "add", 5).Remote()
	if err != nil {
		return fmt.Errorf("un-routed Calculator.add failed after group declaration: %w", err)
	}
	result, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("get un-routed Calculator.add failed: %w", err)
	}
	if result != 5 {
		return fmt.Errorf("un-routed Calculator.add = %d, want 5", result)
	}
	fmt.Printf("P2 un-routed Calculator.add(5) = %d on default executor\n", result)
	return nil
}
