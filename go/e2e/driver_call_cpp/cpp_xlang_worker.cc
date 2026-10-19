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

// cpp_xlang_worker registers the C++ functions and actors exercised by the
// driver_call_cpp Go driver over the Go -> C++ cross-language channel.
//
// Every remote function is registered with the exact name the Go driver
// submits in its CppFunctionDescriptor ("add", "log_event", "concat",
// "echo_list", "echo_map", "echo_student", and the "Calculator" actor via
// "create_actor"). The C++ worker detects a cross-language invocation by a
// non-empty Caller() field and strips the 9-byte length header before
// msgpack-decoding each argument (task_executor.cc:180-183).
//
// Build & deploy:
//
//	bazel build //go/e2e/driver_call_cpp:cpp_xlang_worker.so
//	cp bazel-bin/go/e2e/driver_call_cpp/cpp_xlang_worker.so <job_dir>/
//
// The .so must be reachable via the job's code_search_path so the C++ worker
// loads it through FunctionHelper::LoadFunctionsFromPaths (RAY_REMOTE macros
// populate remote_funcs_ / remote_member_funcs_).
#include <ray/api.h>

#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <thread>
#include <vector>

/// Cross-language user-defined struct. The field names must serialize to the
/// same msgpack keys that the Go side emits for its exported struct fields.
struct Student {
  std::string name;
  int age;
  MSGPACK_DEFINE(name, age);
};

/// ---- Module-level functions (Go -> C++ task calls) ----

/// Two-int add returning an int.
int add(int x, int y) { return x + y; }

/// A void function that executes successfully (no return value).
void log_event(const std::string &event) { (void)event; }

/// String round-trip.
std::string concat(const std::string &a, const std::string &b) { return a + b; }

/// Int slice round-trip.
std::vector<int> echo_list(const std::vector<int> &list) { return list; }

/// String -> int map round-trip.
std::map<std::string, int> echo_map(const std::map<std::string, int> &entries) {
  return entries;
}

/// Fully-exported struct round-trip.
Student echo_student(const Student &s) { return s; }

/// ---- Actor (Go -> C++ actor calls) ----

/// Calculator is the cross-language actor class referenced by the Go driver.
class Calculator {
 public:
  int value;

  explicit Calculator(int init) { value = init; }

  /// Actor method returning an int.
  int add(int x) {
    value += x;
    return value;
  }

  int multiply(int x) {
    value *= x;
    return value;
  }

  int get_value() { return value; }

  /// sleep blocks this instance for ms milliseconds and returns the current
  /// value. It lets the Go driver verify that a concurrency group declared on a
  /// C++ actor (CppActorCreator.WithConcurrencyGroups) actually serializes
  /// method calls routed to it by name (CppActorTaskCaller.WithConcurrencyGroup):
  /// two concurrent sleep(1500) calls in a MaxCalls=1 group must take ~3s total
  /// instead of ~1.5s.
  int sleep(int ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    return value;
  }
};

/// Actor factory function. The Go driver submits the creation descriptor
/// with functionName "create_actor" (api.RemoteCppActor("create_actor",
/// "Calculator")), and the C++ worker's ACTOR_CREATION_TASK branch looks the
/// name up in the plain (non-member) function table as-is. Registering a
/// global factory under the bare "create_actor" name makes the lookup match
/// without relying on any "ClassName::" qualification in the C++ runtime.
///
/// Ownership note: the created actor is intentionally leaked here on purpose.
/// The C++ runtime serializes only the actor's address (uint64_t) for
/// cross-language calls and never takes ownership of the object, so there is
/// no owner that could safely delete it; the object lives for the process
/// lifetime, matching how the actor handle round-trips the raw pointer.
Calculator *create_actor(int init) { return new Calculator(init); }

RAY_REMOTE(add, log_event, concat, echo_list, echo_map, echo_student);

RAY_REMOTE(create_actor,
           &Calculator::add,
           &Calculator::multiply,
           &Calculator::get_value,
           &Calculator::sleep);
