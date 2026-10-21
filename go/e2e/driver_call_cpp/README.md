# driver_call_cpp — Go -> C++ cross-language E2E

Verifies **Go -> C++ calls** and the **C++ function descriptor** surface.

## Coverage

| Case | Coverage |
|---|---|
| `testCppFunctionDescriptor` | `CppFunctionDescriptor` construction, `FunctionDescriptorFromListWithLanguage(LanguageCpp, ...)` parsing, `ToList()` 3-element `[functionName, caller, className]`, `GetLanguage()`, `Hash()` stability, invalid-input rejection |
| `testAdd` | `RemoteCpp[int]("add", "")` + `Call(10, 20)` -> 30 |
| `testVoid` | `RemoteCppVoid("log_event", "")` returns no value, `ObjectRef == nil` |
| `testCrossLanguageTypes` | string / int slice / string->int map / fully-exported struct cross-language round trip |
| `testActor` | `RemoteCppActor("create_actor", "Calculator")` creates a C++ Actor, `CppActorTask[int](handle, "add", ...)` method call with state accumulation, call fails after `KillWithNoRestart()` (confirmed by polling) |

## How it works

- The Go driver constructs a `CppFunctionDescriptor` (caller="GO"); arguments
  are serialized through `convertArgs` into cross-language types (XLANG
  metadata + 9-byte length header) and submitted as a `LanguageCpp` task.
- The raylet starts the C++ default worker through `cpp_worker_command`; the
  worker loads `cpp_xlang_worker.so` from the job's `code_search_path` via
  `FunctionHelper::LoadFunctionsFromPaths` (the `RAY_REMOTE` macro registers
  the functions/Actor).
- The C++ worker strips the 9-byte length header, deserializes and executes
  the arguments as msgpack, and returns the result in XLANG format; the Go
  side decodes it through the `parseLengthHeader` path.

### Cross-language struct encoding convention

C++ `MSGPACK_DEFINE(name, age)` packs the struct as a **positional msgpack
array**. On the Go side the serializer's encoder enables
`UseArrayEncodedStructs(true)` for cross-language types (see `newEncoder` in
`go/internal/runtime/serializer/msgpack_serializer.go`, used by both `Encode`
and `EncodeToBuffer`), so a fully-exported struct encodes as an array matching
the C++ layout; Go-internal types with only private fields keep map encoding.
The decode side accepts both array and map automatically.

### Actor factory function name

The C++ `task_executor.cc` `ACTOR_CREATION_TASK` branch looks up the
descriptor's `FunctionName` directly in the plain function table (it does not
concatenate a `ClassName::` prefix). Therefore `cpp_xlang_worker.cc` registers
the factory as the **global function** `create_actor` (not
`Calculator::create_actor`), exactly matching the function name the Go driver
submits via `RemoteCppActor("create_actor", "Calculator")`. Actor methods are
still registered as member functions (`&Calculator::add`, etc.), and the C++
`ACTOR_TASK` branch concatenates `&ClassName::MethodName` automatically for
cross-language calls.

## Build and run

Prerequisites: a local Ray cluster is running
(`RAY_ENABLE_GO_SETUP_WORKER=1 ray start --head`), and site-packages has the
latest `ray/go/lib/go_runtime.so` and `ray/go/cmd/raygo` synced (`bazel run
//go:gen_ray_go_pkg` then sync; the C++ infrastructure `ray/cpp/default_worker`
and `ray/cpp/lib/libray_api.so` must also be in place).

```bash
# 1. Build the driver and the C++ worker plugin
bazel build //go/e2e/driver_call_cpp:driver_call_cpp
bazel build //go/e2e/driver_call_cpp:cpp_xlang_worker.so

# 2. Deploy to a run directory (.so must be alongside the driver for code_search_path)
mkdir -p /tmp/cpp_job_dir
cp bazel-bin/go/e2e/driver_call_cpp/driver_call_cpp_/driver_call_cpp /tmp/cpp_job_dir/
cp bazel-bin/go/e2e/driver_call_cpp/cpp_xlang_worker.so /tmp/cpp_job_dir/

# 3. Submit as a Ray job
cd /tmp/cpp_job_dir && ray job submit --working-dir . -- ./driver_call_cpp
```

Expected output (partial):

```
parsed CppFunctionDescriptor: &{CreateFunctionName:add Caller:GO ClassName:}, language=2
ToList() = [add GO ] (3 elements, matches Java CppFunctionDescriptor.toList())
add(10, 20) = 30 (expected 30)
log_event returned ref==nil: true, err=<nil>
echo_student({tom 20}) = {Name:tom Age:20}
created Calculator actor, ID=...
Calculator.add(10) after add(5) = 15 (expected 15)
call after KillWithNoRestart correctly failed: ...
=== All Go->C++ cross-language tests passed! ===
```

## Files

- `main.go` — the Go driver (implements all five test cases)
- `cpp_xlang_worker.cc` — the C++ worker plugin (registers add/log_event/
  concat/echo_list/echo_map/echo_student and the Calculator actor)
- `BUILD.bazel` — build targets for the driver (go_binary) and the worker
  plugin (cc_binary `.so`)
