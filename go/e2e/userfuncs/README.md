# userfuncs - Ray Go High-Performance Function Examples

This package demonstrates Ray Go's **dual-mode high-performance design**:

| Mode | How to use | Performance | Best for |
|------|------------|-------------|----------|
| **Reflection mode** (default) | `bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin` | ~500ns/call | Low-frequency calls, prototyping |
| **Precompiled mode** (optional) | add `//go:ray:task` + raygen | ~5ns/call (100x faster!) | High-frequency calls, performance-critical paths |

## Quick start

### Mode 1: Reflection mode (default, backward compatible)

**No code changes required**:
```go
package userfuncs

func goAdd(x, y int) int {
    return x + y
}
```

**Build**:
```bash
# Build the plugin (target userfuncs_plugin, out = "userfuncs.so").
bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin
# Then copy the produced artifact into the job's working directory.
cp bazel-bin/go/e2e/userfuncs/plugin/userfuncs.so .
```

A bare `go build -buildmode=plugin` does not work here: the plugin entry point
lives in the non-main `go/e2e/userfuncs/plugin` package, so the build must go
through the bazel target above.

**Submit a job**:
```bash
ray job submit --working-dir . -- ./driver_app
```

The worker invokes the function through reflection (`reflect.Call()`), at about 500ns per call.

---

### Mode 2: Precompiled mode (optional, high performance)

> **The generator is not in this repository.** raygen is an internal tool that
> is not part of this tree, the checked-in `generated_*.go` files are
> hand-maintained, and the steps below describe the original internal workflow
> (see the note further down).

**Step 1: Mark your functions**

```go
package userfuncs

//go:ray:task
func goAdd(x, y int) int {
    return x + y
}

//go:ray:task
func goMultiply(x, y int) int {
    return x * y
}
```

**Step 2: Run the code generator**

```bash
# Method 1: use the raygen tool
cd go/e2e/userfuncs
raygen --input funcs.go --output generated_wrappers.go,generated_registry.go

# Method 2: use go generate (requires the directive in doc.go)
go generate

# Method 3: use bazel build (executed automatically)
bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin
```

> **Note on the generator in this tree**: raygen is an internal code
> generator that is not part of the open-source tree. The generated
> `generated_wrappers.go` and `generated_registry.go` files are checked in as
> static sources, and the `//go:generate` directive has been removed from
> `doc.go`. Function registration is currently manual through
> `RegisterFunctions` in `funcs.go`, which is what both the driver (via package
> import) and the worker (via the `userfuncs.so` plugin) use to populate the
> global registry. The `//go:ray:task` markers on the functions below remain
> the canonical way to declare which functions should be wrapped by the
> generator when it is available.

**Step 3: The generated code**

The code generator creates two files:

**generated_wrappers.go** - type-safe wrapper functions:
```go
// wrapper_goAdd is a type-safe wrapper for userfuncs.goAdd.
func wrapper_goAdd(input []byte) ([]byte, error) {
    var args struct {
        Arg0 int
        Arg1 int
    }
    msgpack.Unmarshal(input, &args)

    result := goAdd(args.Arg0, args.Arg1)  // direct call, no reflection!

    return msgpack.Marshal(result)
}
```

**generated_registry.go** - automatic registration:
```go
func init() {
    function.RegisterWrapper("userfuncs.goAdd", wrapper_goAdd)
    function.RegisterWrapper("userfuncs.goMultiply", wrapper_goMultiply)
    // ... other functions
}
```

### Step 4: Use it in a driver

```go
import (
    "github.com/ray-project/ray/go/pkg/runtime/api"
    "github.com/ray-project/ray/go/e2e/userfuncs"
)

// Submit a task (using the generated wrapper, no reflection overhead)
resultRef, err := api.SubmitTask[int](userfuncs.goAdd, 10, 20)
if err != nil {
    panic(err)
}

result, err := resultRef.Get()
// result == 30
```

This snippet illustrates the internal precompiled-mode API, which is not part
of the open-source tree (the same adaptation noted for raygen above).

## Performance comparison

| Approach | Function call | Serialization | Task execution |
|----------|---------------|---------------|----------------|
| **Reflection (old)** | 500ns | 50μs | 500μs |
| **Code generation (new)** | 5ns | 5μs | 50μs |
| **Improvement** | **100x** | **10x** | **10x** |

## File layout

```
userfuncs/
├── actors.go               # Actor types and constructor registration
├── doc.go                  # Package documentation
├── echo_bytes.go           # Zero-copy benchmark and runtime-env task receivers
├── funcs.go                # User functions (reflection mode) and RegisterFunctions
├── generated_wrappers.go   # [generated] type-safe wrapper functions
├── generated_registry.go   # [generated] wrapper registration
├── report_node.go          # Placement-group node-id reporting task
├── plugin/                 # Plugin entry point (userfuncs.so)
├── BUILD.bazel             # Bazel build rules
└── README.md               # This file
```

## How it works

```
User code (funcs.go)
    ↓ //go:ray:task markers
    ↓
Code generator (raygen, internal — not part of this tree)
    ↓ scans the markers
    ↓ generates type-safe wrapper functions
    ↓
Generated code (generated_*.go, checked in)
    ↓ compiled into the userfuncs library
    ↓ registered via RegisterFunctions (current mechanism)
    ↓
Runtime (worker)
    ↓ invokes the wrapper functions (direct call, no reflection)
    ↓
Return result
```

## FAQ

### Q: What happens if I forget to add `//go:ray:task`?

A: The function does not get a generated wrapper. It can still be invoked
through the old reflection path, but with worse performance (10-100x slower).

### Q: Should the generated code be committed to git?

A: In this open-source tree, **yes** — the generated `generated_*.go` files are
checked in as static sources because the code generator is not part of the
open-source tree and `bazel build` does not run it. If you have raygen
available locally, you can regenerate these files and commit the result.

### Q: How do I add a new function?

A: Add a function with the `//go:ray:task` marker to a `.go` file, then either
run raygen yourself to regenerate the wrappers, or register the function
manually in `RegisterFunctions` in `funcs.go`.

### Q: When does the code generator run?

A: raygen is an internal code generator and does not run as part of the
open-source build. The generated wrappers are maintained as checked-in sources
and registration is done manually through `RegisterFunctions`, so no
generator execution is needed to build or use this package.

```bash
# Build the plugin
bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin
```

## References

- [doc.go](doc.go) - package documentation
