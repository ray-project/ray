#!/usr/bin/env python3
"""
Cross-language Python -> Go end-to-end test (extended from the former
test_python_call_go_simple.py).

Covers two call forms of Python -> Go:
  Part A: stateless remote functions (go_function) — goAdd/goMultiply/goConcat/goCompute
          plus concurrent calls (originally test_python_call_go_simple.py).
  Part B: Go actor created and driven from Python (the fourth cross-language
          scenario), in particular a logically-void
          method (Counter.Void) — the cross-language void regression
          (added in this change).

Regression target (Part B): a cross-language actor method call defaults to
num_returns = 1 (DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS, hardcoded in
python/ray/actor.py for cross-language methods), while a Go void method
(func (c *Counter) Void()) produces zero SerializedObjects. Before the padding
fix in convertGoResultToC (go/internal/runtime/cgo/task_executor.go), C++ left
the return slot nullptr and CoreWorker::ExecuteTask CHECK-failed on
objects_valid in task_receiver.cc, crashing the Go worker. After the fix, the
padding slot is a cross-language nil object (9-byte length header + msgpack nil,
XLANG metadata) and ray.get returns None.

NOTE: this lane runs against a real cluster (cluster mode). Ray's
`ray.init(local_mode=True)` entry is removed in Ray 2.56 (it raises
RuntimeError), and the padding under test is the cluster worker's — a
same-process runner would not exercise it either way.

Prerequisites:
  bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin
  ray start --head --port=6379

Usage:
  python go/e2e/test_python_call_go.py --ray-address=localhost:6379
  python go/e2e/test_python_call_go.py    # connects to the local cluster
"""

import os
import sys

# Add Ray Python to path
RAY_PYTHON_PATH = os.path.join(os.path.dirname(__file__), "../../python")
sys.path.insert(0, RAY_PYTHON_PATH)

import ray  # noqa: E402
from ray.cross_language import go_actor_class, go_function  # noqa: E402
from ray.job_config import JobConfig  # noqa: E402

# module_name / package_path must match what api.RegisterActorClass derives from
# the (*Counter)(nil) type (DescriptorPartsFromType): module "github.com/ray-project/ray",
# package "go/e2e/userfuncs", type "Counter". Passing a bare "userfuncs" module name
# fails Go-side validation (isValidModulePath requires a '/' or "main").
COUNTER_DESC = dict(
    module_name="github.com/ray-project/ray",
    package_path="go/e2e/userfuncs",
    actor_type="Counter",
)


def _code_search_path():
    """Return the code search path for the Go worker to locate userfuncs.so.

    The cross-language call is gated by worker.load_code_from_local, which is
    only True when ray.init() is given a JobConfig with a non-empty
    code_search_path. The same list is propagated verbatim to the Go worker
    (GCS -> worker_pool -> --code-search-path / RAY_CODE_SEARCH_PATH), where
    each entry is opened directly with plugin.Open().

    Use a relative path: the Go worker is launched through `raygo setup_worker`,
    which cd's into the job's working_dir (see WorkingDirPlugin.ModifyContext ->
    CommandPrefix -> RuntimeEnvContext.ExecWorker). Because the working_dir is
    content-addressed (_ray_pkg_<hash>) and auto-downloaded to every node, a
    relative path resolves to the local copy on each node and does not depend
    on any node-specific absolute path. The "./" prefix is required so that
    dlopen (plugin.Open) resolves against cwd instead of LD_LIBRARY_PATH.
    """
    return ["./userfuncs.so"]


def _init_ray(args):
    if args.ray_address:
        print(f"Connecting to Ray at {args.ray_address}...")
        ray.init(
            address=args.ray_address,
            job_config=JobConfig(code_search_path=_code_search_path()),
        )
    else:
        print("Starting Ray local cluster...")
        ray.init(job_config=JobConfig(code_search_path=_code_search_path()))
    print(f"Ray initialized: {ray.is_initialized()}")


def _test_go_functions():
    """Part A: stateless remote function calls (originally test_python_call_go_simple.py)."""
    # ============================================================
    # Test 1: Call Go's goAdd(x, y) -> int
    # ============================================================
    print("\n" + "=" * 60)
    print("Test 1: Python calling Go function goAdd(10, 20)")
    print("=" * 60)

    # Create Go function reference.
    # module_name / package_path must match the descriptor the Go worker derives
    # by reflection when registering the function. The worker reflects the symbol
    # "github.com/ray-project/ray/go/e2e/userfuncs.goAdd" and splits it into
    #   module  = "github.com/ray-project/ray"
    #   package = "go/e2e/userfuncs"
    #   function= "goAdd"
    # Passing "userfuncs" as module_name fails Go-side validation
    # (isValidModulePath requires a '/' or "main").
    # function_name: the exported function name
    go_add = go_function(
        module_name="github.com/ray-project/ray",
        package_path="go/e2e/userfuncs",
        function_name="goAdd",
    )

    # Call from Python
    print("Executing: ray.get(go_add.remote(10, 20))")
    result = ray.get(go_add.remote(10, 20))
    print(f"Result: {result}")
    assert result == 30, f"Expected 30, got {result}"
    print("✓ Test 1 passed!")

    # ============================================================
    # Test 2: Call Go's goMultiply(x, y) -> int
    # ============================================================
    print("\n" + "=" * 60)
    print("Test 2: Python calling Go function goMultiply(6, 7)")
    print("=" * 60)

    go_multiply = go_function(
        module_name="github.com/ray-project/ray",
        package_path="go/e2e/userfuncs",
        function_name="goMultiply",
    )

    print("Executing: ray.get(go_multiply.remote(6, 7))")
    result = ray.get(go_multiply.remote(6, 7))
    print(f"Result: {result}")
    assert result == 42, f"Expected 42, got {result}"
    print("✓ Test 2 passed!")

    # ============================================================
    # Test 3: Call Go's goConcat(a, b) -> string
    # ============================================================
    print("\n" + "=" * 60)
    print("Test 3: Python calling Go function goConcat('Hello, ', 'World!')")
    print("=" * 60)

    go_concat = go_function(
        module_name="github.com/ray-project/ray",
        package_path="go/e2e/userfuncs",
        function_name="goConcat",
    )

    print("Executing: ray.get(go_concat.remote('Hello, ', 'World!'))")
    result = ray.get(go_concat.remote("Hello, ", "World!"))
    print(f"Result: {result}")
    assert result == "Hello, World!", f"Expected 'Hello, World!', got {result}"
    print("✓ Test 3 passed!")

    # ============================================================
    # Test 4: Call Go's goCompute(x, y, z) -> int
    # ============================================================
    print("\n" + "=" * 60)
    print("Test 4: Python calling Go function goCompute(2, 3, 4)")
    print("=" * 60)

    go_compute = go_function(
        module_name="github.com/ray-project/ray",
        package_path="go/e2e/userfuncs",
        function_name="goCompute",
    )

    print("Executing: ray.get(go_compute.remote(2, 3, 4))")
    result = ray.get(go_compute.remote(2, 3, 4))
    print(f"Result: {result}")
    assert result == 10, f"Expected 10 (2*3+4), got {result}"
    print("✓ Test 4 passed!")

    # ============================================================
    # Test 5: Concurrent calls from Python to Go
    # ============================================================
    print("\n" + "=" * 60)
    print("Test 5: Python making concurrent calls to goAdd")
    print("=" * 60)

    futures = [go_add.remote(i, i * 10) for i in range(5)]
    results = ray.get(futures)
    expected = [i + i * 10 for i in range(5)]

    print(f"Results: {results}")
    print(f"Expected: {expected}")
    assert results == expected, f"Expected {expected}, got {results}"
    print("✓ Test 5 passed!")


def _test_go_actor():
    """Part B: Go actor driven from Python, including the void-method regression
    (added in this change)."""
    # ============================================================
    # Test 6: create the Go actor (ACTOR_CREATION_TASK, num_returns = 0)
    # ============================================================
    print("\n" + "=" * 60)
    print("Test 6: create Go Counter actor with initial value 10")
    print("=" * 60)
    Counter = go_actor_class(**COUNTER_DESC)
    actor = Counter.remote(10)
    print("✓ Actor created (no crash on creation)")

    # ============================================================
    # Test 7: value-returning method (baseline, Add -> int)
    # ============================================================
    print("\n" + "=" * 60)
    print("Test 7: Python calling value-returning Go method Add(5)")
    print("=" * 60)
    result = ray.get(actor.Add.remote(5))
    print(f"Result: {result}")
    assert result == 15, f"Expected 15 (10 + 5), got {result}"
    print("✓ Test 7 passed!")

    # ============================================================
    # Test 8: void method (the regression: 0 results vs num_returns = 1)
    # ============================================================
    print("\n" + "=" * 60)
    print("Test 8: Python calling void Go method Void()")
    print("=" * 60)
    # Pre-fix this crashes the Go worker (RAY_CHECK in task_receiver.cc); the
    # subsequent Value() call would then fail with a worker-died error.
    result = ray.get(actor.Void.remote())
    print(f"Result: {result}")
    assert result is None, f"Expected None for void method, got {result!r}"
    print("✓ Test 8 passed!")

    # ============================================================
    # Test 9: state persists after the void call (worker still alive)
    # ============================================================
    print("\n" + "=" * 60)
    print("Test 9: Value() after Void() -> worker alive, state preserved")
    print("=" * 60)
    result = ray.get(actor.Value.remote())
    print(f"Result: {result}")
    assert result == 15, f"Expected 15 (unchanged by Void), got {result}"
    print("✓ Test 9 passed!")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Test Python calling Go functions and Go actor methods"
    )
    parser.add_argument(
        "--ray-address",
        type=str,
        default=None,
        help="Ray cluster address, e.g., 'localhost:6379'",
    )
    args = parser.parse_args()

    _init_ray(args)

    # Part A: stateless remote functions
    _test_go_functions()

    # Part B: Go actor (incl. cross-language void regression)
    _test_go_actor()

    ray.shutdown()
    print("\n" + "=" * 60)
    print(
        "✅ All tests passed! Python successfully called Go functions and Go actor methods via Ray!"
    )
    print("=" * 60)


if __name__ == "__main__":
    main()
