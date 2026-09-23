"""Every Ray Data release test must be covered by the cluster health guards.

The guards (OOM worker kills, unexpected worker failures, dead nodes, object-store
utilization) live in ``release/nightly_tests/dataset/benchmark.py`` and only run
when the workload script uses ``Benchmark``. A test whose script does not use it
must instead keep the ``RAYTEST_*`` env vars in ``cluster.byod.runtime_env`` so the
Anyscale job wrapper enforces them. Without this test, adding such a script would
silently drop the guards.
"""
import os
import re
import sys
from typing import Optional, Set

import pytest

from ray_release.bazel import bazel_runfile
from ray_release.config import read_and_validate_release_test_collection

DATA_RELEASE_TEST_FILES = [
    "release/release_data_tests.yaml",
    "release/release_multimodal_inference_benchmarks_tests.yaml",
]
GUARD_ENV_PREFIX = "RAYTEST_"
_LOCAL_IMPORT_RE = re.compile(
    r"^(?:from\s+(\w+)\s+import|import\s+(\w+)(?:\s+as\s+\w+)?\s*$)", re.M
)
_BENCHMARK_CTOR_RE = re.compile(r"\bBenchmark\(")


def _entry_script(script: str) -> Optional[str]:
    match = re.search(r"python\s+(\S+\.py)", script)
    return match.group(1) if match else None


def _uses_benchmark(path: str, seen: Set[str]) -> bool:
    """True if the script, or a sibling module it imports, constructs ``Benchmark``."""
    path = os.path.realpath(path)
    if path in seen:
        return False
    seen.add(path)
    with open(path) as f:
        text = f.read()
    if _BENCHMARK_CTOR_RE.search(text):
        return True
    directory = os.path.dirname(path)
    for match in _LOCAL_IMPORT_RE.finditer(text):
        module = match.group(1) or match.group(2)
        sibling = os.path.join(directory, f"{module}.py")
        if os.path.exists(sibling) and _uses_benchmark(sibling, seen):
            return True
    return False


def test_every_data_release_test_is_guarded():
    tests = read_and_validate_release_test_collection(DATA_RELEASE_TEST_FILES)
    assert tests, "no data release tests were loaded"

    unguarded = []
    for test in tests:
        script = (test.get("run") or {}).get("script", "")
        entry = _entry_script(script)
        assert entry, f"{test.get_name()}: could not find the python entrypoint"
        path = bazel_runfile("release", test["working_dir"], entry)
        assert os.path.exists(path), f"{test.get_name()}: {path} does not exist"

        if _uses_benchmark(path, set()):
            continue
        if any(k.startswith(GUARD_ENV_PREFIX) for k in test.get_byod_runtime_env()):
            continue
        unguarded.append(f"{test.get_name()} ({entry})")

    assert not unguarded, (
        "These release tests neither use `Benchmark` (which runs the cluster health "
        "checks) nor set RAYTEST_* guards in cluster.byod.runtime_env:\n  "
        + "\n  ".join(unguarded)
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
