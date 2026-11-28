"""User threads inside a Ray task or actor run in parallel on free-threaded Python.

Run on a free-threaded interpreter with ``PYTHON_GIL=0``, because Ray's extension
module does not declare free-threading support and would otherwise re-enable the
GIL on import::

    PYTHON_GIL=0 python3.15t -m pytest -v python/ray/tests/test_free_threaded_worker_threads.py
"""

import sys
import sysconfig
import threading
import time
from typing import Any, Dict, List

import pytest

import ray

pytestmark = pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"),
    reason="Requires a free-threaded CPython build.",
)

NUM_THREADS = 4
WORK = 2_000_000
MODULUS = 1_000_003
# Closed form of _burn(WORK): sum of i*i for i < WORK, mod MODULUS.
EXPECTED = ((WORK - 1) * WORK * (2 * WORK - 1) // 6) % MODULUS


def _burn(n: int, out: List[Dict[str, Any]], index: int) -> None:
    start = time.monotonic()
    # A local alias avoids refcount contention on a shared global across threads.
    modulus = MODULUS
    total = 0
    for i in range(n):
        total = (total + i * i) % modulus
    out[index] = {"result": total, "start": start, "end": time.monotonic()}


def _run_threads(num_threads: int, work: int) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = [{} for _ in range(num_threads)]
    threads = [
        threading.Thread(target=_burn, args=(work, results, i))
        for i in range(num_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return {
        "free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
        "gil_enabled": sys._is_gil_enabled(),
        "threads": results,
    }


@ray.remote(num_cpus=NUM_THREADS)
def run_threads_task(num_threads: int, work: int) -> Dict[str, Any]:
    return _run_threads(num_threads, work)


@ray.remote(num_cpus=NUM_THREADS)
class ThreadsActor:
    def run(self, num_threads: int, work: int) -> Dict[str, Any]:
        return _run_threads(num_threads, work)


@pytest.mark.parametrize(
    "ray_start_regular_shared", [{"num_cpus": NUM_THREADS}], indirect=True
)
@pytest.mark.parametrize("caller", ["task", "actor"])
def test_threads_run_in_parallel_with_gil_disabled(
    ray_start_regular_shared, caller: str
):
    assert sys._is_gil_enabled() is False, "Run the driver with PYTHON_GIL=0."

    if caller == "task":
        run = ray.get(run_threads_task.remote(NUM_THREADS, WORK))
    else:
        actor = ThreadsActor.remote()
        run = ray.get(actor.run.remote(NUM_THREADS, WORK))
        ray.kill(actor)

    assert run["free_threaded_build"] is True
    assert run["gil_enabled"] is False
    threads = run["threads"]
    assert [t["result"] for t in threads] == [EXPECTED] * NUM_THREADS
    # Every thread had started before any thread finished.
    assert max(t["start"] for t in threads) < min(t["end"] for t in threads)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
