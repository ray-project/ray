"""A free-threaded worker joins a regular-CPython cluster via `py_executable`.

Runs on a regular CPython driver and cluster. Set RAY_TEST_FREE_THREADED_PYTHON to a
free-threaded interpreter (e.g. python3.15t) whose environment has a cp315t build of
the same Ray version::

    RAY_TEST_FREE_THREADED_PYTHON=/path/to/python3.15t \
        python -m pytest -v python/ray/tests/test_free_threaded_py_executable.py
"""

import os
import subprocess
import sys
import sysconfig
import threading
import time
from pathlib import Path
from typing import Any, Dict, List

import pytest

import ray
from ray.exceptions import RuntimeEnvSetupError, TaskCancelledError

FT_PYTHON = os.environ.get("RAY_TEST_FREE_THREADED_PYTHON")
NUM_THREADS = 4
WORK = 2_000_000
MODULUS = 1_000_003


def _expected(work: int) -> int:
    """Closed form of _burn(work): sum of i*i for i < work, mod MODULUS."""
    return ((work - 1) * work * (2 * work - 1) // 6) % MODULUS


# The free-threaded worker does not have pytest, so ship this module by value.
ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])


def _free_threaded_ray_available() -> bool:
    if not FT_PYTHON or sysconfig.get_config_var("Py_GIL_DISABLED"):
        return False
    probe = (
        "import sys, sysconfig, ray; "
        "sys.exit(0 if sysconfig.get_config_var('Py_GIL_DISABLED') else 1)"
    )
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    env["PYTHON_GIL"] = "0"
    return subprocess.run([FT_PYTHON, "-c", probe], env=env).returncode == 0


pytestmark = pytest.mark.skipif(
    not _free_threaded_ray_available(),
    reason=(
        "Requires a regular CPython driver and RAY_TEST_FREE_THREADED_PYTHON set to "
        "a free-threaded interpreter with a cp315t Ray build."
    ),
)


@pytest.fixture
def ft_runtime_env() -> Dict[str, Any]:
    return {"py_executable": FT_PYTHON, "env_vars": {"PYTHON_GIL": "0"}}


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
        "executable": sys.executable,
        "ray_file": ray.__file__,
        "free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
        "gil_enabled": sys._is_gil_enabled(),
        "threads": results,
    }


def _assert_ran_in_parallel(run: Dict[str, Any]) -> None:
    assert run["free_threaded_build"] is True
    assert run["gil_enabled"] is False
    threads = run["threads"]
    assert [t["result"] for t in threads] == [_expected(WORK)] * len(threads)
    # Every thread had started before any thread finished.
    assert max(t["start"] for t in threads) < min(t["end"] for t in threads)


class Payload:
    """Defined in the driver; must round-trip through the free-threaded worker."""

    def __init__(self, values: List[int]):
        self.values = values

    def total(self) -> int:
        return sum(self.values)


@ray.remote(num_cpus=NUM_THREADS)
def run_threads_task(num_threads: int, work: int) -> Dict[str, Any]:
    return _run_threads(num_threads, work)


@ray.remote(num_cpus=NUM_THREADS)
class ThreadsActor:
    def run(self, num_threads: int, work: int) -> Dict[str, Any]:
        return _run_threads(num_threads, work)


@ray.remote
def echo_payload(payload: Payload) -> Payload:
    assert sys._is_gil_enabled() is False
    return Payload([v * 2 for v in payload.values])


@pytest.fixture
def ray_cluster(shutdown_only):
    assert sys._is_gil_enabled() is True
    ray.init(num_cpus=NUM_THREADS, include_dashboard=False)


def test_task_and_actor_threads_run_in_parallel(ray_cluster, ft_runtime_env):
    task_run = ray.get(
        run_threads_task.options(runtime_env=ft_runtime_env).remote(NUM_THREADS, WORK)
    )
    assert task_run["executable"] != sys.executable
    # The worker imports Ray from its own environment, not the cluster's.
    assert task_run["ray_file"] != ray.__file__
    _assert_ran_in_parallel(task_run)

    actor = ThreadsActor.options(runtime_env=ft_runtime_env).remote()
    _assert_ran_in_parallel(ray.get(actor.run.remote(NUM_THREADS, WORK)))
    ray.kill(actor)

    # A regular task in the same job still runs in a regular worker.
    regular_run = ray.get(run_threads_task.remote(NUM_THREADS, WORK))
    assert regular_run["gil_enabled"] is True
    assert regular_run["free_threaded_build"] is False


def test_driver_defined_code_and_arguments_round_trip(ray_cluster, ft_runtime_env):
    result = ray.get(
        echo_payload.options(runtime_env=ft_runtime_env).remote(Payload([1, 2, 3]))
    )
    assert isinstance(result, Payload)
    assert result.total() == 12


def test_regular_ray_on_pythonpath_fails_with_clear_error(ray_cluster):
    # The worker inherits PYTHONPATH and would pick up the cluster's regular-ABI Ray.
    cluster_ray_root = os.path.dirname(os.path.dirname(ray.__file__))
    runtime_env = {
        "py_executable": f"env PYTHONPATH={cluster_ray_root} {FT_PYTHON}",
        "env_vars": {"PYTHON_GIL": "0"},
    }

    with pytest.raises(RuntimeEnvSetupError) as exc_info:
        ray.get(
            run_threads_task.options(runtime_env=runtime_env).remote(NUM_THREADS, WORK),
            timeout=60,
        )

    message = str(exc_info.value)
    assert "cannot import Ray" in message
    assert "free-threaded" in message
    assert "PYTHONPATH" in message


@ray.remote(num_cpus=NUM_THREADS)
def run_threads_until(started_file: str, seconds: float) -> None:
    deadline = time.monotonic() + seconds

    def spin() -> None:
        while time.monotonic() < deadline:
            pass

    threads = [threading.Thread(target=spin) for _ in range(NUM_THREADS)]
    for t in threads:
        t.start()
    Path(started_file).write_text("started")
    for t in threads:
        t.join()


@pytest.mark.parametrize("force", [False, True])
def test_cancel_task_running_threads(
    ray_cluster, ft_runtime_env, tmp_path: Path, force: bool
):
    started_file = tmp_path / "started"
    ref = run_threads_until.options(runtime_env=ft_runtime_env).remote(
        str(started_file), 5
    )
    deadline = time.monotonic() + 60
    while not started_file.exists():
        assert time.monotonic() < deadline, "The task never started its threads."
        time.sleep(0.05)

    ray.cancel(ref, force=force)
    # Without force the interrupt is only delivered once the task's threads are
    # joined, exactly as on a regular worker.
    with pytest.raises(TaskCancelledError):
        ray.get(ref, timeout=60)

    # The runtime env still serves new tasks.
    _assert_ran_in_parallel(
        ray.get(
            run_threads_task.options(runtime_env=ft_runtime_env).remote(
                NUM_THREADS, WORK
            ),
            timeout=120,
        )
    )


@ray.remote(num_cpus=1)
def stress_task(num_threads: int, work: int, payload_size: int) -> Dict[str, Any]:
    import gc

    # GC runs on its own thread so it overlaps the user threads. It is paced because
    # back-to-back collections stop the world on free-threaded CPython and starve them.
    stop = threading.Event()

    def collect_garbage() -> None:
        while not stop.is_set():
            gc.collect()
            time.sleep(0.005)

    gc_thread = threading.Thread(target=collect_garbage)
    gc_thread.start()
    try:
        run = _run_threads(num_threads, work)
    finally:
        stop.set()
        gc_thread.join()
    run["payload"] = b"x" * payload_size
    return run


def test_stress_background_threads_with_user_threads(ray_cluster, ft_runtime_env):
    num_threads, work, payload_size = 2, 1_000_000, 1_000_000
    expected = _expected(work)
    stress = stress_task.options(runtime_env=ft_runtime_env)

    for _ in range(10):
        refs = [
            stress.remote(num_threads, work, payload_size)
            for _ in range(2 * NUM_THREADS)
        ]
        for run in ray.get(refs, timeout=180):
            assert run["free_threaded_build"] is True
            assert run["gil_enabled"] is False
            assert [t["result"] for t in run["threads"]] == [expected] * num_threads
            assert len(run["payload"]) == payload_size
        # Dropping the refs fires out-of-scope callbacks while new tasks run.
        del refs


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
