"""Worker teardown after the task execution loop exits.

Regression tests for https://github.com/ray-project/ray/issues/62442: once
``run_task_loop()`` returns, the C++ core worker has been released, and a later
call into it from the atexit ``shutdown()`` hook quick-exits the process
mid-teardown, which shows up as an access violation on Windows.
"""
import subprocess
import sys

import pytest

import ray
from ray._private.worker import Worker


class _FakeCoreWorker:
    def __init__(self, loop_exit=None, during_loop=None):
        self._loop_exit = loop_exit
        self._during_loop = during_loop

    def run_task_loop(self):
        if self._during_loop is not None:
            self._during_loop()
        if self._loop_exit is not None:
            raise self._loop_exit

    def get_job_config(self):
        raise AssertionError("called into a released core worker")


@pytest.fixture
def worker(monkeypatch):
    # main_loop installs a SIGTERM handler; keep the test process's own.
    monkeypatch.setattr(ray._private.utils, "set_sigterm_handler", lambda handler: None)
    return Worker()


def test_main_loop_releases_core_worker_after_task_loop(worker):
    worker.core_worker = _FakeCoreWorker()

    with pytest.raises(SystemExit) as exc_info:
        worker.main_loop()

    assert exc_info.value.code == 0
    assert not hasattr(worker, "core_worker")
    # What disconnect() reads during the atexit shutdown: it must not reach
    # the released core worker.
    assert worker.job_logging_config is None


def test_main_loop_releases_core_worker_when_task_loop_is_interrupted(worker):
    """The SIGTERM handler raises SystemExit out of the task loop."""
    worker.core_worker = _FakeCoreWorker(loop_exit=SystemExit(1))

    with pytest.raises(SystemExit) as exc_info:
        worker.main_loop()

    assert exc_info.value.code == 1
    assert not hasattr(worker, "core_worker")
    assert worker.job_logging_config is None


def test_main_loop_exits_cleanly_if_core_worker_was_already_dropped(worker):
    """A task calling ray.shutdown() deletes the handle before the loop returns."""

    def _shutdown_from_a_task():
        del worker.core_worker

    worker.core_worker = _FakeCoreWorker(during_loop=_shutdown_from_a_task)

    with pytest.raises(SystemExit) as exc_info:
        worker.main_loop()

    assert exc_info.value.code == 0
    assert not hasattr(worker, "core_worker")


_ACTOR_POOL_WITH_RUNTIME_ENV = """
import ray
from ray.util.actor_pool import ActorPool


def worker_process_setup_hook():
    return None


@ray.remote(num_cpus=1)
class Actor:
    def double(self, v):
        return 2 * v


ray.init(
    num_cpus=2,
    runtime_env={"worker_process_setup_hook": worker_process_setup_hook},
    include_dashboard=False,
)
pool = ActorPool([Actor.remote() for _ in range(2)])
print(list(pool.map(lambda a, v: a.double.remote(v), [1, 2])))
# No explicit ray.shutdown(): the workers are torn down by the driver's exit.
"""


@pytest.mark.skipif(
    sys.platform != "win32",
    reason="The access violation only surfaces on Windows; elsewhere the "
    "worker quick-exits silently.",
)
def test_actor_pool_with_runtime_env_exits_without_access_violation():
    """The reproduction from #62442, end to end."""
    proc = subprocess.run(
        [sys.executable, "-c", _ACTOR_POOL_WITH_RUNTIME_ENV],
        capture_output=True,
        text=True,
        timeout=180,
    )
    output = proc.stdout + proc.stderr

    assert proc.returncode == 0, output
    assert "[2, 4]" in proc.stdout, output
    # Worker crashes are forwarded to the driver's output, but do not change
    # its exit code, so they have to be looked for.
    assert "fatal exception" not in output, output
    assert "access violation" not in output, output
    assert "in job_logging_config" not in output, output


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
