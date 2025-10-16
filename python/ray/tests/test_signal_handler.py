import os
import signal
import sys
import tempfile
import time

import pytest

import ray
from ray._private.test_utils import run_string_as_driver_nonblocking


@ray.remote
class SimpleActor:
    def pid(self) -> int:
        return os.getpid()

    def ping(self) -> str:
        return "ok"


@ray.remote
class AsyncioActor:
    async def pid(self) -> int:
        return os.getpid()

    async def ping(self) -> str:
        return "ok"

    async def run(self):
        import asyncio

        while True:
            await asyncio.sleep(0.1)


@pytest.fixture
def ray_start():
    ray.init()
    try:
        yield
    finally:
        ray.shutdown()


def _expect_actor_dies(ref_fn, timeout_s=30):
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            ray.get(ref_fn(), timeout=1)
        except ray.exceptions.GetTimeoutError:
            continue
        except ray.exceptions.RayActorError:
            return
        except Exception:
            return
    raise AssertionError("Actor did not die within timeout")


def test_asyncio_actor_force_exit_is_immediate():
    """Calling force_exit_worker() inside an asyncio actor should exit immediately."""
    ray.init()

    @ray.remote
    class A:
        async def ping(self):
            return "ok"

        async def force_quit(self):
            from ray._private.worker import global_worker

            global_worker.core_worker.force_exit_worker("user", b"force from test")
            return "unreachable"

    a = A.remote()
    assert ray.get(a.ping.remote()) == "ok"
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.force_quit.remote())
    ray.shutdown()


@pytest.mark.parametrize("sig", [signal.SIGTERM])
def test_worker_single_signal_graceful(ray_start, sig):
    """Single signal to a worker should cause it to terminate immediately."""
    a = SimpleActor.remote()
    pid = ray.get(a.pid.remote())
    os.kill(pid, sig)
    _expect_actor_dies(lambda: a.ping.remote())


@pytest.mark.parametrize("sig1,sig2", [(signal.SIGINT, signal.SIGTERM)])
def test_worker_double_signal_forced(ray_start, sig1, sig2):
    """First SIGINT requests graceful shutdown; second signal should force termination."""
    a = SimpleActor.remote()
    pid = ray.get(a.pid.remote())
    os.kill(pid, sig1)
    time.sleep(0.1)
    os.kill(pid, sig2)
    _expect_actor_dies(lambda: a.ping.remote())


def test_asyncio_actor_signal_SIGTERM(ray_start):
    """Asyncio actor should terminate upon receiving SIGTERM."""
    a = AsyncioActor.remote()
    pid = ray.get(a.pid.remote())
    a.run.remote()
    assert ray.get(a.ping.remote()) == "ok"
    os.kill(pid, signal.SIGTERM)
    _expect_actor_dies(lambda: a.ping.remote())


def test_driver_sigterm_graceful():
    """Driver should exit gracefully on first SIGTERM  and atexit should run."""
    with tempfile.TemporaryDirectory() as td:
        flag = os.path.join(td, "driver_atexit_flag.txt")
        ready = os.path.join(td, "driver_ready.txt")
        driver_code = (
            "import atexit, os, time, ray\n"
            "flag=os.environ['FLAG_PATH']\n"
            "ready=os.environ['READY_PATH']\n"
            "def on_exit():\n"
            "    with open(flag,'w') as f: f.write('ok')\n"
            "atexit.register(on_exit)\n"
            "ray.init()\n"
            "with open(ready,'w') as f: f.write('ready')\n"
            "time.sleep(1000)\n"
        )
        env = os.environ.copy()
        env["FLAG_PATH"] = flag
        env["READY_PATH"] = ready
        p = run_string_as_driver_nonblocking(driver_code, env=env)
        try:
            # Wait for driver to finish ray.init() and install handlers
            start = time.time()
            while time.time() - start < 10 and not os.path.exists(ready):
                time.sleep(0.05)
            os.kill(p.pid, signal.SIGTERM)
            _ = p.wait(timeout=10)
        finally:
            try:
                p.kill()
            except Exception:
                pass
        assert os.path.exists(flag)
        with open(flag, "r") as f:
            assert f.read() == "ok"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
