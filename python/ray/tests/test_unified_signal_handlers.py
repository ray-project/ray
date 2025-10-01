import os
import signal
import sys
import time

import pytest

import ray


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
    a = SimpleActor.remote()
    pid = ray.get(a.pid.remote())
    os.kill(pid, sig)
    _expect_actor_dies(lambda: a.ping.remote())


@pytest.mark.parametrize("sig1,sig2", [(signal.SIGTERM, signal.SIGTERM)])
def test_worker_double_signal_forced(ray_start, sig1, sig2):
    a = SimpleActor.remote()
    pid = ray.get(a.pid.remote())
    os.kill(pid, sig1)
    time.sleep(0.1)
    os.kill(pid, sig2)
    _expect_actor_dies(lambda: a.ping.remote())


def test_asyncio_actor_signal_SIGTERM(ray_start):
    a = AsyncioActor.remote()
    pid = ray.get(a.pid.remote())
    a.run.remote()
    assert ray.get(a.ping.remote()) == "ok"
    os.kill(pid, signal.SIGTERM)
    _expect_actor_dies(lambda: a.ping.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
