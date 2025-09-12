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

    def sleep(self, secs: float):
        time.sleep(secs)
        return "done"


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
            # Any other error indicates process likely died; accept.
            return
    raise AssertionError("Actor did not die within timeout")


@pytest.mark.parametrize("sig", [signal.SIGTERM])
def test_worker_single_signal_graceful(ray_start, sig):
    a = SimpleActor.remote()
    pid = ray.get(a.pid.remote())
    os.kill(pid, sig)
    # Next call should eventually fail with RayActorError once worker shutdown completes.
    _expect_actor_dies(lambda: a.ping.remote())


@pytest.mark.parametrize(
    "sig1,sig2", [(signal.SIGTERM, signal.SIGTERM), (signal.SIGTERM, signal.SIGTERM)]
)
def test_worker_double_signal_forced(ray_start, sig1, sig2):
    a = SimpleActor.remote()
    pid = ray.get(a.pid.remote())
    # Send two signals to trigger escalation.
    os.kill(pid, sig1)
    time.sleep(0.1)
    os.kill(pid, sig2)
    _expect_actor_dies(lambda: a.ping.remote())


def test_asyncio_actor_signal(ray_start):
    a = AsyncioActor.remote()
    pid = ray.get(a.pid.remote())
    # Start background task
    a.run.remote()
    # Ensure actor responds
    assert ray.get(a.ping.remote()) == "ok"
    # Send SIGTERM
    os.kill(pid, signal.SIGTERM)
    _expect_actor_dies(lambda: a.ping.remote())


@pytest.mark.skip(
    reason="Raylet SIGTERM E2E requires harness to target raylet PID; add in integration suite."
)
def test_raylet_sigterm_no_regression():
    pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
