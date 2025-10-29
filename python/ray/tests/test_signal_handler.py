import asyncio
import os
import signal
import sys
import tempfile
import textwrap
import time

import pytest

import ray
from ray._common.test_utils import wait_for_condition
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
        while True:
            await asyncio.sleep(0.1)


def _expect_actor_dies(actor, timeout_s=30):
    """Wait for an actor to die and raise if it doesn't die within timeout.
    
    Args:
        actor: Ray actor handle to monitor.
        timeout_s: Maximum time to wait for actor death in seconds.
        
    Raises:
        AssertionError: If actor doesn't die within timeout_s.
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout_s:
        try:
            ray.get(actor.ping.remote(), timeout=1)
        except ray.exceptions.GetTimeoutError:
            continue
        except ray.exceptions.RayActorError:
            return
        except Exception:
            return
    raise AssertionError("Actor did not die within timeout")


def test_asyncio_actor_force_exit_is_immediate(ray_start):
    """Calling force_exit_worker() inside an asyncio actor should exit immediately."""
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


def test_worker_sigterm_terminates_immediately(ray_start):
    """Worker should terminate immediately upon receiving SIGTERM."""
    a = SimpleActor.remote()
    pid = ray.get(a.pid.remote())
    os.kill(pid, signal.SIGTERM)
    _expect_actor_dies(a)


def test_worker_sigterm_during_blocking_get(ray_start):
    """SIGTERM should force exit even when worker is blocked on ray.get()."""
    
    @ray.remote
    class BlockedActor:
        def pid(self) -> int:
            return os.getpid()
        
        def block_on_get(self):
            @ray.remote
            def never_returns():
                time.sleep(10000)
            
            ray.get(never_returns.remote())
    
    a = BlockedActor.remote()
    pid = ray.get(a.pid.remote())
    a.block_on_get.remote()
    time.sleep(0.1)
    os.kill(pid, signal.SIGTERM)
    _expect_actor_dies(a)


def test_asyncio_actor_sigterm_termination(ray_start):
    """Asyncio actor should terminate upon receiving SIGTERM."""
    a = AsyncioActor.remote()
    pid = ray.get(a.pid.remote())
    a.run.remote()
    assert ray.get(a.ping.remote()) == "ok"
    os.kill(pid, signal.SIGTERM)
    _expect_actor_dies(a)


def test_asyncio_actor_double_signal(ray_start):
    """Asyncio actor should force-exit on second signal even if first is slow."""
    
    @ray.remote
    class AsyncBlockedActor:
        async def pid(self) -> int:
            return os.getpid()
        
        async def ping(self) -> str:
            return "ok"
        
        async def block_forever(self):
            """Simulate blocking to test double-signal escalation."""
            await asyncio.sleep(10000)
    
    a = AsyncBlockedActor.remote()
    pid = ray.get(a.pid.remote())
    a.block_forever.remote()
    time.sleep(0.1)
    assert ray.get(a.ping.remote()) == "ok"
    
    os.kill(pid, signal.SIGINT)
    time.sleep(0.1)
    
    os.kill(pid, signal.SIGTERM)
    _expect_actor_dies(a)


def test_driver_sigterm_graceful():
    """Driver should exit gracefully on first SIGTERM and atexit should run."""
    with tempfile.TemporaryDirectory() as td:
        flag = os.path.join(td, "driver_atexit_flag.txt")
        ready = os.path.join(td, "driver_ready.txt")
        driver_code = textwrap.dedent(f"""
            import atexit, os, time, ray
            
            def on_exit():
                with open('{flag}', 'w') as f:
                    f.write('ok')
            
            atexit.register(on_exit)
            ray.init()
            with open('{ready}', 'w') as f:
                f.write('ready')
            time.sleep(1000)
        """)
        p = run_string_as_driver_nonblocking(driver_code)
        try:
            wait_for_condition(lambda: os.path.exists(ready), timeout=10)
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


def test_driver_double_sigterm_forced():
    """Driver should force-exit on second SIGTERM if first is slow."""
    with tempfile.TemporaryDirectory() as td:
        flag = os.path.join(td, "driver_atexit_flag.txt")
        ready = os.path.join(td, "driver_ready.txt")
        driver_code = textwrap.dedent(f"""
            import atexit, os, time, ray
            
            def on_exit():
                time.sleep(10)
                with open('{flag}', 'w') as f:
                    f.write('ok')
            
            atexit.register(on_exit)
            ray.init()
            with open('{ready}', 'w') as f:
                f.write('ready')
            time.sleep(1000)
        """)
        p = run_string_as_driver_nonblocking(driver_code)
        try:
            wait_for_condition(lambda: os.path.exists(ready), timeout=10)
            os.kill(p.pid, signal.SIGTERM)
            time.sleep(0.1)
            os.kill(p.pid, signal.SIGTERM)
            
            start_wait = time.monotonic()
            p.wait(timeout=2)
            wait_time = time.monotonic() - start_wait
            
            assert wait_time < 2, f"Should exit quickly via forced path, but took {wait_time}s"
        finally:
            try:
                p.kill()
            except Exception:
                pass
        
        assert not os.path.exists(flag), "Slow atexit should not complete on forced exit"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
