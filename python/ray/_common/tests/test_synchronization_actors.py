import asyncio
import pytest
import ray
from ray._common.synchronization_actors import SignalActor, Semaphore
from ray._private.test_utils import wait_for_condition


@pytest.fixture(scope="module")
def ray_init():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@pytest.mark.asyncio
async def test_signal_actor_basic(ray_init):
    signal = SignalActor.remote()

    # Test initial state
    assert await signal.cur_num_waiters.remote() == 0

    # Test send and wait
    signal.send.remote()
    await signal.wait.remote()
    assert await signal.cur_num_waiters.remote() == 0


@pytest.mark.asyncio
async def test_signal_actor_multiple_waiters(ray_init):
    signal = SignalActor.remote()

    async def waiter():
        await signal.wait.remote()

    # Create multiple waiters
    for _ in range(3):
        signal.wait.remote()

    # Check number of waiters
    wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 3)

    # Send signal and wait for all waiters
    signal.send.remote()

    # Verify all waiters are done
    wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 0)


@pytest.mark.asyncio
async def test_semaphore_basic(ray_init):
    sema = Semaphore.remote(value=2)

    # Test initial state
    wait_for_condition(lambda: ray.get(sema.locked.remote()) is False)

    # Test acquire and release
    await sema.acquire.remote()
    await sema.acquire.remote()
    wait_for_condition(lambda: ray.get(sema.locked.remote()) is True)

    await sema.release.remote()
    await sema.release.remote()
    wait_for_condition(lambda: ray.get(sema.locked.remote()) is False)


@pytest.mark.asyncio
async def test_semaphore_concurrent(ray_init):
    sema = Semaphore.remote(value=2)

    async def worker():
        await sema.acquire.remote()
        await asyncio.sleep(0.1)
        await sema.release.remote()

    # Create multiple workers
    workers = [worker() for _ in range(4)]

    # Run workers concurrently
    await asyncio.gather(*workers)

    # Verify semaphore is not locked
    wait_for_condition(lambda: ray.get(sema.locked.remote()) is False)
