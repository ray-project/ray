import asyncio

import ray


@ray.remote(num_cpus=0)
class SignalActor:
    def __init__(self):
        self.ready_event = asyncio.Event()
        self.num_waiters = 0

    def send(self, clear: bool = False):
        self.ready_event.set()
        if clear:
            self.ready_event.clear()

    async def wait(self, should_wait: bool = True):
        if should_wait:
            self.num_waiters += 1
            await self.ready_event.wait()
            self.num_waiters -= 1

    async def cur_num_waiters(self) -> int:
        return self.num_waiters


@ray.remote(num_cpus=0)
class Semaphore:
    def __init__(self, value: int = 1):
        self._sema = asyncio.Semaphore(value=value)

    async def acquire(self):
        await self._sema.acquire()

    async def release(self):
        self._sema.release()

    async def locked(self) -> bool:
        return self._sema.locked()
