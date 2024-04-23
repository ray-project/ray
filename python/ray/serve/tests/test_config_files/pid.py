import asyncio
import os
import time

from ray import serve


@serve.deployment
class f:
    def __init__(self, async_wait: bool = False):
        # whether to use wait() (busy spin) or async_wait() (busy spin
        # while yielding event loop)
        self._async = async_wait

        # to be updated through reconfigure()
        self.name = "default_name"

        # used to block calls to __call__()
        self.ready = True
        # used to check how many times __call__() has been called
        self.counter = 0

        # used to block calls to health_check()
        self.health_check_ready = True
        # used to check how many times health_check() has been called
        self.health_check_counter = 0

    async def get_counter(self, health_check=False) -> int:
        if health_check:
            return self.health_check_counter
        else:
            return self.counter

    def send(self, clear=False, health_check=False):
        if health_check:
            self.health_check_ready = not clear
        else:
            self.ready = not clear

    def wait(self, _health_check=False):
        if _health_check:
            while not self.health_check_ready:
                time.sleep(0.1)
        else:
            while not self.ready:
                time.sleep(0.1)

    async def async_wait(self, _health_check=False):
        if _health_check:
            while not self.health_check_ready:
                await asyncio.sleep(0.1)
        else:
            while not self.ready:
                await asyncio.sleep(0.1)

    def reconfigure(self, config: dict):
        self.name = config.get("name", "default_name")

    async def __call__(self):
        self.counter += 1
        if self._async:
            await self.async_wait()
        else:
            self.wait()

        return os.getpid(), self.name

    async def check_health(self):
        self.health_check_counter += 1
        if self._async:
            await self.async_wait(_health_check=True)
        else:
            self.wait(_health_check=True)


node = f.bind()
dup_node = f.bind()
async_node = f.bind(async_wait=True)
