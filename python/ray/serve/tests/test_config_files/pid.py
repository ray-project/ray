from ray import serve

# from ray.serve.deployment_graph import RayServeDAGHandle
import os
import time
import asyncio


@serve.deployment
class f:
    def __init__(self, async_wait: bool = False):
        self._async = async_wait
        self.name = "default_name"
        # for __call__()
        self.ready = True
        self.counter = 0
        # for check_health()
        self.health_check_ready = True
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

    def wait(self, health_check=False):
        if health_check:
            while not self.health_check_ready:
                time.sleep(0.1)
        else:
            while not self.ready:
                time.sleep(0.1)

    async def async_wait(self, health_check=False):
        if health_check:
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
            await self.async_wait(health_check=True)
        else:
            self.wait(health_check=True)


node = f.bind()
dup_node = f.bind()
async_node = f.bind(async_wait=True)
