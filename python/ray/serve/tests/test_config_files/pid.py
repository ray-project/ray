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
        self.ready = True
        self.counter = 0

    async def get_counter(self) -> int:
        return self.counter

    def send(self, clear=False):
        self.ready = not clear

    def wait(self):
        while not self.ready:
            time.sleep(0.1)

    async def async_wait(self):
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


node = f.bind()
dup_node = f.bind()
async_node = f.bind(async_wait=True)
