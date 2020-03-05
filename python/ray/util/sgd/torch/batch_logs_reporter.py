import asyncio

import ray

@ray.remote(num_cpus=1)
class BatchLogsReporter:
    def __init__(self, world_rank):
        self.world_rank = world_rank
        self._logs_ready = asyncio.Event()
        self._read_finished = asyncio.Event()

    async def _send(self, data, done=False):
        print('Send called')
        self._logs = {
            "done": done,
            "world_rank": self.world_rank,
            "data": data
        }
        self._logs_ready.set()
        # await self._read_finished.wait()
        # self._read_finished.clear()

    async def _read(self):
        print('Read called')
        await self._logs_ready.wait()
        self._logs_ready.clear()
        print('Read returned', self._logs)
        return self._logs

    async def _finish_read(self):
        self._read_finished.set()
