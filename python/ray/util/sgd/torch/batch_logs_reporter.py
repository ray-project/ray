import asyncio

import ray

@ray.remote(num_cpus=0)
class BatchLogsReporter:
    def __init__(self, world_rank):
        self.world_rank = world_rank

        self._logs = {
            "done": False,
            "new_data": False,
            "world_rank": self.world_rank,
            "data": None
        }

    def _send(self, data, done=False):
        self._logs = {
            "done": done,
            "new_data": True,
            "world_rank": self.world_rank,
            "data": data
        }

    def _read(self):
        res = self._logs

        self._logs = {
            "done": res["done"],
            "new_data": False,
            "world_rank": self.world_rank,
            "data": None
        }

        return res
