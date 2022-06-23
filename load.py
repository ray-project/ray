import aiohttp
import os

import ray
from ray.serve.utils import get_current_node_resource_key

PARALLELISM = int(os.environ.get("PARALLELISM", "8"))


@ray.remote(num_cpus=0)
class LoadTester:
    def __init__(self):
        self._num_seen = 0
        self._seen_actors = set()

    async def run(self):
        async with aiohttp.ClientSession() as session:
            while True:
                async with session.get('http://localhost:8000') as resp:
                    self._seen_actors.add(await resp.text())
                    new_num_seen = len(self._seen_actors)
                    if new_num_seen > self._num_seen:
                        self._num_seen = new_num_seen
                        print(f"Got responses from {new_num_seen} replicas.")


ray.init()
lt = LoadTester.options(resources={get_current_node_resource_key(): 0.01}).remote()
ray.get([lt.run.remote() for _ in range(PARALLELISM)])
