import aiohttp
import os

import ray

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


lt = LoadTester.remote()
ray.get([lt.run.remote() for _ in range(PARALLELISM)])
