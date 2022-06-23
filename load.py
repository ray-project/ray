import aiohttp
import os

import ray

PARALLELISM = os.environ.get("PARALLELISM", 10)


@ray.remote(num_cpus=0)
class LoadTester:
    async def run(self):
        async with aiohttp.ClientSession() as session:
            while True:
                async with session.get('http://localhost:8000') as resp:
                    print(resp.status)


lt = LoadTester.remote()
ray.get([lt.run.remote() for _ in range(PARALLELISM)])
