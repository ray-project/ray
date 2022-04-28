import time

import ray

@ray.remote
class DeploymentHandleClient:
    def __init__(self, deployment_handle, method_name=None):
        self.deployment_handle = deployment_handle
        self.method_name = method_name

    def ready(self):
        return "ok"

    async def do_queries(self, num, data):
        if self.method_name:
            handle = getattr(self.deployment_handle, self.method_name)
        else:
            handle = self.deployment_handle

        for _ in range(num):
            await handle.remote(data)

async def measure_latency(async_fn, args, expected_output, num_requests=10):
    # warmup for 1sec
    start = time.time()
    while time.time() - start < 1:
        await async_fn(args)

    latency_stats = []
    for _ in range(num_requests):
        start = time.time()
        await async_fn(args) == expected_output
        end = time.time()
        latency_stats.append(end - start)

    return latency_stats
