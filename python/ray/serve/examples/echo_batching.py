"""
This example has backend which has batching functionality enabled.
"""

import ray
from ray import serve


class MagicCounter:
    def __init__(self, increment):
        self.increment = increment

    @serve.batch(max_batch_size=5)
    async def handle_batch(self, base_numbers):
        # Must preserve the batch size.
        result = []
        for base_num in base_numbers:
            ret_str = "Number: {} Batch size: {}".format(
                base_num, len(base_numbers))
            result.append(ret_str)
        return result

    async def __call__(self, starlette_request, base_number=None):
        return await self.handle_batch(base_number)


serve.start()
serve.create_backend("counter:v1", MagicCounter, 42)  # increment=42
serve.create_endpoint("magic_counter", backend="counter:v1", route="/counter")

handle = serve.get_handle("magic_counter")
future_list = []

# fire 30 requests
for r in range(30):
    print("> [REMOTE] Pinging handle.remote(base_number={})".format(r))
    f = handle.remote(base_number=r)
    future_list.append(f)

# get results of queries as they complete
left_futures = future_list
while left_futures:
    completed_futures, remaining_futures = ray.wait(left_futures, timeout=0.05)
    if len(completed_futures) > 0:
        result = ray.get(completed_futures[0])
        print("< " + result)
    left_futures = remaining_futures
