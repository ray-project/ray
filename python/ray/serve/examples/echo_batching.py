"""
This example has backend which has batching functionality enabled.
"""

import ray
from ray import serve


class MagicCounter:
    def __init__(self, increment):
        self.increment = increment

    @serve.accept_batch
    def __call__(self, starlette_request, base_number=None):
        # __call__ fn should preserve the batch size
        # base_number is a python list

        if serve.context.batch_size is not None:
            batch_size = serve.context.batch_size
            result = []
            for base_num in base_number:
                ret_str = "Number: {} Batch size: {}".format(
                    base_num, batch_size)
                result.append(ret_str)
            return result
        return ""


client = serve.start()
# specify max_batch_size in BackendConfig
backend_config = {"max_batch_size": 5}
client.create_backend(
    "counter:v1", MagicCounter, 42, config=backend_config)  # increment=42
print("Backend Config for backend: 'counter:v1'")
print(backend_config)
client.create_endpoint("magic_counter", backend="counter:v1", route="/counter")

handle = client.get_handle("magic_counter")
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
