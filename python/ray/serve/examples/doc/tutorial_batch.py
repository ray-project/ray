# yapf: disable
# __doc_import_begin__
from typing import List, Union
import time

import numpy as np
import requests
from starlette.requests import Request

import ray
from ray import serve
from ray.serve import ServeRequest
# __doc_import_end__
# yapf: enable


# __doc_define_servable_begin__
class BatchAdder:
    @serve.batch(max_batch_size=4)
    async def handle_batch(self, requests: List[Union[Request, ServeRequest]]):
        numbers = [int(request.query_params["number"]) for request in requests]

        input_array = np.array(numbers)
        print("Our input array has shape:", input_array.shape)
        # Sleep for 200ms, this could be performing CPU intensive computation
        # in real models
        time.sleep(0.2)
        output_array = input_array + 1
        return output_array.astype(int).tolist()

    async def __call__(self, request: Union[Request, ServeRequest]):
        return await self.handle_batch(request)


# __doc_define_servable_end__

# __doc_deploy_begin__
ray.init(num_cpus=8)
serve.start()
serve.create_backend("adder:v0", BatchAdder)
serve.create_endpoint(
    "adder", backend="adder:v0", route="/adder", methods=["GET"])
# __doc_deploy_end__


# __doc_query_begin__
@ray.remote
def send_query(number):
    resp = requests.get("http://localhost:8000/adder?number={}".format(number))
    return int(resp.text)


# Let's use Ray to send all queries in parallel
results = ray.get([send_query.remote(i) for i in range(9)])
print("Result returned:", results)
# Output
# (pid=...) Our input array has shape: (1,)
# (pid=...) Our input array has shape: (4,)
# (pid=...) Our input array has shape: (4,)
# Result returned: [1, 2, 3, 4, 5, 6, 7, 8, 9]
# __doc_query_end__

# __doc_query_handle_begin__
handle = serve.get_handle("adder")
print(handle)
# Output
# RayServeHandle(
#    Endpoint="adder",
#    Traffic={'adder:v0': 1}
# )

input_batch = list(range(9))
print("Input batch is", input_batch)
# Input batch is [0, 1, 2, 3, 4, 5, 6, 7, 8]

result_batch = ray.get([handle.remote(number=i) for i in input_batch])
# Output
# (pid=...) Current context is python
# (pid=...) Our input array has shape: (1,)
# (pid=...) Current context is python
# (pid=...) Our input array has shape: (4,)
# (pid=...) Current context is python
# (pid=...) Our input array has shape: (4,)

print("Result batch is", result_batch)
# Result batch is [1, 2, 3, 4, 5, 6, 7, 8, 9]
# __doc_query_handle_end__
