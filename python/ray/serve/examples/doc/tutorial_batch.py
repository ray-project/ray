# yapf: disable
# __doc_import_begin__
import ray
from ray import serve

from typing import List
import time

import numpy as np
import requests
# __doc_import_end__
# yapf: enable


# __doc_define_servable_v0_begin__
@serve.accept_batch
def batch_adder_v0(flask_requests: List):
    numbers = [int(request.args["number"]) for request in flask_requests]

    input_array = np.array(numbers)
    print("Our input array has shape:", input_array.shape)
    # Sleep for 200ms, this could be performing CPU intensive computation
    # in real models
    time.sleep(0.2)
    output_array = input_array + 1
    return output_array.astype(int).tolist()


# __doc_define_servable_v0_end__

# __doc_deploy_begin__
serve.init()
serve.create_backend("adder:v0", batch_adder_v0, config={"max_batch_size": 4})
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


# __doc_define_servable_v1_begin__
@serve.accept_batch
def batch_adder_v1(flask_requests: List, *, numbers: List = []):
    # Depending on request context, we process the input data differently.
    print("Current context is", "web" if serve.context.web else "python")
    if serve.context.web:
        # If the requests come from web request, we parse the flask request
        # to numbers
        numbers = [int(request.args["number"]) for request in flask_requests]
    else:
        # Otherwise, we are processing requests invoked directly from Python.
        numbers = numbers

    input_array = np.array(numbers)
    print("Our input array has shape:", input_array.shape)
    # Sleep for 200ms, this could be performing CPU intensive computation
    # in real models
    time.sleep(0.2)
    output_array = input_array + 1
    return output_array.astype(int).tolist()


# __doc_define_servable_v1_end__

# __doc_deploy_v1_begin__
serve.create_backend("adder:v1", batch_adder_v1, config={"max_batch_size": 4})
serve.set_traffic("adder", {"adder:v1": 1})
# __doc_deploy_v1_end__

# __doc_query_handle_begin__
handle = serve.get_handle("adder")
print(handle)
# Output
# RayServeHandle(
#    Endpoint="adder",
#    Traffic={'adder:v1': 1}
# )

input_batch = list(range(9))
print("Input batch is", input_batch)
# Input batch is [0, 1, 2, 3, 4, 5, 6, 7, 8]

result_batch = ray.get([handle.remote(numbers=i) for i in input_batch])
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
