# fmt: off
# __doc_import_begin__
from typing import List
import time

import numpy as np
import requests
from starlette.requests import Request

import ray
from ray import serve
# __doc_import_end__
# fmt: on


# __doc_define_servable_begin__
@serve.deployment
class BatchAdder:
    def __init__(self, matrix: np.ndarray):
        self.matrix = matrix

    @serve.batch(max_batch_size=4)
    async def handle_batch(self, numbers: List[np.array]):
        input_array = np.column_stack(numbers)
        print("Our input array has shape:", input_array.shape)
        output_array = self.matrix.dot(input_array)
        return output_array.transpose().tolist()

    async def __call__(self, request: Request):
        return await self.handle_batch(int(request.query_params["number"]))
    # __doc_define_servable_end__

# __doc_deploy_begin__
adder = BatchAdder.bind()
# __doc_deploy_end__
