# fmt: off
# __doc_import_begin__
from typing import List

import numpy as np
from starlette.requests import Request

from ray import serve
# __doc_import_end__
# fmt: on


# __doc_define_servable_begin__
@serve.deployment
class BatchAdder:
    def __init__(self, matrix: np.ndarray):
        self.matrix = matrix

    @serve.batch(max_batch_size=10)
    async def handle_batch(self, numbers: List):
        numbers = [await request.json() for request in numbers]
        input_array = np.column_stack(numbers)
        print("Our input array has shape:", input_array.shape)
        output_array = self.matrix.dot(input_array)
        return output_array.transpose().tolist()

    async def __call__(self, request: Request):
        return await self.handle_batch(request)
        # __doc_define_servable_end__


# __doc_deploy_begin__
matrix = np.random.rand(2, 2)
adder = BatchAdder.bind(matrix)
# __doc_deploy_end__
