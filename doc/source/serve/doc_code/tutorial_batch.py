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

    @serve.batch(max_batch_size=4)
    async def handle_batch(self, arrays: List):
        input_matrix = np.column_stack(arrays)
        print("Our input array has shape:", input_matrix.shape)

        output_array = self.matrix.dot(input_matrix)
        return output_array.transpose().tolist()

    async def __call__(self, request: Request):
        return await self.handle_batch(await request.json())
        # __doc_define_servable_end__


# __doc_deploy_begin__
matrix = np.random.rand(50, 50)
adder = BatchAdder.bind(matrix)
# __doc_deploy_end__
