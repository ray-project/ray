from typing import List

import ray
from ray.serve import SingleQuery
from ray.serve.utils.debug import print_debug


def single_input(func):
    func.ray_serve_single_input = True
    return func


class RayServeMixin:
    serve_method = "__call__"

    def _dispatch(self, input_batch: List[SingleQuery]):
        method = getattr(self, self.serve_method)
        print_debug("entering actor dispatch scope", method)
        if hasattr(method, "ray_serve_single_input"):
            for inp in input_batch:
                result = method(inp.data)
                ray.worker.global_worker.put_object(inp.result_oid, result)
        else:
            batch = [inp.data for inp in input_batch]
            result = method(batch)
            for res, inp in zip(result, input_batch):
                ray.worker.global_worker.put_object(inp.result_oid, res)
