from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import traceback
from typing import List

import ray
from ray.experimental.serve import SingleQuery


def batched_input(func):
    """Decorator to mark an actor method as accepting only a single input.

    By default methods accept a batch.
    """
    func.ray_serve_batched_input = True
    return func


def _execute_and_seal_error(method, arg, method_name):
    """Execute method with arg and return the result.

    If the method fails, return a RayTaskError so it can be sealed in the
    resultOID and retried by user.
    """
    try:
        return method(arg)
    except Exception:
        return ray.worker.RayTaskError(method_name, traceback.format_exc())


class RayServeMixin:
    """Enable a ray actor to interact with ray.serve

    Usage:
    ```
        @ray.remote
        class MyActor(RayServeMixin):
            # This is optional, by default it is "__call__"
            serve_method = 'my_method'

            def my_method(self, arg):
                ...
    ```
    """

    serve_method = "__call__"

    def _dispatch(self, input_batch: List[SingleQuery]):
        """Helper method to dispatch a batch of input to self.serve_method."""
        method = getattr(self, self.serve_method)
        if hasattr(method, "ray_serve_batched_input"):
            batch = [inp.data for inp in input_batch]
            result = _execute_and_seal_error(method, batch, self.serve_method)
            for res, inp in zip(result, input_batch):
                ray.worker.global_worker.put_object(inp.result_object_id, res)
        else:
            for inp in input_batch:
                result = _execute_and_seal_error(method, inp.data,
                                                 self.serve_method)
                ray.worker.global_worker.put_object(inp.result_object_id,
                                                    result)
